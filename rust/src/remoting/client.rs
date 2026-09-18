//! socket 长连接客户端（对应 `org.apache.rocketmq.remoting.netty.NettyRemotingClient` 的核心能力）。
//!
//! 提供：惰性建连 + 复用、同步 / 异步 / oneway 调用、opaque 匹配、半包重组、
//! broker 主动请求派发、超时控制、TLS、RPC 钩子。
//!
//! 每条连接一个读任务 + 一个写任务（TLS 因为 `native-tls` 是同步 API，跑在专用
//! 阻塞线程上），在途请求按 opaque 全局登记，与 Java 的 `responseTable` 一致。

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc as std_mpsc, Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::{rmq_debug, rmq_warn};
use crate::remoting::protocol::remoting_command::RemotingCommand;
use crate::remoting::rpchook::RPCHook;

pub const MAX_FRAME_LENGTH: i32 = 16 * 1024 * 1024;

/// TLS 读线程单次持锁上限：读阻塞时写线程最多等这么久。
const TLS_READ_TIMEOUT: Duration = Duration::from_millis(500);

/// 异步调用回调（对应 Java `InvokeCallback`）。
pub type InvokeCallback = Box<dyn FnOnce(Result<RemotingCommand>) + Send + 'static>;

/// broker 主动请求处理器（对应 Java `NettyRequestProcessor#processRequest`）。
///
/// 需要回包时调用 [`ResponseSink::respond`]（opaque 由框架回填）；oneway 请求直接返回。
/// 实现内部可自行 spawn，不要长时间阻塞。
pub trait RequestProcessor: Send + Sync + 'static {
    fn process(&self, request: RemotingCommand, addr: String, sink: ResponseSink);
}

/// 把响应写回对端。该路径**不执行 RPC 钩子**：响应报文不需要也不能带签名。
#[derive(Clone)]
pub struct ResponseSink {
    inner: Arc<Inner>,
    addr: String,
    opaque: i32,
    /// 对端用 oneway 推请求时不能回包（Java `NettyRemotingServer#processRequest` 同规则）。
    wants_reply: bool,
}

impl ResponseSink {
    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn respond(&self, mut response: RemotingCommand) {
        if !self.wants_reply {
            rmq_debug!(
                "remoting: drop response (code={}) for oneway request opaque={} from {}",
                response.code,
                self.opaque,
                self.addr
            );
            return;
        }
        response.opaque = self.opaque;
        let inner = self.inner.clone();
        let addr = self.addr.clone();
        self.inner.spawn("response write", async move {
            if let Err(e) = write_frame(&inner, &addr, &mut response).await {
                rmq_warn!("remoting: failed to write response (code={}) to {}: {}", response.code, addr, e);
            }
        });
    }
}

#[derive(Debug, Clone)]
pub struct RemotingClientConfig {
    pub connect_timeout_millis: i64,
    pub invoke_timeout_millis: i32,
    pub tls_enable: bool,
    /// 对应 Java `tls.test.mode.enable`（默认 true）：信任自签证书、不校验主机名。
    pub tls_test_mode: bool,
}

impl Default for RemotingClientConfig {
    fn default() -> Self {
        RemotingClientConfig {
            connect_timeout_millis: 3000,
            invoke_timeout_millis: 3000,
            tls_enable: env_bool("ROCKETMQ_TLS_ENABLE", false),
            tls_test_mode: env_bool("ROCKETMQ_TLS_TEST_MODE", true),
        }
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => default,
    }
}

struct Pending {
    sender: oneshot::Sender<RemotingCommand>,
    request: Option<RemotingCommand>,
    addr: String,
}

#[derive(Default)]
struct State {
    conns: HashMap<String, Arc<Connection>>,
    pending: HashMap<i32, Pending>,
}

enum WriterHandle {
    Async(tokio::sync::mpsc::UnboundedSender<Vec<u8>>),
    Blocking(Arc<std_mpsc::SyncSender<Vec<u8>>>),
}

impl WriterHandle {
    fn send(&self, data: Vec<u8>) -> bool {
        match self {
            WriterHandle::Async(tx) => tx.send(data).is_ok(),
            WriterHandle::Blocking(tx) => tx.send(data).is_ok(),
        }
    }
}

enum TaskHandle {
    Async(JoinHandle<()>),
    Blocking,
}

struct Connection {
    addr: String,
    writer: WriterHandle,
    alive: AtomicBool,
    tasks: Mutex<Vec<TaskHandle>>,
}

impl Connection {
    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    fn mark_dead(&self) {
        self.alive.store(false, Ordering::Release);
    }

    fn abort_tasks(&self) {
        let mut tasks = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        for task in tasks.drain(..) {
            if let TaskHandle::Async(handle) = task {
                handle.abort();
            }
        }
    }
}

struct Inner {
    config: RemotingClientConfig,
    state: Mutex<State>,
    /// 每个地址一把异步锁：并发请求只允许一个任务去建连，其余等它建完复用。
    connect_gates: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    hooks: RwLock<Vec<Arc<dyn RPCHook>>>,
    processors: RwLock<HashMap<i32, Arc<dyn RequestProcessor>>>,
    running: AtomicBool,
    /// 首次需要 spawn 时才绑定的 tokio 句柄（`None` 表示还没绑定）。
    /// 构造 `RemotingClient` 可能在运行时之外（Python 的构造与运行时无关），
    /// 所以这里不能急切 `Handle::current()`；所有 spawn 点都在运行时内部。
    handle: OnceLock<tokio::runtime::Handle>,
}

/// 长连接 remoting 客户端，可跨任务克隆。
#[derive(Clone)]
pub struct RemotingClient {
    inner: Arc<Inner>,
}

impl Default for RemotingClient {
    fn default() -> Self {
        RemotingClient::new()
    }
}

impl RemotingClient {
    pub fn new() -> RemotingClient {
        RemotingClient::with_config(RemotingClientConfig::default())
    }

    pub fn with_config(config: RemotingClientConfig) -> RemotingClient {
        RemotingClient {
            inner: Arc::new(Inner {
                config,
                state: Mutex::new(State::default()),
                connect_gates: Mutex::new(HashMap::new()),
                hooks: RwLock::new(Vec::new()),
                processors: RwLock::new(HashMap::new()),
                running: AtomicBool::new(true),
                handle: OnceLock::new(),
            }),
        }
    }

    pub fn config(&self) -> &RemotingClientConfig {
        &self.inner.config
    }

    /// 当前 tokio 句柄；不在运行时上下文里时返回 `None`（Python 的构造与运行时无关，
    /// 这里同样不假设构造点有运行时）。
    pub fn runtime_handle(&self) -> Option<tokio::runtime::Handle> {
        self.inner.runtime_handle()
    }

    // ---------------- 钩子 / 处理器 ----------------
    pub fn register_rpc_hook(&self, hook: Arc<dyn RPCHook>) {
        self.inner.hooks.write().unwrap_or_else(|e| e.into_inner()).push(hook);
    }

    pub fn unregister_rpc_hook(&self, hook: &Arc<dyn RPCHook>) {
        let target = Arc::as_ptr(hook) as *const ();
        self.inner
            .hooks
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|h| Arc::as_ptr(h) as *const () != target);
    }

    pub fn rpc_hook_count(&self) -> usize {
        self.inner.hooks.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn register_processor(&self, request_code: i32, processor: Arc<dyn RequestProcessor>) {
        self.inner
            .processors
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(request_code, processor);
    }

    pub fn unregister_processor(&self, request_code: i32) -> Option<Arc<dyn RequestProcessor>> {
        self.inner
            .processors
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&request_code)
    }

    // ---------------- RPC ----------------
    pub async fn invoke_sync(
        &self,
        addr: &str,
        request: &mut RemotingCommand,
        timeout_millis: Option<i64>,
    ) -> Result<RemotingCommand> {
        let timeout = timeout_millis.unwrap_or(self.inner.config.invoke_timeout_millis as i64);
        let opaque = request.opaque;
        let (tx, rx) = oneshot::channel();
        self.inner.register_pending(opaque, tx, Some(request.clone()), addr);
        if let Err(e) = send_request(&self.inner, addr, request).await {
            self.inner.cancel(opaque);
            return Err(e);
        }
        match tokio::time::timeout(Duration::from_millis(timeout.max(1) as u64), rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => {
                self.inner.cancel(opaque);
                Err(Error::SendRequest {
                    addr: addr.to_string(),
                    message: "connection closed".into(),
                })
            }
            Err(_) => {
                self.inner.cancel(opaque);
                Err(Error::Timeout { addr: addr.to_string(), timeout_millis: timeout })
            }
        }
    }

    /// 对应 Java `invokeAsync`：不阻塞调用方，结果交给回调。
    pub fn invoke_async(
        &self,
        addr: &str,
        mut request: RemotingCommand,
        callback: InvokeCallback,
        timeout_millis: Option<i64>,
    ) {
        let inner = self.inner.clone();
        let addr = addr.to_string();
        self.inner.spawn("invoke_async", async move {
            let result = invoke_sync_inner(&inner, &addr, &mut request, timeout_millis).await;
            callback(result);
        });
    }

    pub async fn invoke_oneway(&self, addr: &str, request: &mut RemotingCommand) -> Result<()> {
        request.mark_oneway_rpc();
        send_request(&self.inner, addr, request).await
    }

    // ---------------- 连接状态 ----------------
    pub fn is_channel_writable(&self, addr: &str) -> bool {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .conns
            .get(addr)
            .map(|c| c.is_alive())
            .unwrap_or(false)
    }

    pub fn close_channel(&self, addr: &str) {
        close_channel(&self.inner, addr);
    }

    pub fn connection_addrs(&self) -> Vec<String> {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut addrs: Vec<String> = state.conns.keys().cloned().collect();
        addrs.sort();
        addrs
    }

    pub fn in_flight_count(&self) -> usize {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.pending.len()
    }

    /// 对应 Java `NettyRemotingClient#start`（`MQClientInstance#start` 会调它）：
    /// 把 `shutdown` 关掉的传输重新打开。连接本来就是按需建的，所以只需翻回 running 位。
    pub fn start(&self) {
        self.inner.running.store(true, Ordering::Release);
    }

    pub fn shutdown(&self) {
        if !self.inner.running.swap(false, Ordering::SeqCst) {
            return;
        }
        let addrs: Vec<String> = {
            let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            state.conns.keys().cloned().collect()
        };
        for addr in addrs {
            close_channel(&self.inner, &addr);
        }
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.pending.clear();
    }
}

impl Inner {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    fn runtime_handle(&self) -> Option<tokio::runtime::Handle> {
        if let Some(handle) = self.handle.get() {
            return Some(handle.clone());
        }
        let handle = tokio::runtime::Handle::try_current().ok()?;
        // 并发下可能两个任务同时 try_current，set 失败无害，句柄等价。
        let _ = self.handle.set(handle.clone());
        Some(handle)
    }

    /// 在当前运行时上派后台任务；没有可用运行时时只记日志，不 panic。
    fn spawn<F>(&self, what: &str, task: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        match self.runtime_handle() {
            Some(handle) => {
                handle.spawn(task);
            }
            None => rmq_warn!("remoting: no tokio runtime in context, {what} dropped"),
        }
    }

    fn register_pending(
        self: &Arc<Inner>,
        opaque: i32,
        sender: oneshot::Sender<RemotingCommand>,
        request: Option<RemotingCommand>,
        addr: &str,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.pending.insert(opaque, Pending { sender, request, addr: addr.to_string() });
    }

    fn cancel(&self, opaque: i32) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.pending.remove(&opaque);
    }
}

async fn invoke_sync_inner(
    inner: &Arc<Inner>,
    addr: &str,
    request: &mut RemotingCommand,
    timeout_millis: Option<i64>,
) -> Result<RemotingCommand> {
    let timeout = timeout_millis.unwrap_or(inner.config.invoke_timeout_millis as i64);
    let opaque = request.opaque;
    let (tx, rx) = oneshot::channel();
    inner.register_pending(opaque, tx, Some(request.clone()), addr);
    if let Err(e) = send_request(inner, addr, request).await {
        inner.cancel(opaque);
        return Err(e);
    }
    match tokio::time::timeout(Duration::from_millis(timeout.max(1) as u64), rx).await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(_)) => {
            inner.cancel(opaque);
            Err(Error::SendRequest { addr: addr.to_string(), message: "connection closed".into() })
        }
        Err(_) => {
            inner.cancel(opaque);
            Err(Error::Timeout { addr: addr.to_string(), timeout_millis: timeout })
        }
    }
}

/// 对应 Java `doBeforeRpcHooks` + 写出：**必须在 encode 之前**跑钩子，
/// ACL 签名覆盖的必须是真正上线的那份 extFields。
async fn send_request(inner: &Arc<Inner>, addr: &str, request: &mut RemotingCommand) -> Result<()> {
    {
        let hooks = inner.hooks.read().unwrap_or_else(|e| e.into_inner());
        for hook in hooks.iter() {
            hook.do_before_request(addr, request);
        }
    }
    write_frame(inner, addr, request).await
}

async fn write_frame(inner: &Arc<Inner>, addr: &str, request: &mut RemotingCommand) -> Result<()> {
    let conn = get_or_create(inner, addr).await?;
    let data = request.encode();
    if conn.writer.send(data) {
        Ok(())
    } else {
        conn.mark_dead();
        close_channel(inner, &conn.addr);
        Err(Error::SendRequest { addr: addr.to_string(), message: "connection closed".into() })
    }
}

fn close_channel(inner: &Arc<Inner>, addr: &str) {
    let conn = {
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.conns.remove(addr)
    };
    if let Some(conn) = conn {
        conn.mark_dead();
        conn.abort_tasks();
    }
    fail_pending_for(inner, addr);
}

/// 连接死亡：把该地址上的在途请求全部失败掉（丢弃 sender 即可唤醒 `invoke_sync`）。
fn fail_pending_for(inner: &Arc<Inner>, addr: &str) {
    let dropped: Vec<Pending> = {
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let keys: Vec<i32> = state
            .pending
            .iter()
            .filter(|(_, p)| p.addr == addr)
            .map(|(k, _)| *k)
            .collect();
        keys.into_iter().filter_map(|k| state.pending.remove(&k)).collect()
    };
    for pending in dropped {
        drop(pending);
    }
}

fn on_connection_lost(inner: &Arc<Inner>, addr: &str) {
    let matches = {
        let state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.conns.get(addr).map(|c| c.alive.load(Ordering::Acquire)).unwrap_or(false)
    };
    if matches {
        close_channel(inner, addr);
    } else {
        fail_pending_for(inner, addr);
    }
}

/// 读线程解出的每一帧走这里：响应交给在途表，请求交给处理器表。
fn dispatch(inner: &Arc<Inner>, addr: &str, frame: Vec<u8>) {
    let cmd = match RemotingCommand::decode(&frame) {
        Ok(cmd) => cmd,
        Err(e) => {
            rmq_warn!("remoting: failed to decode frame from {addr}: {e}");
            return;
        }
    };
    let opaque = cmd.opaque;
    let pending = {
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        // Python `_dispatch`：响应按 opaque pop；带不上响应标志但 opaque 撞上在途请求
        // 的那条「异常兜底」分支做的是同一个 pop。两条分支动作一致，故只查一次。
        state.pending.remove(&opaque)
    };
    if let Some(pending) = pending {
        let Pending { sender, request, .. } = pending;
        apply_after_response_hooks(inner, addr, request.as_ref(), Some(&cmd));
        let _ = sender.send(cmd);
        return;
    }
    if cmd.is_response_type() {
        rmq_warn!("remoting: response for unknown opaque {opaque} from {addr}");
        return;
    }
    let processor = inner
        .processors
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&cmd.code)
        .cloned();
    let Some(processor) = processor else {
        rmq_warn!("remoting: no processor for request code {} (opaque {opaque}) from {addr}", cmd.code);
        return;
    };
    let sink = ResponseSink {
        inner: inner.clone(),
        addr: addr.to_string(),
        opaque,
        wants_reply: !cmd.is_oneway_rpc(),
    };
    // 处理器 panic 不能带走读线程：与 Python 的 try/except 等价，回一条 SYSTEM_ERROR。
    let panic_sink = sink.clone();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        processor.process(cmd, addr.to_string(), sink)
    }));
    if outcome.is_err() {
        let response = RemotingCommand::create_response(
            crate::remoting::protocol::codes::response_code::SYSTEM_ERROR,
            Some("process request fail".to_string()),
        );
        panic_sink.respond(response);
    }
}

fn apply_after_response_hooks(
    inner: &Arc<Inner>,
    addr: &str,
    request: Option<&RemotingCommand>,
    response: Option<&RemotingCommand>,
) {
    let hooks = inner.hooks.read().unwrap_or_else(|e| e.into_inner());
    if hooks.is_empty() || request.is_none() {
        return;
    }
    for hook in hooks.iter() {
        hook.do_after_response(addr, request, response);
    }
}

async fn get_or_create(inner: &Arc<Inner>, addr: &str) -> Result<Arc<Connection>> {
    if !inner.is_running() {
        return Err(Error::SendRequest {
            addr: addr.to_string(),
            message: "client already shutdown".into(),
        });
    }
    if let Some(conn) = existing(inner, addr) {
        return Ok(conn);
    }
    let gate = connect_gate(inner, addr);
    let _guard = gate.lock().await;
    if let Some(conn) = existing(inner, addr) {
        return Ok(conn);
    }
    let conn = connect(inner, addr).await?;
    let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(existing) = state.conns.get(addr) {
        if existing.is_alive() {
            conn.abort_tasks();
            return Ok(existing.clone());
        }
        existing.abort_tasks();
    }
    let inserted = conn.clone();
    state.conns.insert(addr.to_string(), conn);
    Ok(inserted)
}

fn connect_gate(inner: &Arc<Inner>, addr: &str) -> Arc<tokio::sync::Mutex<()>> {
    let mut gates = inner.connect_gates.lock().unwrap_or_else(|e| e.into_inner());
    gates
        .entry(addr.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

fn existing(inner: &Arc<Inner>, addr: &str) -> Option<Arc<Connection>> {
    let state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
    state.conns.get(addr).filter(|c| c.is_alive()).cloned()
}

async fn connect(inner: &Arc<Inner>, addr: &str) -> Result<Arc<Connection>> {
    let (host, port) = split_host_port(addr)?;
    let socket_addr = resolve(&host, port).await?;
    if inner.config.tls_enable {
        connect_tls(inner, addr, &host, socket_addr).await
    } else {
        connect_plain(inner, socket_addr, addr).await
    }
}

async fn connect_plain(inner: &Arc<Inner>, socket_addr: SocketAddr, addr: &str) -> Result<Arc<Connection>> {
    let stream = tokio::time::timeout(
        Duration::from_millis(inner.config.connect_timeout_millis.max(1) as u64),
        TcpStream::connect(socket_addr),
    )
    .await
    .map_err(|_| Error::Connect { addr: addr.to_string() })?
    .map_err(|_| Error::Connect { addr: addr.to_string() })?;
    let _ = stream.set_nodelay(true);
    let (read_half, write_half) = tokio::io::split(stream);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

    let conn = Arc::new(Connection {
        addr: addr.to_string(),
        writer: WriterHandle::Async(tx),
        alive: AtomicBool::new(true),
        tasks: Mutex::new(Vec::new()),
    });

    let reader_inner = inner.clone();
    let reader_addr = addr.to_string();
    let reader = tokio::spawn(async move {
        let mut read_half = read_half;
        let mut len_buf = [0u8; 4];
        loop {
            if !reader_inner.is_running() {
                break;
            }
            if read_half.read_exact(&mut len_buf).await.is_err() {
                break;
            }
            let total = i32::from_be_bytes(len_buf);
            if total <= 0 || total > MAX_FRAME_LENGTH {
                rmq_warn!("remoting: bad frame length {total} from {reader_addr}");
                break;
            }
            let mut frame = vec![0u8; 4 + total as usize];
            frame[..4].copy_from_slice(&len_buf);
            if read_half.read_exact(&mut frame[4..]).await.is_err() {
                break;
            }
            dispatch(&reader_inner, &reader_addr, frame);
        }
        on_connection_lost(&reader_inner, &reader_addr);
    });

    let writer_inner = inner.clone();
    let writer_addr = addr.to_string();
    let writer = tokio::spawn(async move {
        let mut write_half = write_half;
        while let Some(data) = rx.recv().await {
            if write_half.write_all(&data).await.is_err() || write_half.flush().await.is_err() {
                break;
            }
        }
        on_connection_lost(&writer_inner, &writer_addr);
    });

    {
        let mut tasks = conn.tasks.lock().unwrap_or_else(|e| e.into_inner());
        tasks.push(TaskHandle::Async(reader));
        tasks.push(TaskHandle::Async(writer));
    }
    Ok(conn)
}

/// TLS 走阻塞线程：`native-tls` 没有 async 接口，读写共用一把锁，
/// 读侧带 [`TLS_READ_TIMEOUT`] 超时以保证写不被读阻塞饿死。
async fn connect_tls(
    inner: &Arc<Inner>,
    addr: &str,
    host: &str,
    socket_addr: SocketAddr,
) -> Result<Arc<Connection>> {
    let tcp = tokio::time::timeout(
        Duration::from_millis(inner.config.connect_timeout_millis.max(1) as u64),
        TcpStream::connect(socket_addr),
    )
    .await
    .map_err(|_| Error::Connect { addr: addr.to_string() })?
    .map_err(|_| Error::Connect { addr: addr.to_string() })?;
    let _ = tcp.set_nodelay(true);
    let std_stream = tcp
        .into_std()
        .map_err(|_| Error::Connect { addr: addr.to_string() })?;
    let _ = std_stream.set_nonblocking(false);

    let domain = host.to_string();
    let test_mode = inner.config.tls_test_mode;
    let handshake_addr = addr.to_string();
    let stream = tokio::task::spawn_blocking(move || {
        let mut builder = native_tls::TlsConnector::builder();
        if test_mode {
            builder.danger_accept_invalid_certs(true);
            builder.danger_accept_invalid_hostnames(true);
        }
        let connector = builder.build().map_err(|e| {
            Error::Connect { addr: format!("{handshake_addr} (tls connector: {e})") }
        })?;
        let mut tls = connector.connect(&domain, std_stream).map_err(|e| Error::Connect {
            addr: format!("{handshake_addr} (tls handshake: {e})"),
        })?;
        // 读超时设在握手之后：握手期间必须允许完整阻塞。
        let _ = tls.get_mut().set_read_timeout(Some(TLS_READ_TIMEOUT));
        Ok::<_, Error>(tls)
    })
    .await
    .map_err(|_| Error::Connect { addr: addr.to_string() })??;

    let shared = Arc::new(Mutex::new(stream));
    let alive = Arc::new(AtomicBool::new(true));
    let (tx, rx) = std_mpsc::sync_channel::<Vec<u8>>(1024);
    let rx = Arc::new(Mutex::new(rx));

    let conn = Arc::new(Connection {
        addr: addr.to_string(),
        writer: WriterHandle::Blocking(Arc::new(tx)),
        alive: AtomicBool::new(true),
        tasks: Mutex::new(vec![TaskHandle::Blocking]),
    });

    spawn_tls_writer(inner.clone(), addr, shared.clone(), rx.clone(), alive.clone());
    spawn_tls_reader(inner.clone(), addr, shared, alive);
    Ok(conn)
}

fn spawn_tls_reader(inner: Arc<Inner>, addr: &str, shared: SharedTls, alive: Arc<AtomicBool>) {
    let addr = addr.to_string();
    std::thread::spawn(move || tls_read_loop(inner, addr, shared, alive));
}

fn spawn_tls_writer(
    inner: Arc<Inner>,
    addr: &str,
    shared: SharedTls,
    rx: Arc<Mutex<std_mpsc::Receiver<Vec<u8>>>>,
    alive: Arc<AtomicBool>,
) {
    let addr = addr.to_string();
    std::thread::spawn(move || {
        while let Ok(data) = rx.lock().unwrap_or_else(|e| e.into_inner()).recv() {
            if !alive.load(Ordering::Acquire) || !inner.is_running() {
                break;
            }
            let mut guard = shared.lock().unwrap_or_else(|e| e.into_inner());
            let result = guard.write_all(&data).and_then(|_| guard.flush());
            drop(guard);
            if result.is_err() {
                break;
            }
        }
        on_connection_lost(&inner, &addr);
    });
}

type SharedTls = Arc<Mutex<native_tls::TlsStream<std::net::TcpStream>>>;

fn tls_read_loop(inner: Arc<Inner>, addr: String, shared: SharedTls, alive: Arc<AtomicBool>) {
    let mut len_buf = [0u8; 4];
    let mut header_done = false;
    loop {
        if !inner.is_running() || !alive.load(Ordering::Acquire) {
            break;
        }
        if !header_done {
            match tls_read_exact(&shared, &mut len_buf) {
                Ok(true) => header_done = true,
                Ok(false) => continue,
                Err(_) => break,
            }
        }
        let total = i32::from_be_bytes(len_buf);
        if total <= 0 || total > MAX_FRAME_LENGTH {
            rmq_warn!("remoting: bad tls frame length {total} from {addr}");
            break;
        }
        let mut frame = vec![0u8; 4 + total as usize];
        frame[..4].copy_from_slice(&len_buf);
        match tls_read_exact(&shared, &mut frame[4..]) {
            Ok(true) => {}
            Ok(false) => continue,
            Err(_) => break,
        }
        header_done = false;
        dispatch(&inner, &addr, frame);
    }
    alive.store(false, Ordering::Release);
    on_connection_lost(&inner, &addr);
}

/// `Ok(true)` 读满，`Ok(false)` 读超时（调用方重试），`Err` 连接不可用。
fn tls_read_exact(shared: &SharedTls, buf: &mut [u8]) -> std::result::Result<bool, std::io::Error> {
    let mut guard = shared.lock().unwrap_or_else(|e| e.into_inner());
    let mut filled = 0usize;
    while filled < buf.len() {
        match guard.read(&mut buf[filled..]) {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => filled += n,
            Err(e) if is_timeout(&e) => {
                if filled == 0 {
                    return Ok(false);
                }
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

fn is_timeout(e: &std::io::Error) -> bool {
    matches!(e.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock)
}

pub fn split_host_port(addr: &str) -> Result<(String, u16)> {
    let (host, port) = crate::common::util_all::parse_addr(addr);
    let port: u16 = port.parse().map_err(|_| Error::SendRequest {
        addr: addr.to_string(),
        message: format!("bad port in address {addr:?}"),
    })?;
    if host.is_empty() {
        return Err(Error::SendRequest { addr: addr.to_string(), message: "empty host".into() });
    }
    Ok((host, port))
}

async fn resolve(host: &str, port: u16) -> Result<SocketAddr> {
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    let host = host.to_string();
    tokio::task::spawn_blocking(move || {
        use std::net::ToSocketAddrs;
        (host.as_str(), port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut it| it.next())
            .ok_or_else(|| Error::Connect { addr: format!("dns lookup failed for {host}:{port}") })
    })
    .await
    .map_err(|_| Error::Connect { addr: "dns lookup task panicked".into() })?
}


#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use super::*;
    use crate::remoting::protocol::codes::{request_code, response_code, serialize_type};

    async fn read_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.ok()?;
        let total = i32::from_be_bytes(len_buf);
        if total <= 0 || total > MAX_FRAME_LENGTH {
            return None;
        }
        let mut frame = vec![0u8; 4 + total as usize];
        frame[..4].copy_from_slice(&len_buf);
        stream.read_exact(&mut frame[4..]).await.ok()?;
        Some(frame)
    }

    async fn write_command(stream: &mut TcpStream, cmd: &mut RemotingCommand) {
        let bytes = cmd.encode();
        let _ = stream.write_all(&bytes).await;
        let _ = stream.flush().await;
    }

    fn request(code: i32, remark: &str) -> RemotingCommand {
        let mut cmd = RemotingCommand::create_request_command(code, None);
        cmd.remark = Some(remark.to_string());
        cmd
    }

    fn response_for(request: &RemotingCommand, code: i32) -> RemotingCommand {
        let mut cmd = RemotingCommand::create_response(code, Some("pong".to_string()));
        cmd.opaque = request.opaque;
        cmd.serialize_type_current_rpc = request.serialize_type_current_rpc;
        cmd
    }

    /// 最简 broker：读一帧、原 opaque 回一帧。
    async fn echo_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    while let Some(frame) = read_frame(&mut stream).await {
                        let cmd = RemotingCommand::decode(&frame).unwrap();
                        if cmd.is_oneway_rpc() {
                            continue;
                        }
                        let mut response = response_for(&cmd, response_code::SUCCESS);
                        if let Some(remark) = cmd.remark.clone() {
                            response.remark = Some(format!("pong:{remark}"));
                        }
                        write_command(&mut stream, &mut response).await;
                    }
                });
            }
        });
        (addr, task)
    }

    #[tokio::test]
    async fn invoke_sync_round_trip() {
        let (addr, _server) = echo_server().await;
        let client = RemotingClient::new();
        let mut cmd = request(request_code::SEND_MESSAGE_V2, "hi");
        let response = client.invoke_sync(&addr, &mut cmd, Some(3000)).await.unwrap();
        assert_eq!(response.code, response_code::SUCCESS);
        assert_eq!(response.opaque, cmd.opaque, "响应必须带回请求的 opaque");
        assert_eq!(response.remark.as_deref(), Some("pong:hi"));
        assert!(response.is_response_type());
        assert_eq!(client.connection_addrs(), vec![addr.clone()]);
        client.shutdown();
    }

    #[tokio::test]
    async fn half_packet_reassembly() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let frame = read_frame(&mut stream).await.unwrap();
            let cmd = RemotingCommand::decode(&frame).unwrap();
            let bytes = response_for(&cmd, response_code::SUCCESS).encode();
            // 拆成 1 字节 + 余下，验证半包 / 粘包重组
            for chunk in bytes.chunks(1) {
                stream.write_all(chunk).await.unwrap();
                stream.flush().await.unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        let client = RemotingClient::new();
        let mut cmd = request(request_code::PULL_MESSAGE, "x");
        let response = client.invoke_sync(&addr, &mut cmd, Some(5000)).await.unwrap();
        assert_eq!(response.code, response_code::SUCCESS);
        client.shutdown();
    }

    #[tokio::test]
    async fn concurrent_requests_match_their_own_opaques() {
        let (addr, _server) = echo_server().await;
        let client = RemotingClient::new();
        let mut handles = Vec::new();
        for i in 0..8 {
            let client = client.clone();
            let addr = addr.clone();
            handles.push(tokio::spawn(async move {
                let mut cmd = request(request_code::SEND_MESSAGE, &format!("n{i}"));
                let response = client.invoke_sync(&addr, &mut cmd, Some(5000)).await.unwrap();
                assert_eq!(response.opaque, cmd.opaque);
                response.remark.unwrap()
            }));
        }
        let mut remarks = Vec::new();
        for handle in handles {
            remarks.push(handle.await.unwrap());
        }
        remarks.sort();
        let expected: Vec<String> = (0..8).map(|i| format!("pong:n{i}")).collect();
        assert_eq!(remarks, expected);
        client.shutdown();
    }

    #[tokio::test]
    async fn invoke_oneway_does_not_wait() {
        let (addr, _server) = echo_server().await;
        let client = RemotingClient::new();
        let mut cmd = request(request_code::HEART_BEAT, "once");
        client.invoke_oneway(&addr, &mut cmd).await.unwrap();
        assert!(cmd.is_oneway_rpc());
        assert_eq!(client.in_flight_count(), 0, "oneway 不登记在途表");
        client.shutdown();
    }

    #[tokio::test]
    async fn timeout_when_broker_stays_silent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    while read_frame(&mut stream).await.is_some() {}
                });
            }
        });
        let client = RemotingClient::new();
        let mut cmd = request(request_code::PULL_MESSAGE, "quiet");
        let err = client.invoke_sync(&addr, &mut cmd, Some(200)).await.unwrap_err();
        assert!(matches!(err, Error::Timeout { .. }), "got {err}");
        assert!(err.to_string().contains("200ms"));
        assert_eq!(client.in_flight_count(), 0, "超时后必须清掉在途表");
        client.shutdown();
    }

    #[tokio::test]
    async fn connect_failure_maps_to_connect_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let client = RemotingClient::new();
        let mut cmd = request(request_code::HEART_BEAT, "dead");
        let err = client.invoke_sync(&addr, &mut cmd, Some(1000)).await.unwrap_err();
        assert!(matches!(err, Error::Connect { .. }), "got {err}");
        assert!(!client.is_channel_writable(&addr));
    }

    #[tokio::test]
    async fn bad_port_is_reported() {
        let client = RemotingClient::new();
        let mut cmd = request(request_code::HEART_BEAT, "x");
        let err = client.invoke_sync("127.0.0.1:notaport", &mut cmd, Some(500)).await.unwrap_err();
        assert!(matches!(err, Error::SendRequest { .. }), "got {err}");
    }

    #[tokio::test]
    async fn close_channel_forces_reconnect() {
        let (addr, _server) = echo_server().await;
        let client = RemotingClient::new();
        let mut cmd = request(request_code::SEND_MESSAGE, "first");
        client.invoke_sync(&addr, &mut cmd, Some(3000)).await.unwrap();
        client.close_channel(&addr);
        assert!(!client.is_channel_writable(&addr));
        let mut second = request(request_code::SEND_MESSAGE, "second");
        let response = client.invoke_sync(&addr, &mut second, Some(3000)).await.unwrap();
        assert_eq!(response.remark.as_deref(), Some("pong:second"));
        client.shutdown();
    }

    /// broker 主动推请求（PUSH_REPLY_MESSAGE_TO_CLIENT），客户端处理器必须回响应。
    #[tokio::test]
    async fn broker_pushed_request_reaches_processor() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_addr = listener.local_addr().unwrap();
        let (push_tx, push_rx) = tokio::sync::oneshot::channel::<RemotingCommand>();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut push = RemotingCommand::create_request_command(
                request_code::PUSH_REPLY_MESSAGE_TO_CLIENT,
                None,
            );
            push.opaque = 9_876;
            push.mark_oneway_rpc();
            let bytes = push.encode();
            stream.write_all(&bytes).await.unwrap();
            stream.flush().await.unwrap();
            // 上面那条 oneway 不应有响应；再发一条请求型的，要求回包
            let mut need_reply =
                RemotingCommand::create_request_command(request_code::CHECK_TRANSACTION_STATE, None);
            need_reply.opaque = 5_555;
            let bytes = need_reply.encode();
            stream.write_all(&bytes).await.unwrap();
            stream.flush().await.unwrap();
            loop {
                let frame = read_frame(&mut stream).await.expect("客户端应回包");
                let cmd = RemotingCommand::decode(&frame).unwrap();
                if cmd.is_response_type() {
                    let _ = push_tx.send(cmd);
                    break;
                }
            }
        });

        struct Echo;
        impl RequestProcessor for Echo {
            fn process(&self, request: RemotingCommand, _addr: String, sink: ResponseSink) {
                let mut response = RemotingCommand::create_response(response_code::SUCCESS, None);
                response.set_body(request.body().map(|b| b.to_vec()));
                sink.respond(response);
            }
        }
        let client_addr = listen_addr.to_string();
        let connect = {
            let client = RemotingClient::new();
            client.register_processor(request_code::PUSH_REPLY_MESSAGE_TO_CLIENT, Arc::new(Echo));
            client.register_processor(request_code::CHECK_TRANSACTION_STATE, Arc::new(Echo));
            // 先建连（服务端 accept 后才会推请求）
            let mut warm = request(request_code::HEART_BEAT, "warm");
            warm.mark_oneway_rpc();
            client.invoke_oneway(&client_addr, &mut warm).await.unwrap();
            client
        };
        let response = tokio::time::timeout(Duration::from_secs(5), push_rx).await.unwrap().unwrap();
        assert_eq!(response.code, response_code::SUCCESS);
        assert_eq!(response.opaque, 5_555, "响应必须回填 broker 请求的 opaque");
        assert!(response.is_response_type());
        server.await.unwrap();
        connect.shutdown();
    }

    #[tokio::test]
    async fn rpc_hooks_run_before_encode() {
        struct MarkingHook;
        impl RPCHook for MarkingHook {
            fn do_before_request(&self, _addr: &str, request: &mut RemotingCommand) {
                request.add_ext_field("Hooked", "yes");
            }
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (tx, rx) = tokio::sync::oneshot::channel::<RemotingCommand>();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let frame = read_frame(&mut stream).await.unwrap();
            let cmd = RemotingCommand::decode(&frame).unwrap();
            let mut response = response_for(&cmd, response_code::SUCCESS);
            write_command(&mut stream, &mut response).await;
            let _ = tx.send(cmd);
        });
        let client = RemotingClient::new();
        client.register_rpc_hook(Arc::new(MarkingHook));
        assert_eq!(client.rpc_hook_count(), 1);
        let mut cmd = request(request_code::SEND_MESSAGE, "hook");
        client.invoke_sync(&addr, &mut cmd, Some(3000)).await.unwrap();
        let on_wire = rx.await.unwrap();
        assert_eq!(on_wire.get_ext_field("Hooked"), Some("yes"), "钩子必须作用于上线报文");
        client.shutdown();
    }

    #[tokio::test]
    async fn rocketmq_serialize_type_round_trips() {
        let (addr, _server) = echo_server().await;
        let client = RemotingClient::new();
        let mut cmd = request(request_code::SEND_MESSAGE, "binary");
        cmd.serialize_type_current_rpc = serialize_type::ROCKETMQ;
        let response = client.invoke_sync(&addr, &mut cmd, Some(3000)).await.unwrap();
        assert_eq!(
            response.serialize_type_current_rpc,
            serialize_type::ROCKETMQ,
            "私有二进制协议位必须在高 8 位往返"
        );
        client.shutdown();
    }

    #[test]
    fn address_splitting() {
        assert_eq!(split_host_port("127.0.0.1:10911").unwrap().1, 10911);
        assert_eq!(split_host_port("[::1]:9876").unwrap(), ("::1".to_string(), 9876));
        assert!(split_host_port("host:").is_err());
        assert!(split_host_port(":10911").is_err());
    }

    #[test]
    fn frame_length_guard() {
        assert_eq!(MAX_FRAME_LENGTH, 16 * 1024 * 1024);
    }
}