//! Provides: lazy connect + reuse, sync / async / oneway invocation, opaque matching,
//! partial / coalesced frame reassembly, broker-initiated request dispatch, timeout control, TLS, RPC hooks.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{mpsc as std_mpsc, Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::remoting::protocol::codes::response_code;
use crate::remoting::protocol::remoting_command::{next_opaque, RemotingCommand};
use crate::remoting::rpchook::RPCHook;
use crate::{rmq_debug, rmq_error, rmq_warn};

pub const MAX_FRAME_LENGTH: i32 = 16 * 1024 * 1024;

/// A connection that carried no traffic for this long is closed by the sweeper.
/// A configured value that is not positive restores this default instead of switching the sweep off:
/// a transport that never reaps is not a knob anyone asked for.
pub const DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS: i64 = 120 * 1000;

/// How often the sweeper looks at the idle clock.
const SWEEP_INTERVAL: Duration = Duration::from_millis(100);

/// Milliseconds on a clock that only moves forward, so a wall-clock jump cannot mass-close live connections.
///
/// One-based on purpose: zero is what an unstamped connection carries, so the first reading of the
/// process must not look like one, or a connection stamped that early would never age.
fn mono_now_ms() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    (EPOCH.get_or_init(Instant::now).elapsed().as_millis() + 1)
        .try_into()
        .unwrap_or(u64::MAX)
}

/// Connection identity: monotonically increasing, binds in-flight requests to "the connection that actually wrote them out".
/// An address cannot serve as identity -- switching connections at the same address (GO_AWAY resend, reconnect after EOF) is a common shape,
/// so closing by address would wrongly fail requests on the new connection, see [`fail_pending_for`].
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

fn next_conn_id() -> u64 {
    NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
}

/// Backoff when a single TLS read/write attempt gets no data: the socket is non-blocking, this short sleep avoids busy-spinning,
/// while keeping the hold time of [`TlsStream`]'s shared bidirectional lock at a single-syscall order.
const TLS_POLL_INTERVAL: Duration = Duration::from_millis(2);

pub type InvokeCallback = Box<dyn FnOnce(Result<RemotingCommand>) + Send + 'static>;

/// Call [`ResponseSink::respond`] when a reply is needed (the framework fills in the opaque); oneway requests return directly.
/// Implementations may spawn internally; do not block for long.
pub trait RequestProcessor: Send + Sync + 'static {
    fn process(&self, request: RemotingCommand, addr: String, sink: ResponseSink);
}

/// Writes the response back to the peer. This path does **not** run RPC hooks: response messages neither need nor may carry a signature.
#[derive(Clone)]
pub struct ResponseSink {
    inner: Arc<Inner>,
    addr: String,
    opaque: i32,
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
                rmq_warn!(
                    "remoting: failed to write response (code={}) to {}: {}",
                    response.code,
                    addr,
                    e
                );
            }
        });
    }
}

#[derive(Debug, Clone)]
pub struct RemotingClientConfig {
    pub connect_timeout_millis: i64,
    pub invoke_timeout_millis: i32,
    pub tls_enable: bool,
    pub tls_test_mode: bool,
    pub enable_reconnect_for_go_away: bool,
    pub socks_proxy: Option<crate::remoting::socks::SocksProxyConfig>,
    pub channel_idle_timeout_millis: i64,
    pub close_socket_on_sync_timeout: bool,
}

impl Default for RemotingClientConfig {
    fn default() -> Self {
        RemotingClientConfig {
            connect_timeout_millis: 3000,
            invoke_timeout_millis: 3000,
            tls_enable: env_bool("ROCKETMQ_TLS_ENABLE", false),
            tls_test_mode: env_bool("ROCKETMQ_TLS_TEST_MODE", true),
            enable_reconnect_for_go_away: true,
            socks_proxy: None,
            channel_idle_timeout_millis: DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS,
            close_socket_on_sync_timeout: true,
        }
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

struct Pending {
    sender: oneshot::Sender<RemotingCommand>,
    request: Option<RemotingCommand>,
    addr: String,
    conn_id: Option<u64>,
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
    /// Connection identity unique within this process, see [`next_conn_id`].
    id: u64,
    addr: String,
    writer: WriterHandle,
    alive: Arc<AtomicBool>,
    tasks: Mutex<Vec<TaskHandle>>,
    /// When this connection last carried bytes in either direction, on the clock of [`mono_now_ms`].
    /// It starts at zero while the connection is still being established: an entry that never went
    /// up is not an idle one either.
    last_activity_ms: Arc<AtomicU64>,
    /// Whether the last write on this socket found room in the send buffer. It only ever describes
    /// the socket, never how much work the write lane has queued behind it.
    send_ready: Arc<AtomicBool>,
}

impl Connection {
    fn new(addr: &str, writer: WriterHandle, tasks: Vec<TaskHandle>) -> Arc<Connection> {
        Arc::new(Connection {
            id: next_conn_id(),
            addr: addr.to_string(),
            writer,
            alive: Arc::new(AtomicBool::new(true)),
            tasks: Mutex::new(tasks),
            last_activity_ms: Arc::new(AtomicU64::new(0)),
            send_ready: Arc::new(AtomicBool::new(true)),
        })
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    fn mark_dead(&self) {
        self.alive.store(false, Ordering::Release);
    }

    fn touch(&self) {
        self.last_activity_ms.store(mono_now_ms(), Ordering::Release);
    }

    fn idle_millis(&self, now: u64) -> Option<u64> {
        let last = self.last_activity_ms.load(Ordering::Acquire);
        if last == 0 {
            return None;
        }
        Some(now.saturating_sub(last))
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
    /// One async lock per address: among concurrent requests only one task connects, the others wait for it and reuse the connection.
    connect_gates: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    hooks: RwLock<Vec<Arc<dyn RPCHook>>>,
    processors: RwLock<HashMap<i32, Arc<dyn RequestProcessor>>>,
    running: AtomicBool,
    /// tokio handle bound only when a spawn is first needed (`None` means not bound yet).
    /// `RemotingClient` may be constructed outside a tokio runtime,
    /// so an eager `Handle::current()` is impossible here; every spawn point is inside a runtime.
    handle: OnceLock<tokio::runtime::Handle>,
    /// The idle limit and the close-on-timeout rule are tunable after construction, so they live in
    /// the transport's own atomics rather than in the immutable config snapshot.
    channel_idle_timeout_ms: AtomicI64,
    close_socket_on_sync_timeout: AtomicBool,
    sweeper: Mutex<Option<JoinHandle<()>>>,
}

/// A non-positive configured value restores the default; the sweep is always installed.
fn normalized_idle_timeout_millis(millis: i64) -> i64 {
    if millis > 0 {
        millis
    } else {
        DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS
    }
}

/// Long-connection remoting client, clonable across tasks.
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
        let channel_idle_timeout_ms = normalized_idle_timeout_millis(config.channel_idle_timeout_millis);
        let close_socket_on_sync_timeout = config.close_socket_on_sync_timeout;
        RemotingClient {
            inner: Arc::new(Inner {
                config,
                state: Mutex::new(State::default()),
                connect_gates: Mutex::new(HashMap::new()),
                hooks: RwLock::new(Vec::new()),
                processors: RwLock::new(HashMap::new()),
                running: AtomicBool::new(true),
                handle: OnceLock::new(),
                channel_idle_timeout_ms: AtomicI64::new(channel_idle_timeout_ms),
                close_socket_on_sync_timeout: AtomicBool::new(close_socket_on_sync_timeout),
                sweeper: Mutex::new(None),
            }),
        }
    }

    /// Milliseconds a connection may carry no traffic before the sweeper closes it.
    pub fn channel_idle_timeout_millis(&self) -> i64 {
        self.inner
            .channel_idle_timeout_ms
            .load(Ordering::Acquire)
    }

    /// Setting a value that is not positive restores [`DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS`]:
    /// this is a limit on how long a connection may sit idle, not a switch for the sweep.
    pub fn set_channel_idle_timeout_millis(&self, millis: i64) {
        self.inner
            .channel_idle_timeout_ms
            .store(normalized_idle_timeout_millis(millis), Ordering::Release);
    }

    /// Whether a synchronous call whose wait ran out closes the connection it waited on.
    pub fn close_socket_on_sync_timeout(&self) -> bool {
        self.inner
            .close_socket_on_sync_timeout
            .load(Ordering::Acquire)
    }

    pub fn set_close_socket_on_sync_timeout(&self, close: bool) {
        self.inner
            .close_socket_on_sync_timeout
            .store(close, Ordering::Release);
    }

    /// Closes every connection idle strictly longer than the configured limit and returns how many went.
    pub fn sweep_idle_connections(&self) -> usize {
        sweep_idle_connections(&self.inner)
    }

    pub fn config(&self) -> &RemotingClientConfig {
        &self.inner.config
    }

    /// The current tokio handle; returns `None` outside a runtime context
    /// (the construction point does not assume a runtime exists, the handle is bound at the first spawn).
    pub fn runtime_handle(&self) -> Option<tokio::runtime::Handle> {
        self.inner.runtime_handle()
    }

    // ---------------- Hooks / processors ----------------
    pub fn register_rpc_hook(&self, hook: Arc<dyn RPCHook>) {
        self.inner
            .hooks
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .push(hook);
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
        self.inner
            .hooks
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// A **snapshot** of the registered hooks (registration order = execution order).
    /// Lets unit tests / diagnostics verify the hook chain order (Namespace → Stream → user hooks, see
    /// `crate::remoting::rpchook`); the send path itself does not read it.
    pub fn rpc_hooks(&self) -> Vec<Arc<dyn RPCHook>> {
        self.inner
            .hooks
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
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

    pub async fn invoke_sync(
        &self,
        addr: &str,
        request: &mut RemotingCommand,
        timeout_millis: Option<i64>,
    ) -> Result<RemotingCommand> {
        invoke_impl(&self.inner, addr, request, timeout_millis).await
    }

    /// Goes through the same invoke path as the synchronous call, so the async path has the same GO_AWAY reconnect semantics.
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
            let result = invoke_impl(&inner, &addr, &mut request, timeout_millis).await;
            callback(result);
        });
    }

    pub async fn invoke_oneway(&self, addr: &str, request: &mut RemotingCommand) -> Result<()> {
        request.mark_oneway_rpc();
        send_request(&self.inner, addr, request).await.map(|_| ())
    }

    // ---------------- Connection state ----------------
    /// Answers whether a send to this address would find room on the socket -- not whether the
    /// transport happens to have one open. An address with no live connection is writable, because
    /// the next send simply connects; `false` is reserved for a socket whose send buffer is full.
    pub fn is_channel_writable(&self, addr: &str) -> bool {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        match state.conns.get(addr).filter(|c| c.is_alive()) {
            None => true,
            Some(conn) => conn.send_ready.load(Ordering::Acquire),
        }
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

    pub fn start(&self) {
        self.inner.running.store(true, Ordering::Release);
    }

    pub fn shutdown(&self) {
        if !self.inner.running.swap(false, Ordering::SeqCst) {
            return;
        }
        stop_sweeper(&self.inner);
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
        // Under concurrency two tasks may call try_current at once; a failed set is harmless because the handles are equivalent.
        let _ = self.handle.set(handle.clone());
        Some(handle)
    }

    /// Spawns a background task on the current runtime; when no runtime is available it only logs, it does not panic.
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
        state.pending.insert(
            opaque,
            Pending {
                sender,
                request,
                addr: addr.to_string(),
                conn_id: None,
            },
        );
    }

    fn attach_pending_conn(&self, opaque: i32, conn_id: u64) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(pending) = state.pending.get_mut(&opaque) {
            pending.conn_id = Some(conn_id);
        }
    }

    fn cancel(&self, opaque: i32) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.pending.remove(&opaque);
    }
}

async fn invoke_impl(
    inner: &Arc<Inner>,
    addr: &str,
    request: &mut RemotingCommand,
    timeout_millis: Option<i64>,
) -> Result<RemotingCommand> {
    let timeout = timeout_millis.unwrap_or(inner.config.invoke_timeout_millis as i64);
    let started = Instant::now();
    let response = invoke_with_timeout(inner, addr, request, timeout).await?;
    if response.code != response_code::GO_AWAY {
        return Ok(response);
    }
    if !inner.config.enable_reconnect_for_go_away {
        return Err(Error::SendRequest {
            addr: addr.to_string(),
            message: format!("Receive GO_AWAY from channel {addr}"),
        });
    }
    rmq_warn!("remoting: receive GO_AWAY from {addr}, reconnect and retry once");
    close_channel(inner, addr);
    let spent = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
    let retry_timeout = (timeout - spent).max(1);
    let mut retry = request.clone();
    // The resend must use a **new** opaque: the old request already got its answer, reusing it would misroute the next response.
    retry.opaque = next_opaque();
    let response = invoke_with_timeout(inner, addr, &mut retry, retry_timeout).await?;
    if response.code == response_code::GO_AWAY {
        return Err(Error::SendRequest {
            addr: addr.to_string(),
            message: format!("Receive GO_AWAY twice in request from channel {addr}"),
        });
    }
    Ok(response)
}

async fn invoke_with_timeout(
    inner: &Arc<Inner>,
    addr: &str,
    request: &mut RemotingCommand,
    timeout: i64,
) -> Result<RemotingCommand> {
    let opaque = request.opaque;
    let (tx, rx) = oneshot::channel();
    inner.register_pending(opaque, tx, Some(request.clone()), addr);
    match send_request(inner, addr, request).await {
        Ok(conn_id) => inner.attach_pending_conn(opaque, conn_id),
        Err(e) => {
            inner.cancel(opaque);
            return Err(e);
        }
    }
    match tokio::time::timeout(Duration::from_millis(timeout.max(1) as u64), rx).await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(_)) => {
            inner.cancel(opaque);
            Err(Error::SendRequest {
                addr: addr.to_string(),
                message: "connection closed".into(),
            })
        }
        Err(_) => {
            // This call shape owns the only copy of the wait, so the entry is reclaimed exactly here:
            // one line per expired request, naming the address and the opaque it never answered.
            rmq_warn!(
                "remoting: in-flight request to {addr} expired (opaque={opaque}, timeout={timeout} ms), failed with a timeout"
            );
            inner.cancel(opaque);
            close_socket_on_timeout(inner, addr, timeout, opaque);
            Err(Error::Timeout {
                addr: addr.to_string(),
                timeout_millis: timeout,
            })
        }
    }
}

/// The close-after-timeout rule, on the only budget this port can hand the wait.
///
/// The budget is not reduced by the connect or the encode ahead of the wait, so what remains when
/// the wait runs out is the whole timeout: the rule then keeps the socket exactly when no wait
/// could ever have been answered on that budget.
fn close_socket_on_timeout(inner: &Arc<Inner>, addr: &str, timeout: i64, opaque: i32) {
    if !inner.close_socket_on_sync_timeout.load(Ordering::Acquire) {
        return;
    }
    let left_millis = timeout;
    if !(left_millis > 100 || left_millis > timeout / 4) {
        return;
    }
    rmq_warn!(
        "remoting: request to {addr} timed out (opaque={opaque}, timeout={timeout} ms), closing the connection"
    );
    close_channel(inner, addr);
}

async fn send_request(
    inner: &Arc<Inner>,
    addr: &str,
    request: &mut RemotingCommand,
) -> Result<u64> {
    {
        let hooks = inner.hooks.read().unwrap_or_else(|e| e.into_inner());
        for hook in hooks.iter() {
            hook.do_before_request(addr, request);
        }
    }
    write_frame(inner, addr, request).await
}

async fn write_frame(inner: &Arc<Inner>, addr: &str, request: &mut RemotingCommand) -> Result<u64> {
    let conn = get_or_create(inner, addr).await?;
    let data = request.encode();
    if conn.writer.send(data) {
        Ok(conn.id)
    } else {
        conn.mark_dead();
        close_channel(inner, &conn.addr);
        Err(Error::SendRequest {
            addr: addr.to_string(),
            message: "connection closed".into(),
        })
    }
}

/// Closes by **address**: callers only hold an address (GO_AWAY resend, explicit `close_channel`, `shutdown`,
/// write failure), so this unpublishes the current connection of that address and fails only the in-flight requests owned by **that connection**.
fn close_channel(inner: &Arc<Inner>, addr: &str) {
    let conn = {
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.conns.remove(addr)
    };
    if let Some(conn) = conn {
        conn.mark_dead();
        conn.abort_tasks();
        fail_pending_for(inner, conn.id);
    }
}

/// Claiming is keyed by `conn_id`, not by address: switching connections at the same address is a common shape (reconnect after EOF, GO_AWAY resend),
/// so if the old connection's reader closed by address on its way out it would also fail the requests just registered on the **new** connection.
/// Requests not written out yet have `conn_id` = `None` and must not be dragged in either.
fn fail_pending_for(inner: &Arc<Inner>, conn_id: u64) {
    let dropped: Vec<Pending> = {
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let keys: Vec<i32> = state
            .pending
            .iter()
            .filter(|(_, p)| p.conn_id == Some(conn_id))
            .map(|(k, _)| *k)
            .collect();
        keys.into_iter()
            .filter_map(|k| state.pending.remove(&k))
            .collect()
    };
    if dropped.is_empty() {
        return;
    }
    // Log the address: several connections may close at the same moment, only addr tells which one broke.
    let addr = &dropped[0].addr;
    rmq_warn!(
        "remoting: connection to {addr} (id={conn_id}) closed, {} in-flight request(s) failed fast",
        dropped.len()
    );
    for pending in dropped {
        drop(pending);
    }
}

/// Called when a read/write task exits: `conn_id` is the identity of **this own** connection.
/// The table entry is removed only while it is still this connection -- once a new connection replaced it, it must not be touched,
/// but the requests owned by this connection are always closed immediately so callers do not wait for the timeout.
fn on_connection_lost(inner: &Arc<Inner>, addr: &str, conn_id: u64) {
    let mine = {
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.conns.get(addr).map(|c| c.id) == Some(conn_id) {
            state.conns.remove(addr)
        } else {
            None
        }
    };
    if let Some(conn) = mine {
        conn.mark_dead();
        conn.abort_tasks();
    }
    fail_pending_for(inner, conn_id);
}

/// Closes every connection idle strictly longer than the limit and returns how many went.
///
/// Reaping goes through the same route a peer disconnect takes: the entry leaves the table first,
/// then the connection is marked dead and its tasks aborted, and only the requests owned by **that**
/// connection fail fast.
fn sweep_idle_connections(inner: &Arc<Inner>) -> usize {
    let limit = inner
        .channel_idle_timeout_ms
        .load(Ordering::Acquire)
        .max(0) as u64;
    let now = mono_now_ms();
    let doomed: Vec<Arc<Connection>> = {
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let stale: Vec<String> = state
            .conns
            .iter()
            .filter(|(_, c)| c.idle_millis(now).map(|idle| idle > limit).unwrap_or(false))
            .map(|(addr, _)| addr.clone())
            .collect();
        stale.into_iter().filter_map(|addr| state.conns.remove(&addr)).collect()
    };
    if doomed.is_empty() {
        return 0;
    }
    let closed = doomed.len();
    for conn in doomed {
        rmq_warn!(
            "remoting: connection to {} idle for {} ms (limit {} ms), closing it",
            conn.addr,
            conn.idle_millis(now).unwrap_or(0),
            limit
        );
        conn.mark_dead();
        conn.abort_tasks();
        fail_pending_for(inner, conn.id);
    }
    closed
}

/// Installs the sweeper on first use. It is spawned from inside the runtime that a connection was
/// created on, so a client that only ever calls synchronously outside a runtime has no sweep task --
/// and equally no idle connections, because establishing one needs this same path.
fn ensure_sweeper(inner: &Arc<Inner>) {
    let mut slot = inner.sweeper.lock().unwrap_or_else(|e| e.into_inner());
    if slot.is_some() {
        return;
    }
    let Some(handle) = inner.runtime_handle() else {
        return;
    };
    // A weak reference keeps the task from owning the client it sweeps: an Arc here would be a cycle
    // (Inner holds the JoinHandle, the task would hold Inner) and nothing would ever be freed.
    let weak = Arc::downgrade(inner);
    *slot = Some(handle.spawn(async move {
        loop {
            tokio::time::sleep(SWEEP_INTERVAL).await;
            let Some(inner) = weak.upgrade() else { break };
            if !inner.is_running() {
                break;
            }
            sweep_idle_connections(&inner);
        }
    }));
}

fn stop_sweeper(inner: &Arc<Inner>) {
    let handle = inner.sweeper.lock().unwrap_or_else(|e| e.into_inner()).take();
    if let Some(handle) = handle {
        handle.abort();
    }
}

/// Every frame the reader decodes comes through here: responses go to the in-flight table, requests to the processor table.
///
/// The answer says whether the byte stream can still be trusted: `false` means the frame that came
/// off it is not a frame at all, so everything after it sits at the wrong offset and the caller has
/// to take the connection down instead of reading on.
fn dispatch(inner: &Arc<Inner>, addr: &str, frame: Vec<u8>) -> bool {
    let cmd = match RemotingCommand::decode(&frame) {
        Ok(cmd) => cmd,
        Err(e) => {
            // Reported as the connection-level event it is: the bytes cannot be attributed to any
            // request, so there is nothing to answer and the stream no longer lines up.
            rmq_error!(
                "remoting: cannot decode frame from {addr} ({} bytes), closing the connection: {e}",
                frame.len()
            );
            return false;
        }
    };
    let opaque = cmd.opaque;
    let pending = {
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        // Responses pop by opaque; the "abnormal fallback" branch for a frame without the response flag
        // whose opaque collides with an in-flight request performs the same pop. Both branches act alike, so one lookup suffices.
        state.pending.remove(&opaque)
    };
    if let Some(pending) = pending {
        let Pending {
            sender, request, ..
        } = pending;
        apply_after_response_hooks(inner, addr, request.as_ref(), Some(&cmd));
        let _ = sender.send(cmd);
        return true;
    }
    if cmd.is_response_type() {
        // A frame that answers nothing we are waiting for: drop it loudly, but the stream itself is
        // still aligned, so the connection stays.
        rmq_warn!(
            "remoting: response from {addr} does not match any in-flight request (opaque={opaque}), dropped"
        );
        return true;
    }
    let processor = inner
        .processors
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&cmd.code)
        .cloned();
    let Some(processor) = processor else {
        rmq_warn!(
            "remoting: no processor for request code {} (opaque {opaque}) from {addr}",
            cmd.code
        );
        return true;
    };
    let sink = ResponseSink {
        inner: inner.clone(),
        addr: addr.to_string(),
        opaque,
        wants_reply: !cmd.is_oneway_rpc(),
    };
    // A processor panic must not take the reader thread down with it: catch it and answer with SYSTEM_ERROR.
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
    true
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
    // The connection is registered: the idle clock starts here, and the sweeper that reads that
    // clock has to be running for whatever call shape created it.
    inserted.touch();
    drop(state);
    ensure_sweeper(inner);
    Ok(inserted)
}

fn connect_gate(inner: &Arc<Inner>, addr: &str) -> Arc<tokio::sync::Mutex<()>> {
    let mut gates = inner
        .connect_gates
        .lock()
        .unwrap_or_else(|e| e.into_inner());
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
    let timeout = Duration::from_millis(inner.config.connect_timeout_millis.max(1) as u64);
    let stream = match inner.config.socks_proxy.as_ref() {
        Some(proxy) => {
            crate::remoting::socks::connect_via_proxy(proxy, &host, port, timeout).await?
        }
        None => {
            let socket_addr = resolve(&host, port).await?;
            tcp_connect(socket_addr, timeout, addr).await?
        }
    };
    if inner.config.tls_enable {
        connect_tls(inner, addr, &host, stream).await
    } else {
        connect_plain(inner, stream, addr).await
    }
}

async fn tcp_connect(socket_addr: SocketAddr, timeout: Duration, addr: &str) -> Result<TcpStream> {
    let stream = tokio::time::timeout(timeout, TcpStream::connect(socket_addr))
        .await
        .map_err(|_| Error::Connect {
            addr: addr.to_string(),
        })?
        .map_err(|_| Error::Connect {
            addr: addr.to_string(),
        })?;
    let _ = stream.set_nodelay(true);
    Ok(stream)
}

async fn connect_plain(
    inner: &Arc<Inner>,
    stream: TcpStream,
    addr: &str,
) -> Result<Arc<Connection>> {
    // Two handles on the same socket rather than one split stream: the write lane has to be able
    // to ask the socket itself whether it still takes bytes, and that question is only answerable
    // on a `TcpStream`, which a borrowed write half is not.
    let std_stream = stream
        .into_std()
        .map_err(|_| Error::Connect { addr: addr.to_string() })?;
    let read_std = std_stream
        .try_clone()
        .map_err(|_| Error::Connect { addr: addr.to_string() })?;
    let read_stream =
        TcpStream::from_std(read_std).map_err(|_| Error::Connect { addr: addr.to_string() })?;
    let stream =
        TcpStream::from_std(std_stream).map_err(|_| Error::Connect { addr: addr.to_string() })?;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

    let conn = Connection::new(addr, WriterHandle::Async(tx), Vec::new());
    let conn_id = conn.id;

    let reader_inner = inner.clone();
    let reader_addr = addr.to_string();
    let reader_activity = conn.last_activity_ms.clone();
    let reader = tokio::spawn(async move {
        let mut read_stream = read_stream;
        let mut len_buf = [0u8; 4];
        loop {
            if !reader_inner.is_running() {
                break;
            }
            if read_stream.read_exact(&mut len_buf).await.is_err() {
                break;
            }
            // Bytes arrived: the connection is not the idle one, whatever happens to them next.
            reader_activity.store(mono_now_ms(), Ordering::Release);
            let total = i32::from_be_bytes(len_buf);
            if total <= 0 || total > MAX_FRAME_LENGTH {
                // The length prefix itself is out of range, so the bytes on the stream no longer
                // line up with the frame boundaries. This is the same event as an undecodable frame
                // one level earlier, and it is reported once, here: what follows cannot be
                // attributed to any request, so the connection goes.
                rmq_error!("remoting: illegal frame length {total} from {reader_addr}, closing the connection");
                break;
            }
            let mut frame = vec![0u8; 4 + total as usize];
            frame[..4].copy_from_slice(&len_buf);
            if read_stream.read_exact(&mut frame[4..]).await.is_err() {
                break;
            }
            if !dispatch(&reader_inner, &reader_addr, frame) {
                break;
            }
        }
        on_connection_lost(&reader_inner, &reader_addr, conn_id);
    });

    let writer_inner = inner.clone();
    let writer_addr = addr.to_string();
    let writer_activity = conn.last_activity_ms.clone();
    let writer_ready = conn.send_ready.clone();
    let writer = tokio::spawn(async move {
        let stream = stream;
        while let Some(data) = rx.recv().await {
            if write_tracked(&stream, &data, &writer_ready).await.is_err() {
                break;
            }
            // The bytes went out: the connection just did real work, so its idle clock restarts here.
            writer_activity.store(mono_now_ms(), Ordering::Release);
        }
        on_connection_lost(&writer_inner, &writer_addr, conn_id);
    });

    {
        let mut tasks = conn.tasks.lock().unwrap_or_else(|e| e.into_inner());
        tasks.push(TaskHandle::Async(reader));
        tasks.push(TaskHandle::Async(writer));
    }
    Ok(conn)
}

/// Puts one frame on the socket, saying plainly when the send buffer has no room.
///
/// The readiness question is asked **before** the bytes move, from the write lane itself: a peer
/// that stopped reading shows up here as the socket refusing more, and that is the only state this
/// transport reports as unwritable. Once the whole frame is out the socket takes bytes again, so
/// the answer flips back.
async fn write_tracked(stream: &TcpStream, data: &[u8], ready: &AtomicBool) -> std::io::Result<()> {
    let mut off = 0usize;
    while off < data.len() {
        match stream.try_write(&data[off..]) {
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Ok(n) => off += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                ready.store(false, Ordering::Release);
                // Wait for room instead of claiming the socket is fine: the peer that stopped
                // reading is what this answer exists to report.
                stream.writable().await?;
            }
            Err(e) => return Err(e),
        }
    }
    ready.store(true, Ordering::Release);
    Ok(())
}

/// TLS runs on dedicated threads: `native-tls` has no async interface, one [`TlsStream`] shares a single lock for both directions.
///
/// ⚠  After the handshake the socket is set to **non-blocking**, reads and writes retry with a short sleep of [`TLS_POLL_INTERVAL`].
/// The old "blocking read + read timeout" approach would sit on the lock for a whole timeout period on a partial frame and starve the writer:
/// measured against a real TLS broker this showed up as every name server request timing out at 5s with the responses then arriving in batches
/// (each one answered by `remoting: response from ... does not match any in-flight request`).
/// Non-blocking + short sleep presses the lock hold time down to one syscall.
async fn connect_tls(
    inner: &Arc<Inner>,
    addr: &str,
    host: &str,
    tcp: TcpStream,
) -> Result<Arc<Connection>> {
    let _ = tcp.set_nodelay(true);
    let std_stream = tcp.into_std().map_err(|_| Error::Connect {
        addr: addr.to_string(),
    })?;
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
        let connector = builder.build().map_err(|e| Error::Connect {
            addr: format!("{handshake_addr} (tls connector: {e})"),
        })?;
        let mut tls = connector
            .connect(&domain, std_stream)
            .map_err(|e| Error::Connect {
                addr: format!("{handshake_addr} (tls handshake: {e})"),
            })?;
        // Non-blocking is set after the handshake: during the handshake full blocking must be allowed.
        let _ = tls.get_mut().set_nonblocking(true);
        Ok::<_, Error>(tls)
    })
    .await
    .map_err(|_| Error::Connect {
        addr: addr.to_string(),
    })??;

    let shared = Arc::new(Mutex::new(stream));
    let (tx, rx) = std_mpsc::sync_channel::<Vec<u8>>(1024);
    let rx = Arc::new(Mutex::new(rx));

    let conn = Connection::new(
        addr,
        WriterHandle::Blocking(Arc::new(tx)),
        vec![TaskHandle::Blocking],
    );

    // The reader is a plain std thread with no tokio context of its own, while broker-pushed requests (transaction check-back,
    // heartbeat response) are processed synchronously on that thread and processors rely on the runtime to spawn background tasks. This function is async,
    // so it certainly runs inside a runtime: take the handle here (also caching it in Inner for other threads).
    let runtime = inner.runtime_handle();
    spawn_tls_writer(inner.clone(), addr, shared.clone(), rx.clone(), &conn);
    spawn_tls_reader(inner.clone(), addr, shared, runtime, &conn);
    Ok(conn)
}

fn spawn_tls_reader(
    inner: Arc<Inner>,
    addr: &str,
    shared: SharedTls,
    runtime: Option<tokio::runtime::Handle>,
    conn: &Connection,
) {
    let addr = addr.to_string();
    let alive = conn.alive.clone();
    let activity = conn.last_activity_ms.clone();
    let conn_id = conn.id;
    std::thread::spawn(move || {
        // enter() makes `Handle::try_current()` on the reader behave like the plaintext tokio read task,
        // otherwise in a pure TLS session broker check-back requests reach the processor but cannot be spawned.
        let _guard = runtime.as_ref().map(|handle| handle.enter());
        tls_read_loop(inner, addr, shared, alive, activity, conn_id);
    });
}

fn spawn_tls_writer(
    inner: Arc<Inner>,
    addr: &str,
    shared: SharedTls,
    rx: Arc<Mutex<std_mpsc::Receiver<Vec<u8>>>>,
    conn: &Connection,
) {
    let addr = addr.to_string();
    let alive = conn.alive.clone();
    let activity = conn.last_activity_ms.clone();
    let ready = conn.send_ready.clone();
    let conn_id = conn.id;
    std::thread::spawn(move || {
        loop {
            let data = match rx.lock().unwrap_or_else(|e| e.into_inner()).recv() {
                Ok(data) => data,
                Err(_) => break,
            };
            if !alive.load(Ordering::Acquire) || !inner.is_running() {
                break;
            }
            if tls_write_all(&shared, &data, &ready).is_err() {
                break;
            }
            // The bytes went out: the connection just did real work, so its idle clock restarts here.
            activity.store(mono_now_ms(), Ordering::Release);
        }
        on_connection_lost(&inner, &addr, conn_id);
    });
}

type SharedTls = Arc<Mutex<native_tls::TlsStream<std::net::TcpStream>>>;

/// Takes and releases the lock once per chunk written, sleeping [`TLS_POLL_INTERVAL`] before resuming on `WouldBlock`.
///
/// The lock must be taken once per attempt: the reader waits on the same lock, holding it for long blocks the peer's requests.
fn tls_write_all(shared: &SharedTls, data: &[u8], ready: &AtomicBool) -> std::io::Result<()> {
    let mut off = 0usize;
    while off < data.len() {
        let written = {
            let mut guard = shared.lock().unwrap_or_else(|e| e.into_inner());
            match guard.write(&data[off..]) {
                Ok(0) => Err(std::io::ErrorKind::WriteZero.into()),
                result => result,
            }
        };
        match written {
            Ok(n) => off += n,
            Err(e) if is_timeout(&e) => {
                // The socket refuses more right now: that is exactly the state the writability
                // answer is about, so say so and keep the lane waiting for room.
                ready.store(false, Ordering::Release);
                std::thread::sleep(TLS_POLL_INTERVAL);
            }
            Err(e) => return Err(e),
        }
    }
    loop {
        let flushed = { shared.lock().unwrap_or_else(|e| e.into_inner()).flush() };
        match flushed {
            Ok(()) => {
                ready.store(true, Ordering::Release);
                return Ok(());
            }
            Err(e) if is_timeout(&e) => {
                ready.store(false, Ordering::Release);
                std::thread::sleep(TLS_POLL_INTERVAL);
            }
            Err(e) => return Err(e),
        }
    }
}

fn tls_read_loop(
    inner: Arc<Inner>,
    addr: String,
    shared: SharedTls,
    alive: Arc<AtomicBool>,
    activity: Arc<AtomicU64>,
    conn_id: u64,
) {
    let mut len_buf = [0u8; 4];
    let mut header_done = false;
    loop {
        if !inner.is_running() || !alive.load(Ordering::Acquire) {
            break;
        }
        if !header_done {
            match tls_read_exact(&shared, &mut len_buf) {
                Ok(true) => header_done = true,
                Ok(false) => {
                    std::thread::sleep(TLS_POLL_INTERVAL);
                    continue;
                }
                Err(_) => break,
            }
        }
        // Bytes arrived: the connection is not the idle one, whatever happens to them next.
        activity.store(mono_now_ms(), Ordering::Release);
        let total = i32::from_be_bytes(len_buf);
        if total <= 0 || total > MAX_FRAME_LENGTH {
            // The length prefix itself is out of range, so the bytes on the stream no longer line
            // up with the frame boundaries: the same event as an undecodable frame one level
            // earlier, reported once here, and the connection goes.
            rmq_error!("remoting: illegal tls frame length {total} from {addr}, closing the connection");
            break;
        }
        let mut frame = vec![0u8; 4 + total as usize];
        frame[..4].copy_from_slice(&len_buf);
        match tls_read_exact(&shared, &mut frame[4..]) {
            Ok(true) => {}
            Ok(false) => {
                std::thread::sleep(TLS_POLL_INTERVAL);
                continue;
            }
            Err(_) => break,
        }
        header_done = false;
        if !dispatch(&inner, &addr, frame) {
            break;
        }
    }
    alive.store(false, Ordering::Release);
    on_connection_lost(&inner, &addr, conn_id);
}

/// `Ok(true)` buffer filled, `Ok(false)` no data for now (caller retries), `Err` connection unusable.
///
/// ⚠ Every `read` **takes and releases the lock separately**; the lock must not be held for a whole frame: `TlsStream` shares one lock both ways, a reader
/// holding it while waiting for a partial frame stops the writer from sending the next request (on a real broker this shows up as requests timing out
/// at 5s in batches and responses arriving in batches). Partial-frame progress lives in `filled`, so yielding the lock loses no data.
fn tls_read_exact(shared: &SharedTls, buf: &mut [u8]) -> std::result::Result<bool, std::io::Error> {
    let mut filled = 0usize;
    while filled < buf.len() {
        let read = {
            let mut guard = shared.lock().unwrap_or_else(|e| e.into_inner());
            guard.read(&mut buf[filled..])
        };
        match read {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => filled += n,
            Err(e) if is_timeout(&e) => {
                if filled == 0 {
                    return Ok(false);
                }
                std::thread::sleep(TLS_POLL_INTERVAL);
            }
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

fn is_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
}

pub fn split_host_port(addr: &str) -> Result<(String, u16)> {
    let (host, port) = crate::common::util_all::parse_addr(addr);
    let port: u16 = port.parse().map_err(|_| Error::SendRequest {
        addr: addr.to_string(),
        message: format!("bad port in address {addr:?}"),
    })?;
    if host.is_empty() {
        return Err(Error::SendRequest {
            addr: addr.to_string(),
            message: "empty host".into(),
        });
    }
    Ok((host, port))
}

pub(crate) async fn resolve(host: &str, port: u16) -> Result<SocketAddr> {
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
            .ok_or_else(|| Error::Connect {
                addr: format!("dns lookup failed for {host}:{port}"),
            })
    })
    .await
    .map_err(|_| Error::Connect {
        addr: "dns lookup task panicked".into(),
    })?
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    // `AsyncReadExt` arrives through the module-level `use` via `use super::*`; the fake peers
    // here also write, which needs the matching write trait.
    use tokio::io::AsyncWriteExt;
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

    /// Minimal broker: read one frame, answer one frame with the same opaque.
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
        let response = client
            .invoke_sync(&addr, &mut cmd, Some(3000))
            .await
            .unwrap();
        assert_eq!(response.code, response_code::SUCCESS);
        assert_eq!(response.opaque, cmd.opaque, "the response must carry back the request opaque");
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
            // Split into 1 byte + the rest to verify partial / coalesced frame reassembly
            for chunk in bytes.chunks(1) {
                stream.write_all(chunk).await.unwrap();
                stream.flush().await.unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });
        let client = RemotingClient::new();
        let mut cmd = request(request_code::PULL_MESSAGE, "x");
        let response = client
            .invoke_sync(&addr, &mut cmd, Some(5000))
            .await
            .unwrap();
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
                let response = client
                    .invoke_sync(&addr, &mut cmd, Some(5000))
                    .await
                    .unwrap();
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
        assert_eq!(client.in_flight_count(), 0, "oneway does not register in the in-flight table");
        client.shutdown();
    }

    #[tokio::test]
    async fn timeout_when_broker_stays_silent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move { while read_frame(&mut stream).await.is_some() {} });
            }
        });
        let client = RemotingClient::new();
        let mut cmd = request(request_code::PULL_MESSAGE, "quiet");
        let err = client
            .invoke_sync(&addr, &mut cmd, Some(200))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Timeout { .. }), "got {err}");
        assert!(err.to_string().contains("200ms"));
        assert_eq!(client.in_flight_count(), 0, "the in-flight table must be cleared after a timeout");
        client.shutdown();
    }

    /// GO_AWAY server: the first `go_away_conns` connections always answer GO_AWAY, the rest answer SUCCESS.
    /// Returns the connection-index counter so assertions can check "the resend really switched to a new connection".
    async fn go_away_server(go_away_conns: usize) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let conns = Arc::new(AtomicUsize::new(0));
        let counter = conns.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let idx = counter.fetch_add(1, Ordering::SeqCst);
                let go_away = idx < go_away_conns;
                tokio::spawn(async move {
                    while let Some(frame) = read_frame(&mut stream).await {
                        let cmd = RemotingCommand::decode(&frame).unwrap();
                        let code = if go_away {
                            response_code::GO_AWAY
                        } else {
                            response_code::SUCCESS
                        };
                        let mut response = response_for(&cmd, code);
                        write_command(&mut stream, &mut response).await;
                    }
                });
            }
        });
        (addr, conns)
    }

    #[tokio::test]
    async fn go_away_reconnects_and_retries_once() {
        let (addr, conns) = go_away_server(1).await;
        let client = RemotingClient::new();
        let mut cmd = request(request_code::SEND_MESSAGE_V2, "goaway");
        let response = client
            .invoke_sync(&addr, &mut cmd, Some(3000))
            .await
            .unwrap();
        assert_eq!(response.code, response_code::SUCCESS, "the resend must get a real answer");
        assert_eq!(conns.load(Ordering::SeqCst), 2, "after GO_AWAY a new connection must be used");
        assert_eq!(client.in_flight_count(), 0);
        client.shutdown();
    }

    #[tokio::test]
    async fn second_go_away_fails_instead_of_looping() {
        let (addr, conns) = go_away_server(usize::MAX).await;
        let client = RemotingClient::new();
        let mut cmd = request(request_code::SEND_MESSAGE_V2, "goaway");
        let err = client
            .invoke_sync(&addr, &mut cmd, Some(3000))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::SendRequest { .. }), "got {err}");
        assert!(
            err.to_string().contains("GO_AWAY twice"),
            "the wording aligns with the send-request error text: {err}"
        );
        assert_eq!(conns.load(Ordering::SeqCst), 2, "retry only once, no infinite reconnect loop");
        assert_eq!(client.in_flight_count(), 0);
        client.shutdown();
    }

    #[tokio::test]
    async fn go_away_without_reconnect_flag_surfaces_error() {
        let (addr, conns) = go_away_server(usize::MAX).await;
        let client = RemotingClient::with_config(RemotingClientConfig {
            enable_reconnect_for_go_away: false,
            ..RemotingClientConfig::default()
        });
        let mut cmd = request(request_code::SEND_MESSAGE_V2, "goaway");
        let err = client
            .invoke_sync(&addr, &mut cmd, Some(3000))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::SendRequest { .. }), "got {err}");
        assert!(err.to_string().contains("Receive GO_AWAY from channel"));
        assert_eq!(conns.load(Ordering::SeqCst), 1, "with the flag off it must not reconnect");
        client.shutdown();
    }

    #[tokio::test]
    async fn connect_failure_maps_to_connect_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let client = RemotingClient::new();
        let mut cmd = request(request_code::HEART_BEAT, "dead");
        let err = client
            .invoke_sync(&addr, &mut cmd, Some(1000))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Connect { .. }), "got {err}");
        // "No connection" is not "cannot write": the next send simply connects.
        assert!(client.is_channel_writable(&addr));
    }

    #[tokio::test]
    async fn bad_port_is_reported() {
        let client = RemotingClient::new();
        let mut cmd = request(request_code::HEART_BEAT, "x");
        let err = client
            .invoke_sync("127.0.0.1:notaport", &mut cmd, Some(500))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::SendRequest { .. }), "got {err}");
    }

    #[tokio::test]
    async fn close_channel_forces_reconnect() {
        let (addr, _server) = echo_server().await;
        let client = RemotingClient::new();
        let mut cmd = request(request_code::SEND_MESSAGE, "first");
        client
            .invoke_sync(&addr, &mut cmd, Some(3000))
            .await
            .unwrap();
        client.close_channel(&addr);
        assert!(client.connection_addrs().is_empty(), "the closed address leaves the connection table");
        // "No connection" is not "cannot write": the next send simply connects.
        assert!(client.is_channel_writable(&addr));
        let mut second = request(request_code::SEND_MESSAGE, "second");
        let response = client
            .invoke_sync(&addr, &mut second, Some(3000))
            .await
            .unwrap();
        assert_eq!(response.remark.as_deref(), Some("pong:second"));
        client.shutdown();
    }


    /// Polls until the condition holds, at most 5s; panics on timeout (panicking is acceptable in tests).
    async fn wait_until(cond: impl Fn() -> bool, what: &str) {
        for _ in 0..500 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for: {what}");
    }

    /// The first `drop_conns` connections close right after reading one frame and never reply; later connections answer SUCCESS as usual.
    async fn eof_server(drop_conns: usize) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let conns = Arc::new(AtomicUsize::new(0));
        let counter = conns.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let idx = counter.fetch_add(1, Ordering::SeqCst);
                if idx < drop_conns {
                    tokio::spawn(async move {
                        // Close only after receiving the request: creates the EOF shape of "request written out, peer never replies".
                        let frame = read_frame(&mut stream).await;
                        assert!(frame.is_some(), "a request should have arrived before the close");
                        drop(stream);
                    });
                    continue;
                }
                tokio::spawn(async move {
                    while let Some(frame) = read_frame(&mut stream).await {
                        let cmd = RemotingCommand::decode(&frame).unwrap();
                        let mut response = response_for(&cmd, response_code::SUCCESS);
                        write_command(&mut stream, &mut response).await;
                    }
                });
            }
        });
        (addr, conns)
    }

    /// Only accepts, reads the request, then neither replies nor closes: the request stays in-flight,
    /// until the client itself winds it up (timeout, failFast, shutdown).
    async fn silent_server() -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let frames = Arc::new(AtomicUsize::new(0));
        let counter = frames.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let counter = counter.clone();
                tokio::spawn(async move {
                    while read_frame(&mut stream).await.is_some() {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
        });
        (addr, frames)
    }

    #[tokio::test]
    async fn connection_eof_fails_in_flight_request_fast() {
        let (addr, conns) = eof_server(usize::MAX).await;
        let client = RemotingClient::new();
        let started = Instant::now();
        let mut cmd = request(request_code::SEND_MESSAGE_V2, "eof");
        let err = client
            .invoke_sync(&addr, &mut cmd, Some(30_000))
            .await
            .unwrap_err();
        let cost = started.elapsed();

        assert!(
            matches!(err, Error::SendRequest { .. }),
            "must be a send failure, got {err}"
        );
        assert!(
            err.to_string().contains("connection closed"),
            "wording must match the requestFail phrasing: {err}"
        );
        assert!(
            cost < Duration::from_secs(5),
            "must fail fast within milliseconds, not wait out the full 30s timeout: {cost:?}"
        );
        assert_eq!(client.in_flight_count(), 0, "the in-flight table must be cleared after fail-fast");
        assert_eq!(conns.load(Ordering::SeqCst), 1);
        client.shutdown();
    }

    #[tokio::test]
    async fn connection_eof_fails_async_callback_exactly_once() {
        let (addr, _conns) = eof_server(usize::MAX).await;
        let client = RemotingClient::new();
        let hits = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = oneshot::channel();
        let counter = hits.clone();
        client.invoke_async(
            &addr,
            request(request_code::SEND_MESSAGE_V2, "eof"),
            Box::new(move |result| {
                counter.fetch_add(1, Ordering::SeqCst);
                let _ = tx.send(result);
            }),
            Some(30_000),
        );

        let result = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("callback never fired")
            .unwrap();
        let err = result.unwrap_err();
        assert!(matches!(err, Error::SendRequest { .. }), "got {err}");
        assert_eq!(hits.load(Ordering::SeqCst), 1, "the callback must be delivered exactly once");
        client.shutdown();
    }

    /// Winding up a dead connection must not drag in other connections: in-flight requests on another connection stay untouched.
    #[tokio::test]
    async fn fail_fast_leaves_other_connections_alone() {
        let (alive_addr, frames) = silent_server().await;
        let (doomed_addr, _c) = eof_server(usize::MAX).await;
        let client = RemotingClient::new();

        let slow = {
            let client = client.clone();
            let addr = alive_addr.clone();
            tokio::spawn(async move {
                let mut cmd = request(request_code::PULL_MESSAGE, "slow");
                client.invoke_sync(&addr, &mut cmd, Some(30_000)).await
            })
        };
        wait_until(|| frames.load(Ordering::SeqCst) >= 1, "alive connection received the request").await;

        let mut cmd = request(request_code::SEND_MESSAGE_V2, "eof");
        let err = client
            .invoke_sync(&doomed_addr, &mut cmd, Some(30_000))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::SendRequest { .. }), "got {err}");

        assert_eq!(client.in_flight_count(), 1, "only the requests on the failed connection itself should be failed");
        assert!(!slow.is_finished(), "requests on the alive connection must not be caught in the blast");
        slow.abort();
        client.shutdown();
    }

    /// After fail-fast the same address can still build a new connection and complete a new request: the dead connection must already be unpublished from the connection table.
    #[tokio::test]
    async fn same_address_works_after_fail_fast() {
        let (addr, conns) = eof_server(1).await;
        let client = RemotingClient::new();

        let mut first = request(request_code::SEND_MESSAGE_V2, "eof");
        let err = client
            .invoke_sync(&addr, &mut first, Some(30_000))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::SendRequest { .. }), "got {err}");
        assert!(
            client.connection_addrs().is_empty(),
            "the dead connection must be unpublished from the connection table"
        );
        // And the address is writable again: the next request builds a fresh connection.
        assert!(client.is_channel_writable(&addr));

        let mut second = request(request_code::SEND_MESSAGE_V2, "ok");
        let response = client
            .invoke_sync(&addr, &mut second, Some(5_000))
            .await
            .unwrap();
        assert_eq!(response.code, response_code::SUCCESS);
        assert_eq!(
            conns.load(Ordering::SeqCst),
            2,
            "the second request must go over the newly created connection"
        );
        client.shutdown();
    }

    /// Shutdown must also settle in-flight requests: the caller gets a send failure instead of hanging forever.
    #[tokio::test]
    async fn shutdown_drains_in_flight_requests() {
        let (addr, frames) = silent_server().await;
        let client = RemotingClient::new();
        let waiter = {
            let client = client.clone();
            let addr = addr.clone();
            tokio::spawn(async move {
                let mut cmd = request(request_code::PULL_MESSAGE, "pending");
                client.invoke_sync(&addr, &mut cmd, Some(30_000)).await
            })
        };
        wait_until(|| frames.load(Ordering::SeqCst) >= 1, "request written out").await;

        client.shutdown();
        let result = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("caller was left hanging")
            .unwrap();
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::SendRequest { .. }),
            "shutdown must drain immediately, got {err}"
        );
        assert_eq!(client.in_flight_count(), 0);
    }

    /// The broker actively pushes a request (PUSH_REPLY_MESSAGE_TO_CLIENT); the client processor must answer with a response.
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
            // The oneway above must not get a response; then send a request-type one that expects a reply
            let mut need_reply = RemotingCommand::create_request_command(
                request_code::CHECK_TRANSACTION_STATE,
                None,
            );
            need_reply.opaque = 5_555;
            let bytes = need_reply.encode();
            stream.write_all(&bytes).await.unwrap();
            stream.flush().await.unwrap();
            loop {
                let frame = read_frame(&mut stream).await.expect("the client should reply");
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
            // Connect first (the server pushes only after accepting)
            let mut warm = request(request_code::HEART_BEAT, "warm");
            warm.mark_oneway_rpc();
            client.invoke_oneway(&client_addr, &mut warm).await.unwrap();
            client
        };
        let response = tokio::time::timeout(Duration::from_secs(5), push_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.code, response_code::SUCCESS);
        assert_eq!(response.opaque, 5_555, "the response must carry back the broker request's opaque");
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
        client
            .invoke_sync(&addr, &mut cmd, Some(3000))
            .await
            .unwrap();
        let on_wire = rx.await.unwrap();
        assert_eq!(
            on_wire.get_ext_field("Hooked"),
            Some("yes"),
            "the hook must apply to the on-wire message"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn rocketmq_serialize_type_round_trips() {
        let (addr, _server) = echo_server().await;
        let client = RemotingClient::new();
        let mut cmd = request(request_code::SEND_MESSAGE, "binary");
        cmd.serialize_type_current_rpc = serialize_type::ROCKETMQ;
        let response = client
            .invoke_sync(&addr, &mut cmd, Some(3000))
            .await
            .unwrap();
        assert_eq!(
            response.serialize_type_current_rpc,
            serialize_type::ROCKETMQ,
            "the private binary protocol bits must round-trip in the high 8 bits"
        );
        client.shutdown();
    }

    #[test]
    fn address_splitting() {
        assert_eq!(split_host_port("127.0.0.1:10911").unwrap().1, 10911);
        assert_eq!(
            split_host_port("[::1]:9876").unwrap(),
            ("::1".to_string(), 9876)
        );
        assert!(split_host_port("host:").is_err());
        assert!(split_host_port(":10911").is_err());
    }

    #[test]
    fn frame_length_guard() {
        assert_eq!(MAX_FRAME_LENGTH, 16 * 1024 * 1024);
    }

    // ------------------------------------------------------------- TLS regression net
    //
    // Why this offline test group exists: the TLS branch (`connect_tls` and its read/write loops) is the only
    // place in the whole transport where a single change can deadlock silently, and every one of its known bugs reproduce on a single machine:
    //   * `TlsStream` shares one lock between both directions: the read thread holds the lock while waiting for a partial frame → the write thread starves, showing up as
    //     requests timing out in batches while responses come back in batches (observed against a real TLS broker; see the connect_tls comment);
    //   * the socket is set non-blocking after the handshake, so `tls_read_exact` / `tls_write_all` must keep continuing the reads and writes themselves;
    //     missing one progress step corrupts the frame.
    // Verifying these two against a real TLS broker is expensive (needs a broker with certificates) and CI has no such machine at all;
    // forging a self-signed certificate with openssl locally plus an in-process TLS server pins both down,
    // without depending on a real broker.

    const TLS_P12_PASSWORD: &str = "rmq-rust-test";

    /// Forge a self-signed certificate with the openssl CLI and export it as PKCS#12 (all three
    /// `Identity::from_pkcs12` backends support it, sidestepping the PKCS#1/PKCS#8 PEM header platform differences).
    /// Returns `None` when openssl is missing and the tests skip themselves -- what is under test is the
    /// TLS read/write loop; a missing tool must not turn the whole gate red.
    fn tls_identity_pkcs12() -> Option<Vec<u8>> {
        let openssl = match std::env::var("OPENSSL_BIN") {
            Ok(v) => v,
            Err(_) => {
                // When the env var is unset, probe PATH once: skip directly if not found (often absent on Windows CI)
                std::process::Command::new("openssl")
                    .arg("-version")
                    .output()
                    .ok()?
                    .status
                    .success()
                    .then(|| "openssl".to_string())?
            }
        };
        let dir = std::env::temp_dir().join(format!("rmq_rust_tls_{}_{}", std::process::id(), {
            static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            SEQ.fetch_add(1, Ordering::SeqCst)
        }));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).ok()?;
        let cert = dir.join("cert.pem");
        let key = dir.join("key.pem");
        let p12 = dir.join("identity.p12");
        let ok = std::process::Command::new(&openssl)
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-keyout",
                key.to_str()?,
                "-out",
                cert.to_str()?,
                "-days",
                "1",
                "-nodes",
                "-subj",
                "/CN=127.0.0.1",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        // Pin the legacy PBE explicitly: OpenSSL 3 defaults to exporting PKCS#12 v2.0 (AES-256 + SHA-256 KDF),
        // which macOS SecPKCS12Import cannot parse and fails with "MAC verification failed".
        // SHA1-3DES + SHA1 MAC imports on OpenSSL 1.x/3.x and on all three TLS backends.
        let exported = ok
            && std::process::Command::new(&openssl)
                .args([
                    "pkcs12",
                    "-export",
                    "-inkey",
                    key.to_str()?,
                    "-in",
                    cert.to_str()?,
                    "-out",
                    p12.to_str()?,
                    "-passout",
                    &format!("pass:{TLS_P12_PASSWORD}"),
                    "-keypbe",
                    "PBE-SHA1-3DES",
                    "-certpbe",
                    "PBE-SHA1-3DES",
                    "-macalg",
                    "sha1",
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
        let der = std::fs::read(&p12).ok();
        let _ = std::fs::remove_dir_all(&dir);
        if exported {
            der
        } else {
            None
        }
    }

    /// In-process TLS frame server: reads one frame → replies one frame (same opaque).
    /// When `write_chunk` is `Some(n)` the response is written out in n-byte chunks, to force the client's
    /// `tls_read_exact` partial-frame branch; when the request body is non-empty the response remark echoes back `len:<byte count>`,
    /// proving that `tls_write_all` sent the large body intact.
    struct TlsFrameServer {
        addr: String,
        stop: Arc<AtomicBool>,
    }

    impl Drop for TlsFrameServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
        }
    }

    impl TlsFrameServer {
        fn start(
            identity_pkcs12: &[u8],
            write_chunk: Option<usize>,
        ) -> std::result::Result<Self, String> {
            use std::net::TcpListener;
            let identity = native_tls::Identity::from_pkcs12(identity_pkcs12, TLS_P12_PASSWORD)
                .map_err(|e| format!("identity: {e}"))?;
            let acceptor =
                native_tls::TlsAcceptor::new(identity).map_err(|e| format!("acceptor: {e}"))?;
            let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
            let addr = listener
                .local_addr()
                .map_err(|e| e.to_string())?
                .to_string();
            listener.set_nonblocking(true).map_err(|e| e.to_string())?;
            let stop = Arc::new(AtomicBool::new(false));
            let accept_stop = stop.clone();
            std::thread::spawn(move || {
                while !accept_stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let conn_stop = accept_stop.clone();
                            let acceptor = acceptor.clone();
                            std::thread::spawn(move || {
                                // On macOS/BSD the accepted socket inherits the listener's non-blocking flag,
                                // while the OpenSSL/SecureTransport handshakes both require blocking semantics;
                                // leaving it non-blocking fails at once with WouldBlock ⇒ seen as "peer closed during handshake".
                                let _ = stream.set_nonblocking(false);
                                // The handshake runs on the connection thread: getting stuck on the accept thread
                                // makes the whole server play dead for later connections (a half-open connection clogs the handshake in the kernel queue).
                                let Ok(mut tls) = acceptor.accept(stream) else {
                                    return;
                                };
                                serve_tls_frames(&mut tls, write_chunk, conn_stop);
                            });
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        Err(_) => return,
                    }
                }
            });
            Ok(TlsFrameServer { addr, stop })
        }
    }

    fn serve_tls_frames(
        tls: &mut native_tls::TlsStream<std::net::TcpStream>,
        write_chunk: Option<usize>,
        stop: Arc<AtomicBool>,
    ) {
        let mut head = [0u8; 4];
        while !stop.load(Ordering::Acquire) {
            if !read_block(tls, &mut head) {
                return;
            }
            // totalLength covers "the 4-byte headerLength slot + header + body",
            // so another total bytes must be read — the same accounting as read_frame / tls_read_loop.
            let total = i32::from_be_bytes(head);
            if total <= 0 || total > MAX_FRAME_LENGTH {
                return;
            }
            let mut frame = head.to_vec();
            frame.resize(4 + total as usize, 0);
            if !read_block(tls, &mut frame[4..]) {
                return;
            }
            let Ok(cmd) = RemotingCommand::decode(&frame) else {
                return;
            };
            if cmd.is_oneway_rpc() {
                continue;
            }
            let body_len = cmd.body.as_ref().map_or(0, Vec::len);
            let remark = match cmd.remark.clone() {
                Some(remark) if body_len > 0 => format!("{remark}:len:{body_len}"),
                Some(remark) => format!("pong:{remark}"),
                None => String::new(),
            };
            let mut response = response_for(&cmd, response_code::SUCCESS);
            if !remark.is_empty() {
                response.remark = Some(remark);
            }
            let bytes = response.encode();
            match write_chunk {
                Some(n) => {
                    for chunk in bytes.chunks(n) {
                        if tls.write_all(chunk).is_err() || tls.flush().is_err() {
                            return;
                        }
                        // Keep the partial frame genuinely on the wire: chunks are not coalesced, the client must continue reading on its own
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
                None => {
                    if tls.write_all(&bytes).is_err() || tls.flush().is_err() {
                        return;
                    }
                }
            }
        }
    }

    fn read_block(tls: &mut native_tls::TlsStream<std::net::TcpStream>, buf: &mut [u8]) -> bool {
        let mut filled = 0usize;
        while filled < buf.len() {
            match tls.read(&mut buf[filled..]) {
                Ok(0) => return false,
                Ok(n) => filled += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(_) => return false,
            }
        }
        true
    }

    /// TLS client: `tls_enable` on, `tls_test_mode` explicitly true (a self-signed certificate skips hostname checks),
    /// everything else at defaults.
    fn tls_client() -> RemotingClient {
        RemotingClient::with_config(RemotingClientConfig {
            tls_enable: true,
            tls_test_mode: true,
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn tls_round_trips_and_reassembles_half_packets() {
        let Some(pkcs12) = tls_identity_pkcs12() else {
            eprintln!("skip: openssl unavailable, cannot forge a TLS certificate");
            return;
        };
        // 3 bytes per chunk: even the 4-byte frame header spans two reads, exactly tls_read_exact's continuation path
        let server = TlsFrameServer::start(&pkcs12, Some(3)).expect("TLS server failed to start");
        let client = tls_client();
        let mut cmd = request(request_code::GET_ROUTEINFO_BY_TOPIC, "tls");
        let response = client
            .invoke_sync(&server.addr, &mut cmd, Some(10_000))
            .await
            .expect("TLS round trip failed");
        assert_eq!(response.code, response_code::SUCCESS);
        assert_eq!(response.opaque, cmd.opaque, "the TLS response must carry back the request's opaque");
        assert_eq!(response.remark.as_deref(), Some("pong:tls"));
        client.shutdown();
    }

    #[tokio::test]
    async fn tls_write_all_sends_a_large_body_intact() {
        let Some(pkcs12) = tls_identity_pkcs12() else {
            eprintln!("skip: openssl unavailable, cannot forge a TLS certificate");
            return;
        };
        let server = TlsFrameServer::start(&pkcs12, None).expect("TLS server failed to start");
        let client = tls_client();
        // 1MB body: a single write can never finish on a non-blocking socket; tls_write_all must keep writing itself
        let body = vec![0xA5u8; 1024 * 1024];
        let mut cmd = request(request_code::SEND_MESSAGE, "big");
        cmd.body = Some(body);
        let response = client
            .invoke_sync(&server.addr, &mut cmd, Some(15_000))
            .await
            .expect("TLS round trip with a large body failed");
        assert_eq!(
            response.remark.as_deref(),
            Some("big:len:1048576"),
            "the body length received by the server must match what was sent"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn tls_concurrent_requests_share_one_connection_without_starving() {
        let Some(pkcs12) = tls_identity_pkcs12() else {
            eprintln!("skip: openssl unavailable, cannot forge a TLS certificate");
            return;
        };
        let server = TlsFrameServer::start(&pkcs12, Some(7)).expect("TLS server failed to start");
        let client = tls_client();
        let mut handles = Vec::new();
        for i in 0..8 {
            let client = client.clone();
            let addr = server.addr.clone();
            handles.push(tokio::spawn(async move {
                let mut cmd = request(request_code::SEND_MESSAGE, &format!("t{i}"));
                cmd.body = Some(vec![0u8; 4096]);
                let response = client
                    .invoke_sync(&addr, &mut cmd, Some(10_000))
                    .await
                    .expect("concurrent TLS request timed out/failed: read/write lock contention starved one side");
                assert_eq!(response.opaque, cmd.opaque);
                response.remark.unwrap_or_default()
            }));
        }
        let mut remarks = Vec::new();
        for handle in handles {
            remarks.push(handle.await.expect("concurrent task panicked"));
        }
        remarks.sort();
        let expected: Vec<String> = (0..8).map(|i| format!("t{i}:len:4096")).collect();
        assert_eq!(remarks, expected);
        assert_eq!(
            client.connection_addrs().len(),
            1,
            "the 8 requests should reuse one and the same TLS connection"
        );
        client.shutdown();
    }

    /// One-way TLS server: answers the first request (establishing the connection), then **actively pushes** a
    /// reply-expected request and flips `responded` once the client's response arrives. The server thread lives at most until the read timeout (10s).
    fn start_tls_push_server(
        identity_pkcs12: &[u8],
        responded: Arc<AtomicBool>,
    ) -> std::result::Result<String, String> {
        use std::net::TcpListener;
        const TLS_PUSH_OPAQUE: i32 = 4_242_424;
        let identity = native_tls::Identity::from_pkcs12(identity_pkcs12, TLS_P12_PASSWORD)
            .map_err(|e| format!("identity: {e}"))?;
        let acceptor =
            native_tls::TlsAcceptor::new(identity).map_err(|e| format!("acceptor: {e}"))?;
        let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        let addr = listener
            .local_addr()
            .map_err(|e| e.to_string())?
            .to_string();
        std::thread::spawn(move || {
            let Ok((stream, _)) = listener.accept() else {
                return;
            };
            // On macOS/BSD the accepted socket inherits the listener's non-blocking flag; the handshake requires blocking semantics
            let _ = stream.set_nonblocking(false);
            let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
            let Ok(mut tls) = acceptor.accept(stream) else {
                return;
            };
            // 1) Echo the warm-up request back as one frame; only then is the client considered connected
            let Some(warm) = read_tls_frame(&mut tls) else {
                return;
            };
            if warm.is_oneway_rpc() {
                return;
            }
            let mut ack = response_for(&warm, response_code::SUCCESS);
            let ack_bytes = ack.encode();
            if tls.write_all(&ack_bytes).is_err() || tls.flush().is_err() {
                return;
            }
            // 2) From the broker's perspective, push a transaction check-back (not oneway, a reply is mandatory)
            let mut push = RemotingCommand::create_request_command(
                request_code::CHECK_TRANSACTION_STATE,
                None,
            );
            push.opaque = TLS_PUSH_OPAQUE;
            if tls.write_all(&push.encode()).is_err() || tls.flush().is_err() {
                return;
            }
            // 3) Wait for the client to write the response back
            while let Some(cmd) = read_tls_frame(&mut tls) {
                if cmd.opaque == TLS_PUSH_OPAQUE && cmd.is_response_type() {
                    responded.store(true, Ordering::SeqCst);
                    return;
                }
            }
        });
        Ok(addr)
    }

    /// Reads one whole frame and decodes the command; returns `None` on peer close or oversized frames.
    fn read_tls_frame(
        tls: &mut native_tls::TlsStream<std::net::TcpStream>,
    ) -> Option<RemotingCommand> {
        let mut head = [0u8; 4];
        if !read_block(tls, &mut head) {
            return None;
        }
        let total = i32::from_be_bytes(head);
        if total <= 0 || total > MAX_FRAME_LENGTH {
            return None;
        }
        let mut frame = head.to_vec();
        frame.resize(4 + total as usize, 0);
        if !read_block(tls, &mut frame[4..]) {
            return None;
        }
        RemotingCommand::decode(&frame).ok()
    }

    /// The TLS read thread must carry a tokio runtime context.
    ///
    /// The plaintext path satisfies this naturally (the read loop is a tokio task), but TLS is a plain std thread: requests
    /// pushed by the broker (transaction check-backs, heartbeat replies) are processed synchronously on that thread, and the
    /// processor needs the runtime to spawn background tasks; when it cannot, all that is left is a warn — the request arrived, no work was done. Observed on a real TLS broker:
    /// the broker check-backed once every 30s and all three were silently dropped.
    #[tokio::test]
    async fn tls_inbound_request_is_processed_with_a_runtime_context() {
        let Some(pkcs12) = tls_identity_pkcs12() else {
            eprintln!("skip: openssl unavailable, cannot forge a TLS certificate");
            return;
        };
        let responded = Arc::new(AtomicBool::new(false));
        let saw_runtime = Arc::new(AtomicBool::new(false));
        let addr = start_tls_push_server(&pkcs12, responded.clone()).expect("TLS server failed to start");
        struct Probe {
            saw_runtime: Arc<AtomicBool>,
        }
        impl RequestProcessor for Probe {
            fn process(&self, request: RemotingCommand, _addr: String, sink: ResponseSink) {
                self.saw_runtime.store(
                    tokio::runtime::Handle::try_current().is_ok(),
                    Ordering::SeqCst,
                );
                let mut response = RemotingCommand::create_response(response_code::SUCCESS, None);
                response.set_body(request.body().map(|b| b.to_vec()));
                sink.respond(response);
            }
        }
        let client = tls_client();
        client.register_processor(
            request_code::CHECK_TRANSACTION_STATE,
            Arc::new(Probe {
                saw_runtime: saw_runtime.clone(),
            }),
        );

        // Do one normal round trip first: the server pushes a request only once the connection is up
        let mut warm = request(request_code::HEART_BEAT, "warm");
        client
            .invoke_sync(&addr, &mut warm, Some(10_000))
            .await
            .expect("TLS warm-up request failed");
        for _ in 0..200 {
            if responded.load(Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let got_response = responded.load(Ordering::Acquire);
        client.shutdown();
        assert!(
            saw_runtime.load(Ordering::Acquire),
            "the TLS read thread must carry a runtime context, otherwise the processor cannot spawn background tasks"
        );
        assert!(got_response, "the broker-pushed request must be answered and the response written back to the peer");
    }

    #[tokio::test]
    async fn tls_handshake_against_plaintext_port_maps_to_connect_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            // Plaintext echo: close as-is right after receiving the ClientHello, so the handshake is bound to fail
            while let Ok((mut stream, _)) = listener.accept().await {
                let _ = stream.shutdown().await;
            }
        });
        let client = tls_client();
        let mut cmd = request(request_code::GET_ROUTEINFO_BY_TOPIC, "plain");
        let err = client
            .invoke_sync(&addr, &mut cmd, Some(5000))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Connect { .. }),
            "a TLS handshake on a plaintext port must report Connect, got {err}"
        );
        client.shutdown();
    }

    // ------------------------------------------------------------ transport contract legs
    //
    // These legs watch what the transport says as well as what it does, and the log file is the only
    // place the wording lives. The path is decided once for the process, so a leg cannot point the
    // logger at a file of its own; instead it marks where the file ends right now and reads the tail
    // its own traffic appended, filtered by the peer address. Every leg binds its own port, so a
    // line from another leg running in the same process cannot be mistaken for its own.

    /// The lines the transport logged after one mark.
    struct LogTail {
        start: u64,
    }

    impl LogTail {
        fn mark() -> LogTail {
            let start = std::fs::metadata(crate::common::logging::file_path())
                .map(|meta| meta.len())
                .unwrap_or(0);
            LogTail { start }
        }

        /// Appended lines holding **every** needle.
        fn lines_with(&self, needles: &[&str]) -> Vec<String> {
            read_after(self.start)
                .map(|text| {
                    text.lines()
                        .filter(|line| needles.iter().all(|needle| line.contains(needle)))
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default()
        }

        /// Waits (bounded) until `count` matching lines landed, then hands them over.
        ///
        /// The line is written by the reader task, so it can trail the state change it describes.
        async fn wait_for(&self, needles: &[&str], count: usize, what: &str) -> Vec<String> {
            wait_until(|| self.lines_with(needles).len() >= count, what).await;
            self.lines_with(needles)
        }
    }

    fn read_after(start: u64) -> std::io::Result<String> {
        use std::io::{Seek, SeekFrom};
        let mut file = std::fs::File::open(crate::common::logging::file_path())?;
        file.seek(SeekFrom::Start(start))?;
        let mut text = String::new();
        file.read_to_string(&mut text)?;
        Ok(text)
    }

    /// Accepts forever: drains each connection's frames without ever answering, and counts the
    /// connections the client closed.
    async fn quiet_server() -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let closes = Arc::new(AtomicUsize::new(0));
        let counter = closes.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let counter = counter.clone();
                tokio::spawn(async move {
                    while read_frame(&mut stream).await.is_some() {}
                    counter.fetch_add(1, Ordering::SeqCst);
                });
            }
        });
        (addr, closes)
    }

    #[tokio::test]
    async fn undecodable_frame_is_reported_and_takes_the_connection_down() {
        // A frame whose length prefix is legal and whose header is not a command: the bytes after it
        // no longer line up with a frame boundary, so reading on would attribute garbage to requests.
        let (addr, conns, closes) = broken_frame_server(Broken::NotACommand, 1).await;
        let client = RemotingClient::new();
        let tail = LogTail::mark();
        let mut cmd = request(request_code::SEND_MESSAGE_V2, "garbage");
        let err = client
            .invoke_sync(&addr, &mut cmd, Some(5000))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::SendRequest { .. }),
            "the broken frame answered nothing, so the wait must end as a send failure, got {err}"
        );

        let lines = tail
            .wait_for(
                &[addr.as_str(), "cannot decode frame"],
                1,
                "the undecodable frame is reported by name",
            )
            .await;
        assert_eq!(lines.len(), 1, "one report for one broken stream");
        assert!(lines[0].contains("[ERROR]"), "not a warning: {}", lines[0]);

        wait_until(
            || client.connection_addrs().is_empty(),
            "the broken connection leaves the table",
        )
        .await;
        wait_until(
            || closes.load(Ordering::SeqCst) == 1,
            "the peer sees the connection closed",
        )
        .await;
        assert_eq!(client.in_flight_count(), 0, "the request it never answered is gone too");

        // The next request goes over a fresh connection and is served like any other.
        let mut next = request(request_code::SEND_MESSAGE_V2, "served");
        let response = client
            .invoke_sync(&addr, &mut next, Some(5000))
            .await
            .unwrap();
        assert_eq!(response.code, response_code::SUCCESS);
        assert_eq!(
            conns.load(Ordering::SeqCst),
            2,
            "the next request rode a new connection, not the broken stream"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn illegal_frame_length_is_reported_once_at_the_level_that_caught_it() {
        // The length prefix itself is out of range: this is the same event one level earlier, and the
        // bytes behind it were never parsed as a command, so there is no decode failure to report.
        let (addr, _conns, closes) = broken_frame_server(Broken::IllegalLength, 1).await;
        let client = RemotingClient::new();
        let tail = LogTail::mark();
        let mut cmd = request(request_code::SEND_MESSAGE_V2, "huge");
        let err = client
            .invoke_sync(&addr, &mut cmd, Some(5000))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::SendRequest { .. }), "got {err}");

        let lines = tail
            .wait_for(
                &[addr.as_str(), "illegal frame length"],
                1,
                "the out-of-range length is reported",
            )
            .await;
        assert_eq!(lines.len(), 1, "the event is reported once");
        assert!(lines[0].contains("[ERROR]"), "{}", lines[0]);
        assert!(
            lines[0].contains("2147483647"),
            "the length that broke the stream is in the line: {}",
            lines[0]
        );
        assert!(
            tail.lines_with(&[addr.as_str(), "cannot decode frame"])
                .is_empty(),
            "the same event must not also be reported as a decode failure"
        );

        wait_until(
            || client.connection_addrs().is_empty(),
            "the connection with the impossible length is gone",
        )
        .await;
        wait_until(
            || closes.load(Ordering::SeqCst) == 1,
            "the peer sees the connection closed",
        )
        .await;
        client.shutdown();
    }

    /// The opaque the stray response carries: real opaques count up from zero, so this one answers nothing.
    const STRAY_OPAQUE: i32 = -987_654;

    #[tokio::test]
    async fn unmatched_response_is_named_and_the_connection_survives() {
        let (addr, conns, closes) = stray_response_server().await;
        let client = RemotingClient::new();
        let tail = LogTail::mark();
        let mut cmd = request(request_code::SEND_MESSAGE, "first");
        let response = client.invoke_sync(&addr, &mut cmd, Some(5000)).await.unwrap();

        let lines = tail
            .wait_for(
                &[addr.as_str(), "does not match any in-flight request"],
                1,
                "the stray response is warned about",
            )
            .await;
        assert_eq!(lines.len(), 1, "one line for one stray frame");
        assert!(lines[0].contains("[WARN]"), "{}", lines[0]);
        assert!(
            lines[0].contains(&format!("opaque={STRAY_OPAQUE}")),
            "the warning names the opaque it could not place: {}",
            lines[0]
        );
        assert_eq!(
            response.code,
            response_code::SUCCESS,
            "the request the stray did not answer still got its own answer"
        );

        // A frame that answers nothing is still a frame: the stream lines up, so the connection stays.
        assert_eq!(client.connection_addrs(), vec![addr.clone()]);
        assert_eq!(closes.load(Ordering::SeqCst), 0, "reporting a stray is not a connection event");
        let mut second = request(request_code::SEND_MESSAGE, "second");
        let second_response = client
            .invoke_sync(&addr, &mut second, Some(5000))
            .await
            .unwrap();
        assert_eq!(second_response.code, response_code::SUCCESS);
        assert_eq!(
            conns.load(Ordering::SeqCst),
            1,
            "the second request rode the connection the stray arrived on"
        );
        client.shutdown();
    }

    #[tokio::test]
    async fn writable_answers_for_the_send_buffer_not_for_having_a_connection() {
        let (addr, conns) = stalled_echo_server(Duration::from_millis(1500)).await;
        let client = RemotingClient::new();
        assert!(
            client.is_channel_writable(&addr),
            "an address with no live connection is writable: the next send simply connects"
        );
        assert!(client.connection_addrs().is_empty());

        // 12 MB body: more than both socket buffers even at their autosetup maxima, so a peer that
        // does not read leaves the write nowhere to go.
        let mut cmd = request(request_code::SEND_MESSAGE, "stalled");
        cmd.body = Some(vec![0xA5u8; 12 * 1024 * 1024]);
        let call = {
            let client = client.clone();
            let addr = addr.clone();
            tokio::spawn(async move { client.invoke_sync(&addr, &mut cmd, Some(30_000)).await })
        };
        wait_until(
            || !client.is_channel_writable(&addr),
            "a write parked on a full send buffer reports the socket as unwritable",
        )
        .await;
        assert_eq!(
            client.connection_addrs(),
            vec![addr.clone()],
            "the connection is up; only its send buffer has no room"
        );
        assert!(
            !call.is_finished(),
            "the request waits for room instead of failing fast"
        );

        let response = call
            .await
            .unwrap()
            .expect("the request still completes once the peer reads");
        assert_eq!(response.code, response_code::SUCCESS);
        wait_until(
            || client.is_channel_writable(&addr),
            "once the frame is on the wire the socket takes bytes again",
        )
        .await;
        assert_eq!(conns.load(Ordering::SeqCst), 1, "one connection carried it all");
        client.shutdown();
    }

    #[tokio::test]
    async fn idle_timeout_default_survives_a_nonsense_setting() {
        let client = RemotingClient::new();
        assert_eq!(
            client.channel_idle_timeout_millis(),
            DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS
        );
        assert_eq!(DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS, 120 * 1000);

        // A limit that is not positive restores the default instead of switching the sweep off.
        client.set_channel_idle_timeout_millis(0);
        assert_eq!(client.channel_idle_timeout_millis(), DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS);
        client.set_channel_idle_timeout_millis(-1);
        assert_eq!(client.channel_idle_timeout_millis(), DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS);
        let configured = RemotingClient::with_config(RemotingClientConfig {
            channel_idle_timeout_millis: -5000,
            ..RemotingClientConfig::default()
        });
        assert_eq!(
            configured.channel_idle_timeout_millis(),
            DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS,
            "the same normalization at construction time"
        );
    }

    #[tokio::test]
    async fn the_transport_reaps_a_quiet_connection_and_leaves_a_busy_one() {
        let (quiet_addr, quiet_closes) = quiet_server().await;
        let (busy_addr, busy_closes) = quiet_server().await;
        let client = RemotingClient::with_config(RemotingClientConfig {
            channel_idle_timeout_millis: 300,
            ..RemotingClientConfig::default()
        });
        let tail = LogTail::mark();
        for addr in [&quiet_addr, &busy_addr] {
            let mut beat = request(request_code::HEART_BEAT, "beat");
            beat.mark_oneway_rpc();
            client.invoke_oneway(addr, &mut beat).await.unwrap();
        }
        wait_until(
            || client.connection_addrs().len() == 2,
            "both connections are registered",
        )
        .await;

        // One write every 100 ms against a 300 ms limit: this connection is never the idle one.
        let stop = Arc::new(AtomicBool::new(false));
        let beating = {
            let client = client.clone();
            let addr = busy_addr.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    let mut beat = request(request_code::HEART_BEAT, "beat");
                    beat.mark_oneway_rpc();
                    let _ = client.invoke_oneway(&addr, &mut beat).await;
                }
            })
        };

        wait_until(
            || client.connection_addrs() == vec![busy_addr.clone()],
            "only the quiet connection is reaped",
        )
        .await;
        assert_eq!(quiet_closes.load(Ordering::SeqCst), 1, "the reap closed the socket");

        // Bytes went out a moment ago, so no connection here is past the limit -- however short it is.
        let mut fresh = request(request_code::HEART_BEAT, "fresh");
        fresh.mark_oneway_rpc();
        client.invoke_oneway(&busy_addr, &mut fresh).await.unwrap();
        assert_eq!(
            client.sweep_idle_connections(),
            0,
            "a connection that just carried traffic is inside the limit"
        );

        stop.store(true, Ordering::Release);
        beating.await.unwrap();
        wait_until(
            || client.connection_addrs().is_empty(),
            "the connection the beats kept alive is reaped once they stop",
        )
        .await;
        assert_eq!(busy_closes.load(Ordering::SeqCst), 1);

        for addr in [&quiet_addr, &busy_addr] {
            let lines = tail
                .wait_for(&[addr.as_str(), "idle for", "closing it"], 1, "the reap is logged")
                .await;
            assert_eq!(lines.len(), 1, "one warning per reaped connection for {addr}");
            assert!(lines[0].contains("[WARN]"), "{}", lines[0]);
            assert!(
                lines[0].contains("limit 300 ms"),
                "the warning states the limit it broke: {}",
                lines[0]
            );
        }
    }

    #[tokio::test]
    async fn every_expired_request_gets_its_own_warning() {
        let (addr, _frames) = silent_server().await;
        let client = RemotingClient::new();
        // This leg is about the in-flight table; whether the socket goes with a timed-out wait is the
        // rule under T6, so switch it off and let both waits run on one connection.
        client.set_close_socket_on_sync_timeout(false);
        let tail = LogTail::mark();
        let mut commands = vec![
            request(request_code::PULL_MESSAGE, "slow-a"),
            request(request_code::PULL_MESSAGE, "slow-b"),
        ];
        let opaques: Vec<i32> = commands.iter().map(|cmd| cmd.opaque).collect();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        for cmd in commands.drain(..) {
            let tx = tx.clone();
            client.invoke_async(&addr, cmd, Box::new(move |result| {
                let _ = tx.send(result);
            }), Some(200));
        }
        for _ in 0..2 {
            let result = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("a callback never fired")
                .expect("the callback channel closed");
            let err = result.unwrap_err();
            assert!(
                matches!(err, Error::Timeout { .. }),
                "the caller gets the timeout it asked for, got {err}"
            );
        }
        assert_eq!(
            client.in_flight_count(),
            0,
            "both expired entries were reclaimed, none left to answer a late frame"
        );
        assert_eq!(client.connection_addrs(), vec![addr.clone()]);

        let lines = tail
            .wait_for(&[addr.as_str(), "failed with a timeout"], 2, "one warning per expired request")
            .await;
        assert_eq!(lines.len(), 2, "two requests are two separate reports");
        assert!(lines.iter().all(|line| line.contains("[WARN]")));
        for opaque in &opaques {
            assert_eq!(
                lines
                    .iter()
                    .filter(|line| line.contains(&format!("opaque={opaque}")))
                    .count(),
                1,
                "each line names the opaque it gave up on"
            );
        }
        client.shutdown();
    }

    /// One synchronous call whose wait runs out, as the transport answered it.
    struct SyncTimeoutRun {
        timed_out: bool,
        connection_kept: bool,
        peer_closes: usize,
        close_warns: usize,
        expiry_warns: usize,
    }

    /// Brings one connection up, then waits on a request the peer never answers.
    ///
    /// The warm-up matters: "kept" and "closed" have to be statements about a socket that exists.
    async fn run_sync_timeout(close_on_timeout: bool, timeout: i64) -> SyncTimeoutRun {
        let (addr, closes) = quiet_server().await;
        let client = RemotingClient::new();
        client.set_close_socket_on_sync_timeout(close_on_timeout);
        let tail = LogTail::mark();
        let mut warm = request(request_code::HEART_BEAT, "warm");
        warm.mark_oneway_rpc();
        client.invoke_oneway(&addr, &mut warm).await.unwrap();
        let registered = vec![addr.clone()];
        wait_until(
            || client.connection_addrs() == registered,
            "the warm-up connection is registered",
        )
        .await;

        let mut cmd = request(request_code::PULL_MESSAGE, "never-answered");
        let result = client.invoke_sync(&addr, &mut cmd, Some(timeout)).await;
        let timed_out = matches!(result, Err(Error::Timeout { .. }));
        // Give a close the chance to reach the peer before the counters are read.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let run = SyncTimeoutRun {
            timed_out,
            connection_kept: !client.connection_addrs().is_empty(),
            peer_closes: closes.load(Ordering::SeqCst),
            close_warns: tail.lines_with(&[addr.as_str(), "closing the connection"]).len(),
            expiry_warns: tail.lines_with(&[addr.as_str(), "failed with a timeout"]).len(),
        };
        client.shutdown();
        run
    }

    #[tokio::test]
    async fn sync_timeout_closes_the_connection_by_default() {
        let on = run_sync_timeout(true, 400).await;
        assert!(on.timed_out, "the caller still gets the timeout it asked for");
        assert!(!on.connection_kept, "by default the connection the wait ran out on is closed");
        assert_eq!(on.peer_closes, 1, "the peer sees the close");
        assert_eq!(on.close_warns, 1);
        assert_eq!(on.expiry_warns, 1);

        let off = run_sync_timeout(false, 400).await;
        assert!(off.timed_out, "the timeout is still reported to the caller");
        assert!(off.connection_kept, "with the knob off the socket is kept -- the timeout is caller-side");
        assert_eq!(off.peer_closes, 0);
        assert_eq!(off.close_warns, 0);
        assert_eq!(off.expiry_warns, 1);
    }

    /// The two floors of the close decision, on the only budget this port can hand the wait.
    ///
    /// This transport does not subtract the connect cost, so the time still available when the wait
    /// began is the whole timeout. The floors then leave exactly one shape that keeps the socket: a
    /// budget of zero, which no wait can ever be answered on.
    #[tokio::test]
    async fn sync_timeout_close_rule_has_one_floor_shape() {
        let zero = run_sync_timeout(true, 0).await;
        assert!(zero.timed_out, "a zero budget still reports the timeout to the caller");
        assert!(zero.connection_kept, "a zero budget is at both floors, so the connection is kept");
        assert_eq!(zero.close_warns, 0);

        let just_above = run_sync_timeout(true, 101).await;
        assert!(!just_above.connection_kept, "one millisecond above the floor is a close, not a keep");
        assert_eq!(just_above.close_warns, 1);
    }

    /// How a peer breaks the frame stream on the connections marked broken.
    #[derive(Clone, Copy)]
    enum Broken {
        /// A legal length prefix in front of a header that is not a command.
        NotACommand,
        /// A length prefix outside the legal range: the frame boundary itself is gone.
        IllegalLength,
    }

    impl Broken {
        fn bytes(self) -> Vec<u8> {
            match self {
                // totalLength 8, headerLength 4 in JSON mode: the prefix promises a frame, and the
                // four header bytes behind it are not a header.
                Broken::NotACommand => vec![0, 0, 0, 8, 0, 0, 0, 4, 0xFF, 0xFF, 0xFF, 0xFF],
                Broken::IllegalLength => vec![0x7F, 0xFF, 0xFF, 0xFF],
            }
        }
    }

    /// The first `broken_conns` connections get a frame that is not a command; later ones echo.
    /// Counts the connections seen and the ones the client closed.
    async fn broken_frame_server(
        broken: Broken,
        broken_conns: usize,
    ) -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let conns = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let counted = conns.clone();
        let closed = closes.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let idx = counted.fetch_add(1, Ordering::SeqCst);
                let broken_conn = idx < broken_conns;
                let closed = closed.clone();
                tokio::spawn(async move {
                    if broken_conn {
                        let bytes = broken.bytes();
                        let _ = stream.write_all(&bytes).await;
                        let _ = stream.flush().await;
                    }
                    while let Some(frame) = read_frame(&mut stream).await {
                        if broken_conn {
                            continue;
                        }
                        let cmd = RemotingCommand::decode(&frame).unwrap();
                        let mut response = response_for(&cmd, response_code::SUCCESS);
                        if let Some(remark) = cmd.remark.clone() {
                            response.remark = Some(format!("pong:{remark}"));
                        }
                        write_command(&mut stream, &mut response).await;
                    }
                    closed.fetch_add(1, Ordering::SeqCst);
                });
            }
        });
        (addr, conns, closes)
    }

    /// Echoes every request, but puts one extra response with an unknown opaque on the stream first.
    async fn stray_response_server() -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let conns = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let counted = conns.clone();
        let closed = closes.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                counted.fetch_add(1, Ordering::SeqCst);
                let closed = closed.clone();
                tokio::spawn(async move {
                    let mut stray_pending = true;
                    while let Some(frame) = read_frame(&mut stream).await {
                        let cmd = RemotingCommand::decode(&frame).unwrap();
                        if stray_pending {
                            stray_pending = false;
                            let mut stray = response_for(&cmd, response_code::SUCCESS);
                            stray.opaque = STRAY_OPAQUE;
                            write_command(&mut stream, &mut stray).await;
                        }
                        let mut response = response_for(&cmd, response_code::SUCCESS);
                        write_command(&mut stream, &mut response).await;
                    }
                    closed.fetch_add(1, Ordering::SeqCst);
                });
            }
        });
        (addr, conns, closes)
    }

    /// Reads nothing for `stall` after accepting, then answers like the echo broker.
    async fn stalled_echo_server(stall: Duration) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let conns = Arc::new(AtomicUsize::new(0));
        let counted = conns.clone();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                counted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    tokio::time::sleep(stall).await;
                    while let Some(frame) = read_frame(&mut stream).await {
                        let cmd = RemotingCommand::decode(&frame).unwrap();
                        let mut response = response_for(&cmd, response_code::SUCCESS);
                        write_command(&mut stream, &mut response).await;
                    }
                });
            }
        });
        (addr, conns)
    }
}
