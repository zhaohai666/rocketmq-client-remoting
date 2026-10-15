
use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{Error, Result};
use crate::remoting::client::split_host_port;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocksVersion {
    V4,
    V5,
}

/// `addr` supports scheme prefixes to pick the version: `socks5://host:port` (default) and
/// `socks4://host:port`; if either `username` or `password` is non-empty we assume auth is required
/// (SOCKS5 username/password subnegotiation; in SOCKS4 the username is the userid, there is no password field).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SocksProxyConfig {
    /// Proxy address `host:port` (may carry a `socks5://` / `socks4://` prefix).
    pub addr: String,
    /// Auth username (SOCKS5 subnegotiation / SOCKS4 userid). `None`/empty = no auth.
    pub username: Option<String>,
    /// Auth password (used only by the SOCKS5 subnegotiation; SOCKS4 has no password field).
    pub password: Option<String>,
}

impl SocksProxyConfig {
    pub fn new(addr: impl Into<String>) -> SocksProxyConfig {
        SocksProxyConfig {
            addr: addr.into(),
            username: None,
            password: None,
        }
    }

    pub fn with_auth(
        addr: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> SocksProxyConfig {
        SocksProxyConfig {
            addr: addr.into(),
            username: Some(username.into()),
            password: Some(password.into()),
        }
    }

    /// Parse the protocol version and the bare address out of `addr`; defaults to SOCKS5.
    fn version_and_addr(&self) -> (SocksVersion, &str) {
        let addr = self.addr.trim();
        if let Some(rest) = addr
            .strip_prefix("socks4://")
            .or_else(|| addr.strip_prefix("socks4:"))
        {
            (SocksVersion::V4, rest)
        } else if let Some(rest) = addr
            .strip_prefix("socks5://")
            .or_else(|| addr.strip_prefix("socks5:"))
        {
            (SocksVersion::V5, rest)
        } else {
            (SocksVersion::V5, addr)
        }
    }

    fn credentials(&self) -> Option<(&str, &str)> {
        let username = self.username.as_deref().filter(|u| !u.is_empty())?;
        Some((username, self.password.as_deref().unwrap_or("")))
    }
}

/// SOCKS5 subnegotiation method numbers.
const METHOD_NO_AUTH: u8 = 0x00;
const METHOD_USERNAME_PASSWORD: u8 = 0x02;
const METHOD_NO_ACCEPTABLE: u8 = 0xFF;

/// SOCKS5 username/password subnegotiation version number (RFC 1929, fixed 0x01).
const SUBNEGOTIATION_VERSION: u8 = 0x01;

/// Connect to `target_host:target_port` via the proxy: TCP connect to the proxy + SOCKS handshake (CONNECT).
pub(crate) async fn connect_via_proxy(
    proxy: &SocksProxyConfig,
    target_host: &str,
    target_port: u16,
    timeout: Duration,
) -> Result<TcpStream> {
    let deadline = Instant::now() + timeout;
    let (version, proxy_addr) = proxy.version_and_addr();
    let (proxy_host, proxy_port) = split_host_port(proxy_addr).map_err(|_| Error::Connect {
        addr: format!("bad socks proxy addr {:?}", proxy.addr),
    })?;
    let socket_addr = super::client::resolve(&proxy_host, proxy_port).await?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    let mut stream = tokio::time::timeout(remaining, TcpStream::connect(socket_addr))
        .await
        .map_err(|_| Error::Connect {
            addr: format!("socks proxy {proxy_addr}"),
        })?
        .map_err(|_| Error::Connect {
            addr: format!("socks proxy {proxy_addr}"),
        })?;
    let _ = stream.set_nodelay(true);

    let remaining = deadline.saturating_duration_since(Instant::now());
    tokio::time::timeout(remaining, async {
        match version {
            SocksVersion::V5 => {
                socks5_handshake(&mut stream, proxy.credentials(), target_host, target_port).await
            }
            SocksVersion::V4 => {
                socks4_handshake(&mut stream, proxy.credentials(), target_host, target_port).await
            }
        }
    })
    .await
    .map_err(|_| Error::Connect {
        addr: format!("socks handshake with {proxy_addr} for {target_host}:{target_port} timeout"),
    })??;
    Ok(stream)
}

fn connect_error(host: &str, port: u16, reason: impl std::fmt::Display) -> Error {
    Error::Connect {
        addr: format!("{host}:{port} via socks proxy ({reason})"),
    }
}

async fn socks5_handshake(
    stream: &mut TcpStream,
    credentials: Option<(&str, &str)>,
    host: &str,
    port: u16,
) -> Result<()> {
    // Method selection: with credentials advertise both "no auth + username/password" (the server picks one),
    // without credentials advertise only "no auth".
    let methods: &[u8] = if credentials.is_some() {
        &[METHOD_NO_AUTH, METHOD_USERNAME_PASSWORD]
    } else {
        &[METHOD_NO_AUTH]
    };
    let mut request = Vec::with_capacity(2 + methods.len());
    request.push(0x05);
    request.push(methods.len() as u8);
    request.extend_from_slice(methods);
    stream
        .write_all(&request)
        .await
        .map_err(|e| connect_error(host, port, format!("send method selection: {e}")))?;

    let mut chosen = [0u8; 2];
    stream
        .read_exact(&mut chosen)
        .await
        .map_err(|e| connect_error(host, port, format!("read method selection: {e}")))?;
    if chosen[0] != 0x05 {
        return Err(connect_error(
            host,
            port,
            format!("bad socks version in reply: {}", chosen[0]),
        ));
    }
    match chosen[1] {
        METHOD_NO_AUTH => {}
        METHOD_USERNAME_PASSWORD => {
            let Some((username, password)) = credentials else {
                return Err(connect_error(
                    host,
                    port,
                    "server requires username/password auth but no credentials configured",
                ));
            };
            username_password_subnegotiation(stream, username, password, host, port).await?;
        }
        METHOD_NO_ACCEPTABLE => {
            return Err(connect_error(
                host,
                port,
                "no acceptable auth method (server replied 0xFF)",
            ));
        }
        other => {
            return Err(connect_error(
                host,
                port,
                format!("server chose unsupported method {other:#04x}"),
            ));
        }
    }

    let mut request = vec![0x05u8, 0x01, 0x00];
    if let Ok(v4) = host.parse::<Ipv4Addr>() {
        request.push(0x01);
        request.extend_from_slice(&v4.octets());
    } else if let Ok(v6) = host.parse::<Ipv6Addr>() {
        request.push(0x04);
        request.extend_from_slice(&v6.octets());
    } else {
        let bytes = host.as_bytes();
        if bytes.is_empty() || bytes.len() > 255 {
            return Err(connect_error(
                host,
                port,
                "target host is not a valid domain",
            ));
        }
        request.push(0x03);
        request.push(bytes.len() as u8);
        request.extend_from_slice(bytes);
    }
    request.extend_from_slice(&port.to_be_bytes());
    stream
        .write_all(&request)
        .await
        .map_err(|e| connect_error(host, port, format!("send connect request: {e}")))?;

    let mut head = [0u8; 4];
    stream
        .read_exact(&mut head)
        .await
        .map_err(|e| connect_error(host, port, format!("read connect reply: {e}")))?;
    if head[0] != 0x05 {
        return Err(connect_error(
            host,
            port,
            format!("bad socks version in connect reply: {}", head[0]),
        ));
    }
    if head[1] != 0x00 {
        return Err(connect_error(host, port, socks5_reply_reason(head[1])));
    }
    // BND.ADDR + BND.PORT just get read off; their content means nothing to the client.
    let trail_len = match head[3] {
        0x01 => 4 + 2,
        0x04 => 16 + 2,
        0x03 => 1 + 2, // 1-byte length + the domain read separately afterwards
        other => {
            return Err(connect_error(
                host,
                port,
                format!("bad address type in connect reply: {other:#04x}"),
            ));
        }
    };
    let mut trail = vec![0u8; trail_len];
    stream
        .read_exact(&mut trail)
        .await
        .map_err(|e| connect_error(host, port, format!("read connect reply: {e}")))?;
    if head[3] == 0x03 {
        let domain_len = trail[0] as usize;
        let mut rest = vec![0u8; domain_len];
        stream
            .read_exact(&mut rest)
            .await
            .map_err(|e| connect_error(host, port, format!("read connect reply: {e}")))?;
    }
    Ok(())
}

async fn username_password_subnegotiation(
    stream: &mut TcpStream,
    username: &str,
    password: &str,
    host: &str,
    port: u16,
) -> Result<()> {
    let (u, p) = (username.as_bytes(), password.as_bytes());
    // RFC 1929: username and password are each limited to 1..255 bytes.
    if u.is_empty() || u.len() > 255 || p.len() > 255 {
        return Err(connect_error(
            host,
            port,
            "invalid username/password length",
        ));
    }
    let mut request = Vec::with_capacity(3 + u.len() + p.len());
    request.push(SUBNEGOTIATION_VERSION);
    request.push(u.len() as u8);
    request.extend_from_slice(u);
    request.push(p.len() as u8);
    request.extend_from_slice(p);
    stream
        .write_all(&request)
        .await
        .map_err(|e| connect_error(host, port, format!("send auth: {e}")))?;
    let mut reply = [0u8; 2];
    stream
        .read_exact(&mut reply)
        .await
        .map_err(|e| connect_error(host, port, format!("read auth reply: {e}")))?;
    if reply[0] != SUBNEGOTIATION_VERSION {
        return Err(connect_error(
            host,
            port,
            format!("bad auth subnegotiation version: {}", reply[0]),
        ));
    }
    if reply[1] != 0x00 {
        return Err(connect_error(
            host,
            port,
            format!("username/password auth rejected (status {})", reply[1]),
        ));
    }
    Ok(())
}

fn socks5_reply_reason(rep: u8) -> String {
    match rep {
        0x01 => "general socks server failure".into(),
        0x02 => "connection not allowed by ruleset".into(),
        0x03 => "network unreachable".into(),
        0x04 => "host unreachable".into(),
        0x05 => "connection refused".into(),
        0x06 => "TTL expired".into(),
        0x07 => "command not supported".into(),
        0x08 => "address type not supported".into(),
        other => format!("unknown reply code {other:#04x}"),
    }
}

/// SOCKS4 / 4a CONNECT. Only the username in `credentials` is meaningful (userid),
/// SOCKS4 has no password field. When the target is a domain, use the 4a extension (the domain follows DESTPORT).
async fn socks4_handshake(
    stream: &mut TcpStream,
    credentials: Option<(&str, &str)>,
    host: &str,
    port: u16,
) -> Result<()> {
    let userid = credentials.map(|(u, _)| u).unwrap_or("");
    let mut request: Vec<u8> = Vec::with_capacity(9 + userid.len() + host.len());
    request.push(0x04);
    request.push(0x01);
    request.extend_from_slice(&port.to_be_bytes());
    let domain = match host.parse::<Ipv4Addr>() {
        Ok(v4) => {
            request.extend_from_slice(&v4.octets());
            None
        }
        Err(_) => {
            // 4a: the IP is written as 0.0.0.x (x != 0), the real domain follows the userid.
            request.extend_from_slice(&[0, 0, 0, 1]);
            Some(host)
        }
    };
    request.extend_from_slice(userid.as_bytes());
    request.push(0x00);
    if let Some(domain) = domain {
        request.extend_from_slice(domain.as_bytes());
        request.push(0x00);
    }
    stream
        .write_all(&request)
        .await
        .map_err(|e| connect_error(host, port, format!("send connect request: {e}")))?;

    let mut reply = [0u8; 8];
    stream
        .read_exact(&mut reply)
        .await
        .map_err(|e| connect_error(host, port, format!("read connect reply: {e}")))?;
    if reply[1] != 0x5A {
        return Err(connect_error(
            host,
            port,
            format!("socks4 connect rejected (code {:#04x})", reply[1]),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::io::copy_bidirectional;
    use tokio::net::{TcpListener, TcpStream as TokioTcpStream};

    use super::*;

    /// Event log: the fake proxy records the negotiation here so assertions can inspect it.
    type Log = Arc<Mutex<Vec<String>>>;

    fn log_of(log: &Log) -> Vec<String> {
        log.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn log_has(log: &Log, needle: &str) -> bool {
        log_of(log).iter().any(|e| e.contains(needle))
    }

    /// Dial a free port to serve as the fake proxy's "target echo service".
    async fn echo_target() -> (String, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 512];
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if stream.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });
        (addr, task)
    }

    async fn read_method_selection(stream: &mut TokioTcpStream) -> Vec<u8> {
        let mut head = [0u8; 2];
        stream.read_exact(&mut head).await.unwrap();
        assert_eq!(head[0], 0x05, "the client's method selection must start with VER=0x05");
        let mut methods = vec![0u8; head[1] as usize];
        stream.read_exact(&mut methods).await.unwrap();
        methods
    }

    async fn read_connect_request(stream: &mut TokioTcpStream) -> (String, u16) {
        let mut head = [0u8; 4];
        stream.read_exact(&mut head).await.unwrap();
        assert_eq!(head[0], 0x05);
        assert_eq!(head[1], 0x01, "only CONNECT is supported");
        let target = match head[3] {
            0x01 => {
                let mut ip = [0u8; 4];
                stream.read_exact(&mut ip).await.unwrap();
                Ipv4Addr::from(ip).to_string()
            }
            0x04 => {
                let mut ip = [0u8; 16];
                stream.read_exact(&mut ip).await.unwrap();
                Ipv6Addr::from(ip).to_string()
            }
            0x03 => {
                let mut len = [0u8; 1];
                stream.read_exact(&mut len).await.unwrap();
                let mut domain = vec![0u8; len[0] as usize];
                stream.read_exact(&mut domain).await.unwrap();
                String::from_utf8(domain).unwrap()
            }
            other => panic!("bad atyp {other}"),
        };
        let mut port = [0u8; 2];
        stream.read_exact(&mut port).await.unwrap();
        (target, u16::from_be_bytes(port))
    }

    async fn reply_connect_success(stream: &mut TokioTcpStream) {
        stream
            .write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();
    }

    /// Fake SOCKS5 proxy.
    ///
    /// `auth` = `Some((user, pass, accept))`: offer only the username/password method, pass traffic through
    /// only when the credentials match and `accept` is true; `None`: offer only the no-auth method.
    /// After a successful handshake, splice the connection bidirectionally onto `target`.
    async fn fake_socks5(auth: Option<(String, String, bool)>, target: &str, log: Log) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let target = target.to_string();
        let (host, port) = target.split_once(':').unwrap();
        let (host, port) = (host.to_string(), port.to_string());
        let target_port: u16 = port.parse().unwrap();
        tokio::spawn(async move {
            let mut stream = listener.accept().await.unwrap().0;
            let methods = read_method_selection(&mut stream).await;
            log.lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(format!("methods:{methods:?}"));
            match auth {
                None => {
                    stream.write_all(&[0x05, METHOD_NO_AUTH]).await.unwrap();
                }
                Some((user, pass, accept)) => {
                    stream
                        .write_all(&[0x05, METHOD_USERNAME_PASSWORD])
                        .await
                        .unwrap();
                    let mut ver = [0u8; 1];
                    stream.read_exact(&mut ver).await.unwrap();
                    assert_eq!(ver[0], SUBNEGOTIATION_VERSION);
                    let mut ulen = [0u8; 1];
                    stream.read_exact(&mut ulen).await.unwrap();
                    let mut got_user = vec![0u8; ulen[0] as usize];
                    stream.read_exact(&mut got_user).await.unwrap();
                    let mut plen = [0u8; 1];
                    stream.read_exact(&mut plen).await.unwrap();
                    let mut got_pass = vec![0u8; plen[0] as usize];
                    stream.read_exact(&mut got_pass).await.unwrap();
                    log.lock().unwrap_or_else(|e| e.into_inner()).push(format!(
                        "auth:{}:{}",
                        String::from_utf8_lossy(&got_user),
                        String::from_utf8_lossy(&got_pass)
                    ));
                    let ok = accept && got_user == user.as_bytes() && got_pass == pass.as_bytes();
                    stream
                        .write_all(&[SUBNEGOTIATION_VERSION, !ok as u8])
                        .await
                        .unwrap();
                    if !ok {
                        return;
                    }
                }
            }
            let (t_host, t_port) = read_connect_request(&mut stream).await;
            log.lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(format!("connect:{t_host}:{t_port}"));
            if t_host == host && t_port == target_port {
                reply_connect_success(&mut stream).await;
                let mut upstream = TokioTcpStream::connect((host, target_port)).await.unwrap();
                let _ = copy_bidirectional(&mut stream, &mut upstream).await;
            } else {
                stream
                    .write_all(&[0x05, 0x04, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                    .await
                    .unwrap();
            }
        });
        addr
    }

    #[tokio::test]
    async fn socks5_no_auth_round_trip_through_proxy() {
        let (target, _t) = echo_target().await;
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let proxy_addr = fake_socks5(None, &target, log.clone()).await;

        let mut stream = connect_via_proxy(
            &SocksProxyConfig::new(&proxy_addr),
            target_host_of(&target),
            target_port_of(&target),
            Duration::from_secs(30),
        )
        .await
        .expect("no-auth SOCKS5 connection must succeed");
        stream.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping", "data through the proxy must reach the target echo service unchanged");
        assert!(log_has(&log, "methods:[0]"), "without credentials only the no-auth method is advertised");
        assert!(
            log_has(&log, &format!("connect:{target}")),
            "the CONNECT target received by the proxy must be the original address"
        );
    }

    #[tokio::test]
    async fn socks5_username_password_subnegotiation_round_trip() {
        let (target, _t) = echo_target().await;
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let proxy_addr = fake_socks5(
            Some(("alice".into(), "secret".into(), true)),
            &target,
            log.clone(),
        )
        .await;

        let mut stream = connect_via_proxy(
            &SocksProxyConfig::with_auth(&proxy_addr, "alice", "secret"),
            target_host_of(&target),
            target_port_of(&target),
            Duration::from_secs(30),
        )
        .await
        .expect("SOCKS5 connection with auth must succeed");
        stream.write_all(b"pong").await.unwrap();
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong");
        assert!(
            log_has(&log, "methods:[0, 2]"),
            "with credentials both the no-auth and username/password methods must be advertised"
        );
        assert!(log_has(&log, "auth:alice:secret"), "credentials must go on the wire verbatim");
    }

    #[tokio::test]
    async fn socks5_rejects_wrong_password() {
        let (target, _t) = echo_target().await;
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let proxy_addr = fake_socks5(
            Some(("alice".into(), "secret".into(), true)),
            &target,
            log.clone(),
        )
        .await;

        let err = connect_via_proxy(
            &SocksProxyConfig::with_auth(&proxy_addr, "alice", "wrong"),
            target_host_of(&target),
            target_port_of(&target),
            Duration::from_secs(35),
        )
        .await
        .expect_err("a wrong password must be rejected");
        assert!(
            err.to_string().contains("rejected"),
            "the message must state that auth was rejected: {err}"
        );
        assert!(log_has(&log, "auth:alice:wrong"));
        assert!(!log_has(&log, "connect:"), "auth failure must not proceed to CONNECT");
    }

    #[tokio::test]
    async fn socks5_rejects_when_no_acceptable_method() {
        // The server only accepts username/password while the client has no credentials configured => the server picks 0x02 and the client cannot supply credentials.
        let (target, _t) = echo_target().await;
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let proxy_addr = fake_socks5(
            Some(("alice".into(), "secret".into(), true)),
            &target,
            log.clone(),
        )
        .await;

        let err = connect_via_proxy(
            &SocksProxyConfig::new(&proxy_addr),
            target_host_of(&target),
            target_port_of(&target),
            Duration::from_secs(35),
        )
        .await
        .expect_err("no credentials against mandatory auth must fail");
        assert!(
            err.to_string()
                .contains("requires username/password auth but no credentials configured"),
            "got {err}"
        );
        assert!(!log_has(&log, "connect:"));
    }

    /// The server replies 0xFF directly (not one acceptable method) -- a hard rejection per RFC 1929.
    #[tokio::test]
    async fn socks5_no_acceptable_method_reply_is_reported() {
        let (target, _t) = echo_target().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let mut stream = listener.accept().await.unwrap().0;
            let mut head = [0u8; 2];
            stream.read_exact(&mut head).await.unwrap();
            let mut methods = vec![0u8; head[1] as usize];
            stream.read_exact(&mut methods).await.unwrap();
            stream
                .write_all(&[0x05, METHOD_NO_ACCEPTABLE])
                .await
                .unwrap();
        });
        let err = connect_via_proxy(
            &SocksProxyConfig::new(&proxy_addr),
            target_host_of(&target),
            target_port_of(&target),
            Duration::from_secs(35),
        )
        .await
        .expect_err("0xFF must raise an error");
        assert!(
            err.to_string().contains("no acceptable auth method"),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn socks5_connect_failure_is_reported() {
        // Wrong target address, the fake proxy replies 0x04 (host unreachable).
        let (target, _t) = echo_target().await;
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let proxy_addr = fake_socks5(None, &target, log.clone()).await;

        let err = connect_via_proxy(
            &SocksProxyConfig::new(&proxy_addr),
            "203.0.113.1",
            1,
            Duration::from_secs(35),
        )
        .await
        .expect_err("a failed CONNECT must raise an error");
        assert!(
            err.to_string().contains("host unreachable"),
            "the CONNECT failure code must be translated into plain words: {err}"
        );
    }

    #[tokio::test]
    async fn socks4_round_trip_through_proxy() {
        let (target, _t) = echo_target().await;
        let (t_host, t_port) = {
            let (h, p) = target.split_once(':').unwrap();
            (h.to_string(), p.parse::<u16>().unwrap())
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap().to_string();
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let log2 = log.clone();
        tokio::spawn(async move {
            let mut stream = listener.accept().await.unwrap().0;
            // request: VER CMD PORT IP USERID 0x00
            let mut head = [0u8; 8];
            stream.read_exact(&mut head).await.unwrap();
            assert_eq!(head[0], 0x04);
            assert_eq!(head[1], 0x01);
            let port = u16::from_be_bytes([head[2], head[3]]);
            let ip = Ipv4Addr::new(head[4], head[5], head[6], head[7]);
            let mut userid = Vec::new();
            loop {
                let mut byte = [0u8; 1];
                stream.read_exact(&mut byte).await.unwrap();
                if byte[0] == 0 {
                    break;
                }
                userid.push(byte[0]);
            }
            log2.lock().unwrap_or_else(|e| e.into_inner()).push(format!(
                "req:{ip}:{port}:{}",
                String::from_utf8_lossy(&userid)
            ));
            stream
                .write_all(&[0, 0x5A, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
            let mut upstream = TokioTcpStream::connect((ip, port)).await.unwrap();
            let _ = copy_bidirectional(&mut stream, &mut upstream).await;
        });

        let mut stream = connect_via_proxy(
            &SocksProxyConfig {
                addr: format!("socks4://{proxy_addr}"),
                username: Some("uid".into()),
                password: None,
            },
            &t_host,
            t_port,
            Duration::from_secs(30),
        )
        .await
        .expect("the SOCKS4 connection must succeed");
        stream.write_all(b"ok").await.unwrap();
        let mut buf = [0u8; 2];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ok");
        assert!(
            log_has(&log, &format!("req:{t_host}:{t_port}:uid")),
            "the SOCKS4 request shape (IP + userid) must be correct: {:?}",
            log_of(&log)
        );
    }

    #[tokio::test]
    async fn socks4_rejection_is_reported() {
        let (target, _t) = echo_target().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let mut stream = listener.accept().await.unwrap().0;
            let mut head = [0u8; 8];
            stream.read_exact(&mut head).await.unwrap();
            // read off the userid
            loop {
                let mut byte = [0u8; 1];
                if stream.read_exact(&mut byte).await.is_err() || byte[0] == 0 {
                    break;
                }
            }
            // 91 = request rejected or failed
            stream
                .write_all(&[0, 0x5B, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
        });
        let (t_host, t_port) = {
            let (h, p) = target.split_once(':').unwrap();
            (h.to_string(), p.parse::<u16>().unwrap())
        };
        let err = connect_via_proxy(
            &SocksProxyConfig::new(format!("socks4://{proxy_addr}")),
            &t_host,
            t_port,
            Duration::from_secs(35),
        )
        .await
        .expect_err("a SOCKS4 rejection must raise an error");
        assert!(err.to_string().contains("rejected"), "got {err}");
    }

    #[test]
    fn proxy_addr_scheme_parsing() {
        let v5 = SocksProxyConfig::new("10.0.0.1:1080");
        assert_eq!(
            v5.version_and_addr(),
            (SocksVersion::V5, "10.0.0.1:1080"),
            "the default is SOCKS5"
        );
        let explicit = SocksProxyConfig::new("socks5://10.0.0.1:1080");
        assert_eq!(explicit.version_and_addr().1, "10.0.0.1:1080");
        let v4 = SocksProxyConfig::new("socks4://10.0.0.1:1080");
        assert_eq!(v4.version_and_addr(), (SocksVersion::V4, "10.0.0.1:1080"));
        let auth = SocksProxyConfig::with_auth("p:1080", "u", "w");
        assert_eq!(auth.credentials(), Some(("u", "w")));
        // An empty username is equivalent to no auth (matches the repo-wide "blank = unset" convention, as with unitName).
        assert_eq!(SocksProxyConfig::new("p:1080").credentials(), None);
        assert_eq!(
            SocksProxyConfig {
                addr: "p:1080".into(),
                username: Some(String::new()),
                password: Some("p".into()),
            }
            .credentials(),
            None
        );
        // Password only without a username: the SOCKS5 subnegotiation requires a username, so this also counts as no auth.
        assert_eq!(
            SocksProxyConfig {
                addr: "p:1080".into(),
                username: None,
                password: Some("p".into()),
            }
            .credentials(),
            None
        );
        // Username only without a password: the username/password subnegotiation allows an empty password.
        assert_eq!(
            SocksProxyConfig {
                addr: "p:1080".into(),
                username: Some("u".into()),
                password: None,
            }
            .credentials(),
            Some(("u", ""))
        );
    }

    fn target_port_of(addr: &str) -> u16 {
        addr.split_once(':').unwrap().1.parse().unwrap()
    }

    fn target_host_of(addr: &str) -> &str {
        addr.split_once(':').unwrap().0
    }

    /// End to end: once `RemotingClient` is configured with `socks_proxy`, one synchronous RPC must really
    /// go through the proxy (the proxy log shows a CONNECT, the target echo service only ever sees the proxy's connection).
    /// This guards the wiring in `connect()`: TCP to the proxy → SOCKS handshake → remoting frames.
    #[tokio::test]
    async fn remoting_client_round_trips_through_socks_proxy() {
        use crate::remoting::client::{RemotingClient, RemotingClientConfig};
        use crate::remoting::protocol::codes::{request_code, response_code};
        use crate::remoting::protocol::remoting_command::RemotingCommand;

        // Target: a minimal remoting echo (replies SUCCESS with the same opaque).
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = listener.local_addr().unwrap().to_string();
        let (t_host, t_port) = {
            let (h, p) = target.split_once(':').unwrap();
            (h.to_string(), p.parse::<u16>().unwrap())
        };
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    loop {
                        let mut len_buf = [0u8; 4];
                        if stream.read_exact(&mut len_buf).await.is_err() {
                            break;
                        }
                        let total = i32::from_be_bytes(len_buf);
                        let mut frame = vec![0u8; 4 + total.max(0) as usize];
                        frame[..4].copy_from_slice(&len_buf);
                        if stream.read_exact(&mut frame[4..]).await.is_err() {
                            break;
                        }
                        let Ok(cmd) = RemotingCommand::decode(&frame) else {
                            break;
                        };
                        if cmd.is_oneway_rpc() {
                            continue;
                        }
                        let mut response =
                            RemotingCommand::create_response(response_code::SUCCESS, None);
                        response.opaque = cmd.opaque;
                        let bytes = response.encode();
                        if stream.write_all(&bytes).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let proxy_addr = fake_socks5(None, &target, log.clone()).await;

        let client = RemotingClient::with_config(RemotingClientConfig {
            socks_proxy: Some(SocksProxyConfig::new(&proxy_addr)),
            ..RemotingClientConfig::default()
        });
        let mut cmd = RemotingCommand::create_request_command(request_code::HEART_BEAT, None);
        cmd.remark = Some("via-socks".to_string());
        let response = client
            .invoke_sync(&target, &mut cmd, Some(10_000))
            .await
            .expect("a synchronous RPC through the SOCKS proxy must succeed");
        assert_eq!(response.code, response_code::SUCCESS);
        assert_eq!(response.opaque, cmd.opaque);
        assert!(
            log_has(&log, &format!("connect:{t_host}:{t_port}")),
            "the CONNECT must hit the proxy and its target must be the original address: {:?}",
            log_of(&log)
        );
        assert!(
            log_has(&log, "methods:[0]"),
            "the proxy must see the SOCKS5 method negotiation: {:?}",
            log_of(&log)
        );
        client.shutdown();
    }
}
