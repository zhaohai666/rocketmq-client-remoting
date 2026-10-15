//! 1. `MixAll.getWSAddr()`: `http://<domain>:8080/rocketmq/<subgroup>`; when the domain already carries a port
//!    (contains `:`), `:8080` is not appended.
//! 2. `fetchNSAddr(true, 3000)`: HTTP GET (timeout 3000ms); a non-blank unitName makes the URL append
//!    `-<unitName>?nofix=1`; a non-empty para appends `?k=v&...`. On **code==200** the response body goes through
//!    `clearNewLine` (trim, then cut at the first `\r` or `\n`) to become the NS address string; otherwise None is returned.
//! 3. `MQClientAPIImpl.fetchNameServerAddr()`: the fetched string is applied only when **different** from the last one
//!    (`updateNameServerAddressList`, split by `;`); when identical, nothing happens.
//! 4. `MQClientInstance`: **if and only if** no static namesrvAddr is configured, start() fetches once first,
//!    and schedules 10s / 2min periodic refresh.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{Error, Result};

pub const DEFAULT_NAMESRV_ADDR_LOOKUP: &str = "jmenv.tbsite.net";
pub const DEFAULT_DOMAIN_SUBGROUP: &str = "nsaddr";
/// Environment variable read when the domain is not given explicitly.
pub const NAMESRV_DOMAIN_ENV: &str = "ROCKETMQ_NAMESRV_DOMAIN";
pub const DEFAULT_TIMEOUT_MILLIS: u64 = 3000;

pub fn clear_new_line(content: &str) -> String {
    let s = content.trim();
    if let Some(i) = s.find('\r') {
        return s[..i].to_string();
    }
    if let Some(i) = s.find('\n') {
        return s[..i].to_string();
    }
    s.to_string()
}

#[derive(Clone, Debug)]
pub struct DefaultTopAddressing {
    ws_addr: String,
    unit_name: String,
    para: Vec<(String, String)>,
    timeout_millis: u64,
    ns_addr: Option<String>,
}

impl Default for DefaultTopAddressing {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultTopAddressing {
    /// Domain comes from the env var `ROCKETMQ_NAMESRV_DOMAIN`; unset = dynamic address lookup disabled.
    pub fn new() -> Self {
        Self::from_domain(std::env::var(NAMESRV_DOMAIN_ENV).unwrap_or_default())
    }

    pub fn from_domain(domain: impl Into<String>) -> Self {
        let domain = domain.into();
        let ws_addr = if domain.is_empty() {
            String::new()
        } else {
            Self::get_ws_addr(&domain, None)
        };
        Self { ws_addr, unit_name: String::new(), para: Vec::new(), timeout_millis: DEFAULT_TIMEOUT_MILLIS, ns_addr: None }
    }

    /// Gives the full WS address directly (for unit tests / special deployments).
    pub fn from_ws_addr(ws_addr: impl Into<String>) -> Self {
        let mut ta = Self::from_domain("");
        ta.ws_addr = ws_addr.into();
        ta
    }

    pub fn with_subgroup(mut self, subgroup: &str) -> Self {
        if !self.ws_addr.is_empty() {
            // Only meaningful when derived from a domain; rebuilding the last URL segment is enough.
            if let Some(i) = self.ws_addr.rfind('/') {
                self.ws_addr = format!("{}/{}", &self.ws_addr[..i], subgroup);
            }
        }
        self
    }

    pub fn with_unit_name(mut self, unit_name: impl Into<String>) -> Self {
        self.unit_name = unit_name.into();
        self
    }

    pub fn with_para(mut self, para: Vec<(String, String)>) -> Self {
        self.para = para;
        self
    }

    pub fn with_timeout_millis(mut self, timeout_millis: u64) -> Self {
        self.timeout_millis = timeout_millis;
        self
    }

    /// Mirrors `MixAll.getWSAddr`: when the domain already carries a port (contains `:`), the default `:8080` is not appended.
    pub fn get_ws_addr(domain: &str, subgroup: Option<&str>) -> String {
        let grp = subgroup.unwrap_or(DEFAULT_DOMAIN_SUBGROUP);
        if domain.contains(':') {
            return format!("http://{domain}/rocketmq/{grp}");
        }
        format!("http://{domain}:8080/rocketmq/{grp}")
    }

    /// Whether dynamic address lookup is available (domain explicitly configured).
    pub fn is_configured() -> bool {
        !std::env::var(NAMESRV_DOMAIN_ENV).unwrap_or_default().is_empty()
    }

    /// Mirrors the URL assembly in `fetchNSAddr` (unitName / para rules copied rule by rule).
    pub fn build_url(&self) -> String {
        let mut url = self.ws_addr.clone();
        let has_unit = !self.unit_name.trim().is_empty();
        if !self.para.is_empty() {
            url = if has_unit {
                format!("{url}-{}?nofix=1&", self.unit_name)
            } else {
                format!("{url}?")
            };
            let parts: Vec<String> =
                self.para.iter().map(|(k, v)| format!("{k}={v}")).collect();
            url.push_str(&parts.join("&"));
        } else if has_unit {
            url = format!("{url}-{}?nofix=1", self.unit_name);
        }
        url
    }

    pub fn ws_addr(&self) -> &str {
        &self.ws_addr
    }

    pub fn ns_addr(&self) -> Option<&str> {
        self.ns_addr.as_deref()
    }

    pub fn timeout_millis(&self) -> u64 {
        self.timeout_millis
    }

    pub async fn fetch_ns_addr(&self, verbose: bool) -> Option<String> {
        if self.ws_addr.is_empty() {
            return None;
        }
        let url = self.build_url();
        match self.http_get(&url).await {
            Ok(body) => Some(clear_new_line(&body)),
            Err(e) => {
                if verbose {
                    crate::rmq_error!("fetch nameserver address failed url={url}: {e}");
                } else {
                    crate::rmq_debug!("fetch name server address exception url={url}: {e}");
                }
                None
            }
        }
    }

    pub async fn fetch_and_apply(&mut self) -> Option<String> {
        let addrs = self.fetch_ns_addr(true).await?;
        if addrs.trim().is_empty() {
            return None;
        }
        if addrs == self.ns_addr.as_deref().unwrap_or_default() {
            return None;
        }
        crate::rmq_info!("name server address changed, old={:?}, new={addrs}", self.ns_addr);
        self.ns_addr = Some(addrs);
        self.ns_addr.clone()
    }

    /// Returns the response body text; non-200 / malformed responses return Err (`fetch_ns_addr` handles both as failure).
    async fn http_get(&self, url: &str) -> Result<String> {
        let (host, port, path) = split_http_url(url)?;
        let addr = format!("{host}:{port}");
        let timeout = Duration::from_millis(self.timeout_millis);

        let mut stream = tokio::time::timeout(timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| Error::Timeout { addr: addr.clone(), timeout_millis: self.timeout_millis as i64 })?
            .map_err(|_| Error::Connect { addr: addr.clone() })?;

        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {addr}\r\nAccept: */*\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await.map_err(|e| Error::SendRequest {
            addr: addr.clone(),
            message: e.to_string(),
        })?;

        let raw = read_to_end(&mut stream, timeout).await?;
        let (head, body) =
            split_headers(&raw).ok_or_else(|| Error::Decode("malformed http response".into()))?;
        let status = parse_status_line(head)?;
        if status != 200 {
            return Err(Error::SendRequest { addr, message: format!("http status {status}") });
        }
        Ok(String::from_utf8_lossy(body).into_owned())
    }
}

/// `http://host:port/path?query` → `(host, port, path?query)`; default port 80.
fn split_http_url(url: &str) -> Result<(String, u16, String)> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| Error::Encode(format!("only http:// is supported: {url:?}")))?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    if authority.is_empty() || authority.contains('@') {
        return Err(Error::Encode(format!("bad address server url: {url:?}")));
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => {
            let port: u16 = p
                .parse()
                .map_err(|_| Error::Encode(format!("bad port in {url:?}")))?;
            (h, port)
        }
        None => (authority, 80_u16),
    };
    if host.is_empty() {
        return Err(Error::Encode(format!("empty host in {url:?}")));
    }
    Ok((host.to_string(), port, path))
}

/// Reads to EOF (the request carries `Connection: close`); timeout returns Err.
async fn read_to_end<S>(stream: &mut S, timeout: Duration) -> Result<Vec<u8>>
where
    S: AsyncReadExt + Unpin,
{
    let op = async {
        let mut buf = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            // An oversized response cannot be an address list; fail outright so a malicious body cannot blow up memory.
            if buf.len() > 64 * 1024 {
                return Err(Error::Decode("address server response too large".into()));
            }
        }
        Ok(buf)
    };
    tokio::time::timeout(timeout, op)
        .await
        .map_err(|_| Error::Timeout { addr: "address server".into(), timeout_millis: timeout.as_millis() as i64 })?
}

/// Splits header from body at the first `\r\n\r\n` (tolerating a bare `\n\n`). The separator is ASCII,
/// so it is searched in the bytes directly, avoiding any dependence on UTF-8 decoding.
fn split_headers(raw: &[u8]) -> Option<(&[u8], &[u8])> {
    let (idx, sep_len) = find_header_separator(raw)?;
    Some((&raw[..idx], &raw[idx + sep_len..]))
}

fn find_header_separator(raw: &[u8]) -> Option<(usize, usize)> {
    const CRLF_CRLF: [u8; 4] = *b"\r\n\r\n";
    if let Some(i) = raw.windows(4).position(|w| w == CRLF_CRLF) {
        return Some((i, 4));
    }
    let lf_lf = *b"\n\n";
    raw.windows(2).position(|w| w == lf_lf).map(|i| (i, 2))
}

/// Status line `HTTP/1.1 200 OK` → 200.
fn parse_status_line(head: &[u8]) -> Result<u16> {
    let text = String::from_utf8_lossy(head);
    let first = text.lines().next().unwrap_or_default();
    let mut it = first.split_whitespace();
    let _version = it.next();
    let code = it.next().ok_or_else(|| Error::Decode("http response has no status code".into()))?;
    code.parse()
        .map_err(|_| Error::Decode(format!("bad http status code {code:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------- URL construction ----------------

    #[test]
    fn get_ws_addr_matches_nameserver_url_contract() {
        assert_eq!(
            DefaultTopAddressing::get_ws_addr("jmenv.tbsite.net", None),
            "http://jmenv.tbsite.net:8080/rocketmq/nsaddr"
        );
        // domain carries a port → :8080 not appended
        assert_eq!(
            DefaultTopAddressing::get_ws_addr("host:12345", None),
            "http://host:12345/rocketmq/nsaddr"
        );
        assert_eq!(
            DefaultTopAddressing::get_ws_addr("h", Some("mygrp")),
            "http://h:8080/rocketmq/mygrp"
        );
    }

    #[test]
    fn build_url_follows_unit_and_para_rules() {
        let base = || DefaultTopAddressing::from_ws_addr("http://h:8080/rocketmq/nsaddr");
        assert_eq!(base().build_url(), "http://h:8080/rocketmq/nsaddr");
        assert_eq!(
            base().with_unit_name("unitA").build_url(),
            "http://h:8080/rocketmq/nsaddr-unitA?nofix=1"
        );
        // A blank unitName counts as unset
        assert_eq!(base().with_unit_name("   ").build_url(), "http://h:8080/rocketmq/nsaddr");

        let url = base()
            .with_para(vec![("k1".into(), "v1".into()), ("k2".into(), "v2".into())])
            .build_url();
        assert_eq!(url, "http://h:8080/rocketmq/nsaddr?k1=v1&k2=v2");

        assert_eq!(
            base()
                .with_unit_name("u")
                .with_para(vec![("k".into(), "v".into())])
                .build_url(),
            "http://h:8080/rocketmq/nsaddr-u?nofix=1&k=v"
        );
    }

    #[test]
    fn with_subgroup_rebuilds_last_segment() {
        let ta = DefaultTopAddressing::from_domain("127.0.0.1:8080").with_subgroup("mygrp");
        assert_eq!(ta.ws_addr(), "http://127.0.0.1:8080/rocketmq/mygrp");
    }

    // ---------------- clearNewLine ----------------

    #[test]
    fn clear_new_line_trims_then_cuts() {
        assert_eq!(clear_new_line("  1.2.3.4:9876\r\nrest"), "1.2.3.4:9876");
        assert_eq!(clear_new_line("a:9876\nb:9877"), "a:9876");
        assert_eq!(clear_new_line("  a:9876  "), "a:9876");
        assert_eq!(clear_new_line("   \r\n x"), "x");
        assert_eq!(clear_new_line("\r\n"), "");
    }

    // ---------------- URL parsing ----------------

    #[test]
    fn split_http_url_cases() {
        assert_eq!(
            split_http_url("http://h:8080/rocketmq/nsaddr").unwrap(),
            ("h".to_string(), 8080, "/rocketmq/nsaddr".to_string())
        );
        assert_eq!(
            split_http_url("http://h").unwrap(),
            ("h".to_string(), 80, "/".to_string())
        );
        assert_eq!(
            split_http_url("http://h:9876/a?nofix=1").unwrap(),
            ("h".to_string(), 9876, "/a?nofix=1".to_string())
        );
        assert!(matches!(split_http_url("https://h"), Err(Error::Encode(_))));
        assert!(matches!(split_http_url("http://h:abc/"), Err(Error::Encode(_))));
        assert!(matches!(split_http_url("http://h:80/x"), Ok((_, 80, _))));
        assert!(matches!(split_http_url("http://@h/"), Err(Error::Encode(_))));
    }

    #[test]
    fn http_response_parsing_is_tolerant_but_strict_on_status() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n1.2.3";
        let (head, body) = split_headers(raw).unwrap();
        assert_eq!(parse_status_line(head).unwrap(), 200);
        assert_eq!(body, b"1.2.3");
        // A bare \n\n is also accepted
        assert_eq!(split_headers(b"HTTP/1.0 500\r\n\nx").unwrap().1, b"x");
        assert!(split_headers(b"garbage").is_none());
        assert!(parse_status_line(b"HTTP/1.1").is_err());
        assert!(parse_status_line(b"HTTP/1.1 xx").is_err());
    }

    // ---------------- address fetching (local mock HTTP server, no external network) ----------------

    /// Each connection reads the shared state fresh, so changing the response mid-test takes effect.
    type Respond = std::sync::Arc<std::sync::Mutex<(u16, String)>>;

    struct MockAddrServer {
        port: u16,
        requests: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        respond: Respond,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        task: Option<tokio::task::JoinHandle<()>>,
    }

    impl MockAddrServer {
        async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let requests: std::sync::Arc<std::sync::Mutex<Vec<String>>> = Default::default();
            let respond: Respond =
                std::sync::Arc::new(std::sync::Mutex::new((200, "127.0.0.1:9876".to_string())));
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let req2 = requests.clone();
            let resp2 = respond.clone();
            let task = tokio::spawn(async move {
                let mut rx = rx;
                loop {
                    tokio::select! {
                        _ = &mut rx => break,
                        accepted = listener.accept() => {
                            let (mut sock, _) = match accepted {
                                Ok(v) => v,
                                Err(_) => break,
                            };
                            let req = req2.clone();
                            let resp = resp2.clone();
                            tokio::spawn(async move {
                                let mut buf = [0_u8; 1024];
                                let n = sock.read(&mut buf).await.unwrap_or(0);
                                let first = String::from_utf8_lossy(&buf[..n])
                                    .lines()
                                    .next()
                                    .unwrap_or_default()
                                    .to_string();
                                req.lock().unwrap().push(first);
                                let (status, body) = resp.lock().unwrap().clone();
                                let http = format!(
                                    "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                    body.len()
                                );
                                let _ = sock.write_all(http.as_bytes()).await;
                            });
                        }
                    }
                }
            });
            Self { port, requests, respond, shutdown: Some(tx), task: Some(task) }
        }

        fn top(&self) -> DefaultTopAddressing {
            // domain carries a port → get_ws_addr does not append :8080
            DefaultTopAddressing::from_domain(format!("127.0.0.1:{}", self.port))
        }

        fn paths(&self) -> Vec<String> {
            let reqs = self.requests.lock().unwrap();
            reqs.iter()
                .map(|l| l.split_whitespace().nth(1).unwrap_or_default().to_string())
                .collect()
        }

        fn set_response(&self, status: u16, body: impl Into<String>) {
            *self.respond.lock().unwrap() = (status, body.into());
        }
    }

    impl Drop for MockAddrServer {
        fn drop(&mut self) {
            if let Some(tx) = self.shutdown.take() {
                let _ = tx.send(());
            }
            if let Some(t) = self.task.take() {
                t.abort();
            }
        }
    }

    #[tokio::test]
    async fn status_200_returns_cleared_body() {
        let server = MockAddrServer::start().await;
        server.set_response(200, "10.0.0.1:9876;10.0.0.2:9876\nextra");
        let ta = server.top();
        assert_eq!(
            ta.fetch_ns_addr(false).await.as_deref(),
            Some("10.0.0.1:9876;10.0.0.2:9876")
        );
        assert!(server.paths()[0].starts_with("/rocketmq/nsaddr"));
    }

    #[tokio::test]
    async fn non_200_and_connection_error_return_none() {
        let server = MockAddrServer::start().await;
        server.set_response(500, "boom");
        assert_eq!(server.top().fetch_ns_addr(false).await, None);

        let ta = DefaultTopAddressing::from_domain("127.0.0.1:1").with_timeout_millis(300);
        assert_eq!(ta.fetch_ns_addr(true).await, None);
    }

    #[tokio::test]
    async fn no_domain_means_disabled() {
        if DefaultTopAddressing::is_configured() {
            return; // ROCKETMQ_NAMESRV_DOMAIN is set in the environment, skip the env branch
        }
        let ta = DefaultTopAddressing::new();
        assert_eq!(ta.ws_addr(), "");
        assert_eq!(ta.fetch_ns_addr(true).await, None);
    }

    #[tokio::test]
    async fn fetch_and_apply_only_reports_changes() {
        let server = MockAddrServer::start().await;
        let mut ta = server.top();
        assert_eq!(ta.fetch_and_apply().await.as_deref(), Some("127.0.0.1:9876"));
        assert_eq!(ta.fetch_and_apply().await, None); // identical → not applied
        server.set_response(200, "10.0.0.9:9876");
        assert_eq!(ta.fetch_and_apply().await.as_deref(), Some("10.0.0.9:9876"));
        assert_eq!(ta.ns_addr(), Some("10.0.0.9:9876"));
    }

    #[tokio::test]
    async fn blank_body_is_not_applied() {
        let server = MockAddrServer::start().await;
        server.set_response(200, "   ");
        let mut ta = server.top();
        assert_eq!(ta.fetch_and_apply().await, None);
        assert_eq!(ta.ns_addr(), None);
    }

    #[tokio::test]
    async fn slow_server_hits_timeout() {
        // Listens but never responds → timeout → None, and must not hang
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let held = tokio::spawn(async move {
            let (_sock, _) = listener.accept().await.unwrap();
            futures_sleep_forever().await;
        });
        let ta = DefaultTopAddressing::from_domain(format!("127.0.0.1:{port}"))
            .with_timeout_millis(150);
        let started = std::time::Instant::now();
        assert_eq!(ta.fetch_ns_addr(false).await, None);
        assert!(started.elapsed() < Duration::from_secs(2));
        held.abort();
    }

    async fn futures_sleep_forever() {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}
