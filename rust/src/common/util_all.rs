
use std::net::{SocketAddr, UdpSocket};
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub const YYYY_MM_DD_HH_MM_SS: &str = "%Y-%m-%d %H:%M:%S";

const HEX_ARRAY: &[u8; 16] = b"0123456789ABCDEF";

/// Origin of [`monotonic_millis`], anchored on first call (process start).
static MONOTONIC_ORIGIN: OnceLock<Instant> = OnceLock::new();

/// Milliseconds measured on the monotonic clock:
/// **monotonically increasing**, unaffected by system clock rollback / NTP corrections.
///
/// Why not use the wall clock like [`current_time_millis`]: the send-retry budget and broker-latency
/// fault avoidance both need "how much time elapsed between two events"; once the wall clock steps back the delta goes negative,
/// recording timed-out requests as "extremely fast" and thus rewarding bad brokers.
pub fn monotonic_millis() -> f64 {
    let origin = MONOTONIC_ORIGIN.get_or_init(Instant::now);
    origin.elapsed().as_secs_f64() * 1000.0
}

pub fn nano_time() -> u128 {
    MONOTONIC_ORIGIN.get_or_init(Instant::now).elapsed().as_nanos()
}

pub fn java_string_hash(s: &str) -> i32 {
    let mut h: i32 = 0;
    for ch in s.chars() {
        h = h
            .wrapping_mul(31)
            .wrapping_add(ch as u32 as i32);
    }
    h
}

pub fn current_time_millis() -> i64 {
    now_millis()
}

pub fn current_time_seconds() -> i64 {
    now_millis() / 1000
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn compute_elapse_time_millis(last_time: i64) -> i64 {
    now_millis() - last_time
}

pub fn offset_2_filename(offset: i64) -> String {
    format!("{offset:020}")
}

pub fn time_to_human_string(ts: i64) -> String {
    if ts <= 0 {
        return "-".to_string();
    }
    chrono::DateTime::<chrono::Local>::from(UNIX_EPOCH + std::time::Duration::from_millis(ts as u64))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

pub fn is_blank(s: Option<&str>) -> bool {
    match s {
        None => true,
        Some(v) => v.trim().is_empty(),
    }
}

pub fn is_not_blank(s: Option<&str>) -> bool {
    !is_blank(s)
}

pub fn is_not_blank_str(s: &str) -> bool {
    !s.trim().is_empty()
}

pub fn get_pid() -> u32 {
    std::process::id()
}

pub fn user_home() -> Option<String> {
    ["HOME", "USERPROFILE"]
        .iter()
        .find_map(|key| std::env::var(key).ok().filter(|v| !v.is_empty()))
}

pub fn is_ipv4(addr: &str) -> bool {
    addr.parse::<std::net::Ipv4Addr>().is_ok()
}

pub fn is_ipv6(addr: &str) -> bool {
    addr.parse::<std::net::Ipv6Addr>().is_ok()
}

fn crc32_table() -> &'static [u32; 256] {
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut table = [0u32; 256];
        for (i, slot) in table.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            }
            *slot = c;
        }
        table
    })
}

/// Unmasked CRC-32/ISO-HDLC of `data`, i.e. the full 32-bit checksum.
/// This is *not* the value the store writes into BODYCRC; see [`crc32`].
pub fn crc32_raw(data: &[u8]) -> u32 {
    let table = crc32_table();
    let mut crc = 0xFFFF_FFFFu32;
    for b in data {
        crc = table[((crc ^ (*b as u32)) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

/// The BODYCRC value a store frame carries for `data`: [`crc32_raw`] with the
/// sign bit dropped (`& 0x7FFF_FFFF`). Comparing a full 32-bit CRC against that
/// field instead rejects roughly half of all valid bodies, since about half of
/// all CRCs have the high bit set and the store clears it.
pub fn crc32(data: &[u8]) -> u32 {
    crc32_raw(data) & 0x7FFF_FFFF
}

pub fn bytes_2_string(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX_ARRAY[(b >> 4) as usize] as char);
        out.push(HEX_ARRAY[(b & 0x0F) as usize] as char);
    }
    out
}

pub fn string_2_bytes(hex: &str) -> Option<Vec<u8>> {
    if hex.is_empty() {
        return None;
    }
    let upper = hex.to_ascii_uppercase();
    let chars: Vec<u8> = upper.as_bytes().to_vec();
    if !chars.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(chars.len() / 2);
    for pair in chars.chunks(2) {
        let hi = HEX_ARRAY.iter().position(|c| *c == pair[0])? as u8;
        let lo = HEX_ARRAY.iter().position(|c| *c == pair[1])? as u8;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

/// Probes the local egress IP (connect a UDP socket to a public address and read the sockname; no packet is actually sent).
pub fn local_ip() -> String {
    let socket = match UdpSocket::bind("0.0.0.0:0") {
        Ok(s) => s,
        Err(_) => return "127.0.0.1".to_string(),
    };
    if socket.connect("8.8.8.8:80").is_err() {
        return "127.0.0.1".to_string();
    }
    match socket.local_addr() {
        Ok(SocketAddr::V4(v4)) => v4.ip().to_string(),
        Ok(addr) => addr.ip().to_string(),
        Err(_) => "127.0.0.1".to_string(),
    }
}

/// Splits `host:port`, supports the IPv6 `[::1]:9876` form.
pub fn parse_addr(addr: &str) -> (String, String) {
    if let Some(rest) = addr.strip_prefix('[') {
        if let Some((host, tail)) = rest.split_once(']') {
            return (host.to_string(), tail.trim_start_matches(':').to_string());
        }
    }
    match addr.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.to_string()),
        None => (addr.to_string(), String::new()),
    }
}

pub fn current_day_millis() -> u32 {
    use chrono::Timelike as _;
    let now = chrono::Local::now();
    let secs = now.hour() * 3600 + now.minute() * 60 + now.second();
    secs * 1000 + now.nanosecond() / 1_000_000
}

/// ⚠ Determinism requirement: the "class-load hash" segment must not use the runtime's randomized string hash (per-process
/// variation would make the same instance's ID drift), so it is replaced with the cross-process stable [`java_string_hash`].
/// Length and character set are unchanged; uniqueness is still guaranteed by IP/PID/time/counter.
pub fn create_uniq_id() -> String {
    use std::net::IpAddr;
    use std::sync::atomic::{AtomicU16, Ordering};

    static COUNTER: AtomicU16 = AtomicU16::new(0);
    // First `+= 1` then take `& 0xFFFF`, i.e. the first ID's counter is 1; this does the same, starting from 1.
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed).wrapping_add(1);

    let mut bytes: Vec<u8> = Vec::with_capacity(16);
    let ip = crate::common::mix_all::MixAll::get_ip_str();
    match ip.parse::<IpAddr>() {
        Ok(IpAddr::V4(v4)) => bytes.extend_from_slice(&v4.octets()),
        Ok(IpAddr::V6(v6)) => bytes.extend_from_slice(&v6.octets()),
        Err(_) => bytes.extend_from_slice(ip.as_bytes()),
    }
    bytes.extend_from_slice(&(get_pid() as u16).to_be_bytes());
    bytes.extend_from_slice(&(java_string_hash("RocketMQClient") as u32).to_be_bytes());
    bytes.extend_from_slice(&current_day_millis().to_be_bytes());
    bytes.extend_from_slice(&counter.to_be_bytes());
    bytes_2_string(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_hash_matches_reference_vectors() {
        assert_eq!(java_string_hash("TagA"), 2598919);
        assert_eq!(java_string_hash("TagB"), 2598920);
        assert_eq!(java_string_hash("P"), 80);
        assert_eq!(java_string_hash("PA"), 2545);
        assert_eq!(java_string_hash("*"), 42);
        assert_eq!(java_string_hash(""), 0);
        assert_eq!(java_string_hash("\u{4e2d}"), 20013);
    }

    #[test]
    fn crc32_is_stored_body_crc() {
        assert_eq!(crc32(b""), 0);
        // Below 2^31: the stored field and the unmasked checksum coincide.
        assert_eq!(crc32(b"1234567890"), 639479525);
        assert_eq!(crc32_raw(b"1234567890"), 639479525);
        // High bit set in the checksum: the store clears it, so the field differs.
        assert_eq!(crc32_raw(b"the quick brown fox"), 0x91C1_02CA);
        assert_eq!(crc32(b"the quick brown fox"), 0x11C1_02CA);
        assert_eq!(crc32(b"ba-all-0"), 0x7383_6757);
        assert_ne!(crc32(b"ba-all-0"), crc32_raw(b"ba-all-0"));
    }

    #[test]
    fn hex_round_trip() {
        let bytes = vec![0x00u8, 0x0F, 0xFF, 0xA5];
        let hex = bytes_2_string(&bytes);
        assert_eq!(hex, "000FFFA5");
        assert_eq!(string_2_bytes(&hex).unwrap(), bytes);
        assert_eq!(string_2_bytes(""), None);
    }

    #[test]
    fn offset_filename_is_zero_padded_20() {
        assert_eq!(offset_2_filename(0), "00000000000000000000");
        assert_eq!(offset_2_filename(12345), "00000000000000012345");
    }

    #[test]
    fn blank_helpers() {
        assert!(is_blank(None));
        assert!(is_blank(Some("  \t")));
        assert!(!is_blank(Some("x")));
        assert!(is_not_blank(Some("x")));
    }

    #[test]
    fn addr_parsing() {
        assert_eq!(parse_addr("127.0.0.1:9876"), ("127.0.0.1".into(), "9876".into()));
        assert_eq!(parse_addr("[::1]:9876"), ("::1".into(), "9876".into()));
        assert!(is_ipv4("127.0.0.1"));
        assert!(!is_ipv4("::1"));
        assert!(is_ipv6("::1"));
    }

    #[test]
    fn human_time_uses_dash_for_zero() {
        assert_eq!(time_to_human_string(0), "-");
        assert_eq!(time_to_human_string(-1), "-");
        assert_eq!(time_to_human_string(1_700_000_000_000).len(), 19);
    }

    #[test]
    fn uniq_id_shape_and_uniqueness() {
        let first = create_uniq_id();
        let second = create_uniq_id();
        assert_ne!(first, second, "two consecutive calls in the same process must differ");
        // IPv4: IP(4)+PID(2)+hash(4)+dayMs(4)+counter(2) = 16B -> 32 uppercase hex chars
        if is_ipv4(&crate::common::mix_all::MixAll::get_ip_str()) {
            assert_eq!(first.len(), 32, "length is always 32 under IPv4, got {first}");
        }
        assert!(
            first.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase()),
            "only uppercase hex allowed, got {first}"
        );
        // The last 4 chars are the auto-increment counter; the second is always the first plus 1
        let n = |s: &str| u32::from_str_radix(&s[s.len() - 4..], 16).unwrap();
        assert_eq!(n(&second), n(&first) + 1);
    }

    #[test]
    fn day_millis_stays_in_one_day() {
        let ms = current_day_millis();
        assert!(ms < 86_400_000, "millis of the current day must stay within one day, got {ms}");
    }
}
