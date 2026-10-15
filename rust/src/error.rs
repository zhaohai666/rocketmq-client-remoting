
use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

pub mod client_error_code {
    pub const CONNECT_BROKER_EXCEPTION: i32 = 10001;
    pub const ACCESS_BROKER_TIMEOUT: i32 = 10002;
    pub const BROKER_NOT_EXIST_EXCEPTION: i32 = 10003;
    pub const NO_NAME_SERVER_EXCEPTION: i32 = 10004;
    pub const NOT_FOUND_TOPIC_EXCEPTION: i32 = 10005;
    pub const REQUEST_TIMEOUT_EXCEPTION: i32 = 10006;
    pub const CREATE_REPLY_MESSAGE_EXCEPTION: i32 = 10007;
}

#[derive(Debug)]
pub enum Error {
    /// RemotingCommandException / RemotingNoCodecException
    RemotingCommand(String),
    /// RemotingConnectException
    Connect { addr: String },
    /// RemotingSendRequestException
    SendRequest { addr: String, message: String },
    /// RemotingTimeoutException
    Timeout { addr: String, timeout_millis: i64 },
    /// RemotingTooMuchRequestException
    TooMuchRequest(String),
    /// The broker / name server returned a non-SUCCESS code
    Server { response_code: i32, remark: String },
    /// MQClientException
    Client { response_code: Option<i32>, message: String },
    /// MQBrokerException
    Broker { response_code: i32, message: String },
    RequestTimeout { topic: String, timeout_millis: i64 },
    /// Frame or struct decoding failed
    Decode(String),
    /// Encoding failure / illegal argument
    Encode(String),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::RemotingCommand(m) => write!(f, "remoting command error: {m}"),
            Error::Connect { addr } => write!(f, "connect to {addr} failed"),
            Error::SendRequest { addr, message } => {
                write!(f, "send request to {addr} failed: {message}")
            }
            Error::Timeout { addr, timeout_millis } => write!(
                f,
                "wait response on the channel {addr} timeout, {timeout_millis}ms"
            ),
            Error::TooMuchRequest(m) => write!(f, "too much request: {m}"),
            Error::Server { response_code, remark } => {
                write!(f, "response code {response_code}, remark {remark:?}")
            }
            Error::Client { response_code, message } => match response_code {
                Some(code) => write!(f, "MQClientException(code={code}): {message}"),
                None => write!(f, "MQClientException: {message}"),
            },
            Error::Broker { response_code, message } => {
                write!(f, "MQBrokerException(code={response_code}): {message}")
            }
            // Runtime error text: upper layers and logs match this literal; it must not change.
            Error::RequestTimeout { topic, timeout_millis } => write!(
                f,
                "send request message to <{topic}> OK, but wait reply message timeout, \
                 {timeout_millis} ms."
            ),
            Error::Decode(m) => write!(f, "decode error: {m}"),
            Error::Encode(m) => write!(f, "encode error: {m}"),
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Json(e) => write!(f, "json error: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

impl Error {
    pub fn response_code(&self) -> Option<i32> {
        match self {
            Error::Server { response_code, .. } => Some(*response_code),
            Error::Client { response_code: Some(code), .. } => Some(*code),
            Error::Broker { response_code, .. } => Some(*response_code),
            Error::RequestTimeout { .. } => {
                Some(client_error_code::REQUEST_TIMEOUT_EXCEPTION)
            }
            _ => None,
        }
    }

    pub fn client(message: impl Into<String>) -> Error {
        Error::Client { response_code: None, message: message.into() }
    }

    pub fn client_with_code(code: i32, message: impl Into<String>) -> Error {
        Error::Client { response_code: Some(code), message: message.into() }
    }

    /// The text is pinned verbatim to the recorded wire format: `send request message to <%s> OK, ...`.
    pub fn request_timeout(topic: impl Into<String>, timeout_millis: i64) -> Error {
        Error::RequestTimeout { topic: topic.into(), timeout_millis }
    }
}

/// `bail!("...")` = `return Err(Error::client(format!("...")))`, i.e. the error is returned straight to the caller.
#[macro_export]
macro_rules! bail {
    ($($t:tt)*) => {
        return Err($crate::error::Error::client(format!($($t)*)))
    };
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_timeout_text_matches_golden() {
        // The runtime error text is a literal contract; not a single character may change
        let e = Error::request_timeout("TopicTest", 3000);
        assert_eq!(
            e.to_string(),
            "send request message to <TopicTest> OK, but wait reply message timeout, 3000 ms."
        );
        assert_eq!(e.response_code(), Some(client_error_code::REQUEST_TIMEOUT_EXCEPTION));
    }

    #[test]
    fn test_client_error_codes_match_server_contract() {
        let Ok(root) = std::env::var("ROCKETMQ_SERVER_SRC") else {
            eprintln!("ROCKETMQ_SERVER_SRC not set; skipping the ClientErrorCode comparison");
            return;
        };
        let path = std::path::PathBuf::from(&root)
            .join("client/src/main/java/org/apache/rocketmq/client/common/ClientErrorCode.java");
        let Ok(text) = std::fs::read_to_string(&path) else {
            eprintln!("cannot read {}; skipping the ClientErrorCode comparison", path.display());
            return;
        };
        for (name, ours) in [
            ("CONNECT_BROKER_EXCEPTION", client_error_code::CONNECT_BROKER_EXCEPTION),
            ("ACCESS_BROKER_TIMEOUT", client_error_code::ACCESS_BROKER_TIMEOUT),
            ("BROKER_NOT_EXIST_EXCEPTION", client_error_code::BROKER_NOT_EXIST_EXCEPTION),
            ("NO_NAME_SERVER_EXCEPTION", client_error_code::NO_NAME_SERVER_EXCEPTION),
            ("NOT_FOUND_TOPIC_EXCEPTION", client_error_code::NOT_FOUND_TOPIC_EXCEPTION),
            ("REQUEST_TIMEOUT_EXCEPTION", client_error_code::REQUEST_TIMEOUT_EXCEPTION),
            ("CREATE_REPLY_MESSAGE_EXCEPTION", client_error_code::CREATE_REPLY_MESSAGE_EXCEPTION),
        ] {
            let Some(theirs) = java_const(&text, name) else {
                panic!("server-side ClientErrorCode no longer has constant {name}");
            };
            assert_eq!(theirs, ours, "server-side ClientErrorCode.{name} does not match the rust side");
        }
    }

    fn java_const(text: &str, name: &str) -> Option<i32> {
        let at = text.find(name)?;
        let rhs = &text[at + name.len()..];
        let eq = rhs.find('=')?;
        let digits: String = rhs[eq + 1..]
            .chars()
            .take_while(|c| !matches!(c, ';' | '\n'))
            .filter(|c| !c.is_whitespace())
            .collect();
        digits.parse::<i32>().ok()
    }

    #[test]
    fn response_code_only_on_business_errors() {
        assert_eq!(
            Error::Broker { response_code: 14, message: "x".into() }.response_code(),
            Some(14)
        );
        assert_eq!(
            Error::Server { response_code: 2, remark: "r".into() }.response_code(),
            Some(2)
        );
        assert_eq!(
            Error::client_with_code(-1, "m").response_code(),
            Some(-1)
        );
        assert_eq!(Error::client("m").response_code(), None);
        assert_eq!(
            Error::Timeout { addr: "a".into(), timeout_millis: 1 }.response_code(),
            None
        );
    }
}
