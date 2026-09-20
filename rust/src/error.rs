//! 错误类型（对应 `org.apache.rocketmq.remoting.exception.*` 与 `org.apache.rocketmq.client.exception.*`）。

use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

/// 对应 `org.apache.rocketmq.client.exception.ClientErrorCode`
/// （Python `rocketmq/client/exception.py:ClientErrorCode`）。
///
/// `sendDefaultImpl` 重试耗尽后用它给最终的 `MQClientException` 定性：
/// 连不上 broker→10001，等响应超时→10002，客户端自身问题→10003，
/// 地址服务器没给地址→10004，路由查不到→10005。
pub mod client_error_code {
    pub const CONNECT_BROKER_EXCEPTION: i32 = 10001;
    pub const ACCESS_BROKER_TIMEOUT: i32 = 10002;
    pub const BROKER_NOT_EXIST_EXCEPTION: i32 = 10003;
    pub const NO_NAME_SERVER_EXCEPTION: i32 = 10004;
    pub const NOT_FOUND_TOPIC_EXCEPTION: i32 = 10005;
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
    /// broker / nameserver 返回非 SUCCESS
    Server { response_code: i32, remark: String },
    /// MQClientException
    Client { response_code: Option<i32>, message: String },
    /// MQBrokerException
    Broker { response_code: i32, message: String },
    /// 对应 Python `RequestTimeoutException` / Java `MQClientRequestTimeoutException`
    /// （两者都是 `MQClientException` 的子类）：请求消息**已经发成功**，但超时窗口内
    /// 没等到应答。与 [`Error::Timeout`]（remoting 层的 `RemotingTimeoutException`，
    /// 连响应帧都没等到）区分开 —— 前者应答方可能只是没回，消息其实已投递。
    RequestTimeout { topic: String, timeout_millis: i64 },
    /// 帧或结构体解码失败
    Decode(String),
    /// 编码 / 参数非法
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
            // 文本与 Python `producer._wait_request_response` 抛出的消息逐字一致。
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
    /// 对应 Java/Python 侧的 `response_code`：只有 broker/客户端业务错误才有值。
    pub fn response_code(&self) -> Option<i32> {
        match self {
            Error::Server { response_code, .. } => Some(*response_code),
            Error::Client { response_code: Some(code), .. } => Some(*code),
            Error::Broker { response_code, .. } => Some(*response_code),
            _ => None,
        }
    }

    pub fn client(message: impl Into<String>) -> Error {
        Error::Client { response_code: None, message: message.into() }
    }

    pub fn client_with_code(code: i32, message: impl Into<String>) -> Error {
        Error::Client { response_code: Some(code), message: message.into() }
    }

    /// 对应 Python `raise RequestTimeoutException("send request message to <%s> OK, ...")`。
    pub fn request_timeout(topic: impl Into<String>, timeout_millis: i64) -> Error {
        Error::RequestTimeout { topic: topic.into(), timeout_millis }
    }
}


/// `bail!("...")` = `return Err(Error::client(format!("...")))`，对应 Python 里直接 raise。
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
    fn request_timeout_text_matches_python() {
        // python/rocketmq/client/producer.py::_wait_request_response 的字面量
        let e = Error::request_timeout("TopicTest", 3000);
        assert_eq!(
            e.to_string(),
            "send request message to <TopicTest> OK, but wait reply message timeout, 3000 ms."
        );
        // 它是 MQClientException 的子类语义：没有 broker 侧错误码
        assert_eq!(e.response_code(), None);
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
