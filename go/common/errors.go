// Package common holds the message model, the 17/6-segment storage codec,
// compression, sysflag bits and misc utilities shared by the remoting and
// client layers.
package common

import "fmt"

// ErrorKind discriminates the error families, one per Java exception type
// (RemotingCommandException, RemotingConnectException, MQClientException, ...).
type ErrorKind int

const (
	KindRemotingCommand ErrorKind = iota
	KindConnect
	KindSendRequest
	KindTimeout
	KindTooMuchRequest
	KindServer
	KindClient
	KindBroker
	KindRequestTimeout
	KindDecode
	KindEncode
	KindIO
)

// ClientErrorCode mirrors org.apache.rocketmq.client.exception.ClientErrorCode.
// 10001~10005 classify the send-retry outcome, 10006/10007 have their own
// throw sites (request-reply).
const (
	ConnectBrokerException      int32 = 10001
	AccessBrokerTimeout         int32 = 10002
	BrokerNotExistException     int32 = 10003
	NoNameServerException       int32 = 10004
	NotFoundTopicException      int32 = 10005
	RequestTimeoutException     int32 = 10006
	CreateReplyMessageException int32 = 10007
)

// Error carries its family in Kind; callers branch on Kind (and ResponseCode)
// the way the other four ports branch on the error type.
type Error struct {
	Kind          ErrorKind
	Message       string
	Addr          string // KindConnect / KindSendRequest / KindTimeout
	Topic         string // KindRequestTimeout
	TimeoutMillis int64  // KindTimeout / KindRequestTimeout
	Remark        string // KindServer
	Code          int32  // KindServer / KindBroker / KindClient(HasCode)
	HasCode       bool   // KindClient only: broker code present or not
	Cause         error  // wrapped IO error (KindIO)
}

func (e *Error) Error() string {
	switch e.Kind {
	case KindRemotingCommand:
		return fmt.Sprintf("remoting command error: %s", e.Message)
	case KindConnect:
		return fmt.Sprintf("connect to %s failed", e.Addr)
	case KindSendRequest:
		return fmt.Sprintf("send request to %s failed: %s", e.Addr, e.Message)
	case KindTimeout:
		return fmt.Sprintf("wait response on the channel %s timeout, %dms", e.Addr, e.TimeoutMillis)
	case KindTooMuchRequest:
		return fmt.Sprintf("too much request: %s", e.Message)
	case KindServer:
		return fmt.Sprintf("response code %d, remark %q", e.Code, e.Remark)
	case KindClient:
		if e.HasCode {
			return fmt.Sprintf("MQClientException(code=%d): %s", e.Code, e.Message)
		}
		return fmt.Sprintf("MQClientException: %s", e.Message)
	case KindBroker:
		return fmt.Sprintf("MQBrokerException(code=%d): %s", e.Code, e.Message)
	case KindRequestTimeout:
		return fmt.Sprintf("send request message to <%s> OK, but wait reply message timeout, %d ms.", e.Topic, e.TimeoutMillis)
	case KindDecode:
		return fmt.Sprintf("decode error: %s", e.Message)
	case KindEncode:
		return fmt.Sprintf("encode error: %s", e.Message)
	case KindIO:
		return fmt.Sprintf("io error: %v", e.Cause)
	}
	return fmt.Sprintf("error(%d): %s", int(e.Kind), e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

// ResponseCode returns the broker/client business code when one is attached:
// Server/Broker always, Client only when HasCode, RequestTimeout always
// (Java constructs RequestTimeoutException with REQUEST_TIMEOUT_EXCEPTION).
func (e *Error) ResponseCode() (int32, bool) {
	switch e.Kind {
	case KindServer, KindBroker:
		return e.Code, true
	case KindClient:
		if e.HasCode {
			return e.Code, true
		}
	case KindRequestTimeout:
		return RequestTimeoutException, true
	}
	return 0, false
}

// IsKind reports whether err is an *Error of the given kind.
func IsKind(err error, kind ErrorKind) bool {
	e, ok := err.(*Error)
	return ok && e.Kind == kind
}

func RemotingCommandError(message string) *Error {
	return &Error{Kind: KindRemotingCommand, Message: message}
}

func ConnectError(addr string) *Error {
	return &Error{Kind: KindConnect, Addr: addr}
}

func SendRequestError(addr, message string) *Error {
	return &Error{Kind: KindSendRequest, Addr: addr, Message: message}
}

func TimeoutError(addr string, timeoutMillis int64) *Error {
	return &Error{Kind: KindTimeout, Addr: addr, TimeoutMillis: timeoutMillis}
}

func TooMuchRequestError(message string) *Error {
	return &Error{Kind: KindTooMuchRequest, Message: message}
}

func ServerError(code int32, remark string) *Error {
	return &Error{Kind: KindServer, Code: code, Remark: remark}
}

func ClientError(message string) *Error {
	return &Error{Kind: KindClient, Message: message}
}

func ClientErrorCode(code int32, message string) *Error {
	return &Error{Kind: KindClient, Message: message, Code: code, HasCode: true}
}

func BrokerError(code int32, message string) *Error {
	return &Error{Kind: KindBroker, Code: code, Message: message}
}

func RequestTimeoutError(topic string, timeoutMillis int64) *Error {
	return &Error{Kind: KindRequestTimeout, Topic: topic, TimeoutMillis: timeoutMillis}
}

func DecodeError(message string) *Error {
	return &Error{Kind: KindDecode, Message: message}
}

func EncodeError(message string) *Error {
	return &Error{Kind: KindEncode, Message: message}
}

func IOError(err error) *Error {
	return &Error{Kind: KindIO, Cause: err}
}
