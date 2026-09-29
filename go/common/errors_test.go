package common

import (
	"errors"
	"io"
	"testing"
)

func TestErrorTextsMatchTheOtherPorts(t *testing.T) {
	cases := []struct {
		err  *Error
		want string
	}{
		{RemotingCommandError("bad frame"), "remoting command error: bad frame"},
		{ConnectError("127.0.0.1:10911"), "connect to 127.0.0.1:10911 failed"},
		{SendRequestError("addr", "boom"), "send request to addr failed: boom"},
		{TimeoutError("addr", 3000), "wait response on the channel addr timeout, 3000ms"},
		{TooMuchRequestError("x"), "too much request: x"},
		{ServerError(2, "remark"), `response code 2, remark "remark"`},
		{ClientError("plain"), "MQClientException: plain"},
		{ClientErrorCode(-1, "coded"), "MQClientException(code=-1): coded"},
		{BrokerError(14, "m"), "MQBrokerException(code=14): m"},
		{DecodeError("d"), "decode error: d"},
		{EncodeError("e"), "encode error: e"},
	}
	for _, c := range cases {
		if got := c.err.Error(); got != c.want {
			t.Fatalf("got %q want %q", got, c.want)
		}
	}
}

func TestRequestTimeoutTextMatchesPython(t *testing.T) {
	// python/rocketmq/client/producer.py::_wait_request_response 的字面量
	e := RequestTimeoutError("TopicTest", 3000)
	want := "send request message to <TopicTest> OK, but wait reply message timeout, 3000 ms."
	if e.Error() != want {
		t.Fatalf("got %q", e.Error())
	}
	// Java 构造 RequestTimeoutException 时带上 10006，按码分流依赖它
	if code, ok := e.ResponseCode(); !ok || code != RequestTimeoutException {
		t.Fatalf("code %d %v", code, ok)
	}
}

func TestResponseCodeOnlyOnBusinessErrors(t *testing.T) {
	if code, ok := BrokerError(14, "x").ResponseCode(); !ok || code != 14 {
		t.Fatal("broker errors always carry the code")
	}
	if code, ok := ServerError(2, "r").ResponseCode(); !ok || code != 2 {
		t.Fatal("server errors always carry the code")
	}
	if code, ok := ClientErrorCode(-1, "m").ResponseCode(); !ok || code != -1 {
		t.Fatal("coded client errors carry the code")
	}
	if _, ok := ClientError("m").ResponseCode(); ok {
		t.Fatal("plain client errors have no code")
	}
	if _, ok := TimeoutError("a", 1).ResponseCode(); ok {
		t.Fatal("remoting timeouts have no code")
	}
	if _, ok := ConnectError("a").ResponseCode(); ok {
		t.Fatal("connect errors have no code")
	}
}

func TestClientErrorCodeConstantsMatchJava(t *testing.T) {
	// org.apache.rocketmq.client.exception.ClientErrorCode
	cases := []struct {
		name string
		code int32
	}{
		{"CONNECT_BROKER_EXCEPTION", ConnectBrokerException},
		{"ACCESS_BROKER_TIMEOUT", AccessBrokerTimeout},
		{"BROKER_NOT_EXIST_EXCEPTION", BrokerNotExistException},
		{"NO_NAME_SERVER_EXCEPTION", NoNameServerException},
		{"NOT_FOUND_TOPIC_EXCEPTION", NotFoundTopicException},
		{"REQUEST_TIMEOUT_EXCEPTION", RequestTimeoutException},
		{"CREATE_REPLY_MESSAGE_EXCEPTION", CreateReplyMessageException},
	}
	want := map[string]int32{
		"CONNECT_BROKER_EXCEPTION":       10001,
		"ACCESS_BROKER_TIMEOUT":          10002,
		"BROKER_NOT_EXIST_EXCEPTION":     10003,
		"NO_NAME_SERVER_EXCEPTION":       10004,
		"NOT_FOUND_TOPIC_EXCEPTION":      10005,
		"REQUEST_TIMEOUT_EXCEPTION":      10006,
		"CREATE_REPLY_MESSAGE_EXCEPTION": 10007,
	}
	for _, c := range cases {
		if c.code != want[c.name] {
			t.Fatalf("%s = %d, want %d", c.name, c.code, want[c.name])
		}
	}
}

func TestIsKindAndUnwrap(t *testing.T) {
	if !IsKind(ClientError("x"), KindClient) {
		t.Fatal("client error kind")
	}
	if IsKind(BrokerError(1, "x"), KindClient) {
		t.Fatal("broker error is not client kind")
	}
	if IsKind(io.EOF, KindClient) {
		t.Fatal("foreign error must not match")
	}
	inner := errors.New("dial failed")
	e := IOError(inner)
	if !errors.Is(e, inner) {
		t.Fatal("IO error must unwrap to the cause")
	}
	if !IsKind(e, KindIO) {
		t.Fatal("io kind")
	}
}
