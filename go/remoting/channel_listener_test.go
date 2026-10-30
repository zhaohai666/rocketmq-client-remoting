// Channel-listener contract, offline against an echo peer: a listener is told
// once per connection that comes up (not once per request on a connection that
// already existed), again after a reconnect, and a listener that panics neither
// fails the request that opened the connection nor goes unlogged.
package remoting

import (
	"sync"
	"testing"
	"time"
)

func TestChannelListenerFiresOncePerNewConnection(t *testing.T) {
	addr := echoServer(t)
	client := NewRemotingClient()
	defer client.Shutdown()

	var mu sync.Mutex
	var seen []string
	client.RegisterChannelListener(func(got string) {
		mu.Lock()
		seen = append(seen, got)
		mu.Unlock()
	})
	events := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
	send := func() {
		req := CreateRequestCommand(ReqHeartBeat, nil)
		if _, err := client.InvokeSync(addr, req, 3000); err != nil {
			t.Fatalf("invoke: %v", err)
		}
	}

	send()
	send()
	if got := len(events()); got != 1 {
		t.Fatalf("listener events after two requests on one connection = %d, want 1", got)
	}

	client.CloseChannel(addr)
	deadline := time.Now().Add(5 * time.Second)
	for len(client.ConnectionAddrs()) > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	send()
	got := events()
	if len(got) != 2 {
		t.Fatalf("listener events after a reconnect = %d, want 2", len(got))
	}
	if got[1] != addr {
		t.Fatalf("listener got address %q, want %q", got[1], addr)
	}
}

func TestThrowingChannelListenerDoesNotBreakTheInvoke(t *testing.T) {
	addr := echoServer(t)
	client := NewRemotingClient()
	defer client.Shutdown()
	capture := captureLogs(t)
	client.RegisterChannelListener(func(string) {
		panic("listener blew up")
	})

	req := CreateRequestCommand(ReqHeartBeat, nil)
	response, err := client.InvokeSync(addr, req, 3000)
	if err != nil {
		t.Fatalf("a panicking listener must not fail the invoke: %v", err)
	}
	if response.Code != RespSuccess {
		t.Fatalf("response code = %d, want SUCCESS", response.Code)
	}
	if lines := capture.matching("channel listener for", addr, "listener blew up"); len(lines) != 1 {
		t.Fatalf("want exactly one log line naming the listener panic, got %d", len(lines))
	}
}
