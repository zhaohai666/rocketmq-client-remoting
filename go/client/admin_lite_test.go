// Wire-shape tests for the lite-topic admin RPCs.
package client

import (
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

func liteAnswer(body []byte) func(*remoting.RemotingCommand) *remoting.RemotingCommand {
	return func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return respOK(body)
	}
}

func TestAdminLiteFamilyWireShape(t *testing.T) {
	c := newAdminCluster(t)
	for _, code := range []int32{
		remoting.ReqGetBrokerLiteInfo, remoting.ReqGetParentTopicInfo,
		remoting.ReqGetLiteTopicInfo, remoting.ReqGetLiteClientInfo,
		remoting.ReqGetLiteGroupInfo, remoting.ReqTriggerLiteDispatch,
	} {
		c.brokerAnswer[code] = liteAnswer([]byte(`{"lite":"payload"}`))
	}

	type call struct {
		name string
		code int32
		want map[string]string
		run  func() (map[string]any, error)
	}

	broker := c.brokerSrv.addr
	calls := []call{
		{
			name: "getBrokerLiteInfo", code: remoting.ReqGetBrokerLiteInfo,
			want: map[string]string{},
			run:  func() (map[string]any, error) { return c.admin.GetBrokerLiteInfo(broker, 0) },
		},
		{
			name: "getParentTopicInfo", code: remoting.ReqGetParentTopicInfo,
			want: map[string]string{"topic": "lite-topic"},
			run:  func() (map[string]any, error) { return c.admin.GetParentTopicInfo(broker, "lite-topic", 0) },
		},
		{
			name: "getLiteTopicInfo(both)", code: remoting.ReqGetLiteTopicInfo,
			want: map[string]string{"parentTopic": "lite-topic", "liteTopic": "l1"},
			run: func() (map[string]any, error) {
				return c.admin.GetLiteTopicInfo(broker, "lite-topic", "l1", 0)
			},
		},
		{
			name: "getLiteTopicInfo(nullable omitted)", code: remoting.ReqGetLiteTopicInfo,
			want: map[string]string{"parentTopic": "lite-topic"},
			run: func() (map[string]any, error) {
				return c.admin.GetLiteTopicInfo(broker, "lite-topic", "", 0)
			},
		},
		{
			name: "getLiteClientInfo(clientId set)", code: remoting.ReqGetLiteClientInfo,
			want: map[string]string{"parentTopic": "lite-topic", "group": "GX", "clientId": "cid-9", "maxCount": "1000"},
			run: func() (map[string]any, error) {
				return c.admin.GetLiteClientInfo(broker, "lite-topic", "GX", "cid-9", 0)
			},
		},
		{
			name: "getLiteClientInfo(clientId omitted)", code: remoting.ReqGetLiteClientInfo,
			want: map[string]string{"parentTopic": "lite-topic", "group": "GX", "maxCount": "1000"},
			run: func() (map[string]any, error) {
				return c.admin.GetLiteClientInfo(broker, "lite-topic", "GX", "", 0)
			},
		},
		{
			name: "getLiteGroupInfo(liteTopic set)", code: remoting.ReqGetLiteGroupInfo,
			want: map[string]string{"group": "GX", "liteTopic": "l1", "topK": "7"},
			run: func() (map[string]any, error) {
				return c.admin.GetLiteGroupInfo(broker, "GX", "l1", 7, 0)
			},
		},
		{
			name: "getLiteGroupInfo(topK always sent)", code: remoting.ReqGetLiteGroupInfo,
			want: map[string]string{"group": "GX", "topK": "0"},
			run: func() (map[string]any, error) {
				return c.admin.GetLiteGroupInfo(broker, "GX", "", 0, 0)
			},
		},
		{
			name: "triggerLiteDispatch(clientId set)", code: remoting.ReqTriggerLiteDispatch,
			want: map[string]string{"group": "GX", "clientId": "cid-9"},
			run: func() (map[string]any, error) {
				return nil, c.admin.TriggerLiteDispatch(broker, "GX", "cid-9", 0)
			},
		},
		{
			name: "triggerLiteDispatch(clientId omitted)", code: remoting.ReqTriggerLiteDispatch,
			want: map[string]string{"group": "GX"},
			run: func() (map[string]any, error) {
				return nil, c.admin.TriggerLiteDispatch(broker, "GX", "", 0)
			},
		},
	}

	for _, call := range calls {
		call := call
		t.Run(call.name, func(t *testing.T) {
			before := c.brokerLog.count(call.code)
			out, _ := call.run()
			all := c.brokerLog.all(call.code)
			if len(all) != before+1 {
				t.Fatalf("requests with code %d: %d, want exactly one new", call.code, len(all)-before)
			}
			req := all[before]
			for key, want := range call.want {
				if got := ext(t, req, key); got != want {
					t.Errorf("%s = %q, want %q", key, got, want)
				}
			}
			if req.ExtFields().Len() != len(call.want) {
				t.Errorf("extFields = %v, want exactly %v", req.ExtFields().Keys(), call.want)
			}
			if len(req.Body) != 0 {
				t.Errorf("body = %q, lite requests carry no body", string(req.Body))
			}
			// Every query in the family decodes the JSON object body when the
			// broker sent one.
			if call.code != remoting.ReqTriggerLiteDispatch {
				if out == nil || out["lite"] != "payload" {
					t.Fatalf("result = %v, want the decoded body", out)
				}
			}
		})
	}
}

func TestAdminLiteFamilyNoBodyYieldsNilResult(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqGetBrokerLiteInfo] = liteAnswer(nil)

	out, err := c.admin.GetBrokerLiteInfo(c.brokerSrv.addr, 0)
	if err != nil {
		t.Fatalf("a SUCCESS answer without a body is a nil result, got %v", err)
	}
	if out != nil {
		t.Fatalf("result = %v, want nil", out)
	}
}

func TestAdminLiteFamilySurfacesBrokerRejection(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqGetParentTopicInfo] = func(*remoting.RemotingCommand) *remoting.RemotingCommand {
		return okCode(remoting.RespSystemError, "lite broker offline")
	}

	_, err := c.admin.GetParentTopicInfo(c.brokerSrv.addr, "lite-topic", 0)
	if err == nil {
		t.Fatal("a rejection must surface")
	}
	berr, ok := err.(*common.Error)
	if !ok || berr.Kind != common.KindBroker {
		t.Fatalf("error = %v, want a broker error", err)
	}
	if berr.Code != remoting.RespSystemError || berr.Message != "lite broker offline" {
		t.Fatalf("error = %+v, want the broker code and remark", berr)
	}
	if berr.Addr != c.brokerSrv.addr {
		t.Fatalf("error addr = %q, want the requested address %q", berr.Addr, c.brokerSrv.addr)
	}
}

// The lite family rides the VIP channel like the other broker admin calls:
// with the switch on the request only lands when the rewrite IS applied, and
// a call to the plain address must fail (nothing listens on port-2).
func TestAdminLiteFamilyRidesVIPChannel(t *testing.T) {
	c := newAdminCluster(t)
	c.brokerAnswer[remoting.ReqGetBrokerLiteInfo] = liteAnswer([]byte(`{"lite":"payload"}`))
	c.admin.SetVipChannelEnabled(true)

	published := shiftPort(t, c.brokerSrv.addr, 2)
	out, err := c.admin.GetBrokerLiteInfo(published, 0)
	if err != nil {
		t.Fatalf("the VIP-rewritten call must reach the broker: %v", err)
	}
	if out == nil || out["lite"] != "payload" {
		t.Fatalf("result = %v", out)
	}
	if c.brokerLog.count(remoting.ReqGetBrokerLiteInfo) != 1 {
		t.Fatalf("requests = %d, want 1", c.brokerLog.count(remoting.ReqGetBrokerLiteInfo))
	}

	if _, err := c.admin.GetBrokerLiteInfo(c.brokerSrv.addr, 0); err == nil {
		t.Fatal("with the VIP switch on the plain address must NOT be contacted directly")
	}
	if got := c.brokerLog.count(remoting.ReqGetBrokerLiteInfo); got != 1 {
		t.Fatalf("requests = %d, the plain-address call must not arrive", got)
	}
}
