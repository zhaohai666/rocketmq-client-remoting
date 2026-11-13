package client

import (
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// The AccessChannel knob mirrors Java ClientConfig#accessChannel: the producer
// and push-consumer facades forward it to the trace dispatcher at Start and
// stamp it onto consume hook contexts. LOCAL is the default; CLOUD selects the
// region-prefixed trace topic rmq_sys_TRACE_DATA_<region>.

func TestAccessChannelProducerFacadeDefaultsToLocal(t *testing.T) {
	p := MustNewDefaultMQProducer("GID_access_default")
	if got := p.GetAccessChannel(); got != AccessChannelLocal {
		t.Errorf("default producer accessChannel = %q, want %q", got, AccessChannelLocal)
	}
}

func TestAccessChannelConsumerFacadeDefaultsToLocal(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("CID_access_default")
	if got := c.GetAccessChannel(); got != AccessChannelLocal {
		t.Errorf("default consumer accessChannel = %q, want %q", got, AccessChannelLocal)
	}
}

// Start forwards the facade's CLOUD to the dispatcher. startTraceDispatcher is
// driven directly (same package) with no name server configured: the internal
// trace producer's Start then fails fast, so dispatcher.Start records the
// channel but never spawns its worker goroutine -- no real threads are started.
func TestAccessChannelProducerDispatcherStartsWithCloud(t *testing.T) {
	p := MustNewDefaultMQProducer("GID_access_cloud")
	p.SetEnableTrace(true)
	p.SetAccessChannel(AccessChannelCloud)
	p.startTraceDispatcher()
	d := p.TraceDispatcher()
	if d == nil {
		t.Fatal("trace dispatcher was not created")
	}
	if d.accessChan != AccessChannelCloud {
		t.Errorf("dispatcher accessChan = %q, want %q", d.accessChan, AccessChannelCloud)
	}
}

func TestAccessChannelConsumerDispatcherStartsWithCloud(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("CID_access_cloud")
	c.SetEnableTrace(true)
	c.SetAccessChannel(AccessChannelCloud)
	c.startTraceDispatcher()
	d := c.TraceDispatcher()
	if d == nil {
		t.Fatal("trace dispatcher was not created")
	}
	if d.accessChan != AccessChannelCloud {
		t.Errorf("dispatcher accessChan = %q, want %q", d.accessChan, AccessChannelCloud)
	}
}

// The consume hook context carries the facade's CLOUD.
func TestAccessChannelConsumeHookContextCarriesCloud(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("CID_access_cloud_ctx")
	c.SetAccessChannel(AccessChannelCloud)
	ctx := c.buildConsumeHookContext(nil, common.MessageQueue{})
	if ctx.AccessChannel != AccessChannelCloud {
		t.Errorf("consume hook context accessChannel = %q, want %q",
			ctx.AccessChannel, AccessChannelCloud)
	}
}

// When a TraceContext has no accessChannel, sendTraceData falls back to the
// dispatcher's CLOUD and routes the record to the region-prefixed topic
// rmq_sys_TRACE_DATA_<region>. The dispatcher is driven without its worker (no
// Start), with its internal producer connected to the mock cluster so the
// actual topic put on the wire can be observed.
func TestAccessChannelSendTraceDataFallsBackToDispatcherCloudRegionTopic(t *testing.T) {
	const region = "RegionX"
	regionTopic := TraceTopicPrefix + region

	handler := &sendBrokerOnReq{offset: 7}
	tap := &traceSendTap{inner: handler.handle, traceTopic: regionTopic}
	broker := startMockServer(t, tap.handle)
	ns := startMockServer(t, nameserverOnReq(routes(map[string]string{
		regionTopic: routeBodyFor(broker.addr, ""),
	})))

	d := NewAsyncTraceDispatcher("GID_access_fallback", TraceDispatcherProduce, 1, "", nil)
	d.traceProd.SetNameServerAddresses([]string{ns.addr})
	d.traceProd.SetInstanceName(uniqueClientID(t))
	d.traceProd.SetSendMsgTimeout(3000)
	requireNoError(t, "trace producer start", d.traceProd.Start())
	t.Cleanup(d.traceProd.Shutdown)

	// Stand in for Start(nameSrv, CLOUD): the facade forwards its CLOUD here.
	d.accessChan = AccessChannelCloud

	ctx := &TraceContext{
		TraceType:     TracePub,
		TimeStamp:     common.CurrentTimeMillis(),
		RegionID:      region,
		GroupName:     "GID_access_fallback",
		IsSuccess:     true,
		RequestID:     "REQ-ACCESS-1",
		AccessChannel: "", // empty -> fall back to the dispatcher's CLOUD
		TraceBeans:    []*TraceBean{referenceTraceBean(referenceMsgID1, "KeyA", 0)},
	}
	d.sendTraceData([]*TraceContext{ctx})

	waitFor(t, "trace data on "+regionTopic, func() bool { return tap.count() > 0 })
}
