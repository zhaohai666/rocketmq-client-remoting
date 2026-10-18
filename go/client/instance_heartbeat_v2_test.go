// Instance-level heartbeat V2 gating, driven against an in-process mock broker.
//
// The state machine these tests pin is the one the reference runs
// (MQClientInstance#sendHeartbeatToAllBrokerV2 / #sendHeartbeatToBrokerV2):
//
//	first beat to an address  -> always the WITH-subscription body
//	answer IS_SUPPORT..=true  -> the address joins the V2 set
//	answer IS_SUB_CHANGE=true -> the stored fingerprint is dropped
//	stored fingerprint == now -> a later beat may send the WITHOUT-sub body
//	answer without the flags  -> the address is NOT V2-capable: full body forever
//
// and the switch itself: ClientConfig.useHeartbeatV2 is off by default, and with
// it off the beat must stay byte-compatible with the plain V1 path (fingerprint
// 0, withoutSub false, subscriptions included).
package client

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- mock broker

// v2Broker is a broker that answers HEART_BEAT(34) with a configurable version
// and configurable V2 flags, and records every body it was sent.
type v2Broker struct {
	srv *mockServer

	mu           sync.Mutex
	bodies       []*remoting.HeartbeatData
	supportV2    bool
	showFlags    bool // false = answer like a broker that never heard of V2
	subChange    bool
	code         int32
	remark       string
	version      int32
	answerDelay  time.Duration
	heartbeatLen int
}

func newV2Broker(t *testing.T) *v2Broker {
	t.Helper()
	b := &v2Broker{version: 450}
	b.srv = startMockServer(t, b.handle)
	return b
}

func (b *v2Broker) handle(_ *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
	if req.Code != remoting.ReqHeartBeat {
		return remoting.CreateResponseCommand(remoting.RespSuccess, "")
	}
	hb, err := remoting.DecodeHeartbeatData(req.Body)
	if err != nil {
		return remoting.CreateResponseCommand(remoting.RespSystemError, "bad heartbeat body: "+err.Error())
	}

	b.mu.Lock()
	b.bodies = append(b.bodies, hb)
	b.heartbeatLen = len(b.bodies)
	delay, code, remark, version := b.answerDelay, b.code, b.remark, b.version
	support, show, subChange := b.supportV2, b.showFlags, b.subChange
	b.mu.Unlock()

	// The delay models a broker whose beat is still in flight while the next
	// heartbeat round comes around.
	if delay > 0 {
		time.Sleep(delay)
	}

	resp := remoting.CreateResponseCommand(code, remark)
	resp.Version = version
	if show {
		resp.AddExtField(remoting.ExtIsSupportHeartBeatV2, strconv.FormatBool(support))
		resp.AddExtField(remoting.ExtIsSubChange, strconv.FormatBool(subChange))
	}
	return resp
}

// setFlags flips how the broker answers the next beat.
func (b *v2Broker) setFlags(show, support, subChange bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.showFlags, b.supportV2, b.subChange = show, support, subChange
}

func (b *v2Broker) setCode(code int32, remark string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.code, b.remark = code, remark
}

func (b *v2Broker) setVersion(version int32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.version = version
}

func (b *v2Broker) setAnswerDelay(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.answerDelay = d
}

// beats returns the recorded heartbeat bodies.
func (b *v2Broker) beats() []*remoting.HeartbeatData {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*remoting.HeartbeatData(nil), b.bodies...)
}

func (b *v2Broker) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.bodies)
}

// last returns the most recent body, failing the test when there is none.
func (b *v2Broker) last(t *testing.T) *remoting.HeartbeatData {
	t.Helper()
	got := b.beats()
	if len(got) == 0 {
		t.Fatalf("no heartbeat reached the broker at %s", b.srv.addr)
	}
	return got[len(got)-1]
}

// ------------------------------------------------------------------ fixture

// v2Cluster is one nameserver serving a master(+slave) route plus the matching
// mock brokers, and an instance wired to them with one subscribed consumer.
type v2Cluster struct {
	inst   *Instance
	stub   *stubConsumer
	master *v2Broker
	slave  *v2Broker
	topic  string
	group  string
}

// newV2Cluster starts the cluster. useV2 is ClientConfig.useHeartbeatV2;
// withSlave adds a second broker address that answers like a pre-V2 broker
// unless the test says otherwise.
func newV2Cluster(t *testing.T, useV2, withSlave bool) *v2Cluster {
	t.Helper()
	master := newV2Broker(t)
	master.setFlags(true, true, false) // a V2-capable, up-to-date broker by default
	slave := master
	body := routeBodyFor(master.srv.addr, "")
	if withSlave {
		slave = newV2Broker(t)
		slave.setFlags(true, true, false)
		body = routeBodyFor(master.srv.addr, slave.srv.addr)
	}
	ns := startMockServer(t, nameserverOnReq(map[string]string{"V2Topic": body}))

	cfg := testConfig()
	cfg.UseHeartbeatV2 = useV2
	inst := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, cfg)
	t.Cleanup(inst.Shutdown)
	warmRoute(t, inst, "V2Topic")

	c := &v2Cluster{
		inst: inst, master: master, slave: slave,
		topic: "V2Topic", group: "CG",
		stub: &stubConsumer{group: "CG", subs: []*remoting.SubscriptionData{
			{Topic: "V2Topic", SubString: "TagA", ExpressionType: remoting.ExpressionTypeTag,
				TagsSet: []string{"TagA"}},
		}},
	}
	inst.RegisterConsumer("CG", c.stub)
	return c
}

// beat drives one heartbeat fan-out through the same entry point the periodic
// task and a rebalance use.
func (c *v2Cluster) beat(t *testing.T, isRebalance bool) int {
	t.Helper()
	return c.inst.SendHeartbeatToAllBrokerWithLockV2(isRebalance, 2000)
}

// subCount is the subscription count carried by the Nth recorded beat.
func subCount(t *testing.T, hb *remoting.HeartbeatData, group string) int {
	t.Helper()
	for _, cd := range hb.ConsumerDataSet {
		if cd.GroupName == group {
			return len(cd.SubscriptionDataSet)
		}
	}
	t.Fatalf("heartbeat body has no consumer data for %s: %+v", group, hb.ConsumerDataSet)
	return -1
}

func consumerOf(t *testing.T, hb *remoting.HeartbeatData, group string) *remoting.ConsumerData {
	t.Helper()
	for _, cd := range hb.ConsumerDataSet {
		if cd.GroupName == group {
			return cd
		}
	}
	t.Fatalf("heartbeat body has no consumer data for %s: %+v", group, hb.ConsumerDataSet)
	return nil
}

// ------------------------------------------------------------------- switch off

func TestInstanceHeartbeatV2OffStaysOnThePlainPath(t *testing.T) {
	c := newV2Cluster(t, false, false)

	// The broker is V2-capable, but the switch is off: the beat must be the old
	// one, byte for byte.
	if n := c.beat(t, false); n != 1 {
		t.Fatalf("beat successes = %d, want 1", n)
	}
	hb := c.master.last(t)
	if hb.HeartbeatFingerprint != 0 {
		t.Errorf("fingerprint = %d, want 0 on the plain path", hb.HeartbeatFingerprint)
	}
	if hb.WithoutSub {
		t.Error("withoutSub must stay false on the plain path")
	}
	if got := subCount(t, hb, c.group); got != 1 {
		t.Errorf("subscriptions = %d, want the subscription included", got)
	}
	if hb.ClientID != c.inst.ClientID() {
		t.Errorf("clientID = %q, want %q", hb.ClientID, c.inst.ClientID())
	}

	// Nothing about the V2 bookkeeping may move while the switch is off.
	if c.inst.brokerSupportsHeartbeatV2(c.master.srv.addr) {
		t.Error("a plain beat must not mark the broker V2-capable")
	}
	if got := c.inst.cachedHeartbeatFingerprint(c.master.srv.addr); got != 0 {
		t.Errorf("stored fingerprint = %d, want 0", got)
	}

	// A second beat is still a plain beat (no state to consult).
	c.beat(t, false)
	if hb := c.master.last(t); hb.HeartbeatFingerprint != 0 || hb.WithoutSub {
		t.Errorf("second plain beat changed shape: fp=%d withoutSub=%v",
			hb.HeartbeatFingerprint, hb.WithoutSub)
	}
}

func TestInstanceHeartbeatV2OffIgnoresRebalanceFlag(t *testing.T) {
	c := newV2Cluster(t, false, false)
	c.beat(t, false)
	if n := c.beat(t, true); n != 1 {
		t.Fatalf("rebalance beat with the switch off = %d successes, want 1", n)
	}
	hb := c.master.last(t)
	if hb.HeartbeatFingerprint != 0 || hb.WithoutSub || subCount(t, hb, c.group) != 1 {
		t.Errorf("rebalance must not change the plain body: fp=%d withoutSub=%v subs=%d",
			hb.HeartbeatFingerprint, hb.WithoutSub, subCount(t, hb, c.group))
	}
}

// ---------------------------------------------------------------- old broker

func TestInstanceHeartbeatV2OldBrokerAlwaysGetsSubscriptions(t *testing.T) {
	c := newV2Cluster(t, true, false)
	// SUCCESS with no ext fields at all: every broker predating V2, and any stub.
	c.master.setFlags(false, false, false)

	for beat := 1; beat <= 3; beat++ {
		if n := c.beat(t, false); n != 1 {
			t.Fatalf("beat %d successes = %d, want 1", beat, n)
		}
		hb := c.master.last(t)
		if hb.HeartbeatFingerprint == 0 {
			t.Errorf("beat %d must still carry the fingerprint the switch asked for", beat)
		}
		if hb.WithoutSub {
			t.Errorf("beat %d dropped subscriptions for a broker that never claimed V2", beat)
		}
		if got := subCount(t, hb, c.group); got != 1 {
			t.Errorf("beat %d subscriptions = %d, want 1", beat, got)
		}
		if c.inst.brokerSupportsHeartbeatV2(c.master.srv.addr) {
			t.Errorf("beat %d marked a flag-less broker as V2-capable", beat)
		}
		if got := c.inst.cachedHeartbeatFingerprint(c.master.srv.addr); got != 0 {
			t.Errorf("beat %d stored fingerprint %d for a V1 broker", beat, got)
		}
	}

	// A broker that claims NOT to support V2 (explicit false) behaves the same.
	c.master.setFlags(true, false, false)
	c.beat(t, false)
	if hb := c.master.last(t); hb.WithoutSub || subCount(t, hb, c.group) != 1 {
		t.Error("an explicit IS_SUPPORT_HEART_BEAT_V2=false must keep the full body")
	}
	if c.inst.brokerSupportsHeartbeatV2(c.master.srv.addr) {
		t.Error("IS_SUPPORT_HEART_BEAT_V2=false must not join the V2 set")
	}
}

// ---------------------------------------------------------------- happy path

func TestInstanceHeartbeatV2SecondBeatDropsSubscriptions(t *testing.T) {
	c := newV2Cluster(t, true, false)

	// beat 1: the first beat to an unknown address always carries subscriptions.
	c.beat(t, false)
	first := c.master.last(t)
	if first.WithoutSub || subCount(t, first, c.group) != 1 {
		t.Fatalf("first beat must carry the subscriptions: withoutSub=%v subs=%d",
			first.WithoutSub, subCount(t, first, c.group))
	}
	fingerprint := int32(first.HeartbeatFingerprint)
	if fingerprint == 0 {
		t.Fatal("with the switch on the body must carry a non-zero fingerprint")
	}
	if !c.inst.brokerSupportsHeartbeatV2(c.master.srv.addr) {
		t.Fatal("IS_SUPPORT_HEART_BEAT_V2=true must join the V2 set")
	}
	if got := c.inst.cachedHeartbeatFingerprint(c.master.srv.addr); got != fingerprint {
		t.Fatalf("stored fingerprint = %d, want the acknowledged %d", got, fingerprint)
	}

	// beat 2: unchanged subscriptions, so the body drops them and keeps the
	// SAME fingerprint.
	c.beat(t, false)
	second := c.master.last(t)
	if !second.WithoutSub {
		t.Error("second beat must send the without-subscription body")
	}
	if got := subCount(t, second, c.group); got != 0 {
		t.Errorf("without-sub beat still carries %d subscriptions", got)
	}
	if int32(second.HeartbeatFingerprint) != fingerprint {
		t.Errorf("without-sub fingerprint = %d, want the same %d",
			second.HeartbeatFingerprint, fingerprint)
	}
	if second.ClientID != c.inst.ClientID() {
		t.Errorf("without-sub body lost the clientID: %q", second.ClientID)
	}
	if cd := consumerOf(t, second, c.group); cd.ConsumeType != remoting.ConsumeTypeConsumePassively ||
		cd.MessageModel != remoting.MessageModelClustering {
		t.Errorf("without-sub body lost the consumer identity: %+v", cd)
	}

	// The fingerprint is stable while nothing changes, so the suppression keeps
	// working beat after beat.
	c.beat(t, false)
	if got := c.master.last(t); !got.WithoutSub || int32(got.HeartbeatFingerprint) != fingerprint {
		t.Errorf("third beat went back to the full body: withoutSub=%v fp=%d",
			got.WithoutSub, got.HeartbeatFingerprint)
	}
}

func TestInstanceHeartbeatV2SubChangeDropsFingerprint(t *testing.T) {
	c := newV2Cluster(t, true, false)

	c.beat(t, false)
	stored := int32(c.master.last(t).HeartbeatFingerprint)

	// The broker reports the group's fingerprint differs from the one it holds:
	// the cached value must go, so the NEXT beat re-reports subscriptions. Note
	// that this beat itself was still a suppressed one — the client had no way of
	// knowing before it got the answer.
	c.master.setFlags(true, true, true)
	c.beat(t, false)
	if got := c.inst.cachedHeartbeatFingerprint(c.master.srv.addr); got != 0 {
		t.Errorf("IS_SUB_CHANGE=true left fingerprint %d stored, want it dropped", got)
	}
	if !c.inst.brokerSupportsHeartbeatV2(c.master.srv.addr) {
		t.Error("a sub-change answer still comes from a V2 broker; it must stay in the V2 set")
	}

	c.beat(t, false)
	hb := c.master.last(t)
	if hb.WithoutSub || subCount(t, hb, c.group) != 1 {
		t.Errorf("the beat after IS_SUB_CHANGE=true must carry the subscriptions again: "+
			"withoutSub=%v subs=%d", hb.WithoutSub, subCount(t, hb, c.group))
	}

	// Once the broker stops reporting a change the suppression resumes.
	c.master.setFlags(true, true, false)
	c.beat(t, false)
	if got := c.inst.cachedHeartbeatFingerprint(c.master.srv.addr); got == 0 {
		t.Fatal("an accepted fingerprint must be stored again")
	}
	c.beat(t, false)
	if hb := c.master.last(t); !hb.WithoutSub {
		t.Errorf("after the broker accepted %d the next beat must drop subscriptions, got fp=%d",
			stored, hb.HeartbeatFingerprint)
	}
}

func TestInstanceHeartbeatV2SubscriptionChangeResendsBody(t *testing.T) {
	c := newV2Cluster(t, true, false)

	c.beat(t, false)
	c.beat(t, false)
	if hb := c.master.last(t); !hb.WithoutSub {
		t.Fatal("precondition: the second beat is the without-sub one")
	}
	before := int32(c.master.last(t).HeartbeatFingerprint)

	// Subscribe a second topic: the fingerprint moves, so the subscriptions must
	// travel again on the very next beat.
	c.stub.mu.Lock()
	c.stub.subs = append(c.stub.subs, &remoting.SubscriptionData{
		Topic: "AnotherTopic", SubString: "*", ExpressionType: remoting.ExpressionTypeTag})
	c.stub.mu.Unlock()

	c.beat(t, false)
	hb := c.master.last(t)
	if hb.WithoutSub {
		t.Error("a changed subscription set must be sent WITH subscriptions")
	}
	if got := subCount(t, hb, c.group); got != 2 {
		t.Errorf("subscriptions = %d, want both", got)
	}
	after := int32(hb.HeartbeatFingerprint)
	if after == before {
		t.Error("the fingerprint did not move when a subscription was added")
	}
	if got := c.inst.cachedHeartbeatFingerprint(c.master.srv.addr); got != after {
		t.Errorf("stored fingerprint = %d, want the new %d", got, after)
	}

	c.beat(t, false)
	if hb := c.master.last(t); !hb.WithoutSub || int32(hb.HeartbeatFingerprint) != after {
		t.Error("the suppressed beat must reuse the NEW fingerprint")
	}
}

func TestInstanceHeartbeatV2RebalanceResetsFingerprints(t *testing.T) {
	c := newV2Cluster(t, true, false)

	c.beat(t, false)
	c.beat(t, false)
	if hb := c.master.last(t); !hb.WithoutSub {
		t.Fatal("precondition: subscriptions are being suppressed")
	}

	// The rebalance entry point invalidates every cached fingerprint: the broker
	// must be handed the subscriptions again for the queues this client just
	// picked up.
	c.beat(t, true)
	hb := c.master.last(t)
	if hb.WithoutSub || subCount(t, hb, c.group) != 1 {
		t.Errorf("the rebalance beat must carry subscriptions: withoutSub=%v subs=%d",
			hb.WithoutSub, subCount(t, hb, c.group))
	}
	if got := c.inst.cachedHeartbeatFingerprint(c.master.srv.addr); got == 0 {
		t.Error("the rebalance beat must store the fingerprint it just reported")
	}

	c.beat(t, false)
	if hb := c.master.last(t); !hb.WithoutSub {
		t.Error("suppression must resume after the rebalance beat was accepted")
	}

	// ResetBrokerHeartbeatFingerprints is the same lever on its own.
	c.inst.ResetBrokerHeartbeatFingerprints()
	if got := c.inst.cachedHeartbeatFingerprint(c.master.srv.addr); got != 0 {
		t.Errorf("reset left fingerprint %d cached", got)
	}
	c.beat(t, false)
	if hb := c.master.last(t); hb.WithoutSub {
		t.Error("after an explicit reset the next beat must report subscriptions")
	}
}

func TestInstanceHeartbeatV2BrokerDowngradeGoesBackToFullBody(t *testing.T) {
	c := newV2Cluster(t, true, false)

	c.beat(t, false)
	c.beat(t, false)
	if hb := c.master.last(t); !hb.WithoutSub {
		t.Fatal("precondition: the address is treated as V2")
	}

	// The broker comes back without V2 (no ext fields at all). The beat that was
	// already in flight was the suppressed one — the client had no way of knowing
	// beforehand — but the address must leave the V2 set and lose its cached
	// fingerprint, so subscriptions travel again from the next beat on instead of
	// being suppressed forever.
	c.master.setFlags(false, false, false)
	c.beat(t, false)
	if c.inst.brokerSupportsHeartbeatV2(c.master.srv.addr) {
		t.Error("the downgraded address must leave the V2 set")
	}
	if got := c.inst.cachedHeartbeatFingerprint(c.master.srv.addr); got != 0 {
		t.Errorf("the downgraded address kept fingerprint %d", got)
	}

	c.beat(t, false)
	hb := c.master.last(t)
	if hb.WithoutSub || subCount(t, hb, c.group) != 1 {
		t.Error("a downgraded broker must get the subscription-carrying body")
	}
	if hb.HeartbeatFingerprint == 0 {
		t.Error("the switch is still on, so the full body keeps carrying its fingerprint")
	}

	// And it stays in full-body mode for as long as it claims nothing.
	c.beat(t, false)
	if hb := c.master.last(t); hb.WithoutSub || subCount(t, hb, c.group) != 1 {
		t.Error("a broker that stopped claiming V2 must keep receiving subscriptions")
	}
}

// ---------------------------------------------------------------- per address

func TestInstanceHeartbeatV2TracksEachAddressSeparately(t *testing.T) {
	c := newV2Cluster(t, true, true)
	if c.slave.srv.addr == c.master.srv.addr {
		t.Fatal("fixture must give the slave its own address")
	}
	// The slave is an old broker.
	c.slave.setFlags(false, false, false)

	c.beat(t, false)
	for _, b := range []*v2Broker{c.master, c.slave} {
		if hb := b.last(t); hb.WithoutSub || subCount(t, hb, c.group) != 1 {
			t.Errorf("first beat to %s must carry subscriptions", b.srv.addr)
		}
	}

	c.beat(t, false)
	if hb := c.master.last(t); !hb.WithoutSub {
		t.Errorf("the V2 master must have its subscriptions suppressed, got withoutSub=%v",
			hb.WithoutSub)
	}
	if hb := c.slave.last(t); hb.WithoutSub || subCount(t, hb, c.group) != 1 {
		t.Error("the old slave must keep receiving subscriptions the whole time")
	}
	if !c.inst.brokerSupportsHeartbeatV2(c.master.srv.addr) ||
		c.inst.brokerSupportsHeartbeatV2(c.slave.srv.addr) {
		t.Error("the V2 set must be per address")
	}

	// Beat counts: both addresses got exactly two beats, so the fan-out is the
	// same master+slave shape the plain path uses.
	for _, b := range []*v2Broker{c.master, c.slave} {
		if got := b.count(); got != 2 {
			t.Errorf("%s received %d beats, want 2", b.srv.addr, got)
		}
	}
}

// ------------------------------------------------------------------- failures

func TestInstanceHeartbeatV2ErrorLeavesStateAlone(t *testing.T) {
	c := newV2Cluster(t, true, false)

	c.beat(t, false)
	fingerprint := c.inst.cachedHeartbeatFingerprint(c.master.srv.addr)
	if fingerprint == 0 {
		t.Fatal("precondition: the first beat stored a fingerprint")
	}
	c.inst.forgetHeartbeatV2Broker(c.master.srv.addr)

	c.master.setCode(remoting.RespSystemError, "broker had a bad day")
	if n := c.beat(t, false); n != 0 {
		t.Fatalf("a rejected beat must count 0 successes, got %d", n)
	}
	if got := c.inst.cachedHeartbeatFingerprint(c.master.srv.addr); got != 0 {
		t.Errorf("a failed beat stored fingerprint %d", got)
	}
	if c.inst.brokerSupportsHeartbeatV2(c.master.srv.addr) {
		t.Error("a failed beat must not join the V2 set either")
	}

	// The failure is not sticky: as soon as the broker answers, the state machine
	// resumes with the subscription-carrying beat.
	c.master.setCode(remoting.RespSuccess, "")
	c.beat(t, false)
	hb := c.master.last(t)
	if hb.WithoutSub || subCount(t, hb, c.group) != 1 {
		t.Error("after a failure the next beat must report subscriptions")
	}
	if got := c.inst.cachedHeartbeatFingerprint(c.master.srv.addr); got != fingerprint {
		t.Errorf("recovered fingerprint = %d, want the same %d", got, fingerprint)
	}

	// A transport failure behaves the same way (no address recorded at all).
	drop := newV2Broker(t)
	drop.srv.setDrop(true)
	cfg := testConfig()
	cfg.UseHeartbeatV2 = true
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		"V2Drop": routeBodyFor(drop.srv.addr, ""),
	}))
	dropping := CreateOrGetInstance(uniqueClientID(t), []string{ns.addr}, cfg)
	t.Cleanup(dropping.Shutdown)
	warmRoute(t, dropping, "V2Drop")
	dropping.RegisterConsumer("CG", &stubConsumer{group: "CG", subs: []*remoting.SubscriptionData{
		{Topic: "V2Drop", SubString: "*", ExpressionType: remoting.ExpressionTypeTag}}})
	if n := dropping.SendHeartbeatToAllBrokerWithLockV2(false, 500); n != 0 {
		t.Errorf("a broker that drops the connection must give 0 successes, got %d", n)
	}
	if _, ok := dropping.BrokerVersion(drop.srv.addr); ok {
		t.Error("a beat that never completed must not record a broker version")
	}
}

func TestInstanceHeartbeatV2WithoutConsumersSendsNothing(t *testing.T) {
	c := newV2Cluster(t, true, false)
	c.inst.UnregisterConsumer(c.group)

	if n := c.beat(t, false); n != 0 {
		t.Errorf("no consumers must mean no beat, got %d", n)
	}
	if got := c.master.count(); got != 0 {
		t.Errorf("the broker received %d beats without a consumer", got)
	}
	if c.inst.brokerSupportsHeartbeatV2(c.master.srv.addr) {
		t.Error("an empty beat must not touch the V2 set")
	}
}

func TestInstanceHeartbeatV2ConcurrentBeatIsSkipped(t *testing.T) {
	c := newV2Cluster(t, true, false)
	c.master.setAnswerDelay(300 * time.Millisecond)

	// The reference guards the fan-out with lockHeartbeat.tryLock(): a caller
	// that finds it busy skips its beat instead of queueing behind it.
	done := make(chan int, 1)
	go func() { done <- c.beat(t, false) }()

	waitFor(t, "the slow beat to reach the broker", func() bool {
		return c.master.count() == 1
	})

	if n := c.beat(t, false); n != 0 {
		t.Error("a beat concurrent with one in flight must be skipped, not queued")
	}
	if got := c.master.count(); got != 1 {
		t.Errorf("the skipped beat sent %d extra requests anyway", got-1)
	}

	first := <-done
	if first != 1 {
		t.Fatalf("the in-flight beat reported %d successes", first)
	}
	c.master.setAnswerDelay(0)
	if n := c.beat(t, false); n != 1 {
		t.Errorf("after the lock is free the beat must run again, got %d", n)
	}
}

// ------------------------------------------------------------- version table

func TestInstanceHeartbeatRecordsBrokerVersion(t *testing.T) {
	const seenVersion = int32(450)

	// The version rides the response header on BOTH paths: the plain beat must
	// fill the table exactly like the V2 one, so the heartbeat switch never
	// decides whether a broker's version is known.
	for _, useV2 := range []bool{false, true} {
		c := newV2Cluster(t, useV2, false)
		c.master.setVersion(seenVersion)
		if _, ok := c.inst.BrokerVersion(c.master.srv.addr); ok {
			t.Fatalf("useV2=%v: nothing has been sent yet, but a version is known", useV2)
		}
		if n := c.beat(t, false); n != 1 {
			t.Fatalf("useV2=%v: beat successes = %d", useV2, n)
		}
		if got, ok := c.inst.BrokerVersion(c.master.srv.addr); !ok || got != seenVersion {
			t.Errorf("useV2=%v: BrokerVersion = %d/%v, want %d/true", useV2, got, ok, seenVersion)
		}

		// A later beat with a different version overwrites it, and an unknown
		// address stays unknown.
		c.master.setVersion(seenVersion + 1)
		c.beat(t, false)
		if got, ok := c.inst.BrokerVersion(c.master.srv.addr); !ok || got != seenVersion+1 {
			t.Errorf("useV2=%v: refreshed BrokerVersion = %d/%v", useV2, got, ok)
		}
		if _, ok := c.inst.BrokerVersion("10.9.9.9:10911"); ok {
			t.Errorf("useV2=%v: an address that never answered must be unknown", useV2)
		}
	}
}

// ------------------------------------------------- prepared bodies (public API)

func TestInstancePrepareHeartbeatDataWithoutSubscriptions(t *testing.T) {
	c := newV2Cluster(t, true, false)

	full := c.inst.PrepareHeartbeatData()
	if full.WithoutSub || full.HeartbeatFingerprint != 0 {
		t.Errorf("the plain prepared body is not V1-shaped: withoutSub=%v fp=%d",
			full.WithoutSub, full.HeartbeatFingerprint)
	}
	if subCount(t, full, c.group) != 1 {
		t.Error("the plain prepared body must carry the subscriptions")
	}

	// The V2 sibling: same consumers and client id, subscriptions emptied.
	clone := c.inst.PrepareHeartbeatDataWithoutSubscriptions()
	if !clone.WithoutSub {
		t.Error("PrepareHeartbeatDataWithoutSubscriptions must set withoutSub")
	}
	if clone.ClientID != full.ClientID {
		t.Errorf("clone clientID = %q, want %q", clone.ClientID, full.ClientID)
	}
	if len(clone.ConsumerDataSet) != len(full.ConsumerDataSet) {
		t.Errorf("consumer count changed: %d vs %d",
			len(clone.ConsumerDataSet), len(full.ConsumerDataSet))
	}
	if got := subCount(t, clone, c.group); got != 0 {
		t.Errorf("the without-sub body carries %d subscriptions", got)
	}
	if full.ConsumerDataSet[0] == clone.ConsumerDataSet[0] {
		t.Error("the prepared bodies must not alias each other")
	}

	// The fingerprint pair: the same value on both bodies is what lets the broker
	// compare a group's beats.
	fingerprint := full.ComputeHeartbeatFingerprint()
	full.HeartbeatFingerprint = int64(fingerprint)
	stamped := full.CloneWithoutSubscriptions()
	if stamped.HeartbeatFingerprint != full.HeartbeatFingerprint {
		t.Errorf("the without-sub body fingerprint = %d, want %d",
			stamped.HeartbeatFingerprint, full.HeartbeatFingerprint)
	}
	if fingerprint == 0 {
		t.Error("a body with a subscription must not fingerprint to 0")
	}
}
