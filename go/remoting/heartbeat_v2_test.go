// Heartbeat V2 protocol-level tests: the ext-field answer, the without-sub
// sibling body and the fingerprint normalisation.
//
// The wire facts these pin are the ones a V2 heartbeat gets wrong most easily:
//   - the two flags ride as TEXT and a broker that never heard of V2 simply
//     omits them, so a missing flag must read as false (a client that defaulted
//     to "supported" would stop sending subscriptions to an old broker and the
//     group would silently lose its registration);
//   - the broker dispatches on `heartbeatFingerprint != 0`, so a V1 beat must
//     keep fingerprint 0 / withoutSub false byte for byte;
//   - the fingerprint must ignore everything that differs per beat (subVersion,
//     clientId, the two V2 fields themselves) and nothing else.
package remoting

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// ------------------------------------------------------------------ helpers

func v2Sub(t *testing.T, topic, sub string) *SubscriptionData {
	t.Helper()
	sd, err := FilterAPI{}.BuildSubscriptionData(topic, sub)
	if err != nil {
		t.Fatalf("build subscription for %s/%q: %v", topic, sub, err)
	}
	return sd
}

// heartbeatOf builds a one-consumer body for the fingerprint tests.
func heartbeatOf(clientID, group string, subs ...*SubscriptionData) *HeartbeatData {
	hb := NewHeartbeatData(clientID)
	cd := NewConsumerData(group, ConsumeTypeConsumePassively, MessageModelClustering,
		ConsumeFromWhereLastOffset)
	for _, s := range subs {
		cd.AddSubscriptionData(s)
	}
	hb.AddConsumerData(cd)
	return hb
}

func heartbeatResponse(t *testing.T, version int32, ext map[string]string) *RemotingCommand {
	t.Helper()
	resp := CreateResponseCommand(RespSuccess, "")
	resp.Version = version
	for k, v := range ext {
		resp.AddExtField(k, v)
	}
	return resp
}

// ------------------------------------------------------- response ext fields

func TestDecodeHeartbeatV2ResultExtFields(t *testing.T) {
	cases := []struct {
		name      string
		ext       map[string]string
		SupportV2 bool
		SubChange bool
	}{
		{
			name:      "both true",
			ext:       map[string]string{ExtIsSupportHeartBeatV2: "true", ExtIsSubChange: "true"},
			SupportV2: true, SubChange: true,
		},
		{
			name:      "supported, no change",
			ext:       map[string]string{ExtIsSupportHeartBeatV2: "true", ExtIsSubChange: "false"},
			SupportV2: true,
		},
		{
			// The broker renders Boolean.toString(), so the only accepted spelling
			// of true is a case-insensitive "true".
			name:      "upper case true",
			ext:       map[string]string{ExtIsSupportHeartBeatV2: "TRUE", ExtIsSubChange: "True"},
			SupportV2: true,
			SubChange: true,
		},
		{
			name:      "only sub change set",
			ext:       map[string]string{ExtIsSubChange: "true"},
			SubChange: true,
		},
		{
			name: "both false",
			ext:  map[string]string{ExtIsSupportHeartBeatV2: "false", ExtIsSubChange: "false"},
		},
		{
			// Anything that is not the literal "true" parses as false, which is
			// exactly how these ext-field flags are defined on the wire.
			name: "garbage text",
			ext:  map[string]string{ExtIsSupportHeartBeatV2: "yes", ExtIsSubChange: "1"},
		},
		{
			// An old broker (or a stub) answers SUCCESS with no ext fields at all:
			// the client must stay in "send full heartbeats" mode.
			name: "no ext fields",
			ext:  nil,
		},
		{
			name: "unrelated ext field only",
			ext:  map[string]string{"SOME_OTHER": "true"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := DecodeHeartbeatV2Result(heartbeatResponse(t, 450, c.ext))
			if got.SupportV2 != c.SupportV2 {
				t.Errorf("SupportV2 = %v, want %v", got.SupportV2, c.SupportV2)
			}
			if got.SubChange != c.SubChange {
				t.Errorf("SubChange = %v, want %v", got.SubChange, c.SubChange)
			}
			// The version is read off the response header just like the V1 path
			// reads it, whether or not the flags are there.
			if got.Version != 450 {
				t.Errorf("Version = %d, want the response header version 450", got.Version)
			}
		})
	}
}

func TestDecodeHeartbeatV2ResultNilResponse(t *testing.T) {
	got := DecodeHeartbeatV2Result(nil)
	if got != (HeartbeatV2Result{}) {
		t.Errorf("nil response must read as the zero result, got %+v", got)
	}
}

func TestHeartbeatV2ExtFieldNamesAreWireNames(t *testing.T) {
	// MixAll.IS_SUPPORT_HEART_BEAT_V2 / MixAll.IS_SUB_CHANGE verbatim: a renamed
	// constant silently turns every V2 broker into an old one.
	if ExtIsSupportHeartBeatV2 != "IS_SUPPORT_HEART_BEAT_V2" {
		t.Errorf("support flag = %q", ExtIsSupportHeartBeatV2)
	}
	if ExtIsSubChange != "IS_SUB_CHANGE" {
		t.Errorf("sub-change flag = %q", ExtIsSubChange)
	}
}

// ------------------------------------------------------ without-sub clone

func TestCloneWithoutSubscriptionsKeepsEverythingButSubs(t *testing.T) {
	hb := heartbeatOf("client@A", "CG", v2Sub(t, "Tt", "TagA || TagB"), v2Sub(t, "Other", "*"))
	cd := hb.ConsumerDataSet[0]
	cd.UnitMode = true
	hb.AddProducerData(NewProducerData("PID"))
	hb.HeartbeatFingerprint = 777

	clone := hb.CloneWithoutSubscriptions()

	if clone.WithoutSub != true {
		t.Error("the sibling body must set withoutSub")
	}
	if clone.HeartbeatFingerprint != hb.HeartbeatFingerprint {
		t.Errorf("clone fingerprint = %d, want the same %d", clone.HeartbeatFingerprint,
			hb.HeartbeatFingerprint)
	}
	if clone.ClientID != hb.ClientID {
		t.Errorf("clone clientID = %q, want %q", clone.ClientID, hb.ClientID)
	}
	if len(clone.ConsumerDataSet) != 1 || len(clone.ProducerDataSet) != 1 {
		t.Fatalf("clone sets lost: consumers=%d producers=%d",
			len(clone.ConsumerDataSet), len(clone.ProducerDataSet))
	}
	cc := clone.ConsumerDataSet[0]
	if len(cc.SubscriptionDataSet) != 0 {
		t.Errorf("the without-sub body must carry NO subscription, got %+v", cc.SubscriptionDataSet)
	}
	// Everything the broker's registerConsumerWithoutSub still needs.
	if cc.GroupName != "CG" || cc.ConsumeType != ConsumeTypeConsumePassively ||
		cc.MessageModel != MessageModelClustering || cc.ConsumeFromWhere != ConsumeFromWhereLastOffset ||
		!cc.UnitMode {
		t.Errorf("consumer identity not preserved: %+v", cc)
	}
	if clone.ProducerDataSet[0].GroupName != "PID" {
		t.Errorf("producer data not copied: %+v", clone.ProducerDataSet)
	}

	// An empty subscription list renders as [] on the wire, never as a missing
	// key the broker's fastjson2 would trip over.
	text := string(clone.Encode())
	if !strings.Contains(text, `"subscriptionDataSet":[]`) {
		t.Errorf("without-sub body keeps a stale subscription list: %s", text)
	}
	if !strings.Contains(text, `"withoutSub":true`) ||
		!strings.Contains(text, `"heartbeatFingerprint":777`) {
		t.Errorf("without-sub body shape: %s", text)
	}
	back, err := DecodeHeartbeatData(clone.Encode())
	if err != nil {
		t.Fatalf("the without-sub body must decode as a HeartbeatData: %v", err)
	}
	if !back.WithoutSub || back.HeartbeatFingerprint != 777 ||
		len(back.ConsumerDataSet[0].SubscriptionDataSet) != 0 {
		t.Errorf("round trip lost the V2 fields: %+v", back)
	}

	// The receiver must stay usable: the clone shares nothing mutable with it.
	if hb.ConsumerDataSet[0] == cc {
		t.Error("clone must not hand back the original ConsumerData pointer")
	}
	if len(hb.ConsumerDataSet[0].SubscriptionDataSet) != 2 {
		t.Error("cloning emptied the original subscription set")
	}
	if hb.WithoutSub {
		t.Error("cloning must not mark the original withoutSub")
	}
	cc.GroupName = "MUTATED"
	cc.SubscriptionDataSet = append(cc.SubscriptionDataSet, v2Sub(t, "Late", "*"))
	if hb.ConsumerDataSet[0].GroupName != "CG" ||
		len(hb.ConsumerDataSet[0].SubscriptionDataSet) != 2 {
		t.Error("the clone aliases the original body")
	}
}

func TestCloneWithoutSubscriptionsOfEmptyBody(t *testing.T) {
	clone := NewHeartbeatData("client@empty").CloneWithoutSubscriptions()
	if !clone.WithoutSub || clone.ClientID != "client@empty" {
		t.Errorf("empty body clone: %+v", clone)
	}
	if len(clone.ConsumerDataSet) != 0 || len(clone.ProducerDataSet) != 0 {
		t.Errorf("empty body clone gained entries: %+v", clone)
	}
}

// ------------------------------------------------------------- fingerprint

func TestComputeHeartbeatFingerprintIgnoresPerBeatFields(t *testing.T) {
	a := heartbeatOf("client@A", "CG", v2Sub(t, "Tt", "TagA"))
	b := heartbeatOf("client@ZZZZ", "CG", v2Sub(t, "Tt", "TagA"))

	// subVersion is a wall-clock stamp the client refreshes on every rebalance,
	// so pin two different values and require the same fingerprint.
	a.ConsumerDataSet[0].SubscriptionDataSet[0].SubVersion = 1_700_000_000_000
	b.ConsumerDataSet[0].SubscriptionDataSet[0].SubVersion = 1_700_000_999_999
	if a.ComputeHeartbeatFingerprint() != b.ComputeHeartbeatFingerprint() {
		t.Fatal("clientId and subVersion must not change the fingerprint")
	}

	// The two V2 fields themselves are normalised out.
	withFlags := heartbeatOf("client@A", "CG", v2Sub(t, "Tt", "TagA"))
	withFlags.WithoutSub = true
	withFlags.HeartbeatFingerprint = 12345
	if withFlags.ComputeHeartbeatFingerprint() != a.ComputeHeartbeatFingerprint() {
		t.Error("withoutSub / heartbeatFingerprint must be normalised out of the fingerprint")
	}

	if a.ComputeHeartbeatFingerprint() == 0 {
		t.Error("a real subscription set must not hash to 0, the broker's V1 switch")
	}
}

func TestComputeHeartbeatFingerprintIsStableAcrossBeats(t *testing.T) {
	subs := []*SubscriptionData{v2Sub(t, "Tt", "TagA"), v2Sub(t, "Other", "TagB || TagC")}
	hb := heartbeatOf("client@A", "CG", subs...)
	first := hb.ComputeHeartbeatFingerprint()
	for beat := 0; beat < 5; beat++ {
		rebuilt := heartbeatOf("client@A", "CG",
			v2Sub(t, "Tt", "TagA"), v2Sub(t, "Other", "TagB || TagC"))
		if got := rebuilt.ComputeHeartbeatFingerprint(); got != first {
			t.Fatalf("beat %d fingerprint = %d, want the stable %d", beat, got, first)
		}
	}
	// Setting the fingerprint on the body it was computed from must not change
	// the answer for the next beat (the instance stamps the body in place).
	hb.HeartbeatFingerprint = int64(first)
	hb.WithoutSub = false
	if got := hb.ComputeHeartbeatFingerprint(); got != first {
		t.Errorf("fingerprint moved after stamping the body: %d vs %d", got, first)
	}
}

func TestComputeHeartbeatFingerprintIgnoresOrdering(t *testing.T) {
	// A Go map / a re-subscription gives no stable iteration order; a fingerprint
	// that depended on it would report a subscription change on every beat.
	left := NewHeartbeatData("client@A")
	left.AddConsumerData(NewConsumerData("CG", ConsumeTypeConsumePassively,
		MessageModelClustering, ConsumeFromWhereLastOffset))
	left.AddConsumerData(NewConsumerData("AG", ConsumeTypeConsumePassively,
		MessageModelClustering, ConsumeFromWhereLastOffset))
	left.ConsumerDataSet[0].AddSubscriptionData(v2Sub(t, "Tt", "TagA"))
	left.ConsumerDataSet[0].AddSubscriptionData(v2Sub(t, "Uu", "TagB"))
	left.ConsumerDataSet[1].AddSubscriptionData(v2Sub(t, "Vv", "*"))

	right := NewHeartbeatData("client@A")
	right.AddConsumerData(NewConsumerData("AG", ConsumeTypeConsumePassively,
		MessageModelClustering, ConsumeFromWhereLastOffset))
	right.AddConsumerData(NewConsumerData("CG", ConsumeTypeConsumePassively,
		MessageModelClustering, ConsumeFromWhereLastOffset))
	right.ConsumerDataSet[0].AddSubscriptionData(v2Sub(t, "Vv", "*"))
	right.ConsumerDataSet[1].AddSubscriptionData(v2Sub(t, "Uu", "TagB"))
	right.ConsumerDataSet[1].AddSubscriptionData(v2Sub(t, "Tt", "TagA"))

	if left.ComputeHeartbeatFingerprint() != right.ComputeHeartbeatFingerprint() {
		t.Errorf("consumer / subscription ORDER must not change the fingerprint: %d vs %d",
			left.ComputeHeartbeatFingerprint(), right.ComputeHeartbeatFingerprint())
	}

	// Tag order inside one expression is already sorted by FilterAPI, but a body
	// assembled from a decoded wire payload keeps wire order; the fingerprint
	// normalises that too.
	a := heartbeatOf("client@A", "CG", &SubscriptionData{
		Topic: "Tt", SubString: "TagB || TagA", ExpressionType: ExpressionTypeTag,
		TagsSet: []string{"TagB", "TagA"}, CodeSet: []int32{common.StringHash31("TagB"), common.StringHash31("TagA")},
	})
	b := heartbeatOf("client@A", "CG", &SubscriptionData{
		Topic: "Tt", SubString: "TagB || TagA", ExpressionType: ExpressionTypeTag,
		TagsSet: []string{"TagA", "TagB"}, CodeSet: []int32{common.StringHash31("TagA"), common.StringHash31("TagB")},
	})
	if a.ComputeHeartbeatFingerprint() != b.ComputeHeartbeatFingerprint() {
		t.Error("tag/code SET order must not change the fingerprint")
	}
}

func TestComputeHeartbeatFingerprintChangesWithSubscriptions(t *testing.T) {
	base := heartbeatOf("client@A", "CG", v2Sub(t, "Tt", "TagA"))
	fp := base.ComputeHeartbeatFingerprint()

	changed := map[string]*HeartbeatData{
		"a different tag":        heartbeatOf("client@A", "CG", v2Sub(t, "Tt", "TagB")),
		"one more tag":           heartbeatOf("client@A", "CG", v2Sub(t, "Tt", "TagA || TagB")),
		"a second topic":         heartbeatOf("client@A", "CG", v2Sub(t, "Tt", "TagA"), v2Sub(t, "Uu", "*")),
		"subscribe all":          heartbeatOf("client@A", "CG", v2Sub(t, "Tt", "*")),
		"a different group":      heartbeatOf("client@A", "OTHER", v2Sub(t, "Tt", "TagA")),
		"no subscription at all": NewHeartbeatData("client@A"),
	}
	for name, other := range changed {
		if got := other.ComputeHeartbeatFingerprint(); got == fp {
			t.Errorf("%s must change the fingerprint, both are %d", name, got)
		}
	}

	// The consumer identity counts too: the broker's registration depends on it.
	orderly := heartbeatOf("client@A", "CG", v2Sub(t, "Tt", "TagA"))
	orderly.ConsumerDataSet[0].ConsumeFromWhere = ConsumeFromWhereFirstOffset
	if orderly.ComputeHeartbeatFingerprint() == fp {
		t.Error("consumeFromWhere must be part of the fingerprint")
	}
	unit := heartbeatOf("client@A", "CG", v2Sub(t, "Tt", "TagA"))
	unit.ConsumerDataSet[0].UnitMode = true
	if unit.ComputeHeartbeatFingerprint() == fp {
		t.Error("unitMode must be part of the fingerprint")
	}
}

func TestComputeHeartbeatFingerprintIsUtf16StringHashOfCanonicalText(t *testing.T) {
	// The fingerprint is the 31-based string hash of the normalized JSON
	// rendering -- the same accumulation this client already uses for tag codes and
	// the queue ring. Pinning the pair keeps the two from drifting apart.
	hb := heartbeatOf("client@A", "CG", v2Sub(t, "Tt", "TagA || TagB"))
	hb.AddProducerData(NewProducerData("PID"))
	want := common.StringHash31(hb.fingerprintCanonicalText())
	if got := hb.ComputeHeartbeatFingerprint(); got != want {
		t.Errorf("fingerprint = %d, want StringHash31(canonical text) = %d", got, want)
	}

	text := hb.fingerprintCanonicalText()
	if strings.Contains(text, "client@A") {
		t.Error("the canonical text must blank the clientID")
	}
	if !strings.Contains(text, `"subVersion":0`) {
		t.Errorf("the canonical text must zero every subVersion: %s", text)
	}
	if !strings.Contains(text, `"withoutSub":false`) || !strings.Contains(text, `"heartbeatFingerprint":0`) {
		t.Errorf("the canonical text must normalise both V2 fields: %s", text)
	}
	// The canonical rendering is a fixed shape, so hashing it twice is the same
	// number no matter how the body was assembled.
	if again := common.StringHash31(hb.fingerprintCanonicalText()); again != want {
		t.Errorf("canonical text is not reproducible: %d vs %d", again, want)
	}
}

// V1 byte-compatibility: nothing in the V2 additions may move the plain body's
// shape, because a broker dispatches on fingerprint 0 to take the old
// registration path.
func TestV1HeartbeatBodyKeepsFingerprintZero(t *testing.T) {
	hb := heartbeatOf("client@A", "CG", v2Sub(t, "Tt", "TagA"))
	hb.AddProducerData(NewProducerData("PID"))

	text := string(hb.Encode())
	for _, want := range []string{
		`"heartbeatFingerprint":0`,
		`"withoutSub":false`,
		`"subscriptionDataSet":[{`,
		`"subString":"TagA"`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("plain body missing %s: %s", want, text)
		}
	}
	back, err := DecodeHeartbeatData(hb.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if back.HeartbeatFingerprint != 0 || back.WithoutSub {
		t.Errorf("decoded plain body is not V1-shaped: fp=%d withoutSub=%v",
			back.HeartbeatFingerprint, back.WithoutSub)
	}
}
