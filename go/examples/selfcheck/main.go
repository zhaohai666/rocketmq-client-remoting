// selfcheck is the offline protocol self-test for the Go port — the counterpart
// of python/rocketmq/selfcheck.py (7 checks), cpp/examples/selfcheck.cpp (3) and
// dotnet/examples/RocketMQ.Examples/SelfCheck.cs (3).
//
//	go run ./examples/selfcheck
//
// It needs NO cluster: every check is encode -> bytes -> decode (or a byte-level
// fixture), plus the constants and ext-field spellings that break interop
// silently when they drift. That is the whole point of the tool existing in
// every port — it is the cheap gate you run before touching a live cluster, and
// it is deliberately NOT part of `go test` so a failing build is obvious.
//
// Where a round-trip alone could cancel out a symmetric bug, the check is
// byte-level instead: the magic-v2 fixture is hand-built and the Crc32 vector is
// pinned, so an encoder/decoder pair that agrees only with itself still fails.
//
// Every check prints PASS/FAIL; the process exits non-zero if any failed.
// Last line: `selfcheck: ALL PASS (PASS=<n> FAIL=<n>)`.
package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

var (
	passCount int
	failCount int
)

func check(name string, ok bool, detail string) {
	if ok {
		passCount++
		fmt.Printf("[PASS] %s\n", name)
		return
	}
	failCount++
	if detail == "" {
		fmt.Printf("[FAIL] %s\n", name)
		return
	}
	fmt.Printf("[FAIL] %s - %s\n", name, detail)
}

func main() {
	checkJSONFrame()
	checkBinaryFrame()
	checkHeartbeatKeySpelling()
	checkV2ShortKeys()
	checkMessage17Seg()
	checkLongTopicMagicV2()
	checkBatch6Seg()
	checkMessageIDRoundTrip()
	checkCrc32Vector()
	checkACLSignature()

	fmt.Println()
	if failCount == 0 {
		fmt.Printf("selfcheck: ALL PASS (PASS=%d FAIL=%d)\n", passCount, failCount)
		os.Exit(0)
	}
	fmt.Printf("selfcheck: FAILED (PASS=%d FAIL=%d)\n", passCount, failCount)
	os.Exit(1)
}

// 1) JSON frame round-trip. The serialize type is pinned rather than read from
// ROCKETMQ_SERIALIZE_TYPE, so the check cannot flip because of the shell.
func checkJSONFrame() {
	const name = "JSON frame round-trip (code/opaque/remark/extFields/body)"

	cmd := remoting.CreateRequestCommand(remoting.ReqGetRouteInfoByTopic, nil)
	cmd.SerializeTypeCurrentRPC = remoting.SerializeTypeJSON
	cmd.Remark = "hello 中文"
	cmd.AddExtField("topic", "TopicTest")
	cmd.AddExtField("namesrv", "127.0.0.1:9876")
	cmd.AddExtField("html", "a<b>&c") // must survive unescaped (Java does not escape)
	cmd.SetBody([]byte{0x01, 0x02, 0x03, 'p', 'a', 'y'})

	back, err := remoting.Decode(cmd.Encode())
	if err != nil || back == nil {
		check(name, false, fmt.Sprintf("decode: %v", err))
		return
	}
	ok := back.Code == cmd.Code &&
		back.Opaque == cmd.Opaque &&
		back.Remark == cmd.Remark &&
		!back.IsResponseType() &&
		back.SerializeTypeCurrentRPC == remoting.SerializeTypeJSON &&
		bytes.Equal(back.Body, cmd.Body) &&
		back.ExtFields().Equal(cmd.ExtFields())
	check(name, ok, fmt.Sprintf("code=%d/%d opaque=%d/%d remark=%q body=%q ext=%v",
		back.Code, cmd.Code, back.Opaque, cmd.Opaque, back.Remark, back.Body, back.ExtFields().Keys()))
}

// 2) ROCKETMQ private-binary frame round-trip. Also asserts the protocol type
// really travels in the high bits of the packed header length — that is how the
// peer picks the header codec, and getting it wrong means the broker parses a
// JSON header as binary.
func checkBinaryFrame() {
	const name = "ROCKETMQ-binary frame round-trip (protocol-type bits + remark + ext map)"

	cmd := remoting.CreateRequestCommand(remoting.ReqPullMessage, nil)
	cmd.SerializeTypeCurrentRPC = remoting.SerializeTypeRocketMQ
	cmd.Remark = "binary 字段"
	cmd.AddExtField("consumerGroup", "G")
	cmd.AddExtField("topic", "T")
	cmd.AddExtField("queueId", "3")
	cmd.SetBody([]byte("payload-binary"))

	frame := cmd.Encode()
	if len(frame) < 8 {
		check(name, false, fmt.Sprintf("frame too short: %d", len(frame)))
		return
	}
	packed := int32(frame[4])<<24 | int32(frame[5])<<16 | int32(frame[6])<<8 | int32(frame[7])
	stype := remoting.ProtocolTypeOf(packed)

	back, err := remoting.Decode(frame)
	if err != nil || back == nil {
		check(name, false, fmt.Sprintf("decode: %v (protocol type on the wire = %d)", err, stype))
		return
	}
	ok := stype == remoting.SerializeTypeRocketMQ &&
		back.Code == remoting.ReqPullMessage &&
		back.SerializeTypeCurrentRPC == remoting.SerializeTypeRocketMQ &&
		back.Remark == cmd.Remark &&
		bytes.Equal(back.Body, cmd.Body) &&
		back.ExtFields().Equal(cmd.ExtFields())
	check(name, ok, fmt.Sprintf("wire stype=%d back=%d remark=%q ext=%v",
		stype, back.SerializeTypeCurrentRPC, back.Remark, back.ExtFields().Keys()))
}

// 3) The heartbeat header's ext key is spelled `clientID` (capital ID). Java's
// reflection writes the field name verbatim; "clientId" would be silently
// dropped by the broker's fastjson2 and the client would look unnamed.
func checkHeartbeatKeySpelling() {
	const name = "HEART_BEAT header key spelling is `clientID` (exact case, single field)"

	cmd := remoting.CreateRequestCommand(remoting.ReqHeartBeat,
		&remoting.HeartbeatRequestHeader{ClientID: remoting.StrPtr("127.0.0.1@10911")})
	back, err := remoting.Decode(cmd.Encode())
	if err != nil || back == nil {
		check(name, false, fmt.Sprintf("decode: %v", err))
		return
	}
	v, present := back.GetExtField("clientID")
	ok := present && v == "127.0.0.1@10911" && back.ExtFields().Len() == 1
	check(name, ok, fmt.Sprintf("present=%v value=%q keys=%v", present, v, back.ExtFields().Keys()))
}

// 4) SendMessageRequestHeaderV2 uses the single-letter keys a..n. The check
// asserts the letter set exactly: a long-name leak would be silently dropped by
// the broker AND would not fail a value-only assertion, which is the classic way
// this mapping rots.
func checkV2ShortKeys() {
	const name = "SendMessageRequestHeaderV2 writes exactly the single-letter keys a..n"

	hdr := &remoting.SendMessageRequestHeaderV2{
		ProducerGroup:         remoting.StrPtr("PG_1"),
		Topic:                 remoting.StrPtr("TopicTest"),
		DefaultTopic:          remoting.StrPtr("TBW102"),
		DefaultTopicQueueNums: remoting.I32Ptr(4),
		QueueID:               remoting.I32Ptr(1),
		SysFlag:               remoting.I32Ptr(0),
		BornTimestamp:         remoting.I64Ptr(1700000000000),
		Flag:                  remoting.I32Ptr(0),
		ReconsumeTimes:        remoting.I32Ptr(0),
		UnitMode:              remoting.BoolPtr(false),
		Batch:                 remoting.BoolPtr(false),
	}
	cmd := remoting.CreateRequestCommand(remoting.ReqSendMessageV2, hdr)
	cmd.SerializeTypeCurrentRPC = remoting.SerializeTypeRocketMQ

	back, err := remoting.Decode(cmd.Encode())
	if err != nil || back == nil {
		check(name, false, fmt.Sprintf("decode: %v", err))
		return
	}
	// a,b,c,d,e,f,g,h,j,k,m — i (properties), l (maxReconsumeTimes) and
	// n (brokerName) are left nil, so they must NOT appear.
	want := []string{"a", "b", "c", "d", "e", "f", "g", "h", "j", "k", "m"}
	got := back.ExtFields().Keys()
	sortStrings(got)
	ok := equalStrings(got, want)
	// Round-trip the values through FromExtFields as well: a key that is written
	// but read back from the wrong slot is just as broken.
	h2 := &remoting.SendMessageRequestHeaderV2{}
	h2.FromExtFields(back.ExtFields())
	ok = ok &&
		derefStr(h2.ProducerGroup) == "PG_1" &&
		derefStr(h2.Topic) == "TopicTest" &&
		derefI32(h2.DefaultTopicQueueNums) == 4 &&
		derefI32(h2.QueueID) == 1 &&
		derefI64(h2.BornTimestamp) == 1700000000000 &&
		h2.Properties == nil && h2.MaxReconsumeTimes == nil && h2.BrokerName == nil
	check(name, ok, fmt.Sprintf("keys=%v want=%v queueId=%v bornTs=%v",
		got, want, derefI32(h2.QueueID), derefI64(h2.BornTimestamp)))
}

// 5) MessageExt 17-segment store format: encode -> decode, plus the derived
// msgId (storeHost + commitLogOffset) which is what MQAdminExt lookups key on.
func checkMessage17Seg() {
	const name = "message 17-seg round-trip (all fields + derived msgId/offsetMsgId)"

	ext := common.NewMessageExt()
	ext.Topic = "SelfCheck"
	ext.Body = []byte("hello rocketmq 中文")
	ext.Flag = 2
	ext.BodyCRC = common.Crc32(ext.Body)
	ext.QueueID = 3
	ext.SysFlag = 0
	ext.BornTimestamp = 1700000000000
	ext.StoreTimestamp = 1700000000123
	ext.BornHost, ext.BornHostPort = "127.0.0.1", 54321
	ext.StoreHost, ext.StoreHostPort = "127.0.0.1", 10911
	ext.CommitLogOffset = 1024
	ext.QueueOffset = 88
	ext.ReconsumeTimes = 1
	ext.Properties = common.NewStringMap()
	ext.Properties.Put("TAGS", "TagA")
	ext.Properties.Put("KEYS", "key1 key2")

	raw, err := common.EncodeMessageExt(ext, false)
	if err != nil {
		check(name, false, fmt.Sprintf("encode: %v", err))
		return
	}
	msgs := common.DecodeMessages(raw)
	if len(msgs) != 1 {
		check(name, false, fmt.Sprintf("decoded %d messages, want 1", len(msgs)))
		return
	}
	got := msgs[0]
	// 7F000001 00002A9F (127.0.0.1:10911) + 0000000000000400 (offset 1024).
	const wantMsgID = "7F00000100002A9F0000000000000400"

	tags, _ := got.GetProperty("TAGS")
	keys, _ := got.GetProperty("KEYS")
	ok := got.Topic == "SelfCheck" &&
		bytes.Equal(got.Body, ext.Body) &&
		got.Flag == 2 &&
		got.QueueID == 3 &&
		got.QueueOffset == 88 &&
		got.CommitLogOffset == 1024 &&
		got.BornTimestamp == 1700000000000 &&
		got.StoreTimestamp == 1700000000123 &&
		got.BornHostPort == 54321 && got.StoreHostPort == 10911 &&
		got.ReconsumeTimes == 1 &&
		got.BodyCRC == common.Crc32(ext.Body) &&
		tags == "TagA" && keys == "key1 key2" &&
		got.MsgID == wantMsgID &&
		got.OffsetMsgID == wantMsgID
	check(name, ok, fmt.Sprintf("topic=%q queueOffset=%d msgId=%s offsetMsgId=%s tags=%q keys=%q",
		got.Topic, got.QueueOffset, got.MsgID, got.OffsetMsgID, tags, keys))
}

// 6) Magic-code V2 (long topic > 255 bytes). The frame is HAND-BUILT and the
// properties blob is hard-coded: with the encoder and decoder both ours, a
// shared bug would make a round-trip check pass while a real broker still fails
// to parse us. The only structural difference from V1 is the topic-length field
// width (1 -> 2 bytes), so this fixture is also the guard on that branch.
func checkLongTopicMagicV2() {
	const name = "long-topic (>255B) magic-v2 decode from a hand-built broker frame"

	topic := "SelfCheckLongTopic" + strings.Repeat("x", 260)
	body := []byte("batch-body")
	props := []byte("TAGS\x01TagB\x02") // hard-coded on purpose (see above)

	storeSize := 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 8 +
		4 + len(body) + 2 + len(topic) + 2 + len(props)

	w := common.NewWriterCap(storeSize)
	w.I32(int32(storeSize))          // 1 TOTALSIZE
	w.I32(common.MessageMagicCodeV2) // 2 MAGICCODE
	w.U32(common.Crc32(body))        // 3 BODYCRC
	w.I32(0)                         // 4 QUEUEID
	w.I32(0)                         // 5 FLAG
	w.I64(0)                         // 6 QUEUEOFFSET
	w.I64(2048)                      // 7 PHYSICALOFFSET
	w.I32(0)                         // 8 SYSFLAG
	w.I64(1700000000000)             // 9 BORNTIMESTAMP
	w.Bytes([]byte{127, 0, 0, 1})    // 10 BORNHOST ip
	w.U32(54321)                     // 10 BORNHOST port
	w.I64(1700000000123)             // 11 STORETIMESTAMP
	w.Bytes([]byte{127, 0, 0, 1})    // 12 STOREHOST ip
	w.U32(10911)                     // 12 STOREHOST port
	w.I32(0)                         // 13 RECONSUMETIMES
	w.I64(0)                         // 14 PREPARED TX OFFSET
	w.I32(int32(len(body)))          // 15 BODY length
	w.Bytes(body)                    // 15 BODY
	w.U16(uint32(len(topic)))        // 16 TOPIC length (2 bytes: the v2 difference)
	w.Bytes([]byte(topic))           // 16 TOPIC
	w.U16(uint32(len(props)))        // 17 PROPERTIES length
	w.Bytes(props)                   // 17 PROPERTIES

	msgs := common.DecodeMessages(w.IntoInner())
	if len(msgs) != 1 {
		check(name, false, fmt.Sprintf("decoded %d messages, want 1", len(msgs)))
		return
	}
	got := msgs[0]
	tags, _ := got.GetProperty("TAGS")
	ok := got.Topic == topic &&
		bytes.Equal(got.Body, body) &&
		tags == "TagB" &&
		got.StoreHostPort == 10911 &&
		got.BornHostPort == 54321
	check(name, ok, fmt.Sprintf("topicLen(want %d, got %d) body=%q tags=%q storePort=%d",
		len(topic), len(got.Topic), got.Body, tags, got.StoreHostPort))
}

// 7) Batch container: 6-segment light frames concatenated. The light frame
// carries no topic (the enclosing batch does) so only body/flag/properties come
// back — asserting a topic here would be asserting a field the format does not
// have.
func checkBatch6Seg() {
	const name = "batch 6-seg round-trip (bodies + TAGS, no topic by design)"

	raw := common.EncodeMessages([]*common.Message{
		common.NewMessageWithTags("SelfCheck", []byte("m1"), "TagA", "", 0),
		common.NewMessageWithTags("SelfCheck", []byte("m2"), "TagA", "", 0),
	})
	if n := common.CountInnerMsgNum(raw); n != 2 {
		check(name, false, fmt.Sprintf("countInnerMsgNum = %d, want 2", n))
		return
	}
	msgs := common.DecodeBatchMessages(raw)
	if len(msgs) != 2 {
		check(name, false, fmt.Sprintf("decoded %d messages, want 2", len(msgs)))
		return
	}
	ok := bytes.Equal(msgs[0].Body, []byte("m1")) &&
		bytes.Equal(msgs[1].Body, []byte("m2"))
	for _, m := range msgs {
		tags, _ := m.GetProperty("TAGS")
		ok = ok && tags == "TagA"
	}
	check(name, ok, fmt.Sprintf("count=%d bodies=%q,%q",
		len(msgs), msgs[0].Body, msgs[1].Body))
}

// 8) msgId <-> (ip, port, commitLogOffset). This is the join key between a
// SendResult and the admin query APIs, and it is derived, not transmitted.
func checkMessageIDRoundTrip() {
	const name = "msgId derive/decode round-trip (ip, port, commitLogOffset)"

	addr, err := common.IPAndPortToBytes("127.0.0.1", 10911, false)
	if err != nil {
		check(name, false, fmt.Sprintf("addr: %v", err))
		return
	}
	id := common.CreateMessageID(addr, 1024)
	ip, port, offset, err := common.DecodeMessageID(id)
	ok := err == nil && id == "7F00000100002A9F0000000000000400" &&
		ip == "127.0.0.1" && port == 10911 && offset == 1024
	check(name, ok, fmt.Sprintf("id=%s ip=%s port=%d offset=%d err=%v", id, ip, port, offset, err))
}

// 9) Crc32 contract. Pinned because this is the one place Go deliberately does
// NOT match Java: Java's UtilAll.crc32 returns (int)(value & 0x7FFFFFFF), i.e.
// the top bit cleared, so a Java-written BODYCRC differs from ours by 2^31 and
// cross-language CRC comparison must never be attempted (compare each side's own
// `match` field instead). BODYCRC is not checked on decode by default
// (DecodeOptions.CheckCRC=false), which is what keeps interop working.
func checkCrc32Vector() {
	const name = "Crc32 standard IEEE vectors (b\"\"=0, b\"a\"=0xE8B7BE43, b\"abc\"=0x352441C2)"

	ok := common.Crc32(nil) == 0 &&
		common.Crc32([]byte("a")) == 0xE8B7BE43 &&
		common.Crc32([]byte("abc")) == 0x352441C2
	check(name, ok, fmt.Sprintf("empty=%#x a=%#x abc=%#x",
		common.Crc32(nil), common.Crc32([]byte("a")), common.Crc32([]byte("abc"))))
}

// 10) ACL signature injection. Order matters: AccessKey/SecurityToken must be in
// extFields BEFORE the signature is computed, because the signed content is the
// sorted concatenation of every extField value except Signature, followed by the
// body. SecretKey must never go on the wire.
func checkACLSignature() {
	const name = "ACL signature injection (AccessKey/SecurityToken before Signature, no SecretKey)"

	cred := remoting.NewSessionCredentialsWithToken("AK", "SK", "token123")
	hook, err := remoting.NewAclClientRPCHook(cred)
	if err != nil {
		check(name, false, fmt.Sprintf("hook: %v", err))
		return
	}
	req := remoting.CreateRequestCommand(remoting.ReqSendMessageV2, nil)
	req.AddExtField("topic", "TopicTest")
	hook.DoBeforeRequest("127.0.0.1:9876", req)

	ak, hasAK := req.GetExtField(remoting.CredentialAccessKey)
	tok, hasTok := req.GetExtField(remoting.CredentialSecurityToken)
	sig, hasSig := req.GetExtField(remoting.CredentialSignature)
	want := remoting.CalcSignature(remoting.CombineRequestContent(req), "SK")

	ok := hasAK && ak == "AK" &&
		hasTok && tok == "token123" &&
		hasSig && sig == want &&
		!req.ExtFields().ContainsKey(remoting.CredentialSecretKey)
	check(name, ok, fmt.Sprintf("ak=%q token=%q sigMatches=%v hasSecretKey=%v keys=%v",
		ak, tok, sig == want, req.ExtFields().ContainsKey(remoting.CredentialSecretKey),
		req.ExtFields().Keys()))
}

// ---------------- small helpers (examples only; no production code) ----------------

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func derefStr(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func derefI32(p *int32) int32 {
	if p == nil {
		return -1
	}
	return *p
}

func derefI64(p *int64) int64 {
	if p == nil {
		return -1
	}
	return *p
}
