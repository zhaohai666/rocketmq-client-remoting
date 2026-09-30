// Message trace codec (Java org.apache.rocketmq.client.trace.*), the Go leg of
// the trace stack. The encoded text is what the broker stores under
// RMQ_SYS_TRACE_TOPIC and what the RocketMQ console reads back, so the field
// order is a WIRE FORMAT, not an implementation detail: tests in trace_test.go
// pin it against strings printed by the official Java encoder.
//
// Two Java split semantics are load-bearing and easy to get wrong:
//
//   - CONTENT_SPLITOR (\x01) separates fields inside one record,
//     FIELD_SPLITOR (\x02) separates records and is appended after EVERY record
//     (trailing separators included) — Java String.split drops trailing empty
//     strings, which is exactly what makes the trailing separator harmless.
//   - The decoder must therefore use Java's split rule, not Go's
//     strings.Split: Go keeps the trailing empty part. javaSplit below is the
//     one and only entry point for that.
package client

import (
	"strconv"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// Trace constants (Java org.apache.rocketmq.client.trace.TraceConstants). The
// last two are wire literals too: the internal producer's group name and the
// CLOUD trace topic prefix.
const (
	TraceGroupNamePrefix = "_INNER_TRACE_PRODUCER"
	TraceContentSplitor  = "\u0001"
	TraceFieldSplitor    = "\u0002"
	TraceInstanceName    = "PID_CLIENT_INNER_TRACE_PRODUCER"
	TraceTopicPrefix     = "rmq_sys_TRACE_DATA_"
)

// AccessChannelCloud is Java AccessChannel.CLOUD. It is reachable only when a
// caller explicitly runs a dispatcher in cloud mode; AccessChannelLocal ("LOCAL",
// consume_service.go) is the only value this port produces on its own.
//
// It changes exactly two things: a CLOUD dispatch targets
// "rmq_sys_TRACE_DATA_<region>" instead of RMQ_SYS_TRACE_TOPIC, and a SubAfter
// record on a NON-cloud channel carries two extra trailing fields (timestamp +
// groupName) — the Aliyun trace backend derives those from its envelope, so
// Java omits them for CLOUD.
const AccessChannelCloud = "CLOUD"

// TraceType is Java org.apache.rocketmq.client.trace.TraceType. The string is
// the first field of every encoded record, so these values are wire literals.
type TraceType string

const (
	TracePub            TraceType = "Pub"
	TraceRecall         TraceType = "Recall"
	TraceSubBefore      TraceType = "SubBefore"
	TraceSubAfter       TraceType = "SubAfter"
	TraceEndTransaction TraceType = "EndTransaction"
)

// traceLocalAddress is Java TraceBean's static LOCAL_ADDRESS: the default
// storeHost/clientHost on a bean the encoder did not fill in.
var traceLocalAddress = common.CachedIPStr()

// TraceBean is Java org.apache.rocketmq.client.trace.TraceBean: the traced
// message behind one record.
type TraceBean struct {
	Topic       string
	MsgID       string
	OffsetMsgID string
	Tags        string
	Keys        string
	StoreHost   string
	ClientHost  string
	StoreTime   int64
	RetryTimes  int32
	BodyLength  int
	// MsgType is written as the enum ORDINAL (see common.MessageType); the zero
	// value is NormalMsg, which is also what Java encodes for a null msgType.
	MsgType common.MessageType
	// TransactionState holds the state NAME ("COMMIT_MESSAGE", ...) — Java writes
	// transactionState.name() and the decoder reads the same string back.
	TransactionState string
	TransactionID    string
	// FromTransactionCheck is true when the broker's check-back produced the
	// endTransaction (see EndTransactionContext).
	FromTransactionCheck bool
}

func newTraceBean() *TraceBean {
	return &TraceBean{StoreHost: traceLocalAddress, ClientHost: traceLocalAddress}
}

// TraceContext is Java org.apache.rocketmq.client.trace.TraceContext: one
// trace event (a Pub, a SubBefore, ...).
type TraceContext struct {
	TraceType TraceType
	TimeStamp int64
	RegionID  string
	// RegionName is decoded but never encoded by Java's client-side codec.
	RegionName    string
	GroupName     string
	CostTime      int64
	IsSuccess     bool
	RequestID     string
	ContextCode   int32
	AccessChannel string
	TraceBeans    []*TraceBean
}

// NewTraceContext builds a context with Java's field defaults: isSuccess=true
// and a fresh requestId (MessageClientIDSetter.createUniqID). The request id is
// what lets a console join the SubBefore and SubAfter of one consume, so both
// halves of a consume MUST reuse the same context object (see the hooks).
func NewTraceContext() *TraceContext {
	return &TraceContext{
		TimeStamp: common.CurrentTimeMillis(),
		IsSuccess: true,
		RequestID: common.CreateUniqID(),
	}
}

// TraceTransferBean is Java org.apache.rocketmq.client.trace.TraceTransferBean:
// one encoded chunk plus the keys the receiver can look it up by.
type TraceTransferBean struct {
	TransData string
	// TransKey is the set of business keys: every bean's msgId plus its KEYS
	// property split on spaces. The console resolves a message's trace by these.
	TransKey map[string]struct{}
}

// traceField reads one segment, defaulting to "" when it is missing.
//
// This is a DELIBERATE robustness deviation from Java, mirroring the Python
// port: Java's decoderFromTraceDataString indexes line[7] directly, and a
// SubBefore record of a message WITHOUT keys loses that segment to the trailing
// split rule (Java drops the empty tail together with the record separator) —
// so an AIOOBE kills the whole trace message. A trace reader should not die on
// a legal record, so the missing segment reads as an empty string instead.
func traceField(line []string, index int) string {
	if index < len(line) {
		return line[index]
	}
	return ""
}

// traceMsgType parses an encoded ordinal back into a MessageType. Out-of-range
// values reject the record, the way Java's enum valueOf would throw.
func traceMsgType(text string) (common.MessageType, bool) {
	n, err := strconv.Atoi(text)
	if err != nil || n < 0 || n > int(common.OrderMsg) {
		return common.NormalMsg, false
	}
	return common.MessageType(n), true
}

// EncodeTraceContext mirrors Java TraceDataEncoder.encoderFromContextBean. A nil
// context encodes to nil (Java returns null).
//
// A context with no beans yields a nil-data transfer bean for the single-bean
// types — the dispatcher already skips those, and Java would throw; returning
// the empty bean keeps the boundary panic-free without changing any wire byte.
func EncodeTraceContext(ctx *TraceContext) *TraceTransferBean {
	if ctx == nil {
		return nil
	}
	tb := &TraceTransferBean{TransKey: map[string]struct{}{}}
	switch ctx.TraceType {
	case TracePub:
		if len(ctx.TraceBeans) == 0 {
			return tb
		}
		bean := ctx.TraceBeans[0]
		fields := []string{
			string(TracePub), i64Text(ctx.TimeStamp), ctx.RegionID, ctx.GroupName,
			bean.Topic, bean.MsgID, bean.Tags, bean.Keys, bean.StoreHost,
			strconv.Itoa(bean.BodyLength), i64Text(ctx.CostTime),
			i32Text(int32(bean.MsgType)), bean.OffsetMsgID,
			strconv.FormatBool(ctx.IsSuccess),
		}
		tb.TransData = strings.Join(fields, TraceContentSplitor) + TraceFieldSplitor
	case TraceSubBefore:
		// One record per bean: a batch consume traces every message separately
		// (they share the timestamp/region/group/requestId).
		for _, bean := range ctx.TraceBeans {
			fields := []string{
				string(TraceSubBefore), i64Text(ctx.TimeStamp), ctx.RegionID, ctx.GroupName,
				ctx.RequestID, bean.MsgID, i32Text(bean.RetryTimes), bean.Keys,
			}
			tb.TransData += strings.Join(fields, TraceContentSplitor) + TraceFieldSplitor
		}
	case TraceSubAfter:
		for _, bean := range ctx.TraceBeans {
			fields := []string{
				string(TraceSubAfter), ctx.RequestID, bean.MsgID, i64Text(ctx.CostTime),
				strconv.FormatBool(ctx.IsSuccess), bean.Keys, i32Text(ctx.ContextCode),
			}
			if ctx.AccessChannel != AccessChannelCloud {
				fields = append(fields, i64Text(ctx.TimeStamp), ctx.GroupName)
			}
			tb.TransData += strings.Join(fields, TraceContentSplitor) + TraceFieldSplitor
		}
	case TraceEndTransaction:
		if len(ctx.TraceBeans) == 0 {
			return tb
		}
		bean := ctx.TraceBeans[0]
		fields := []string{
			string(TraceEndTransaction), i64Text(ctx.TimeStamp), ctx.RegionID, ctx.GroupName,
			bean.Topic, bean.MsgID, bean.Tags, bean.Keys, bean.StoreHost,
			i32Text(int32(bean.MsgType)), bean.TransactionID, bean.TransactionState,
			strconv.FormatBool(bean.FromTransactionCheck),
		}
		tb.TransData = strings.Join(fields, TraceContentSplitor) + TraceFieldSplitor
	case TraceRecall:
		if len(ctx.TraceBeans) == 0 {
			return tb
		}
		bean := ctx.TraceBeans[0]
		fields := []string{
			string(TraceRecall), i64Text(ctx.TimeStamp), ctx.RegionID, ctx.GroupName,
			bean.Topic, bean.MsgID, strconv.FormatBool(ctx.IsSuccess),
		}
		tb.TransData = strings.Join(fields, TraceContentSplitor) + TraceFieldSplitor
	}
	for _, bean := range ctx.TraceBeans {
		tb.TransKey[bean.MsgID] = struct{}{}
		if bean.Keys != "" {
			for _, k := range strings.Split(bean.Keys, common.KeySeparator) {
				tb.TransKey[k] = struct{}{}
			}
		}
	}
	return tb
}

// DecodeTraceDataString mirrors Java decoderFromTraceDataString.
//
// Deviation, matching the Python port: a record that fails to decode is skipped
// with a log line instead of aborting the whole payload. Java lets the exception
// escape, which loses every record in the message because of one bad one.
func DecodeTraceDataString(traceData string) []*TraceContext {
	if traceData == "" {
		return nil
	}
	var out []*TraceContext
	for _, record := range javaSplit(traceData, TraceFieldSplitor) {
		if record == "" {
			continue
		}
		ctx := decodeTraceContext(record)
		if ctx != nil {
			out = append(out, ctx)
		}
	}
	return out
}

// decodeTraceContext decodes one FIELD_SPLITOR-delimited record; nil means
// "unrecognised or malformed, skip it".
func decodeTraceContext(record string) *TraceContext {
	line := javaSplit(record, TraceContentSplitor)
	if len(line) == 0 {
		return nil
	}
	switch TraceType(line[0]) {
	case TracePub:
		if len(line) < 12 {
			return nil
		}
		ts, err1 := strconv.ParseInt(line[1], 10, 64)
		bodyLen, err2 := strconv.Atoi(line[9])
		cost, err3 := strconv.ParseInt(line[10], 10, 64)
		msgType, ok := traceMsgType(line[11])
		if err1 != nil || err2 != nil || err3 != nil || !ok {
			return nil
		}
		ctx := NewTraceContext()
		ctx.TraceType = TracePub
		ctx.TimeStamp = ts
		ctx.RegionID = line[2]
		ctx.GroupName = line[3]
		bean := newTraceBean()
		bean.Topic = line[4]
		bean.MsgID = line[5]
		bean.Tags = line[6]
		bean.Keys = line[7]
		bean.StoreHost = line[8]
		bean.BodyLength = bodyLen
		ctx.CostTime = cost
		bean.MsgType = msgType
		// Version tolerance, same branches as Java/Python: 13 fields is the
		// pre-offsetMsgId layout, 14 adds it, 15+ adds clientHost.
		switch {
		case len(line) == 13:
			ctx.IsSuccess = line[12] == "true"
		case len(line) == 14:
			bean.OffsetMsgID = line[12]
			ctx.IsSuccess = line[13] == "true"
		case len(line) >= 15:
			bean.OffsetMsgID = line[12]
			ctx.IsSuccess = line[13] == "true"
			bean.ClientHost = line[14]
		}
		ctx.TraceBeans = []*TraceBean{bean}
		return ctx
	case TraceSubBefore:
		if len(line) < 7 {
			return nil
		}
		ts, err1 := strconv.ParseInt(line[1], 10, 64)
		retry, err2 := strconv.Atoi(line[6])
		if err1 != nil || err2 != nil {
			return nil
		}
		ctx := NewTraceContext()
		ctx.TraceType = TraceSubBefore
		ctx.TimeStamp = ts
		ctx.RegionID = line[2]
		ctx.GroupName = line[3]
		ctx.RequestID = line[4]
		bean := newTraceBean()
		bean.MsgID = line[5]
		bean.RetryTimes = int32(retry)
		// Segment 7 is what a keys-less message loses; see traceField.
		bean.Keys = traceField(line, 7)
		ctx.TraceBeans = []*TraceBean{bean}
		return ctx
	case TraceSubAfter:
		if len(line) < 6 {
			return nil
		}
		cost, err := strconv.ParseInt(line[3], 10, 64)
		if err != nil {
			return nil
		}
		ctx := NewTraceContext()
		ctx.TraceType = TraceSubAfter
		ctx.RequestID = line[1]
		bean := newTraceBean()
		bean.MsgID = line[2]
		bean.Keys = line[5]
		ctx.CostTime = cost
		ctx.IsSuccess = line[4] == "true"
		if len(line) >= 7 {
			if code, cerr := strconv.Atoi(line[6]); cerr == nil {
				ctx.ContextCode = int32(code)
			} else {
				return nil
			}
		}
		if len(line) >= 9 {
			ts, terr := strconv.ParseInt(line[7], 10, 64)
			if terr != nil {
				return nil
			}
			ctx.TimeStamp = ts
			ctx.GroupName = line[8]
		}
		ctx.TraceBeans = []*TraceBean{bean}
		return ctx
	case TraceEndTransaction:
		if len(line) < 13 {
			return nil
		}
		ts, err := strconv.ParseInt(line[1], 10, 64)
		if err != nil {
			return nil
		}
		msgType, ok := traceMsgType(line[9])
		if !ok {
			return nil
		}
		ctx := NewTraceContext()
		ctx.TraceType = TraceEndTransaction
		ctx.TimeStamp = ts
		ctx.RegionID = line[2]
		ctx.GroupName = line[3]
		bean := newTraceBean()
		bean.Topic = line[4]
		bean.MsgID = line[5]
		bean.Tags = line[6]
		bean.Keys = line[7]
		bean.StoreHost = line[8]
		bean.MsgType = msgType
		bean.TransactionID = line[10]
		bean.TransactionState = line[11]
		bean.FromTransactionCheck = line[12] == "true"
		ctx.TraceBeans = []*TraceBean{bean}
		return ctx
	case TraceRecall:
		if len(line) < 7 {
			return nil
		}
		ts, err := strconv.ParseInt(line[1], 10, 64)
		if err != nil {
			return nil
		}
		ctx := NewTraceContext()
		ctx.TraceType = TraceRecall
		ctx.TimeStamp = ts
		ctx.RegionID = line[2]
		ctx.GroupName = line[3]
		bean := newTraceBean()
		bean.Topic = line[4]
		bean.MsgID = line[5]
		ctx.IsSuccess = line[6] == "true"
		ctx.TraceBeans = []*TraceBean{bean}
		return ctx
	}
	return nil
}
