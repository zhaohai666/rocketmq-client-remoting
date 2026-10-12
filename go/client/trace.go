package client

import (
	"strconv"
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

const (
	TraceGroupNamePrefix = "_INNER_TRACE_PRODUCER"
	TraceContentSplitor  = "\u0001"
	TraceFieldSplitor    = "\u0002"
	TraceInstanceName    = "PID_CLIENT_INNER_TRACE_PRODUCER"
	TraceTopicPrefix     = "rmq_sys_TRACE_DATA_"
)

const AccessChannelCloud = "CLOUD"

type TraceType string

const (
	TracePub            TraceType = "Pub"
	TraceRecall         TraceType = "Recall"
	TraceSubBefore      TraceType = "SubBefore"
	TraceSubAfter       TraceType = "SubAfter"
	TraceEndTransaction TraceType = "EndTransaction"
)

var traceLocalAddress = common.CachedIPStr()

type TraceBean struct {
	Topic            string
	MsgID            string
	OffsetMsgID      string
	Tags             string
	Keys             string
	StoreHost        string
	ClientHost       string
	StoreTime        int64
	RetryTimes       int32
	BodyLength       int
	MsgType          common.MessageType
	TransactionState string
	TransactionID    string
	// FromTransactionCheck is true when the broker's check-back produced the
	// endTransaction (see EndTransactionContext).
	FromTransactionCheck bool
}

func newTraceBean() *TraceBean {
	return &TraceBean{StoreHost: traceLocalAddress, ClientHost: traceLocalAddress}
}

type TraceContext struct {
	TraceType     TraceType
	TimeStamp     int64
	RegionID      string
	RegionName    string
	GroupName     string
	CostTime      int64
	IsSuccess     bool
	RequestID     string
	ContextCode   int32
	AccessChannel string
	TraceBeans    []*TraceBean
}

func NewTraceContext() *TraceContext {
	return &TraceContext{
		TimeStamp: common.CurrentTimeMillis(),
		IsSuccess: true,
		RequestID: common.CreateUniqID(),
	}
}

type TraceTransferBean struct {
	TransData string
	// TransKey is the set of business keys: every bean's msgId plus its KEYS
	// property split on spaces. The console resolves a message's trace by these.
	TransKey map[string]struct{}
}

// traceField reads one segment, defaulting to "" when it is missing.
func traceField(line []string, index int) string {
	if index < len(line) {
		return line[index]
	}
	return ""
}

func traceMsgType(text string) (common.MessageType, bool) {
	n, err := strconv.Atoi(text)
	if err != nil || n < 0 || n > int(common.OrderMsg) {
		return common.NormalMsg, false
	}
	return common.MessageType(n), true
}

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

func DecodeTraceDataString(traceData string) []*TraceContext {
	if traceData == "" {
		return nil
	}
	var out []*TraceContext
	for _, record := range splitWithoutTrailingEmpty(traceData, TraceFieldSplitor) {
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
	line := splitWithoutTrailingEmpty(record, TraceContentSplitor)
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
