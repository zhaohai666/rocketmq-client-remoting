// Bodies that a CLIENT (not an admin) produces: the per-queue snapshots that
// ride inside ConsumerRunningInfo, and the direct-consume verdict.
//
// These answer GET_CONSUMER_RUNNING_INFO(307) and CONSUME_MESSAGE_DIRECTLY(309).
// Field NAMES and the always-present/omitted split are pinned to the 5.5.1
// jars; the probe output is quoted per struct below because a wrong key here
// is silently dropped by fastjson2 on the broker side (it reflects by Java
// property name, so `cacheMsgCount` instead of `cachedMsgCount` would just
// disappear from the console).
package remoting

// ProcessQueueInfo mirrors body.ProcessQueueInfo.
//
// Probe (fastjson2 2.0.64, all 14 keys always present because every field is a
// Java primitive):
//
//	empty  : {"cachedMsgCount":0,"cachedMsgMaxOffset":0,"cachedMsgMinOffset":0,
//	          "cachedMsgSizeInMiB":0,"commitOffset":0,"droped":false,
//	          "lastConsumeTimestamp":0,"lastLockTimestamp":0,"lastPullTimestamp":0,
//	          "locked":false,"transactionMsgCount":0,"transactionMsgMaxOffset":0,
//	          "transactionMsgMinOffset":0,"tryUnlockTimes":0}
//	filled : {... "cachedMsgSizeInMiB":3, "commitOffset":12345, "droped":true,
//	          "locked":true, "tryUnlockTimes":9 ...}
//
// Note `cachedMsgSizeInMiB` is a Java **int** (not a float): Java stores
// `(int) (msgSize / (1024 * 1024))`, so it is NOT one of the JavaDouble fields.
// Note the spelling: `droped`, one `p` — Java's field name, not a typo here.
type ProcessQueueInfo struct {
	CommitOffset int64

	CachedMsgMinOffset int64
	CachedMsgMaxOffset int64
	CachedMsgCount     int32
	CachedMsgSizeInMiB int32

	TransactionMsgMinOffset int64
	TransactionMsgMaxOffset int64
	TransactionMsgCount     int32

	Locked            bool
	TryUnlockTimes    int64
	LastLockTimestamp int64

	Droped               bool
	LastPullTimestamp    int64
	LastConsumeTimestamp int64
}

// ToJSONValue renders every field: Java primitives are never omitted.
func (p *ProcessQueueInfo) ToJSONValue() map[string]any {
	return map[string]any{
		"commitOffset": p.CommitOffset,

		"cachedMsgMinOffset": p.CachedMsgMinOffset,
		"cachedMsgMaxOffset": p.CachedMsgMaxOffset,
		"cachedMsgCount":     p.CachedMsgCount,
		"cachedMsgSizeInMiB": p.CachedMsgSizeInMiB,

		"transactionMsgMinOffset": p.TransactionMsgMinOffset,
		"transactionMsgMaxOffset": p.TransactionMsgMaxOffset,
		"transactionMsgCount":     p.TransactionMsgCount,

		"locked":            p.Locked,
		"tryUnlockTimes":    p.TryUnlockTimes,
		"lastLockTimestamp": p.LastLockTimestamp,

		"droped":               p.Droped,
		"lastPullTimestamp":    p.LastPullTimestamp,
		"lastConsumeTimestamp": p.LastConsumeTimestamp,
	}
}

// FromJSONValue reads a ProcessQueueInfo.
func (p *ProcessQueueInfo) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ProcessQueueInfo: expected object")
	}
	p.CommitOffset = jsonI64(obj, "commitOffset", 0)

	p.CachedMsgMinOffset = jsonI64(obj, "cachedMsgMinOffset", 0)
	p.CachedMsgMaxOffset = jsonI64(obj, "cachedMsgMaxOffset", 0)
	p.CachedMsgCount = jsonI32(obj, "cachedMsgCount", 0)
	p.CachedMsgSizeInMiB = jsonI32(obj, "cachedMsgSizeInMiB", 0)

	p.TransactionMsgMinOffset = jsonI64(obj, "transactionMsgMinOffset", 0)
	p.TransactionMsgMaxOffset = jsonI64(obj, "transactionMsgMaxOffset", 0)
	p.TransactionMsgCount = jsonI32(obj, "transactionMsgCount", 0)

	p.Locked = jsonBool(obj, "locked", false)
	p.TryUnlockTimes = jsonI64(obj, "tryUnlockTimes", 0)
	p.LastLockTimestamp = jsonI64(obj, "lastLockTimestamp", 0)

	p.Droped = jsonBool(obj, "droped", false)
	p.LastPullTimestamp = jsonI64(obj, "lastPullTimestamp", 0)
	p.LastConsumeTimestamp = jsonI64(obj, "lastConsumeTimestamp", 0)
	return nil
}

func (p *ProcessQueueInfo) Encode() []byte { return EncodeJSON(p.ToJSONValue()) }

// PopProcessQueueInfo mirrors body.PopProcessQueueInfo (the POP-mode sibling of
// ProcessQueueInfo, and mutually exclusive with it inside a running info).
//
// Probe: {"droped":false,"lastPopTimestamp":0,"waitAckCount":0} — three keys.
type PopProcessQueueInfo struct {
	WaitAckCount     int32
	Droped           bool
	LastPopTimestamp int64
}

// ToJSONValue renders all three keys.
func (p *PopProcessQueueInfo) ToJSONValue() map[string]any {
	return map[string]any{
		"waitAckCount":     p.WaitAckCount,
		"droped":           p.Droped,
		"lastPopTimestamp": p.LastPopTimestamp,
	}
}

// FromJSONValue reads a PopProcessQueueInfo.
func (p *PopProcessQueueInfo) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("PopProcessQueueInfo: expected object")
	}
	p.WaitAckCount = jsonI32(obj, "waitAckCount", 0)
	p.Droped = jsonBool(obj, "droped", false)
	p.LastPopTimestamp = jsonI64(obj, "lastPopTimestamp", 0)
	return nil
}

func (p *PopProcessQueueInfo) Encode() []byte { return EncodeJSON(p.ToJSONValue()) }

// CMResult mirrors org.apache.rocketmq.remoting.protocol.body.CMResult.
//
// Java ordinals (probed): CR_SUCCESS=0, CR_LATER=1, CR_ROLLBACK=2, CR_COMMIT=3,
// CR_THROW_EXCEPTION=4, CR_RETURN_NULL=5. fastjson2 serialises the enum by NAME
// (`"CR_SUCCESS"`), so the wire form is the string, not the ordinal.
type CMResult string

// CMResult values, spelled exactly as Java's enum constants.
const (
	CMResultSuccess        CMResult = "CR_SUCCESS"
	CMResultLater          CMResult = "CR_LATER"
	CMResultRollback       CMResult = "CR_ROLLBACK"
	CMResultCommit         CMResult = "CR_COMMIT"
	CMResultThrowException CMResult = "CR_THROW_EXCEPTION"
	CMResultReturnNull     CMResult = "CR_RETURN_NULL"
)

// ConsumeMessageDirectlyResult mirrors body.ConsumeMessageDirectlyResult — the
// verdict a client reports for CONSUME_MESSAGE_DIRECTLY(309).
//
// Probe:
//
//	empty  : {"autoCommit":true,"order":false,"spentTimeMills":0}
//	filled : {"autoCommit":false,"consumeResult":"CR_SUCCESS","order":true,
//	          "remark":"hello","spentTimeMills":12}
//
// `consumeResult` (null enum) and `remark` (null String) are DROPPED when unset
// — hence the pointers, which is what makes the empty body a three-key object
// rather than a five-key one with two nulls.
//
// Note the non-obvious defaults: `autoCommit` defaults to TRUE and `order` to
// false, so a zero-valued Go struct would report autoCommit=false unless built
// through NewConsumeMessageDirectlyResult.
type ConsumeMessageDirectlyResult struct {
	Order          bool
	AutoCommit     bool
	ConsumeResult  *CMResult
	Remark         *string
	SpentTimeMills int64
}

// NewConsumeMessageDirectlyResult returns the Java-default (order=false,
// autoCommit=true) instance.
func NewConsumeMessageDirectlyResult() *ConsumeMessageDirectlyResult {
	return &ConsumeMessageDirectlyResult{AutoCommit: true}
}

// ToJSONValue renders the body, omitting the two nullable fields when unset.
func (d *ConsumeMessageDirectlyResult) ToJSONValue() map[string]any {
	out := map[string]any{
		"order":          d.Order,
		"autoCommit":     d.AutoCommit,
		"spentTimeMills": d.SpentTimeMills,
	}
	if d.ConsumeResult != nil {
		out["consumeResult"] = string(*d.ConsumeResult)
	}
	if d.Remark != nil {
		out["remark"] = *d.Remark
	}
	return out
}

// FromJSONValue reads a ConsumeMessageDirectlyResult.
func (d *ConsumeMessageDirectlyResult) FromJSONValue(value any) error {
	obj, ok := value.(map[string]any)
	if !ok {
		return decodeErrf("ConsumeMessageDirectlyResult: expected object")
	}
	d.Order = jsonBool(obj, "order", false)
	d.AutoCommit = jsonBool(obj, "autoCommit", true)
	d.SpentTimeMills = jsonI64(obj, "spentTimeMills", 0)
	d.ConsumeResult = nil
	if s, ok := obj["consumeResult"].(string); ok {
		r := CMResult(s)
		d.ConsumeResult = &r
	}
	d.Remark = nil
	if s, ok := obj["remark"].(string); ok {
		d.Remark = &s
	}
	return nil
}

func (d *ConsumeMessageDirectlyResult) Encode() []byte { return EncodeJSON(d.ToJSONValue()) }

// DecodeConsumeMessageDirectlyResult parses a CONSUME_MESSAGE_DIRECTLY(309) reply.
func DecodeConsumeMessageDirectlyResult(data []byte) (*ConsumeMessageDirectlyResult, error) {
	d := NewConsumeMessageDirectlyResult()
	if len(data) == 0 {
		return d, nil
	}
	value, err := DecodeJSON(data)
	if err != nil {
		return nil, err
	}
	if err := d.FromJSONValue(value); err != nil {
		return nil, err
	}
	return d, nil
}
