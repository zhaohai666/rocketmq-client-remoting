package client

import (
	"fmt"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// SendStatus mirrors Java SendStatus (Python SendStatus, Rust SendStatus).
// The numeric values are the enum ordinals, not response codes.
type SendStatus int

const (
	SendOK SendStatus = iota
	FlushDiskTimeout
	FlushSlaveTimeout
	SlaveNotAvailable
)

func (s SendStatus) String() string {
	switch s {
	case SendOK:
		return "SEND_OK"
	case FlushDiskTimeout:
		return "FLUSH_DISK_TIMEOUT"
	case FlushSlaveTimeout:
		return "FLUSH_SLAVE_TIMEOUT"
	case SlaveNotAvailable:
		return "SLAVE_NOT_AVAILABLE"
	}
	return fmt.Sprintf("SendStatus(%d)", int(s))
}

// SendResult mirrors Java SendResult / Python SendResult.
//
// msg_id vs offset_msg_id (Java MQClientAPIImpl.processSendResponse):
//   - msg_id        = the client-side unique id (UNIQ_KEY) — this is what the
//     broker and the dashboard use to join a publish trace with the consume
//     trace, and what END_TRANSACTION carries back;
//   - offset_msg_id = the header's msgId, the broker-generated offset id.
//
// Swapping the two is a silent, hard-to-debug break: everything still
// "succeeds" while traces and console lookups stop matching.
type SendResult struct {
	SendStatus    SendStatus
	MsgID         string
	MessageQueue  common.MessageQueue
	QueueOffset   int64
	TransactionID string
	OffsetMsgID   string
	// RegionID comes from the response header's MSG_REGION; absent falls back
	// to DefaultRegion (Java MixAll.DEFAULT_TRACE_REGION_ID).
	RegionID string
	// TraceOn is `extFields["TRACE_ON"] != "false"`, so absent means TRUE.
	TraceOn bool
	// RecallHandle is only set for scheduled/delayed messages; hand it back to
	// RecallMessage to withdraw the message.
	RecallHandle string
}

func (r SendResult) String() string {
	return fmt.Sprintf("SendResult(status=%s, msgId=%s, offsetMsgId=%s, queue=%s, queueOffset=%d)",
		r.SendStatus, r.MsgID, r.OffsetMsgID, r.MessageQueue, r.QueueOffset)
}

// TransactionSendResult mirrors Java TransactionSendResult: the send result of
// the half message plus the local transaction state the client decided on.
type TransactionSendResult struct {
	SendResult
	LocalTransactionState LocalTransactionState
}

// LocalTransactionState mirrors Java LocalTransactionState (Python
// LocalTransactionState, Rust LocalTransactionState).
type LocalTransactionState int32

const (
	// CommitMessage and RollbackMessage map to MessageSysFlag
	// TRANSACTION_COMMIT_TYPE / TRANSACTION_ROLLBACK_TYPE.
	CommitMessage LocalTransactionState = iota
	RollbackMessage
	// Unknow (Java's spelling) leaves the half message pending: the broker will
	// check it back. It is also what a nil/absent listener decision becomes.
	Unknow
)

func (s LocalTransactionState) String() string {
	switch s {
	case CommitMessage:
		return "COMMIT_MESSAGE"
	case RollbackMessage:
		return "ROLLBACK_MESSAGE"
	case Unknow:
		return "UNKNOW"
	}
	return fmt.Sprintf("LocalTransactionState(%d)", int32(s))
}
