package client

import (
	"fmt"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

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

// Swapping the two is a silent, hard-to-debug break: everything still
// "succeeds" while traces and console lookups stop matching.
type SendResult struct {
	SendStatus    SendStatus
	MsgID         string
	MessageQueue  common.MessageQueue
	QueueOffset   int64
	TransactionID string
	OffsetMsgID   string
	RegionID      string
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

type TransactionSendResult struct {
	SendResult
	LocalTransactionState LocalTransactionState
}

type LocalTransactionState int32

const (
	// CommitMessage and RollbackMessage map to MessageSysFlag
	// TRANSACTION_COMMIT_TYPE / TRANSACTION_ROLLBACK_TYPE.
	CommitMessage LocalTransactionState = iota
	RollbackMessage
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
