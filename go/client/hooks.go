// Send-side (producer):
//
// Consume-side (consumer):
package client

import (
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

type CommunicationMode string

const (
	CommunicationModeSync   CommunicationMode = "SYNC"
	CommunicationModeAsync  CommunicationMode = "ASYNC"
	CommunicationModeOneway CommunicationMode = "ONEWAY"
)

// ------------------------------------------------------------------ send

type SendMessageContext struct {
	Producer          *DefaultMQProducer
	ProducerGroup     string
	Message           *common.Message
	MQ                common.MessageQueue
	BrokerAddr        string
	BornHost          string
	CommunicationMode CommunicationMode
	// SendResult is set before SendMessageAfter on the success path.
	SendResult *SendResult
	// Exception is set before SendMessageAfter on the failure path.
	Exception      error
	MQTraceContext any
	Props          map[string]string
	// MsgType is TransMsgHalf for a transactional half message, DelayMsg for
	// anything carrying a delay/timer property, NormalMsg otherwise.
	MsgType       common.MessageType
	Namespace     string
	AccessChannel string
}

// SendMessageHook is the send aspect SPI.
type SendMessageHook interface {
	HookName() string
	SendMessageBefore(ctx *SendMessageContext)
	SendMessageAfter(ctx *SendMessageContext)
}

func executeSendMessageHooksBefore(hooks []SendMessageHook, ctx *SendMessageContext) {
	for _, hook := range hooks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					common.LogWarnf("failed to executeSendMessageHookBefore: %v", r)
				}
			}()
			hook.SendMessageBefore(ctx)
		}()
	}
}

func executeSendMessageHooksAfter(hooks []SendMessageHook, ctx *SendMessageContext) {
	for _, hook := range hooks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					common.LogWarnf("failed to executeSendMessageHookAfter: %v", r)
				}
			}()
			hook.SendMessageAfter(ctx)
		}()
	}
}

// ------------------------------------------------------- end transaction

type EndTransactionContext struct {
	ProducerGroup string
	Message       *common.Message
	BrokerAddr    string
	MsgID         string
	TransactionID string
	// TransactionState is the verdict the hook should record.
	TransactionState     LocalTransactionState
	FromTransactionCheck bool
	Namespace            string
}

// EndTransactionHook is the "the transaction is over" SPI; the trace
// dispatcher uses it to emit TraceType.EndTransaction.
type EndTransactionHook interface {
	HookName() string
	EndTransaction(ctx *EndTransactionContext)
}

func executeEndTransactionHooks(hooks []EndTransactionHook, ctx *EndTransactionContext) {
	for _, hook := range hooks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					common.LogWarnf("failed to executeEndTransactionHook: %v", r)
				}
			}()
			hook.EndTransaction(ctx)
		}()
	}
}

// ------------------------------------------------------------------ filter

type FilterMessageContext struct {
	ConsumerGroup string
	MsgList       []*common.MessageExt
	MQ            common.MessageQueue
	UnitMode      bool
	Props         map[string]string
	AccessChannel string
}

// FilterMessageHook is the pre-delivery filter SPI.
type FilterMessageHook interface {
	HookName() string
	// FilterMessage may mutate MsgList in place (see the package comment).
	FilterMessage(ctx *FilterMessageContext)
}

func executeFilterHooks(hooks []FilterMessageHook, ctx *FilterMessageContext) {
	for _, hook := range hooks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					common.LogErrorf("execute hook error. hookName=%s: %v", safeHookName(hook), r)
				}
			}()
			hook.FilterMessage(ctx)
		}()
	}
}

func safeHookName(hook any) string {
	defer func() { _ = recover() }()
	type named interface{ HookName() string }
	if h, ok := hook.(named); ok {
		return h.HookName()
	}
	return "unknown"
}

// The guard `!tagsSet.isEmpty() && !isClassFilterMode` is why FilterAPI's
// SUB_ALL path must keep tagsSet EMPTY: subscribing to "*" turns the filter
// off entirely rather than filtering everything out.
func clientSideTagFilter(sub *remoting.SubscriptionData, msgs []*common.MessageExt) []*common.MessageExt {
	if len(msgs) == 0 || sub == nil || len(sub.TagsSet) == 0 || sub.ClassFilterMode {
		return msgs
	}
	accepted := make(map[string]struct{}, len(sub.TagsSet))
	for _, t := range sub.TagsSet {
		accepted[t] = struct{}{}
	}
	out := make([]*common.MessageExt, 0, len(msgs))
	for _, msg := range msgs {
		tags, ok := msg.GetProperty(common.PropertyTags)
		if !ok {
			continue
		}
		if _, hit := accepted[tags]; hit {
			out = append(out, msg)
		}
	}
	return out
}

// ------------------------------------------------------------------ consume

type ConsumeMessageContext struct {
	ConsumerGroup string
	MsgList       []*common.MessageExt
	MQ            common.MessageQueue
	// Success starts false and is set by the caller once the outcome is known.
	Success        bool
	Status         string
	MQTraceContext any
	Props          map[string]string
	Namespace      string
	AccessChannel  string
}

// ConsumeMessageHook wraps the listener call.
type ConsumeMessageHook interface {
	HookName() string
	ConsumeMessageBefore(ctx *ConsumeMessageContext)
	ConsumeMessageAfter(ctx *ConsumeMessageContext)
}

func executeConsumeHookBefore(hooks []ConsumeMessageHook, ctx *ConsumeMessageContext) {
	for _, hook := range hooks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					common.LogWarnf("consumeMessageHook executeHookBefore exception: %v", r)
				}
			}()
			hook.ConsumeMessageBefore(ctx)
		}()
	}
}

func executeConsumeHookAfter(hooks []ConsumeMessageHook, ctx *ConsumeMessageContext) {
	for _, hook := range hooks {
		func() {
			defer func() {
				if r := recover(); r != nil {
					common.LogWarnf("consumeMessageHook executeHookAfter exception: %v", r)
				}
			}()
			hook.ConsumeMessageAfter(ctx)
		}()
	}
}

// ------------------------------------------------------------ forbidden send

type CheckForbiddenContext struct {
	NameServerAddr    string
	ProducerGroup     string
	CommunicationMode CommunicationMode
	Message           *common.Message
	MQ                common.MessageQueue
	BrokerAddr        string
	UnitMode          bool
	Arg               any
	AccessChannel     string
}

// CheckForbiddenHook lets a client reject a message before it goes out.
type CheckForbiddenHook interface {
	HookName() string
	CheckForbidden(ctx *CheckForbiddenContext) error
}

func executeCheckForbiddenHooks(hooks []CheckForbiddenHook, ctx *CheckForbiddenContext) error {
	for _, hook := range hooks {
		if err := hook.CheckForbidden(ctx); err != nil {
			return err
		}
	}
	return nil
}
