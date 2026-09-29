// Client-side hook SPIs (Java org.apache.rocketmq.client.hook.*).
//
// Send-side (producer):
//
//   - CheckForbiddenHook: the odd one out. Its error is NOT swallowed and
//     propagates up the send retry chain, so it is called once per send
//     ATTEMPT (Java hasCheckForbiddenHook is re-evaluated every pass). It runs
//     inside sendKernelImpl AFTER compression and the sysFlag are computed and
//     AFTER the broker address is resolved.
//   - SendMessageHook: before/after around the actual RPC. Panics/errors are
//     swallowed (Java catches Throwable) and the next hook still runs.
//   - EndTransactionHook: fires once per endTransaction, on both the caller
//     path and the broker's check-back path.
//
// Consume-side (consumer):
//
//   - FilterMessageHook: runs on every batch taken from a pull response,
//     BEFORE it is queued. Exceptions are SWALLOWED and the remaining hooks
//     still run (Java PullAPIWrapper.executeHook:171-178). MsgList is mutable:
//     a hook that removes entries makes the caller drop them, and a dropped
//     entry is NOT acked — the consumed offset keeps advancing, i.e. a silent
//     skip.
//   - ConsumeMessageHook: before/after around the listener. Exceptions are
//     logged only — a broken hook must never change the consume outcome
//     (Java's executeHookBefore/After both catch Throwable).
package client

import (
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// CommunicationMode mirrors Java
// org.apache.rocketmq.client.impl.CommunicationMode. Every hook context is
// tagged with the mode of the send that produced it, because hooks (trace in
// particular) render it into their payload.
type CommunicationMode string

const (
	CommunicationModeSync   CommunicationMode = "SYNC"
	CommunicationModeAsync  CommunicationMode = "ASYNC"
	CommunicationModeOneway CommunicationMode = "ONEWAY"
)

// ------------------------------------------------------------------ send

// SendMessageContext is one send's payload
// (Java org.apache.rocketmq.client.hook.SendMessageContext).
//
// MQTraceContext is opaque to this package: a hook stores its own state in
// before and reads it back in after. Nothing else may depend on its type
// (Java declares it Object for exactly that reason).
type SendMessageContext struct {
	Producer      *DefaultMQProducer
	ProducerGroup string
	Message       *common.Message
	MQ            common.MessageQueue
	BrokerAddr    string
	// BornHost is the local address the client would send from
	// (Java context.setBornHost(defaultMQProducer.getClientIP())).
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
	// HookName is the hook entry point name (Java hookName()).
	HookName() string
	SendMessageBefore(ctx *SendMessageContext)
	SendMessageAfter(ctx *SendMessageContext)
}

// executeSendMessageHooksBefore runs the hooks in order. A panic is swallowed
// and the NEXT hook still runs — Java catches Throwable here (line 1159).
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

// executeSendMessageHooksAfter is the mirror image of the before call and
// swallows panics the same way (Java line 1172).
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

// EndTransactionContext is one endTransaction's payload
// (Java org.apache.rocketmq.client.hook.EndTransactionContext).
type EndTransactionContext struct {
	ProducerGroup string
	Message       *common.Message
	BrokerAddr    string
	MsgID         string
	TransactionID string
	// TransactionState is the verdict the hook should record.
	TransactionState LocalTransactionState
	// FromTransactionCheck is true when the broker's check-back produced this
	// endTransaction (Java endTransaction vs processTransactionState).
	FromTransactionCheck bool
	Namespace            string
}

// EndTransactionHook is the "the transaction is over" SPI; the trace
// dispatcher uses it to emit TraceType.EndTransaction.
type EndTransactionHook interface {
	HookName() string
	EndTransaction(ctx *EndTransactionContext)
}

// executeEndTransactionHooks swallows panics (Java line 1188).
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

// FilterMessageContext is one hook invocation's payload
// (Java org.apache.rocketmq.client.hook.FilterMessageContext).
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
	// HookName is the Lua hook entry point name (Java hookName()).
	HookName() string
	// FilterMessage may mutate MsgList in place (see the package comment).
	FilterMessage(ctx *FilterMessageContext)
}

// executeFilterHooks runs the hooks in order. An exception is swallowed and
// the NEXT hook still runs — the Java rule.
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

// clientSideTagFilter is Java PullAPIWrapper.processPullResult:113-122 — the
// second, string-level tag check. The broker filters by the hash of the tag
// (codeSet), so a collision can let a non-matching message through; the client
// re-checks by exact string.
//
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

// ConsumeMessageContext is the consume hook payload
// (Java org.apache.rocketmq.client.hook.ConsumeMessageContext).
type ConsumeMessageContext struct {
	ConsumerGroup string
	MsgList       []*common.MessageExt
	MQ            common.MessageQueue
	// Success starts false and is set by the caller once the outcome is known.
	Success bool
	// Status is the string form of the consume result (Java hookStatus.name()).
	Status string
	// MQTraceContext carries the trace hook's private state from before to
	// after (Java getMqTraceContext()).
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

// CheckForbiddenContext is one send-attempt payload
// (Java org.apache.rocketmq.client.hook.CheckForbiddenContext).
//
// Unlike SendMessageContext it has NO SendResult — at this point nothing has
// been sent yet. Arg carries the business argument of
// send(msg, selector, arg, ...), which Java's 5.x sendKernelImpl does not
// populate (it only exists on the context class); the field is kept so a
// caller that goes through the selector path can supply it.
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
//
// Unlike the other hook kinds its error PROPAGATES: Java declares
// `checkForbidden(context) throws MQClientException` and sendKernelImpl does
// not catch it, so the send fails with the hook's error. Go returns it as a
// plain error for the same effect — a panic is deliberately NOT recovered
// here either, matching Java's "nothing catches you".
type CheckForbiddenHook interface {
	HookName() string
	CheckForbidden(ctx *CheckForbiddenContext) error
}

// executeCheckForbiddenHooks runs the hooks in order and returns the first
// error. No recover(): Java does not swallow, so neither do we.
func executeCheckForbiddenHooks(hooks []CheckForbiddenHook, ctx *CheckForbiddenContext) error {
	for _, hook := range hooks {
		if err := hook.CheckForbidden(ctx); err != nil {
			return err
		}
	}
	return nil
}
