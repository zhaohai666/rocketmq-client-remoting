// Client-side hook SPIs (Java org.apache.rocketmq.client.hook.*), limited to
// the three the consumer path needs:
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
//   - CheckForbiddenHook: the OPPOSITE rule — its exception is NOT swallowed
//     and propagates up the send retry chain, and it is called once per send
//     ATTEMPT. (Consumed by the producer; declared here so the package has one
//     place for the SPI surface.)
package client

import (
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

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
	Status        string
	Props         map[string]string
	AccessChannel string
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

// CheckForbiddenContext is one send-attempt payload.
type CheckForbiddenContext struct {
	NameServerAddr string
	ProducerGroup  string
	MQ             *common.MessageQueue
	Message        *common.Message
	BrokerAddr     string
	UnitMode       bool
	AccessChannel  string
}

// CheckForbiddenHook lets a client reject a message before it goes out.
// Unlike the other two hook kinds its panic PROPAGATES (Java does not catch),
// so the send fails with the hook's error.
type CheckForbiddenHook interface {
	HookName() string
	CheckForbidden(ctx *CheckForbiddenContext)
}

func executeCheckForbiddenHooks(hooks []CheckForbiddenHook, ctx *CheckForbiddenContext) {
	for _, hook := range hooks {
		hook.CheckForbidden(ctx)
	}
}
