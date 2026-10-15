// Message trace hooks (send / consume / transaction end / recall message).
//
// Contract highlights:
//  - The BEFORE and AFTER halves of one event **must share the same TraceContext
//    object** (its internal requestId is what the console uses to stitch the two
//    halves together) — the hook stores it in the context's hookState.
//  - On the consume side msg_id uses the OFFSET form of the id (same convention as
//    SendResult.offsetMsgId); the producer-side Pub trace uses UNIQ_KEY.
//  - Exceptions inside a hook must never bring delivery down: swallow them all.
import {
  ConsumeMessageHook, SendMessageHook, SendMessageContext, ConsumeMessageContext,
} from './hook.ts';
import {
  TraceContext, TraceType, TraceBean, AccessChannelLocal, traceBeanFromMessageExt,
} from './trace_context.ts';
import type { AsyncTraceDispatcher } from './trace_dispatcher.ts';
import { createUniqID } from '../common/messageClientIdSetter.ts';
import { MessageConst } from '../common/messageConst.ts';
import { RequestCode, ResponseCode } from '../remoting/codes.ts';
import { RecallMessageRequestHeader } from '../remoting/headers.ts';
import { NamespaceUtil } from '../remoting/namespace.ts';
import { RecallMessageHandle } from '../common/recall_message_handle.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.trace_hook');

// SendMessageTraceHookImpl records Pub events. The method names follow the
// SendMessageHook sendMessageBefore/sendMessageAfter contract strictly — the
// producer send path iterates hookRegistry.sendMessageHooks and invokes exactly
// these two names directly.
export class SendMessageTraceHookImpl extends SendMessageHook {
  private dispatcher: AsyncTraceDispatcher;

  constructor(dispatcher: AsyncTraceDispatcher) {
    super();
    this.dispatcher = dispatcher;
  }

  getHookName(): string { return 'SendMessageTraceHookImpl'; }

  sendMessageBefore(ctx: SendMessageContext): void {
    try {
      const context = new TraceContext();
      context.traceType = TraceType.PUB;
      context.timeStamp = Date.now();
      context.isSuccess = true;
      context.accessChannel = AccessChannelLocal;
      context.regionId = ctx.brokerAddr || '';
      context.groupName = ctx.producerGroup || '';
      const bean = new TraceBean();
      const msg = ctx.message;
      if (msg) {
        bean.topic = msg.getTopic();
        bean.tags = msg.getProperty(MessageConst.PROPERTY_TAGS) || '';
        bean.keys = msg.getProperty(MessageConst.PROPERTY_KEYS) || '';
        bean.storeHost = '';
        bean.clientHost = '';
        const body = msg.getBody();
        bean.bodyLength = body ? body.length : 0;
        bean.msgId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) || '';
      }
      context.traceBeans = [bean];
      (ctx as any).mqTraceContext = context;
    } catch (e) {
      logger.debug('trace sendBefore error: %s', (e as Error).message);
    }
  }

  sendMessageAfter(ctx: SendMessageContext): void {
    try {
      const context: TraceContext | undefined = (ctx as any).mqTraceContext;
      if (!context || context.traceBeans.length === 0) return;
      const bean = context.traceBeans[0];
      context.costTime = ctx.costTime || (Date.now() - context.timeStamp);
      context.isSuccess = !!(ctx.sendResult && ctx.sendResult.sendStatus === 'SEND_OK');
      if (ctx.sendResult) {
        bean.msgId = ctx.sendResult.msgId || bean.msgId;
        bean.offsetMsgId = ctx.sendResult.offsetMsgId || '';
        context.regionId = ctx.sendResult.regionId || context.regionId;
        if (ctx.sendResult.messageQueue) {
          bean.topic = ctx.sendResult.messageQueue.getTopic() || bean.topic;
        }
      }
      this.dispatcher.append(context);
    } catch (e) {
      logger.debug('trace sendAfter error: %s', (e as Error).message);
    }
  }
}

// ConsumeMessageTraceHookImpl records the SubBefore + SubAfter events. One
// batch consume produces one SubBefore record PER message (they share the
// timestamp/region/group/requestId) and one SubAfter record per message.
export class ConsumeMessageTraceHookImpl extends ConsumeMessageHook {
  private dispatcher: AsyncTraceDispatcher;

  constructor(dispatcher: AsyncTraceDispatcher) {
    super();
    this.dispatcher = dispatcher;
  }

  getHookName(): string { return 'ConsumeMessageTraceHookImpl'; }

  consumeMessageBefore(ctx: ConsumeMessageContext): void {
    try {
      const msgs = ctx.msgList || [];
      if (msgs.length === 0) return;
      const context = new TraceContext();
      context.traceType = TraceType.SUB_BEFORE;
      context.timeStamp = Date.now();
      context.isSuccess = true;
      context.accessChannel = AccessChannelLocal;
      context.groupName = ctx.consumerGroup || '';
      // The request id joins SubBefore and SubAfter of one consume.
      context.requestId = createUniqID();
      context.traceBeans = msgs.map((m: any) => {
        const bean = traceBeanFromMessageExt(m, true);
        bean.clientHost = bean.clientHost || '';
        return bean;
      });
      (ctx as any).mqTraceContext = context;
      // The SubBefore context is appended here (consumeMessageAfter only adds the
      // SubAfter half). Without this line the SubBefore records would never
      // reach the wire.
      if (context.traceBeans.length > 0) this.dispatcher.append(context);
    } catch (e) {
      logger.debug('trace consumeBefore error: %s', (e as Error).message);
    }
  }

  consumeMessageAfter(ctx: ConsumeMessageContext): void {
    try {
      const before: TraceContext | undefined = (ctx as any).mqTraceContext;
      if (!before || before.traceBeans.length === 0) return;
      const msgs = ctx.msgList || [];
      const statusName = ctx.props ? ctx.props['ConsumeContextType'] : undefined;
      const contextCode = consumeReturnTypeCode(statusName);
      for (let i = 0; i < before.traceBeans.length && i < msgs.length; i++) {
        const after = new TraceContext();
        after.traceType = TraceType.SUB_AFTER;
        after.requestId = before.requestId;
        after.costTime = Date.now() - before.timeStamp;
        after.isSuccess = ctx.success === true;
        after.contextCode = contextCode;
        after.accessChannel = AccessChannelLocal;
        after.timeStamp = before.timeStamp;
        after.groupName = before.groupName;
        const bean = traceBeanFromMessageExt(msgs[i], true);
        bean.retryTimes = msgs[i].getReconsumeTimes ? msgs[i].getReconsumeTimes() : 0;
        after.traceBeans = [bean];
        this.dispatcher.append(after);
      }
    } catch (e) {
      logger.debug('trace consumeAfter error: %s', (e as Error).message);
    }
  }
}

// consumeReturnTypeCode maps a ConsumeContextType name back to the ordinal
// stored in the SubAfter contextCode.
function consumeReturnTypeCode(name?: string): number {
  switch (name) {
    case 'TIME_OUT': return 1;
    case 'EXCEPTION': return 2;
    case 'RETURNNULL': return 3;
    case 'FAILED': return 4;
    default: return 0; // SUCCESS
  }
}

// DefaultRecallMessageTraceHook is the RPCHook attached to the remoting client
// together with the trace dispatcher (registered in the producer branch where
// trace is enabled). It watches the RECALL_MESSAGE rpc and appends a Recall
// trace entry for every answered request.
//
// Gated by the system property com.rocketmq.recall.default.trace.enable
// (**off by default**, must be turned on explicitly); the node side reads it from
// the environment variable of the same name.
export class DefaultRecallMessageTraceHook {
  private dispatcher: AsyncTraceDispatcher | null;
  private enableDefaultTrace: boolean;

  constructor(dispatcher: AsyncTraceDispatcher | null) {
    this.dispatcher = dispatcher;
    const raw = process.env['com.rocketmq.recall.default.trace.enable'];
    this.enableDefaultTrace = raw != null && (raw === 'true' || raw === '1' || raw === 'True');
  }

  // doBeforeRequest stays a no-op.
  doBeforeRequest(_remoteAddr: string, _request: any): void { /* no-op */ }

  doAfterResponse(remoteAddr: string, request: any, response: any): void {
    void remoteAddr;
    try {
      if (request == null || request.code !== RequestCode.RECALL_MESSAGE) return;
      if (!this.enableDefaultTrace || response == null || this.dispatcher == null) return;
      const ext = response.extFields || {};
      const regionId = ext[MessageConst.PROPERTY_MSG_REGION];
      if (regionId == null) return;

      const header = new RecallMessageRequestHeader();
      header.fromExtFields(request.extFields || {});
      const topic = NamespaceUtil.withoutNamespace(header.topic != null ? header.topic : '');
      const group = NamespaceUtil.withoutNamespace(header.producerGroup != null ? header.producerGroup : '');
      const handleV1 = RecallMessageHandle.decodeHandle(header.recallHandle);

      const bean = new TraceBean();
      bean.topic = topic;
      bean.msgId = handleV1.messageId != null ? handleV1.messageId : '';

      const context = new TraceContext();
      context.regionId = regionId;
      context.traceBeans = [bean];
      context.traceType = TraceType.RECALL;
      context.groupName = group;
      context.isSuccess = response.code === ResponseCode.SUCCESS;
      this.dispatcher.append(context);
    } catch (e) {
      // Everything is swallowed here too — a trace must never break the rpc path.
    }
  }
}
