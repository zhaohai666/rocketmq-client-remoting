// 消息轨迹 hook（发送/消费/事务结束/回收消息）。
//
// 契约要点：
//  - 同一次事件的 BEFORE 和 AFTER 必须**共用同一个 TraceContext 对象**（内部
//    requestId 是控制台把两端拼起来的钥匙）—— hook 把它塞进上下文的 hookState。
//  - 消费侧的 msg_id 用 OFFSET 形式的 ID（与 SendResult.offsetMsgId 同一口径）；
//    生产侧 Pub 轨迹用 UNIQ_KEY。
//  - Hook 内部异常绝不能拖垮投递：全部 catch 吞掉。
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

// SendMessageTraceHookImpl 记录 Pub 事件。方法名严格沿用 SendMessageHook 的
// sendMessageBefore/sendMessageAfter 契约——producer 发送路径就是遍历
// hookRegistry.sendMessageHooks 并按这两个名字直接调用的。
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
      // SubBefore 上下文在这里 append 出去（consumeMessageAfter 只补 SubAfter
      // 那一半）。少了这一句，SubBefore 记录就永远不会上线。
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

// consumeReturnTypeCode 把 ConsumeContextType 名字映射回 SubAfter contextCode 里存的
// 序号。
function consumeReturnTypeCode(name?: string): number {
  switch (name) {
    case 'TIME_OUT': return 1;
    case 'EXCEPTION': return 2;
    case 'RETURNNULL': return 3;
    case 'FAILED': return 4;
    default: return 0; // SUCCESS
  }
}

// DefaultRecallMessageTraceHook 是跟 trace dispatcher 一起挂在 remoting client 上的
// RPCHook（在 producer 打开 trace 的分支里注册）。它盯住 RECALL_MESSAGE rpc，
// 每个已应答的都补一条 Recall 轨迹。
//
// 由系统属性 com.rocketmq.recall.default.trace.enable 门控（**默认关闭**，需要
// 显式打开）；node 端从同名环境变量读取。
export class DefaultRecallMessageTraceHook {
  private dispatcher: AsyncTraceDispatcher | null;
  private enableDefaultTrace: boolean;

  constructor(dispatcher: AsyncTraceDispatcher | null) {
    this.dispatcher = dispatcher;
    const raw = process.env['com.rocketmq.recall.default.trace.enable'];
    this.enableDefaultTrace = raw != null && (raw === 'true' || raw === '1' || raw === 'True');
  }

  // doBeforeRequest 保持空实现。
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
      // 这里同样吞掉一切异常——轨迹绝不能反过来打断 rpc 通路。
    }
  }
}
