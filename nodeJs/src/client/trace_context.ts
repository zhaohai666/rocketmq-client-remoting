// 消息轨迹类型与编解码。
//
// 编码文本就是 broker 落盘到 RMQ_SYS_TRACE_TOPIC、控制台原样读回的内容，字段顺序
// 属于经典 remoting 协议规定的线上格式，不是实现细节。
//
// 分隔规则容易踩坑，两处关键点必须遵守：
//
//  - CONTENT_SPLITOR（\x01）分隔单条记录内的字段，FIELD_SPLITOR（\x02）分隔记录，
//    并且**每条记录末尾都补一个 FIELD_SPLITOR**。解码时必须丢弃末尾空段，尾部
//    补的分隔符才无害；线上记录条数等于 "\x02" 的出现次数。
//  - JS 原生 split 会保留末尾空串，直接切会多出一个空段并把字段整体错位。统一走
//    下面的 splitTrimmingTrailingEmpty。
import { MessageConst } from '../common/messageConst.ts';

// 轨迹相关常量，最后两个 accessChannel 同样属于线上字面量。
export const TraceGroupNamePrefix = '_INNER_TRACE_PRODUCER';
export const TraceContentSplitor = '\u0001';
export const TraceFieldSplitor = '\u0002';
export const TraceInstanceName = 'PID_CLIENT_INNER_TRACE_PRODUCER';
export const TraceTopicPrefix = 'rmq_sys_TRACE_DATA_';
export const AccessChannelLocal = 'LOCAL';
export const AccessChannelCloud = 'CLOUD';

// 轨迹事件类型。类型字符串就是每条编码记录的首字段，属于线上字面量。
export const TraceType = {
  PUB: 'Pub',
  RECALL: 'Recall',
  SUB_BEFORE: 'SubBefore',
  SUB_AFTER: 'SubAfter',
  END_TRANSACTION: 'EndTransaction',
} as const;
export type TraceType = string;

// 消息类型序号 —— 编码到线上的字段值。
export const TraceMsgType = { NORMAL: 0, ORDER: 1, TRANSACTION: 2, DELAY: 3, BATCH: 4, REPLY: 5 } as const;

// TraceBean：一条轨迹记录背后被追踪的消息。
export class TraceBean {
  topic = '';
  msgId = '';
  offsetMsgId = '';
  tags = '';
  keys = '';
  storeHost = '';
  clientHost = '';
  storeTime = 0;
  retryTimes = 0;
  bodyLength = 0;
  // MsgType is written as the enum ORDINAL; the zero value is NormalMsg.
  msgType: number = TraceMsgType.NORMAL;
  // TransactionState holds the state NAME ("COMMIT_MESSAGE", ...).
  transactionState = '';
  transactionId = '';
  fromTransactionCheck = false;
}

// TraceContext：一次轨迹事件。
export class TraceContext {
  traceType: TraceType = TraceType.PUB;
  timeStamp = 0;
  regionId = '';
  regionName = '';
  groupName = '';
  costTime = 0;
  isSuccess = true;
  requestId = '';
  contextCode = 0;
  accessChannel: string = AccessChannelLocal;
  traceBeans: TraceBean[] = [];
}

// TraceTransferBean：一段编码后的载荷，加上供下游按 key 检索的键集合。
export class TraceTransferBean {
  transData = '';
  transKey = new Set<string>();
}

// traceField 读取一段，缺失时按空串处理。
//
// 这里刻意做健壮性处理：SubBefore 记录若来自一条**未带 keys 的消息**，其 keys 段
// 会被"丢弃末尾空段"的规则吃掉，直接按下标 7 硬取就会越界抛错，进而让整批轨迹消息丢光。
// 读侧不应因合法记录缺段而崩溃，所以缺段读作空串。
function traceField(line: string[], index: number): string {
  return index < line.length ? line[index] : '';
}

// splitTrimmingTrailingEmpty：按分隔符切分，**丢弃末尾空段**（协议规定每条记录末尾都补
// FIELD_SPLITOR）；没有分隔符的字符串原样返回。
export function splitTrimmingTrailingEmpty(text: string, sep: string): string[] {
  const parts = text.split(sep);
  if (parts.length === 1) return parts;
  while (parts.length > 0 && parts[parts.length - 1] === '') parts.pop();
  return parts;
}

function traceMsgType(text: string): number | null {
  const n = parseInt(text, 10);
  if (Number.isNaN(n) || n < 0 || n > TraceMsgType.REPLY) return null;
  return n;
}

// EncodeTraceContext 把一次轨迹事件编码为传输块；上下文为 null 时返回 null。
export function EncodeTraceContext(ctx: TraceContext | null): TraceTransferBean | null {
  if (ctx == null) return null;
  const tb = new TraceTransferBean();
  const join = (fields: string[]) => fields.join(TraceContentSplitor) + TraceFieldSplitor;
  switch (ctx.traceType) {
    case TraceType.PUB: {
      if (ctx.traceBeans.length === 0) return tb;
      const bean = ctx.traceBeans[0];
      tb.transData = join([
        TraceType.PUB, String(ctx.timeStamp), ctx.regionId, ctx.groupName,
        bean.topic, bean.msgId, bean.tags, bean.keys, bean.storeHost,
        String(bean.bodyLength), String(ctx.costTime),
        String(bean.msgType), bean.offsetMsgId,
        String(ctx.isSuccess),
      ]);
      break;
    }
    case TraceType.SUB_BEFORE: {
      // One record per bean: a batch consume traces every message separately
      // (they share the timestamp/region/group/requestId).
      for (const bean of ctx.traceBeans) {
        tb.transData += join([
          TraceType.SUB_BEFORE, String(ctx.timeStamp), ctx.regionId, ctx.groupName,
          ctx.requestId, bean.msgId, String(bean.retryTimes), bean.keys,
        ]);
      }
      break;
    }
    case TraceType.SUB_AFTER: {
      for (const bean of ctx.traceBeans) {
        const fields = [
          TraceType.SUB_AFTER, ctx.requestId, bean.msgId, String(ctx.costTime),
          String(ctx.isSuccess), bean.keys, String(ctx.contextCode),
        ];
        if (ctx.accessChannel !== AccessChannelCloud) {
          // A SubAfter on a NON-cloud channel carries two extra trailing
          // fields (timestamp + groupName).
          fields.push(String(ctx.timeStamp), ctx.groupName);
        }
        tb.transData += join(fields);
      }
      break;
    }
    case TraceType.END_TRANSACTION: {
      if (ctx.traceBeans.length === 0) return tb;
      const bean = ctx.traceBeans[0];
      tb.transData = join([
        TraceType.END_TRANSACTION, String(ctx.timeStamp), ctx.regionId, ctx.groupName,
        bean.topic, bean.msgId, bean.tags, bean.keys, bean.storeHost,
        String(bean.msgType), bean.transactionId, bean.transactionState,
        String(bean.fromTransactionCheck),
      ]);
      break;
    }
    case TraceType.RECALL: {
      if (ctx.traceBeans.length === 0) return tb;
      const bean = ctx.traceBeans[0];
      tb.transData = join([
        TraceType.RECALL, String(ctx.timeStamp), ctx.regionId, ctx.groupName,
        bean.topic, bean.msgId, String(ctx.isSuccess),
      ]);
      break;
    }
    default:
      return tb;
  }
  for (const bean of ctx.traceBeans) {
    tb.transKey.add(bean.msgId);
    if (bean.keys) {
      for (const k of bean.keys.split(' ')) tb.transKey.add(k);
    }
  }
  return tb;
}

// DecodeTraceDataString 解一批轨迹记录。
//
// 健壮性策略：单条解码失败时跳过，不中断整段解析——否则一条坏记录会让整批轨迹全部丢光。
export function DecodeTraceDataString(traceData: string): TraceContext[] {
  if (!traceData) return [];
  const out: TraceContext[] = [];
  for (const record of splitTrimmingTrailingEmpty(traceData, TraceFieldSplitor)) {
    if (record === '') continue;
    const ctx = decodeTraceContext(record);
    if (ctx !== null) out.push(ctx);
  }
  return out;
}

function decodeTraceContext(record: string): TraceContext | null {
  const line = splitTrimmingTrailingEmpty(record, TraceContentSplitor);
  if (line.length === 0) return null;
  const newCtx = (): TraceContext => new TraceContext();
  const newBean = (): TraceBean => new TraceBean();
  switch (line[0]) {
    case TraceType.PUB: {
      if (line.length < 12) return null;
      const ts = parseInt(line[1], 10);
      const bodyLen = parseInt(line[9], 10);
      const cost = parseInt(line[10], 10);
      const msgType = traceMsgType(line[11]);
      if (Number.isNaN(ts) || Number.isNaN(bodyLen) || Number.isNaN(cost) || msgType == null) return null;
      const ctx = newCtx();
      ctx.traceType = TraceType.PUB;
      ctx.timeStamp = ts;
      ctx.regionId = line[2];
      ctx.groupName = line[3];
      const bean = newBean();
      bean.topic = line[4];
      bean.msgId = line[5];
      bean.tags = line[6];
      bean.keys = line[7];
      bean.storeHost = line[8];
      bean.bodyLength = bodyLen;
      ctx.costTime = cost;
      bean.msgType = msgType;
      // Version tolerance: 13 fields is the pre-offsetMsgId layout, 14 adds
      // it, 15+ adds clientHost.
      if (line.length === 13) {
        ctx.isSuccess = line[12] === 'true';
      } else if (line.length === 14) {
        bean.offsetMsgId = line[12];
        ctx.isSuccess = line[13] === 'true';
      } else if (line.length >= 15) {
        bean.offsetMsgId = line[12];
        ctx.isSuccess = line[13] === 'true';
        bean.clientHost = line[14];
      }
      ctx.traceBeans = [bean];
      return ctx;
    }
    case TraceType.SUB_BEFORE: {
      if (line.length < 7) return null;
      const ts = parseInt(line[1], 10);
      const retry = parseInt(line[6], 10);
      if (Number.isNaN(ts) || Number.isNaN(retry)) return null;
      const ctx = newCtx();
      ctx.traceType = TraceType.SUB_BEFORE;
      ctx.timeStamp = ts;
      ctx.regionId = line[2];
      ctx.groupName = line[3];
      ctx.requestId = line[4];
      const bean = newBean();
      bean.msgId = line[5];
      bean.retryTimes = retry;
      // 段 7 是无 keys 的消息会被切掉的段，用 traceField 兜底。
      bean.keys = traceField(line, 7);
      ctx.traceBeans = [bean];
      return ctx;
    }
    case TraceType.SUB_AFTER: {
      if (line.length < 6) return null;
      const cost = parseInt(line[3], 10);
      if (Number.isNaN(cost)) return null;
      const ctx = newCtx();
      ctx.traceType = TraceType.SUB_AFTER;
      ctx.requestId = line[1];
      const bean = newBean();
      bean.msgId = line[2];
      bean.keys = line[5];
      ctx.costTime = cost;
      ctx.isSuccess = line[4] === 'true';
      if (line.length >= 7) {
        const code = parseInt(line[6], 10);
        if (Number.isNaN(code)) return null;
        ctx.contextCode = code;
      }
      if (line.length >= 9) {
        const ts = parseInt(line[7], 10);
        if (Number.isNaN(ts)) return null;
        ctx.timeStamp = ts;
        ctx.groupName = line[8];
      }
      ctx.traceBeans = [bean];
      return ctx;
    }
    case TraceType.END_TRANSACTION: {
      if (line.length < 13) return null;
      const ts = parseInt(line[1], 10);
      if (Number.isNaN(ts)) return null;
      const msgType = traceMsgType(line[9]);
      if (msgType == null) return null;
      const ctx = newCtx();
      ctx.traceType = TraceType.END_TRANSACTION;
      ctx.timeStamp = ts;
      ctx.regionId = line[2];
      ctx.groupName = line[3];
      const bean = newBean();
      bean.topic = line[4];
      bean.msgId = line[5];
      bean.tags = line[6];
      bean.keys = line[7];
      bean.storeHost = line[8];
      bean.msgType = msgType;
      bean.transactionId = line[10];
      bean.transactionState = line[11];
      bean.fromTransactionCheck = line[12] === 'true';
      ctx.traceBeans = [bean];
      return ctx;
    }
    case TraceType.RECALL: {
      if (line.length < 7) return null;
      const ts = parseInt(line[1], 10);
      if (Number.isNaN(ts)) return null;
      const ctx = newCtx();
      ctx.traceType = TraceType.RECALL;
      ctx.timeStamp = ts;
      ctx.regionId = line[2];
      ctx.groupName = line[3];
      const bean = newBean();
      bean.topic = line[4];
      bean.msgId = line[5];
      ctx.isSuccess = line[6] === 'true';
      ctx.traceBeans = [bean];
      return ctx;
    }
    default:
      return null;
  }
}

// 从 MessageExt 构造 TraceBean（消费侧）：msgId 取 OFFSET 形式的 ID，与
// SendResult.offsetMsgId 同一路；生产侧 Pub 轨迹则使用 UNIQ_KEY。
export function traceBeanFromMessageExt(msg: any, offsetBasedId: boolean): TraceBean {
  const bean = new TraceBean();
  bean.topic = msg.getTopic();
  bean.msgId = offsetBasedId
    ? (msg.getOffsetMsgId() || msg.getMsgId())
    : (msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) || msg.getMsgId());
  bean.tags = msg.getProperty(MessageConst.PROPERTY_TAGS) || '';
  bean.keys = msg.getProperty(MessageConst.PROPERTY_KEYS) || '';
  bean.storeHost = msg.getStoreHostString();
  const body = msg.getBody();
  bean.bodyLength = body ? body.length : 0;
  bean.retryTimes = msg.getReconsumeTimes();
  return bean;
}
