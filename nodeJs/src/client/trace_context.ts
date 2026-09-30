// Message trace types + codec (Java org.apache.rocketmq.client.trace.*).
//
// The encoded text is what the broker stores under RMQ_SYS_TRACE_TOPIC and
// what the RocketMQ console reads back, so the field order is a WIRE FORMAT,
// not an implementation detail.
//
// Two Java split semantics are load-bearing and easy to get wrong:
//
//  - CONTENT_SPLITOR (\x01) separates fields inside one record,
//    FIELD_SPLITOR (\x02) separates records and is appended after EVERY
//    record (trailing separators included) — Java String.split drops trailing
//    empty strings, which is exactly what makes the trailing separator
//    harmless. Record count == count("\x02") on the wire.
//  - The decoder must use Java's split rule, not a naive split: JS split keeps
//    the trailing empty part. javaSplit below is the one and only entry point.
import { MessageConst } from '../common/messageConst.ts';

// Trace constants (Java TraceConstants). The last two are wire literals too.
export const TraceGroupNamePrefix = '_INNER_TRACE_PRODUCER';
export const TraceContentSplitor = '\u0001';
export const TraceFieldSplitor = '\u0002';
export const TraceInstanceName = 'PID_CLIENT_INNER_TRACE_PRODUCER';
export const TraceTopicPrefix = 'rmq_sys_TRACE_DATA_';
export const AccessChannelLocal = 'LOCAL';
export const AccessChannelCloud = 'CLOUD';

// TraceType is Java TraceType. The string is the first field of every encoded
// record, so these values are wire literals.
export const TraceType = {
  PUB: 'Pub',
  RECALL: 'Recall',
  SUB_BEFORE: 'SubBefore',
  SUB_AFTER: 'SubAfter',
  END_TRANSACTION: 'EndTransaction',
} as const;
export type TraceType = string;

// MessageType ordinals (Java common.MessageType) — wire values.
export const TraceMsgType = { NORMAL: 0, ORDER: 1, TRANSACTION: 2, DELAY: 3, BATCH: 4, REPLY: 5 } as const;

// TraceBean is Java TraceBean: the traced message behind one record.
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

// TraceContext is Java TraceContext: one trace event.
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

// TraceTransferBean is Java TraceTransferBean: one encoded chunk plus the keys
// the receiver can look it up by.
export class TraceTransferBean {
  transData = '';
  transKey = new Set<string>();
}

// traceField reads one segment, defaulting to "" when it is missing.
//
// This is a DELIBERATE robustness deviation from Java, mirroring the Python
// port: Java's decoder indexes line[7] directly, and a SubBefore record of a
// message WITHOUT keys loses that segment to the trailing split rule — so an
// AIOOBE kills the whole trace message. A trace reader should not die on a
// legal record, so the missing segment reads as an empty string instead.
function traceField(line: string[], index: number): string {
  return index < line.length ? line[index] : '';
}

// javaSplit mirrors Java String#split(sep, 0): trailing empty segments are
// DROPPED, but a string with no separator is returned whole.
export function javaSplit(text: string, sep: string): string[] {
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

// EncodeTraceContext mirrors Java TraceDataEncoder.encoderFromContextBean.
// A nil context encodes to null (Java returns null).
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

// DecodeTraceDataString mirrors Java decoderFromTraceDataString.
//
// Deviation, matching the Python port: a record that fails to decode is
// skipped instead of aborting the whole payload. Java lets the exception
// escape, which loses every record in the message because of one bad one.
export function DecodeTraceDataString(traceData: string): TraceContext[] {
  if (!traceData) return [];
  const out: TraceContext[] = [];
  for (const record of javaSplit(traceData, TraceFieldSplitor)) {
    if (record === '') continue;
    const ctx = decodeTraceContext(record);
    if (ctx !== null) out.push(ctx);
  }
  return out;
}

function decodeTraceContext(record: string): TraceContext | null {
  const line = javaSplit(record, TraceContentSplitor);
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
      // Segment 7 is what a keys-less message loses (Java's AIOOBE quirk).
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

// Helper: build a TraceBean from a MessageExt (consumer side — the msg_id is
// the OFFSET-based ID, aligning with SendResult.offsetMsgId; the Pub trace
// uses UNIQ_KEY instead — cross-port rule #14).
export function traceBeanFromMessageExt(msg: any, offsetBasedId: boolean): TraceBean {
  const bean = new TraceBean();
  bean.topic = msg.getTopic();
  bean.msgId = offsetBasedId
    ? (msg.getOffsetMsgId() || msg.getMsgId())
    : (msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYARRAY) || msg.getMsgId());
  bean.tags = msg.getProperty(MessageConst.PROPERTY_TAGS) || '';
  bean.keys = msg.getProperty(MessageConst.PROPERTY_KEYS) || '';
  bean.storeHost = msg.getStoreHostString();
  const body = msg.getBody();
  bean.bodyLength = body ? body.length : 0;
  bean.retryTimes = msg.getReconsumeTimes();
  return bean;
}
