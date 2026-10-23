// POP is not "another pull". A pull names a queue and an offset and the broker
// answers with messages; a POP asks for a batch from one queue, and the broker
// answers with messages PLUS a checkpoint (POP_CK) describing the pop session
// (popTime, invisibleTime, reviveQid) and per-queue offset tables. The client
// must:
//
//  1. rebuild that checkpoint per message, ending in the message's own queue
//     offset (segment 7) — that value, not the batch start, is what ACK sends;
//  2. ACK with offset = segment 7;
//  3. never commit a consumer offset: POP leaves the offset to the broker's
//     revive logic. The consumer offset table is NOT advanced by a POP at all.
import { MessageExt } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';
import { MixAll } from '../common/mixAll.ts';
import { decodeMessages } from '../common/messageDecoder.ts';
import { ResponseCode } from '../remoting/codes.ts';
import { MQBrokerException } from '../remoting/exception.ts';
import { PopMessageResponseHeader } from '../remoting/headers.ts';
import {
  buildExtraInfo, getStartOffsetInfoMapKey, getQueueOffsetMapKey,
  parseMsgOffsetInfo, parseOrderCountInfo, parseStartOffsetInfo, split,
  getBrokerName, getQueueId, getQueueOffset, getPopTime, getInvisibleTime,
} from '../remoting/extra_info.ts';

export const PopStatus = {
  FOUND: 'FOUND',
  NO_NEW_MSG: 'NO_NEW_MSG',
  POLLING_FULL: 'POLLING_FULL',
  POLLING_NOT_FOUND: 'POLLING_NOT_FOUND',
} as const;
export type PopStatusValue = typeof PopStatus[keyof typeof PopStatus];

export class PopResult {
  popStatus: PopStatusValue = PopStatus.NO_NEW_MSG;
  msgFoundList: MessageExt[] = [];
  restNum = 0;
  // popTime / invisibleTime come from the response header and are what the
  // per-message checkpoint echoes.
  popTime = 0;
  invisibleTime = 0;
}

// ---------------------------------------------------------------------------
// delay levels
// ---------------------------------------------------------------------------

export const POP_DELAY_LEVEL = [
  10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200,
];

export function popDelayLevelSeconds(level: number): number {
  if (level < 0) level = 0;
  if (level >= POP_DELAY_LEVEL.length) return POP_DELAY_LEVEL[POP_DELAY_LEVEL.length - 1];
  return POP_DELAY_LEVEL[level];
}

export function popDelayLevelForElapsed(elapsedMillis: number): number {
  let level = POP_DELAY_LEVEL.length - 1;
  for (; level >= 0; level--) {
    if (elapsedMillis >= POP_DELAY_LEVEL[level] * 1000) {
      level++;
      break;
    }
  }
  return level;
}

// ---------------------------------------------------------------------------
// response processing
// ---------------------------------------------------------------------------

// brokerName is the LOGICAL name the checkpoint must carry (segment 5) and must
// be the same name the ACK later addresses — sending the physical broker name
// here makes every ACK unresolvable.
//
// namespace is the client-side namespace to strip from the topic handed to the
// listener ('' when unset).
export function processPopResponse(
  brokerName: string, responseCode: number, responseRemark: string,
  body: Buffer | null, respHeader: PopMessageResponseHeader,
  requestTopic: string, namespace: string, order: boolean,
  readBody = true,
): PopResult {
  const result = new PopResult();
  switch (responseCode) {
    case ResponseCode.SUCCESS:
      result.popStatus = PopStatus.FOUND;
      if (body && body.length > 0) {
        result.msgFoundList = decodeMessagesForPop(body, readBody);
      }
      break;
    case ResponseCode.POLLING_FULL:
      result.popStatus = PopStatus.POLLING_FULL;
      break;
    case ResponseCode.POLLING_TIMEOUT:
    case ResponseCode.PULL_NOT_FOUND:
      result.popStatus = PopStatus.POLLING_NOT_FOUND;
      break;

    default:
      // The code rides on the typed exception, exactly like the pull kernel's:
      // the POP loop branches on it to tell the broker's own throttle answer (215,
      // short pause) from every other failure, and a bare Error would leave the
      // number only in the text and silently disable that branch.
      throw new MQBrokerException(responseCode,
        `POP response error, code=${responseCode} remark=${responseRemark || ''}`);
  }

  result.restNum = respHeader.restNum ?? 0;
  if (result.popStatus !== PopStatus.FOUND) {
    return result;
  }
  result.popTime = respHeader.popTime ?? 0;
  result.invisibleTime = respHeader.invisibleTime ?? 0;

  const startOffsetInfo = parseStartOffsetInfo(respHeader.startOffsetInfo);
  const msgOffsetInfo = parseMsgOffsetInfo(respHeader.msgOffsetInfo);
  const orderCountInfo = parseOrderCountInfo(respHeader.orderCountInfo);

  const reviveQid = respHeader.reviveQid ?? 0;
  const built = new Map<string, string>();

  for (const msg of result.msgFoundList) {
    if (startOffsetInfo == null) {
      const key = msg.getTopic() + msg.getQueueId();
      let checkpoint = built.get(key);
      if (checkpoint === undefined) {
        checkpoint = buildExtraInfo(msg.getQueueOffset(), result.popTime, result.invisibleTime,
          reviveQid, msg.getTopic(), brokerName, msg.getQueueId());
        built.set(key, checkpoint);
      }
      msg.putProperty(MessageConst.PROPERTY_POP_CK, checkpoint + ' ' + msg.getQueueOffset());
    } else if (!msg.getProperty(MessageConst.PROPERTY_POP_CK)) {
      // That guard is load-bearing on the default retry path. With
      // brokerConfig.popResponseReturnActualRetryTopic=false the broker
      // builds the checkpoint from the PHYSICAL retry topic, so segment 4
      // (the retry marker) is "1", stamps it with putIfAbsent, and only THEN
      // rewrites msg.Topic to the business topic
      // (PopMessageProcessor:849-853). By the time the client sees the
      // message the topic has already been rewritten, so rebuilding from the
      // message would resolve the marker to "0" and derive the ACK topic as
      // the business topic — where the broker holds no such checkpoint. The
      // ACK then becomes a silent no-op whose only symptom is "the message
      // keeps coming back after popInvisibleTime".
      //
      // The lookup key is the message's OWN topic as decoded, which is still
      // the physical topic at this point (the request topic is stamped on at
      // the end of the loop). For a retried message with
      // popResponseReturnActualRetryTopic=true that is
      // `%RETRY%<group>_<topic>`, so the marker resolves to "1" and the entry
      // lines up with the one the broker emitted.
      const queueIDKey = getStartOffsetInfoMapKey(msg.getTopic(), msg.getQueueId());
      const queueOffsetKey = getQueueOffsetMapKey(msg.getTopic(), msg.getQueueId(), msg.getQueueOffset());
      const ckOffset = startOffsetInfo[queueIDKey];
      const msgOffsets = msgOffsetInfo ? msgOffsetInfo[queueIDKey] : undefined;
      if (ckOffset == null || msgOffsets == null) {
        continue;
      }
      const index = msgOffsets.indexOf(msg.getQueueOffset());
      if (index < 0 || index >= msgOffsets.length) continue;
      const msgQueueOffset = msgOffsets[index];
      msg.putProperty(MessageConst.PROPERTY_POP_CK, buildExtraInfo(
        ckOffset, result.popTime, result.invisibleTime, reviveQid,
        msg.getTopic(), brokerName, msg.getQueueId(), msgQueueOffset));

      if (order && orderCountInfo != null) {
        // An orderly POP hides the reconsume count in orderCountInfo; the key
        // is the (queueId,queueOffset) form first, then plain queueId.
        let count = orderCountInfo[queueOffsetKey];
        if (count == null) count = orderCountInfo[queueIDKey];
        if (count != null && count > 0) msg.setReconsumeTimes(count);
      }
    }
    // 1ST_POP_TIME is the FIRST pop of this message across all redeliveries;
    // only set it when absent, never overwrite.
    if (msg.getProperty(MessageConst.PROPERTY_FIRST_POP_TIME) == null) {
      msg.putProperty(MessageConst.PROPERTY_FIRST_POP_TIME, String(result.popTime));
    }
    msg.setBrokerName(brokerName);
    // The topic handed to the listener is the one the caller subscribed to,
    // with the namespace stripped — NOT the physical pop topic.
    msg.setTopic(MixAll.withoutNamespace(requestTopic, namespace));
  }
  return result;
}

export function getRealTopicFromCk(extraInfo: string, topic: string, consumerGroup: string): string {
  let parts: string[];
  try {
    parts = split(extraInfo);
  } catch {
    return topic;
  }
  if (parts.length < 5) return topic;
  const retry = parts[4];
  if (retry === '1') {
    return MixAll.RETRY_GROUP_TOPIC_PREFIX + consumerGroup + '_' + topic;
  }
  if (retry === '2') {
    return MixAll.RETRY_GROUP_TOPIC_PREFIX + consumerGroup + '+' + topic;
  }
  return topic;
}

// ---------------------------------------------------------------------------
// checkpoint readers (batch helpers)
// ---------------------------------------------------------------------------

export function popBatchTimedOut(batch: MessageExt[]): boolean {
  if (batch.length === 0) return true;
  const ck = batch[0].getProperty(MessageConst.PROPERTY_POP_CK);
  if (!ck) return true;
  let popTime: number, invisible: number;
  try {
    popTime = getPopTime(split(ck));
    invisible = getInvisibleTime(split(ck));
  } catch {
    return true;
  }
  if (popTime <= 0 || invisible <= 0) return true;
  return Date.now() - popTime >= invisible;
}

// popInvisibleTimeOf reads the batch's invisibility window (0 when unknown),
// used for the consume-hook timing decision.
export function popInvisibleTimeOf(batch: MessageExt[]): number {
  if (batch.length === 0) return 0;
  const ck = batch[0].getProperty(MessageConst.PROPERTY_POP_CK);
  if (!ck) return 0;
  try {
    return getInvisibleTime(split(ck));
  } catch {
    return 0;
  }
}

// ckParse reads brokerName / queueId / queueOffset out of one checkpoint; null
// on any failure (the caller logs and skips — an unreadable checkpoint can
// never be acked).
export function ckParse(extraInfo: string): { brokerName: string; queueId: number; queueOffset: number } | null {
  try {
    const parts = split(extraInfo);
    return {
      brokerName: getBrokerName(parts),
      queueId: getQueueId(parts),
      queueOffset: getQueueOffset(parts),
    };
  } catch {
    return null;
  }
}

function decodeMessagesForPop(raw: Buffer, readBody = true): MessageExt[] {
  return decodeMessages(raw, readBody);
}
