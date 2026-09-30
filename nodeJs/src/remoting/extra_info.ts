// -*- coding: utf-8 -*-
// POP-mode "extraInfo" (a.k.a. CK string) codec.
// Faithful port of python/rocketmq/remoting/protocol/extra_info.py, which in turn
// mirrors org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil.
//
// The CK string is the core credential of POP mode: the broker does NOT write the
// POP_CK property for ordinary-topic messages (only on the retry-topic re-encode
// path), so the client must reconstruct this string from the response header's
// startOffsetInfo / msgOffsetInfo and then hand it back as the `extraInfo` of
// ACK / CHANGE_MESSAGE_INVISIBLETIME. The format is 7 or 8 SPACE-separated
// segments:
//   ckQueueOffset popTime invisibleTime reviveQid retryFlag brokerName queueId [msgQueueOffset]
// NOTE: the segment separator is a SPACE, not a comma - using the wrong separator
// makes the broker silently fail to parse.
import { MixAll } from '../common/mixAll.ts';

export const KEY_SEPARATOR = ' ';
export const QUEUE_SEPARATOR = ';';
export const OFFSET_SEPARATOR = ',';

export const NORMAL_TOPIC = '0';
export const RETRY_TOPIC = '1';
export const RETRY_TOPIC_V2 = '2';
export const QUEUE_OFFSET = 'qo';

// Fixed revive queue number for ordered consumption (Java KeyBuilder.POP_ORDER_REVIVE_QUEUE).
export const POP_ORDER_REVIVE_QUEUE = 999;

const _POP_RETRY_SEPARATOR_V1 = '_';
const _POP_RETRY_SEPARATOR_V2 = '+';

export function buildPopRetryTopicV1(topic: string, cid: string): string {
  return MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + _POP_RETRY_SEPARATOR_V1 + topic;
}

export function buildPopRetryTopicV2(topic: string, cid: string): string {
  return MixAll.RETRY_GROUP_TOPIC_PREFIX + cid + _POP_RETRY_SEPARATOR_V2 + topic;
}

export function buildPopRetryTopic(topic: string, cid: string, enableRetryV2 = false): string {
  return enableRetryV2 ? buildPopRetryTopicV2(topic, cid) : buildPopRetryTopicV1(topic, cid);
}

export function isPopRetryTopicV2(retryTopic: string | null | undefined): boolean {
  if (!retryTopic) return false;
  return retryTopic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && retryTopic.includes(_POP_RETRY_SEPARATOR_V2);
}

// Mimic Java String.split: drop trailing empty strings (Python's split keeps them).
function _splitDropTrailing(value: string, sep: string): string[] {
  const parts = value.split(sep);
  while (parts.length && parts[parts.length - 1] === '') parts.pop();
  return parts;
}

export function split(extraInfo: string | null | undefined): string[] {
  if (extraInfo == null) throw new Error('split extraInfo is null');
  return _splitDropTrailing(extraInfo, KEY_SEPARATOR);
}

function _require(segments: string[] | null | undefined, need: number, what: string): void {
  if (segments == null || segments.length < need) {
    throw new Error(`${what} fail, extraInfoStrs length ${segments == null ? 0 : segments.length}`);
  }
}

export function getCkQueueOffset(segments: string[] | null | undefined): number {
  _require(segments, 1, 'getCkQueueOffset');
  return parseInt(segments![0], 10);
}

export function getPopTime(segments: string[] | null | undefined): number {
  _require(segments, 2, 'getPopTime');
  return parseInt(segments![1], 10);
}

export function getInvisibleTime(segments: string[] | null | undefined): number {
  _require(segments, 3, 'getInvisibleTime');
  return parseInt(segments![2], 10);
}

export function getReviveQid(segments: string[] | null | undefined): number {
  _require(segments, 4, 'getReviveQid');
  return parseInt(segments![3], 10);
}

export function getRetry(segments: string[] | null | undefined): string {
  _require(segments, 5, 'getRetry');
  return segments![4];
}

export function getBrokerName(segments: string[] | null | undefined): string {
  _require(segments, 6, 'getBrokerName');
  return segments![5];
}

export function getQueueId(segments: string[] | null | undefined): number {
  _require(segments, 7, 'getQueueId');
  return parseInt(segments![6], 10);
}

export function getQueueOffset(segments: string[] | null | undefined): number {
  _require(segments, 8, 'getQueueOffset');
  return parseInt(segments![7], 10);
}

export function retryOfTopic(topic: string): string {
  if (isPopRetryTopicV2(topic)) return RETRY_TOPIC_V2;
  if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) return RETRY_TOPIC;
  return NORMAL_TOPIC;
}

// Build the CK string. Pass msgQueueOffset to get 8 segments; omit for 7 segments.
// Matches the two Java buildExtraInfo overloads. ACK uses the 8-segment form.
export function buildExtraInfo(
  ckQueueOffset: number, popTime: number, invisibleTime: number, reviveQid: number,
  topic: string, brokerName: string, queueId: number, msgQueueOffset?: number | null,
): string {
  const parts = [
    String(ckQueueOffset),
    String(popTime),
    String(invisibleTime),
    String(reviveQid),
    retryOfTopic(topic),
    String(brokerName),
    String(queueId),
  ];
  if (msgQueueOffset != null) parts.push(String(msgQueueOffset));
  return parts.join(KEY_SEPARATOR);
}

export function parseStartOffsetInfo(startOffsetInfo: string | null | undefined): Record<string, number> | null {
  if (!startOffsetInfo) return null;
  const out: Record<string, number> = {};
  const segments = (startOffsetInfo.includes(QUEUE_SEPARATOR)
    ? _splitDropTrailing(startOffsetInfo, QUEUE_SEPARATOR)
    : [startOffsetInfo]);
  for (const one of segments) {
    const parts = one.split(KEY_SEPARATOR);
    if (parts.length !== 3) throw new Error('parse startOffsetInfo error, ' + startOffsetInfo);
    const key = parts[0] + '@' + parts[1];
    if (key in out) throw new Error('parse startOffsetInfo error, duplicate, ' + startOffsetInfo);
    out[key] = parseInt(parts[2], 10);
  }
  return out;
}

export function parseMsgOffsetInfo(msgOffsetInfo: string | null | undefined): Record<string, number[]> | null {
  if (!msgOffsetInfo) return null;
  const out: Record<string, number[]> = {};
  const segments = (msgOffsetInfo.includes(QUEUE_SEPARATOR)
    ? _splitDropTrailing(msgOffsetInfo, QUEUE_SEPARATOR)
    : [msgOffsetInfo]);
  for (const one of segments) {
    const parts = one.split(KEY_SEPARATOR);
    if (parts.length !== 3) throw new Error('parse msgOffsetInfo error, ' + msgOffsetInfo);
    const key = parts[0] + '@' + parts[1];
    if (key in out) throw new Error('parse msgOffsetInfo error, duplicate, ' + msgOffsetInfo);
    out[key] = _splitDropTrailing(parts[2], OFFSET_SEPARATOR).map((x) => parseInt(x, 10));
  }
  return out;
}

export function parseOrderCountInfo(orderCountInfo: string | null | undefined): Record<string, number> | null {
  if (!orderCountInfo) return null;
  const out: Record<string, number> = {};
  const segments = (orderCountInfo.includes(QUEUE_SEPARATOR)
    ? _splitDropTrailing(orderCountInfo, QUEUE_SEPARATOR)
    : [orderCountInfo]);
  for (const one of segments) {
    const parts = one.split(KEY_SEPARATOR);
    if (parts.length !== 3) throw new Error('parse orderCountInfo error, ' + orderCountInfo);
    const key = parts[0] + '@' + parts[1];
    if (key in out) throw new Error('parse orderCountInfo error, duplicate, ' + orderCountInfo);
    out[key] = parseInt(parts[2], 10);
  }
  return out;
}

export function getStartOffsetInfoMapKey(topic: string, key: any): string {
  return retryOfTopic(topic) + '@' + String(key);
}

export function getQueueOffsetKeyValueKey(queueId: any, queueOffset: any): string {
  return QUEUE_OFFSET + String(queueId) + '%' + String(queueOffset);
}

export function getQueueOffsetMapKey(topic: string, queueId: any, queueOffset: any): string {
  return retryOfTopic(topic) + '@' + getQueueOffsetKeyValueKey(queueId, queueOffset);
}

export function isOrder(segments: string[]): boolean {
  return getReviveQid(segments) === POP_ORDER_REVIVE_QUEUE;
}

export function getRealTopic(topic: string, cid: string, retry: string): string {
  if (retry === NORMAL_TOPIC) return topic;
  if (retry === RETRY_TOPIC) return buildPopRetryTopicV1(topic, cid);
  if (retry === RETRY_TOPIC_V2) return buildPopRetryTopicV2(topic, cid);
  throw new Error('getRetry fail, format is wrong');
}

// ---------------------------------------------------------------------------
// Convenience wrappers matching the client-layer names (parse*). These simply
// split the CK string and delegate to the get* accessors above.
// ---------------------------------------------------------------------------
export class ExtraInfoUtil {
  static split = split;
  static getCkQueueOffset = getCkQueueOffset;
  static getPopTime = getPopTime;
  static getInvisibleTime = getInvisibleTime;
  static getReviveQid = getReviveQid;
  static getRetry = getRetry;
  static getBrokerName = getBrokerName;
  static getQueueId = getQueueId;
  static getQueueOffset = getQueueOffset;
  static retryOfTopic = retryOfTopic;
  static buildExtraInfo = buildExtraInfo;
  static parseStartOffsetInfo = parseStartOffsetInfo;
  static parseMsgOffsetInfo = parseMsgOffsetInfo;
  static parseOrderCountInfo = parseOrderCountInfo;
  static getStartOffsetInfoMapKey = getStartOffsetInfoMapKey;
  static getQueueOffsetKeyValueKey = getQueueOffsetKeyValueKey;
  static getQueueOffsetMapKey = getQueueOffsetMapKey;
  static isOrder = isOrder;
  static getRealTopic = getRealTopic;

  // parseBname(extraInfo) -> broker name
  static parseBname(extraInfo: string): string { return getBrokerName(split(extraInfo)); }
  // parseTopic(extraInfo) -> retry flag string ("0"/"1"/"2"); the CK carries no topic text.
  static parseTopic(extraInfo: string): string { return getRetry(split(extraInfo)); }
  // parseOrigTopic(retry, topic, cid) -> the real (possibly retry) topic
  static parseOrigTopic(retry: string, topic: string, cid: string): string { return getRealTopic(topic, cid, retry); }
  static parseQueueId(extraInfo: string): number { return getQueueId(split(extraInfo)); }
  static parseQueueOffset(extraInfo: string): number { return getQueueOffset(split(extraInfo)); }
  // parseOffset(extraInfo, q?) -> ack offset (= consumeQueue offset). The optional q is ignored.
  static parseOffset(extraInfo: string, _q?: any): number { return getQueueOffset(split(extraInfo)); }
  static parsePopTime(extraInfo: string): number { return getPopTime(split(extraInfo)); }
  static parseInvisibleTime(extraInfo: string): number { return getInvisibleTime(split(extraInfo)); }
}

export default ExtraInfoUtil;
