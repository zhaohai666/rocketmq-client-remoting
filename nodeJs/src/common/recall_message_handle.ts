// -*- coding: utf-8 -*-
// RecallMessageHandle (org.apache.rocketmq.common.producer.RecallMessageHandle).
// Faithful port of python/common/recall_message_handle.py.
//
// The handle is produced by the broker (SendMessageProcessor#attachRecallHandle) and returned
// on the SEND response; the client only carries it back to recallMessage. Encoding matches Java
// exactly: base64url("v1 <topic> <brokerName> <timestampStr> <messageId>") — 5 space-separated
// segments, with padding (consistent with Java Base64.getUrlEncoder).

import { MQClientException } from '../remoting/exception.ts';

const SEPARATOR = ' ';
const VERSION_1 = 'v1';
const INVALID_HANDLE = 'recall handle is invalid';

const RMQ_SYS_RECALL_TOPIC_PREFIX = 'rmq_sys_recall_';
const RMQ_SYS_RECALL_TOPIC_SUFFIX = '_request';

// Mirror Java RecallMessageHandle.HandleV1 — timestamps are kept as strings (broker decides
// validity), extra trailing segments are ignored on decode.
export class HandleV1 {
  topic: string | null;
  brokerName: string | null;
  timestampStr: string | null;
  messageId: string | null;

  constructor(
    topic: string | null = null,
    brokerName: string | null = null,
    timestampStr: string | null = null,
    messageId: string | null = null,
  ) {
    this.topic = topic;
    this.brokerName = brokerName;
    this.timestampStr = timestampStr;
    this.messageId = messageId;
  }

  equals(other: any): boolean {
    if (!(other instanceof HandleV1)) return false;
    return this.topic === other.topic &&
      this.brokerName === other.brokerName &&
      this.timestampStr === other.timestampStr &&
      this.messageId === other.messageId;
  }

  toString() {
    return `HandleV1(topic=${this.topic}, broker_name=${this.brokerName}, ` +
      `timestamp_str=${this.timestampStr}, message_id=${this.messageId})`;
  }
}

// base64url with '=' padding (mirrors Java Base64.getUrlEncoder).
export function buildHandle(topic: string, brokerName: string, timestampStr: string, messageId: string): string {
  const raw = [VERSION_1, topic, brokerName, timestampStr, messageId].join(SEPARATOR);
  return Buffer.from(raw, 'utf-8').toString('base64').replace(/\+/g, '-').replace(/\//g, '_');
}

export function decodeHandle(handle: string | null): HandleV1 {
  if (!handle) throw new MQClientException(INVALID_HANDLE);
  let padded = handle;
  const pad = (-handle.length) % 4;
  if (pad < 0) padded = handle + '='.repeat(-pad);
  let raw: Buffer;
  try {
    const b64 = padded.replace(/-/g, '+').replace(/_/g, '/');
    raw = Buffer.from(b64, 'base64');
  } catch (e) {
    throw new MQClientException(INVALID_HANDLE);
  }
  let text: string;
  try {
    text = raw.toString('utf-8');
  } catch (e) {
    throw new MQClientException(INVALID_HANDLE);
  }
  const items = text.split(SEPARATOR);
  if (items.length < 5 || items[0] !== VERSION_1) {
    throw new MQClientException(INVALID_HANDLE);
  }
  // Java reads items[1..4]; extra segments are ignored (tolerant decode).
  return new HandleV1(items[1], items[2], items[3], items[4]);
}

export const RecallMessageHandle = {
  SEPARATOR,
  VERSION_1,
  INVALID_HANDLE,
  RMQ_SYS_RECALL_TOPIC_PREFIX,
  RMQ_SYS_RECALL_TOPIC_SUFFIX,

  composeRecallMessageTopic(originTopic: string): string {
    return RMQ_SYS_RECALL_TOPIC_PREFIX + originTopic;
  },

  composeRecallMessageRequestTopic(originTopic: string): string {
    return RMQ_SYS_RECALL_TOPIC_PREFIX + originTopic + RMQ_SYS_RECALL_TOPIC_SUFFIX;
  },

  getOriginTopicFromRecallTopic(recallTopic: string | null): string | null {
    if (recallTopic == null) return recallTopic;
    let t = recallTopic;
    if (t.startsWith(RMQ_SYS_RECALL_TOPIC_PREFIX)) {
      t = t.slice(RMQ_SYS_RECALL_TOPIC_PREFIX.length);
    }
    if (t.endsWith(RMQ_SYS_RECALL_TOPIC_SUFFIX)) {
      t = t.slice(0, t.length - RMQ_SYS_RECALL_TOPIC_SUFFIX.length);
    }
    return t;
  },

  encodeRecallMessageHandle(topic: string, brokerName: string, timestampStr: string, messageId: string): string {
    return buildHandle(topic, brokerName, timestampStr, messageId);
  },

  parseRecallMessageHandle(rfh: string | null): HandleV1 {
    return decodeHandle(rfh);
  },

  // Build a handle from a received MessageExt (topic/broker/storeTimestamp/msgId).
  generateRecallHandle(msg: any): string {
    return buildHandle(
      msg.getTopic(),
      msg.getBrokerName(),
      String(msg.getStoreTimestamp()),
      msg.getMsgId(),
    );
  },
};

export default RecallMessageHandle;
