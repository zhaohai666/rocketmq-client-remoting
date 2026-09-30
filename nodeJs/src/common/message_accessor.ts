// -*- coding: utf-8 -*-
// MessageAccessor (org.apache.rocketmq.common.message.MessageAccessor).
// Faithful port of python/rocketmq/common/message_accessor.py, extended with the timer /
// delivery / message-type helpers present in Java's MessageAccessor. Property keys reuse
// MessageConst where available; timer/delivery keys not present in the foundation MessageConst
// are declared locally with the exact Java MessageConst names.

import { MessageConst } from './messageConst.ts';
import { MessageType } from './messageType.ts';

// Timer / delivery property keys (org.apache.rocketmq.common.message.MessageConst) not exposed
// by the foundation MessageConst object — declared locally to keep behavior byte-exact.
const PROPERTY_TIMER_DELIVER_MS = 'TIMER_DELIVER_MS';
const PROPERTY_TIMER_DELAY_MS = 'TIMER_DELAY_MS';
const PROPERTY_TIMER_DELAY_SEC = 'TIMER_DELAY_SEC';
const PROPERTY_DELIVERY_TIMESTAMP = 'DELIVERY_TIMESTAMP';

export const MessageAccessor = {
  putProperty(msg: any, name: string, value: string): void {
    msg.putProperty(name, value);
  },
  getProperty(msg: any, name: string): string | null {
    return msg.getProperty(name);
  },
  clearProperty(msg: any, name: string): void {
    msg.removeProperty(name);
  },
  setProperties(msg: any, properties: Record<string, string>): void {
    msg.setProperties(properties);
  },
  getProperties(msg: any): Record<string, string> {
    return msg.getProperties();
  },

  setRegionId(msg: any, regionId: string): void {
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MSG_REGION, regionId);
  },

  // Returns the existing producer-group property, initializing it when blank.
  getOrInitProducerGroup(msg: any, producerGroup: string): string {
    const pg = MessageAccessor.getProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP);
    if (pg == null || pg === '') {
      MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, producerGroup);
      return producerGroup;
    }
    return pg;
  },

  setMessageType(msg: any, messageType: string): void {
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TYPE, messageType);
  },
  getMessageType(msg: any): string | null {
    const s = MessageAccessor.getProperty(msg, MessageConst.PROPERTY_MESSAGE_TYPE);
    if (s != null) {
      for (const k of Object.keys(MessageType)) {
        if ((MessageType as any)[k] === s) return (MessageType as any)[k];
      }
    }
    return null;
  },

  // Sets producer group only when absent (namespace correction before publishing).
  setCorrectionBeforePublish(msg: any, producerGroup: string): void {
    const pg = MessageAccessor.getProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP);
    if (pg == null || pg === '') {
      MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, producerGroup);
    }
  },

  getReconsumeTime(msg: any): string | null {
    return msg.getProperty(MessageConst.PROPERTY_RECONSUME_TIME);
  },
  setReconsumeTime(msg: any, v: any): void {
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME, String(v));
  },

  getMaxReconsumeTimes(msg: any): string | null {
    return msg.getProperty(MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
  },
  setMaxReconsumeTimes(msg: any, v: any): void {
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES, String(v));
  },

  getDeliveryTimestamp(msg: any): string | null {
    return msg.getProperty(PROPERTY_DELIVERY_TIMESTAMP);
  },
  setDeliveryTimestamp(msg: any, ts: any): void {
    MessageAccessor.putProperty(msg, PROPERTY_DELIVERY_TIMESTAMP, String(ts));
  },

  getDelayTimeLevel(msg: any): string | null {
    return msg.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
  },
  setDelayTimeLevel(msg: any, level: any): void {
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL, String(level));
  },

  getTimerDelaySec(msg: any): string | null {
    return msg.getProperty(PROPERTY_TIMER_DELAY_SEC);
  },
  setTimerDelaySec(msg: any, s: any): void {
    MessageAccessor.putProperty(msg, PROPERTY_TIMER_DELAY_SEC, String(s));
  },

  getTimerDeliverMs(msg: any): string | null {
    return msg.getProperty(PROPERTY_TIMER_DELIVER_MS);
  },
  setTimerDeliverMs(msg: any, ms: any): void {
    MessageAccessor.putProperty(msg, PROPERTY_TIMER_DELIVER_MS, String(ms));
  },

  // --- helpers mirrored from the Python port ---
  setKeys(msg: any, keys: string): void {
    msg.putProperty(MessageConst.PROPERTY_KEYS, keys);
  },
  getKeys(msg: any): string | null {
    return msg.getProperty(MessageConst.PROPERTY_KEYS);
  },
  setTags(msg: any, tags: string): void {
    msg.putProperty(MessageConst.PROPERTY_TAGS, tags);
  },
  getTags(msg: any): string | null {
    return msg.getProperty(MessageConst.PROPERTY_TAGS);
  },
  setWaitStoreMsgOK(msg: any, ok: boolean): void {
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_WAIT_STORE_MSG_OK, ok ? 'true' : 'false');
  },
  setTransactionId(msg: any, transactionId: string): void {
    msg.setTransactionId(transactionId);
  },
  getTransactionId(msg: any): string | null {
    return msg.getTransactionId();
  },
  setOriginMessageId(msg: any, originMessageId: string): void {
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_ORIGIN_MESSAGE_ID, originMessageId);
  },
  getOriginMessageId(msg: any): string | null {
    return msg.getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
  },
  setConsumeStartTimestamp(msg: any, ts: any): void {
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CONSUME_START_TIMESTAMP, String(ts));
  },
  getConsumeStartTimestamp(msg: any): string | null {
    return msg.getProperty(MessageConst.PROPERTY_CONSUME_START_TIMESTAMP);
  },
  setTransactionPrepared(msg: any): void {
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, 'true');
  },
  isTransactionPrepared(msg: any): boolean {
    return msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED) === 'true';
  },
  setTransactionPreparedQueueOffset(msg: any, offset: any): void {
    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET, String(offset));
  },
};

export default MessageAccessor;
