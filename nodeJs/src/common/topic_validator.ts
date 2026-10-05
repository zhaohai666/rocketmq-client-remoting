// -*- coding: utf-8 -*-
// TopicValidator (org.apache.rocketmq.common.topic.TopicValidator).
// Faithful port of python/common/topic_validator.py.
//
// The Java validator uses a 128-entry VALID_CHAR_BIT_MAP where code points >= 128 are
// illegal; here we mirror that with a 7-bit ASCII allow-set (any char >= 128 is rejected).

import { MQClientException } from '../remoting/exception.ts';
import { MixAll } from './mixAll.ts';
import { isBlank } from './utilAll.ts';

export const TOPIC_MAX_LENGTH = 127;
// group name participates in composing %RETRY%group / %DLQ%group topic, so it is shorter.
export const GROUP_MAX_LENGTH = 120;
export const RETRY_OR_DLQ_TOPIC_MAX_LENGTH = 255;

export const VALID_CHAR_PATTERN = '^[%|a-zA-Z0-9_-]+$';
// Literal illegal-charset marker (matches Java/Python convention).
export const CHARSET_ILLEGAL = '{}\n';
// Allowed character set (code points 0-127 only); mirrors Python _ALLOWED_CHARS.
export const VALID_CHARS = new Set(
  '%-_|0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'.split(''),
);

export const AUTO_CREATE_TOPIC_KEY_TOPIC = 'TBW102';
export const DEFAULT_TOPIC = 'TBW102';
export const RMQ_SYS_SCHEDULE_TOPIC = 'SCHEDULE_TOPIC_XXXX';
export const RMQ_SYS_BENCHMARK_TOPIC = 'BenchmarkTest';
export const RMQ_SYS_TRANS_HALF_TOPIC = 'RMQ_SYS_TRANS_HALF_TOPIC';
export const RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC = 'RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC';
export const RMQ_SYS_TRACE_TOPIC = 'RMQ_SYS_TRACE_TOPIC';
export const RMQ_SYS_TRANS_OP_HALF_TOPIC = 'RMQ_SYS_TRANS_OP_HALF_TOPIC';
export const RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC = 'RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC';
export const RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC = 'TRANS_CHECK_MAX_TIME_TOPIC';
export const RMQ_SYS_SELF_TEST_TOPIC = 'SELF_TEST_TOPIC';
export const RMQ_SYS_OFFSET_MOVED_EVENT = 'OFFSET_MOVED_EVENT';
export const RMQ_SYS_ROCKSDB_OFFSET_TOPIC = 'CHECKPOINT_TOPIC';
export const PROTOCOL_TOPIC = 'RMQ_SYS_PROTOCOL_TOPIC';

export const SYSTEM_TOPIC_PREFIX = 'rmq_sys_';
export const SYSTEM_TOPIC_PREFIXES = [SYSTEM_TOPIC_PREFIX];

export const SYSTEM_TOPIC_SET = new Set<string>([
  AUTO_CREATE_TOPIC_KEY_TOPIC,
  RMQ_SYS_SCHEDULE_TOPIC,
  RMQ_SYS_BENCHMARK_TOPIC,
  RMQ_SYS_TRANS_HALF_TOPIC,
  RMQ_SYS_TRACE_TOPIC,
  RMQ_SYS_TRANS_OP_HALF_TOPIC,
  RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
  RMQ_SYS_SELF_TEST_TOPIC,
  RMQ_SYS_OFFSET_MOVED_EVENT,
  RMQ_SYS_ROCKSDB_OFFSET_TOPIC,
  RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
  RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
]);

// Topics the client is not allowed to send to directly (broker-internal state streams).
export const NOT_ALLOWED_SEND_TOPIC_SET = new Set<string>([
  RMQ_SYS_SCHEDULE_TOPIC,
  RMQ_SYS_TRANS_HALF_TOPIC,
  RMQ_SYS_TRANS_OP_HALF_TOPIC,
  RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
  RMQ_SYS_SELF_TEST_TOPIC,
  RMQ_SYS_OFFSET_MOVED_EVENT,
  RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
  RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
]);

// Mirrors Python is_topic_or_group_illegal: empty string is NOT illegal (blank-ness is
// handled separately); returns true when any char has code point >= 128 or is not allowed.
export function isTopicOrGroupIllegal(name: string | null): boolean {
  if (name == null) return true;
  for (const ch of name) {
    const code = ch.codePointAt(0) as number;
    if (code >= 128 || !VALID_CHARS.has(ch)) return true;
  }
  return false;
}

export function isSystemTopic(topic: string | null): boolean {
  if (topic == null) return false;
  return SYSTEM_TOPIC_SET.has(topic) || topic.startsWith(SYSTEM_TOPIC_PREFIX);
}

export function isNotAllowedSendTopic(topic: string | null): boolean {
  if (topic == null) return false;
  return NOT_ALLOWED_SEND_TOPIC_SET.has(topic);
}

export const TopicValidator = {
  TOPIC_MAX_LENGTH,
  GROUP_MAX_LENGTH,
  RETRY_OR_DLQ_TOPIC_MAX_LENGTH,
  VALID_CHAR_PATTERN,
  CHARSET_ILLEGAL,
  VALID_CHARS,
  AUTO_CREATE_TOPIC_KEY_TOPIC,
  DEFAULT_TOPIC,
  SYSTEM_TOPIC_PREFIX,
  SYSTEM_TOPIC_PREFIXES,
  PROTOCOL_TOPIC,
  SYSTEM_TOPIC_SET,
  NOT_ALLOWED_SEND_TOPIC_SET,

  isTopicOrGroupIllegal,
  isSystemTopic,
  isNotAllowedSendTopic,

  isRetryTopic(topic: string | null): boolean {
    return MixAll.isRetryTopic(topic);
  },
  isDlqTopic(topic: string | null): boolean {
    return MixAll.isDlqTopic(topic);
  },

  // Throws MQClientException on illegal topic. AUTO_CREATE_TOPIC_KEY_TOPIC, %RETRY% and
  // %DLQ% topics are explicitly allowed (their group part is validated separately).
  validateTopic(topic: string | null): void {
    if (isBlank(topic)) throw new MQClientException('The specified topic is blank');
    if (topic.length > TOPIC_MAX_LENGTH) {
      throw new MQClientException(`The specified topic is longer than topic max length ${TOPIC_MAX_LENGTH}.`);
    }
    if (topic === AUTO_CREATE_TOPIC_KEY_TOPIC) return;
    if (TopicValidator.isRetryTopic(topic) || TopicValidator.isDlqTopic(topic)) return;
    if (isTopicOrGroupIllegal(topic)) {
      throw new MQClientException(
        `The specified topic[${topic}] contains illegal characters, allowing only ${VALID_CHAR_PATTERN}`,
      );
    }
  },
};

export default TopicValidator;
