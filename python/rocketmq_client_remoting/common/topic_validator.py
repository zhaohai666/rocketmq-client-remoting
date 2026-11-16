# -*- coding: utf-8 -*-
from typing import Set

TOPIC_MAX_LENGTH = 127
# group names participate in building %RETRY%<group> / %DLQ%<group> topics, so they are shorter than topic
GROUP_MAX_LENGTH = 120
RETRY_OR_DLQ_TOPIC_MAX_LENGTH = 255

VALID_CHAR_PATTERN = "^[%|a-zA-Z0-9_-]+$"

AUTO_CREATE_TOPIC_KEY_TOPIC = "TBW102"
RMQ_SYS_SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX"
RMQ_SYS_BENCHMARK_TOPIC = "BenchmarkTest"
RMQ_SYS_TRANS_HALF_TOPIC = "RMQ_SYS_TRANS_HALF_TOPIC"
RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC = "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC"
RMQ_SYS_TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC"
RMQ_SYS_TRANS_OP_HALF_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC"
RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC = "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC"
RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC = "TRANS_CHECK_MAX_TIME_TOPIC"
RMQ_SYS_SELF_TEST_TOPIC = "SELF_TEST_TOPIC"
RMQ_SYS_OFFSET_MOVED_EVENT = "OFFSET_MOVED_EVENT"
RMQ_SYS_ROCKSDB_OFFSET_TOPIC = "CHECKPOINT_TOPIC"

SYSTEM_TOPIC_PREFIX = "rmq_sys_"

SYSTEM_TOPIC_SET: Set[str] = {
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
}

# Topics the client must NOT send directly: these are the broker's internal state streams (half messages,
# delayed messages, trace verification, ...); sending user messages to them would pollute the broker's
# transaction/delay/verification logic.
NOT_ALLOWED_SEND_TOPIC_SET: Set[str] = {
    RMQ_SYS_SCHEDULE_TOPIC,
    RMQ_SYS_TRANS_HALF_TOPIC,
    RMQ_SYS_TRANS_OP_HALF_TOPIC,
    RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
    RMQ_SYS_SELF_TEST_TOPIC,
    RMQ_SYS_OFFSET_MOVED_EVENT,
    RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
    RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
}

_ALLOWED_CHARS: Set[str] = set("%-_|") | set("0123456789") | set("abcdefghijklmnopqrstuvwxyz") \
    | set("ABCDEFGHIJKLMNOPQRSTUVWXYZ")


def is_topic_or_group_illegal(name: str) -> bool:
    """Corresponds to ``TopicValidator.isTopicOrGroupIllegal``: an empty string returns False (emptiness is handled by the isBlank step)."""
    return any(ord(ch) >= 128 or ch not in _ALLOWED_CHARS for ch in name)


def is_system_topic(topic: str) -> bool:
    return topic in SYSTEM_TOPIC_SET or topic.startswith(SYSTEM_TOPIC_PREFIX)


def is_not_allowed_send_topic(topic: str) -> bool:
    return topic in NOT_ALLOWED_SEND_TOPIC_SET
