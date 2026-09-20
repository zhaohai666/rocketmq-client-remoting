# -*- coding: utf-8 -*-
"""topic / group 名字的合法性判定（对应 ``org.apache.rocketmq.common.topic.TopicValidator``）。

Java 的字符表白名单是 ``^[%|a-zA-Z0-9_-]+$``，实现方式是一张 128 长的
``VALID_CHAR_BIT_MAP``：**码点 >= 128 一律非法**。这里逐字节照抄，因为 broker 侧
（``TopicConfigValidator``）用的是同一张表，客户端放行而 broker 拒绝只会把错误推迟到
建 topic 那一刻，而名字是**发送路径**上就该定的东西。
"""
from typing import Set

TOPIC_MAX_LENGTH = 127
# group 名要参与拼 %RETRY%group_topic / %DLQ%group_topic，所以比 topic 更短
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

# 客户端**不能直接发**的 topic：这几个是 broker 内部状态流水（半消息、延迟、轨迹校验…），
# 用户发进去会污染 broker 的事务/延迟/校验逻辑。
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
    """对应 ``TopicValidator.isTopicOrGroupIllegal``：空串返回 False（空由 isBlank 那步管）。"""
    return any(ord(ch) >= 128 or ch not in _ALLOWED_CHARS for ch in name)


def is_system_topic(topic: str) -> bool:
    return topic in SYSTEM_TOPIC_SET or topic.startswith(SYSTEM_TOPIC_PREFIX)


def is_not_allowed_send_topic(topic: str) -> bool:
    return topic in NOT_ALLOWED_SEND_TOPIC_SET
