# -*- coding: utf-8 -*-
"""全部请求/响应头（对应 org.apache.rocketmq.remoting.protocol.header.*）。

每个 header 提供 to_ext_fields()（仅输出非 None 字段，与 Java makeCustomHeaderToNet 行为一致）
和 from_ext_fields()（把 ext_fields 映射回字段）。
"""
from __future__ import annotations

from typing import Optional

# ---------------- 基类 ----------------


class CommandCustomHeader:
    def to_ext_fields(self) -> dict:
        raise NotImplementedError

    def check_fields(self) -> None:
        pass


def _ext(fields: dict) -> dict:
    """过滤 None。"""
    return {k: v for k, v in fields.items() if v is not None}


def _i(v) -> Optional[int]:
    return int(v) if v is not None else None


def _l(v) -> Optional[int]:
    return int(v) if v is not None else None


def _b(v) -> Optional[bool]:
    if v is None:
        return None
    return str(v).lower() in ("true", "1")


# ---------------- 发送消息 ----------------


class SendMessageRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.producer_group: Optional[str] = None
        self.topic: Optional[str] = None
        self.default_topic: Optional[str] = None
        self.default_topic_queue_nums: Optional[int] = None
        self.queue_id: Optional[int] = None
        self.sys_flag: Optional[int] = None
        self.born_timestamp: Optional[int] = None
        self.flag: Optional[int] = None
        self.properties: Optional[str] = None
        self.reconsume_times: Optional[int] = None
        self.unit_mode: Optional[bool] = None
        self.max_reconsume_times: Optional[int] = None
        self.batch: Optional[bool] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "producerGroup": self.producer_group,
            "topic": self.topic,
            "defaultTopic": self.default_topic,
            "defaultTopicQueueNums": self.default_topic_queue_nums,
            "queueId": self.queue_id,
            "sysFlag": self.sys_flag,
            "bornTimestamp": self.born_timestamp,
            "flag": self.flag,
            "properties": self.properties,
            "reconsumeTimes": self.reconsume_times,
            "unitMode": self.unit_mode,
            "maxReconsumeTimes": self.max_reconsume_times,
            "batch": self.batch,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.producer_group = ext.get("producerGroup")
        self.topic = ext.get("topic")
        self.default_topic = ext.get("defaultTopic")
        self.default_topic_queue_nums = _i(ext.get("defaultTopicQueueNums"))
        self.queue_id = _i(ext.get("queueId"))
        self.sys_flag = _i(ext.get("sysFlag"))
        self.born_timestamp = _l(ext.get("bornTimestamp"))
        self.flag = _i(ext.get("flag"))
        self.properties = ext.get("properties")
        self.reconsume_times = _i(ext.get("reconsumeTimes"))
        self.unit_mode = _b(ext.get("unitMode"))
        self.max_reconsume_times = _i(ext.get("maxReconsumeTimes"))
        self.batch = _b(ext.get("batch"))


class SendMessageRequestHeaderV2(CommandCustomHeader):
    """短字段名编码（producerGroup->a ...），与 Java V2 严格一致。"""

    def __init__(self):
        self.producer_group: Optional[str] = None
        self.topic: Optional[str] = None
        self.default_topic: Optional[str] = None
        self.default_topic_queue_nums: Optional[int] = None
        self.queue_id: Optional[int] = None
        self.sys_flag: Optional[int] = None
        self.born_timestamp: Optional[int] = None
        self.flag: Optional[int] = None
        self.properties: Optional[str] = None
        self.reconsume_times: Optional[int] = None
        self.unit_mode: Optional[bool] = None
        self.max_reconsume_times: Optional[int] = None
        self.batch: Optional[bool] = None
        # Java SendMessageRequestHeaderV2 还有 private String n; // brokerName
        self.broker_name: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "a": self.producer_group,
            "b": self.topic,
            "c": self.default_topic,
            "d": self.default_topic_queue_nums,
            "e": self.queue_id,
            "f": self.sys_flag,
            "g": self.born_timestamp,
            "h": self.flag,
            "i": self.properties,
            "j": self.reconsume_times,
            "k": self.unit_mode,
            "l": self.max_reconsume_times,
            "m": self.batch,
            "n": self.broker_name,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.producer_group = ext.get("a")
        self.topic = ext.get("b")
        self.default_topic = ext.get("c")
        self.default_topic_queue_nums = _i(ext.get("d"))
        self.queue_id = _i(ext.get("e"))
        self.sys_flag = _i(ext.get("f"))
        self.born_timestamp = _l(ext.get("g"))
        self.flag = _i(ext.get("h"))
        self.properties = ext.get("i")
        self.reconsume_times = _i(ext.get("j"))
        self.unit_mode = _b(ext.get("k"))
        self.max_reconsume_times = _i(ext.get("l"))
        self.batch = _b(ext.get("m"))
        self.broker_name = ext.get("n")


class SendMessageResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.msg_id: Optional[str] = None
        self.queue_id: Optional[int] = None
        self.queue_offset: Optional[int] = None
        self.transaction_id: Optional[str] = None
        self.batch_uniq_id: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "msgId": self.msg_id,
            "queueId": self.queue_id,
            "queueOffset": self.queue_offset,
            "transactionId": self.transaction_id,
            "batchUniqId": self.batch_uniq_id,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.msg_id = ext.get("msgId")
        self.queue_id = _i(ext.get("queueId"))
        self.queue_offset = _l(ext.get("queueOffset"))
        self.transaction_id = ext.get("transactionId")
        self.batch_uniq_id = ext.get("batchUniqId")


# ---------------- 拉取消息 ----------------


class PullMessageRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.topic: Optional[str] = None
        self.lite_topic: Optional[str] = None
        self.queue_id: Optional[int] = None
        self.queue_offset: Optional[int] = None
        self.max_msg_nums: Optional[int] = None
        self.sys_flag: Optional[int] = None
        self.commit_offset: Optional[int] = None
        self.suspend_timeout_millis: Optional[int] = None
        self.subscription: Optional[str] = None
        self.sub_version: Optional[int] = None
        self.expression_type: Optional[str] = None
        self.max_msg_bytes: Optional[int] = None
        self.request_source: Optional[int] = None
        self.proxy_froward_client_id: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "consumerGroup": self.consumer_group,
            "topic": self.topic,
            "liteTopic": self.lite_topic,
            "queueId": self.queue_id,
            "queueOffset": self.queue_offset,
            "maxMsgNums": self.max_msg_nums,
            "sysFlag": self.sys_flag,
            "commitOffset": self.commit_offset,
            "suspendTimeoutMillis": self.suspend_timeout_millis,
            "subscription": self.subscription,
            "subVersion": self.sub_version,
            "expressionType": self.expression_type,
            "maxMsgBytes": self.max_msg_bytes,
            "requestSource": self.request_source,
            "proxyFrowardClientId": self.proxy_froward_client_id,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.consumer_group = ext.get("consumerGroup")
        self.topic = ext.get("topic")
        self.lite_topic = ext.get("liteTopic")
        self.queue_id = _i(ext.get("queueId"))
        self.queue_offset = _l(ext.get("queueOffset"))
        self.max_msg_nums = _i(ext.get("maxMsgNums"))
        self.sys_flag = _i(ext.get("sysFlag"))
        self.commit_offset = _l(ext.get("commitOffset"))
        self.suspend_timeout_millis = _l(ext.get("suspendTimeoutMillis"))
        self.subscription = ext.get("subscription")
        self.sub_version = _l(ext.get("subVersion"))
        self.expression_type = ext.get("expressionType")
        self.max_msg_bytes = _i(ext.get("maxMsgBytes"))
        self.request_source = _i(ext.get("requestSource"))
        self.proxy_froward_client_id = ext.get("proxyFrowardClientId")


class PullMessageResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.next_begin_offset: Optional[int] = None
        self.min_offset: Optional[int] = None
        self.max_offset: Optional[int] = None
        self.suggest_which_broker_id: Optional[int] = None
        self.topic_sys_flag: Optional[int] = None
        self.group_sys_flag: Optional[int] = None
        self.forbidden_type: Optional[int] = None
        self.offset_delta: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "nextBeginOffset": self.next_begin_offset,
            "minOffset": self.min_offset,
            "maxOffset": self.max_offset,
            "suggestWhichBrokerId": self.suggest_which_broker_id,
            "topicSysFlag": self.topic_sys_flag,
            "groupSysFlag": self.group_sys_flag,
            "forbiddenType": self.forbidden_type,
            "offsetDelta": self.offset_delta,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.next_begin_offset = _l(ext.get("nextBeginOffset"))
        self.min_offset = _l(ext.get("minOffset"))
        self.max_offset = _l(ext.get("maxOffset"))
        self.suggest_which_broker_id = _l(ext.get("suggestWhichBrokerId"))
        self.topic_sys_flag = _i(ext.get("topicSysFlag"))
        self.group_sys_flag = _i(ext.get("groupSysFlag"))
        self.forbidden_type = _i(ext.get("forbiddenType"))
        self.offset_delta = _l(ext.get("offsetDelta"))


# ---------------- 偏移量 ----------------


class QueryConsumerOffsetRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.topic: Optional[str] = None
        self.queue_id: Optional[int] = None
        self.set_zero_if_not_found: Optional[bool] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "consumerGroup": self.consumer_group,
            "topic": self.topic,
            "queueId": self.queue_id,
            "setZeroIfNotFound": self.set_zero_if_not_found,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.consumer_group = ext.get("consumerGroup")
        self.topic = ext.get("topic")
        self.queue_id = _i(ext.get("queueId"))
        self.set_zero_if_not_found = _b(ext.get("setZeroIfNotFound"))


class QueryConsumerOffsetResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.offset: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"offset": self.offset})

    def from_ext_fields(self, ext: dict) -> None:
        self.offset = _l(ext.get("offset"))


class UpdateConsumerOffsetRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.topic: Optional[str] = None
        self.queue_id: Optional[int] = None
        self.commit_offset: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "consumerGroup": self.consumer_group,
            "topic": self.topic,
            "queueId": self.queue_id,
            "commitOffset": self.commit_offset,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.consumer_group = ext.get("consumerGroup")
        self.topic = ext.get("topic")
        self.queue_id = _i(ext.get("queueId"))
        self.commit_offset = _l(ext.get("commitOffset"))


class UpdateConsumerOffsetResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.offset: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"offset": self.offset})

    def from_ext_fields(self, ext: dict) -> None:
        self.offset = _l(ext.get("offset"))


class GetMaxOffsetRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None
        self.queue_id: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"topic": self.topic, "queueId": self.queue_id})

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.queue_id = _i(ext.get("queueId"))


class GetMaxOffsetResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.offset: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"offset": self.offset})

    def from_ext_fields(self, ext: dict) -> None:
        self.offset = _l(ext.get("offset"))


class GetMinOffsetRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None
        self.queue_id: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"topic": self.topic, "queueId": self.queue_id})

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.queue_id = _i(ext.get("queueId"))


class GetMinOffsetResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.offset: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"offset": self.offset})

    def from_ext_fields(self, ext: dict) -> None:
        self.offset = _l(ext.get("offset"))


class SearchOffsetRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None
        self.queue_id: Optional[int] = None
        self.timestamp: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"topic": self.topic, "queueId": self.queue_id, "timestamp": self.timestamp})

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.queue_id = _i(ext.get("queueId"))
        self.timestamp = _l(ext.get("timestamp"))


class SearchOffsetResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.offset: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"offset": self.offset})

    def from_ext_fields(self, ext: dict) -> None:
        self.offset = _l(ext.get("offset"))


class GetEarliestMsgStoretimeRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None
        self.queue_id: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"topic": self.topic, "queueId": self.queue_id})

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.queue_id = _i(ext.get("queueId"))


class GetEarliestMsgStoretimeResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.timestamp: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"timestamp": self.timestamp})

    def from_ext_fields(self, ext: dict) -> None:
        self.timestamp = _l(ext.get("timestamp"))


# ---------------- 查询消息 ----------------


class QueryMessageRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None
        self.key: Optional[str] = None
        self.max_num: Optional[int] = None
        self.begin_timestamp: Optional[int] = None
        self.end_timestamp: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "topic": self.topic, "key": self.key, "maxNum": self.max_num,
            "beginTimestamp": self.begin_timestamp, "endTimestamp": self.end_timestamp,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.key = ext.get("key")
        self.max_num = _i(ext.get("maxNum"))
        self.begin_timestamp = _l(ext.get("beginTimestamp"))
        self.end_timestamp = _l(ext.get("endTimestamp"))


class QueryMessageResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.index_last_update_timestamp: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"indexLastUpdateTimestamp": self.index_last_update_timestamp})

    def from_ext_fields(self, ext: dict) -> None:
        self.index_last_update_timestamp = _l(ext.get("indexLastUpdateTimestamp"))


class ViewMessageRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.offset: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"offset": self.offset})

    def from_ext_fields(self, ext: dict) -> None:
        self.offset = _l(ext.get("offset"))


class ViewMessageResponseHeader(CommandCustomHeader):
    def __init__(self):
        pass

    def to_ext_fields(self) -> dict:
        return {}

    def from_ext_fields(self, ext: dict) -> None:
        pass


# ---------------- 心跳 / 注销 ----------------


class HeartbeatRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.client_id: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"clientID": self.client_id})

    def from_ext_fields(self, ext: dict) -> None:
        self.client_id = ext.get("clientID")


class UnregisterClientRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.client_id: Optional[str] = None
        self.producer_group: Optional[str] = None
        self.consumer_group: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "clientID": self.client_id,
            "producerGroup": self.producer_group,
            "consumerGroup": self.consumer_group,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.client_id = ext.get("clientID")
        self.producer_group = ext.get("producerGroup")
        self.consumer_group = ext.get("consumerGroup")


# ---------------- 消费管理 ----------------


class ConsumerSendMsgBackRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.offset: Optional[int] = None
        self.group: Optional[str] = None
        self.delay_level: Optional[int] = None
        self.origin_msg_id: Optional[str] = None
        self.origin_topic: Optional[str] = None
        self.unit_mode: Optional[bool] = None
        self.max_reconsume_times: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "offset": self.offset, "group": self.group, "delayLevel": self.delay_level,
            "originMsgId": self.origin_msg_id, "originTopic": self.origin_topic,
            "unitMode": self.unit_mode, "maxReconsumeTimes": self.max_reconsume_times,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.offset = _l(ext.get("offset"))
        self.group = ext.get("group")
        self.delay_level = _i(ext.get("delayLevel"))
        self.origin_msg_id = ext.get("originMsgId")
        self.origin_topic = ext.get("originTopic")
        self.unit_mode = _b(ext.get("unitMode"))
        self.max_reconsume_times = _i(ext.get("maxReconsumeTimes"))


class GetConsumerListByGroupRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.consumer_group: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"consumerGroup": self.consumer_group})

    def from_ext_fields(self, ext: dict) -> None:
        self.consumer_group = ext.get("consumerGroup")


class GetConsumerListByGroupResponseHeader(CommandCustomHeader):
    def __init__(self):
        pass

    def to_ext_fields(self) -> dict:
        return {}

    def from_ext_fields(self, ext: dict) -> None:
        pass


class NotifyConsumerIdsChangedRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.consumer_group: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"consumerGroup": self.consumer_group})

    def from_ext_fields(self, ext: dict) -> None:
        self.consumer_group = ext.get("consumerGroup")


class GetConsumerConnectionListRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.consumer_group: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"consumerGroup": self.consumer_group})

    def from_ext_fields(self, ext: dict) -> None:
        self.consumer_group = ext.get("consumerGroup")


class GetConsumerRunningInfoRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.client_id: Optional[str] = None
        self.jstack_enabled: Optional[bool] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "consumerGroup": self.consumer_group, "clientId": self.client_id,
            "jstackEnable": self.jstack_enabled,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.consumer_group = ext.get("consumerGroup")
        self.client_id = ext.get("clientId")
        self.jstack_enabled = _b(ext.get("jstackEnable"))


class ConsumeMessageDirectlyResultRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.client_id: Optional[str] = None
        self.msg_id: Optional[str] = None
        self.broker_name: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "consumerGroup": self.consumer_group, "clientId": self.client_id,
            "msgId": self.msg_id, "brokerName": self.broker_name,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.consumer_group = ext.get("consumerGroup")
        self.client_id = ext.get("clientId")
        self.msg_id = ext.get("msgId")
        self.broker_name = ext.get("brokerName")


class ResetOffsetRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None
        self.group: Optional[str] = None
        self.timestamp: Optional[int] = None
        self.is_force: Optional[bool] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "topic": self.topic, "group": self.group, "timestamp": self.timestamp,
            "isForce": self.is_force,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.group = ext.get("group")
        self.timestamp = _l(ext.get("timestamp"))
        self.is_force = _b(ext.get("isForce"))


# ---------------- 锁 ----------------


class LockBatchMqRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.client_id: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"consumerGroup": self.consumer_group, "clientId": self.client_id})

    def from_ext_fields(self, ext: dict) -> None:
        self.consumer_group = ext.get("consumerGroup")
        self.client_id = ext.get("clientId")


class UnlockBatchMqRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.client_id: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"consumerGroup": self.consumer_group, "clientId": self.client_id})

    def from_ext_fields(self, ext: dict) -> None:
        self.consumer_group = ext.get("consumerGroup")
        self.client_id = ext.get("clientId")


# ---------------- 事务 ----------------


class EndTransactionRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.transaction_id: Optional[str] = None
        self.commit_log_offset: Optional[int] = None
        self.commit: Optional[bool] = None
        self.producer_group: Optional[str] = None
        self.tran_state_table_offset: Optional[int] = None
        self.from_transaction_check: Optional[bool] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "transactionId": self.transaction_id, "commitLogOffset": self.commit_log_offset,
            "commit": self.commit, "producerGroup": self.producer_group,
            "tranStateTableOffset": self.tran_state_table_offset,
            "fromTransactionCheck": self.from_transaction_check,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.transaction_id = ext.get("transactionId")
        self.commit_log_offset = _l(ext.get("commitLogOffset"))
        self.commit = _b(ext.get("commit"))
        self.producer_group = ext.get("producerGroup")
        self.tran_state_table_offset = _l(ext.get("tranStateTableOffset"))
        self.from_transaction_check = _b(ext.get("fromTransactionCheck"))


class EndTransactionResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.msg_id: Optional[str] = None
        self.transaction_id: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"msgId": self.msg_id, "transactionId": self.transaction_id})

    def from_ext_fields(self, ext: dict) -> None:
        self.msg_id = ext.get("msgId")
        self.transaction_id = ext.get("transactionId")


class CheckTransactionStateRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.transaction_id: Optional[str] = None
        self.commit_log_offset: Optional[int] = None
        self.producer_group: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "transactionId": self.transaction_id, "commitLogOffset": self.commit_log_offset,
            "producerGroup": self.producer_group,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.transaction_id = ext.get("transactionId")
        self.commit_log_offset = _l(ext.get("commitLogOffset"))
        self.producer_group = ext.get("producerGroup")


class CheckTransactionStateResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.group_name: Optional[str] = None
        self.transaction_state: Optional[int] = None
        self.offset: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "groupName": self.group_name, "transactionState": self.transaction_state,
            "offset": self.offset,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.group_name = ext.get("groupName")
        self.transaction_state = _i(ext.get("transactionState"))
        self.offset = _l(ext.get("offset"))


# ---------------- 管理 / 查询 ----------------


class GetAllTopicConfigRequestHeader(CommandCustomHeader):
    def __init__(self):
        pass

    def to_ext_fields(self) -> dict:
        return {}

    def from_ext_fields(self, ext: dict) -> None:
        pass


class GetAllTopicConfigResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.data_version: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"dataVersion": self.data_version})

    def from_ext_fields(self, ext: dict) -> None:
        self.data_version = ext.get("dataVersion")


class GetTopicConfigRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"topic": self.topic})

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")


class CreateTopicRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None
        self.default_topic: Optional[str] = None
        self.read_queue_nums: Optional[int] = None
        self.write_queue_nums: Optional[int] = None
        self.perm: Optional[int] = None
        self.topic_filter_type: Optional[str] = None
        self.topic_sys_flag: Optional[int] = None
        self.order: Optional[bool] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "topic": self.topic, "defaultTopic": self.default_topic,
            "readQueueNums": self.read_queue_nums, "writeQueueNums": self.write_queue_nums,
            "perm": self.perm, "topicFilterType": self.topic_filter_type,
            "topicSysFlag": self.topic_sys_flag, "order": self.order,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.default_topic = ext.get("defaultTopic")
        self.read_queue_nums = _i(ext.get("readQueueNums"))
        self.write_queue_nums = _i(ext.get("writeQueueNums"))
        self.perm = _i(ext.get("perm"))
        self.topic_filter_type = ext.get("topicFilterType")
        self.topic_sys_flag = _i(ext.get("topicSysFlag"))
        self.order = _b(ext.get("order"))


class DeleteTopicRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"topic": self.topic})

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")


class GetTopicStatsInfoRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"topic": self.topic})

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")


class GetConsumeStatsRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.topic: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"consumerGroup": self.consumer_group, "topic": self.topic})

    def from_ext_fields(self, ext: dict) -> None:
        self.consumer_group = ext.get("consumerGroup")
        self.topic = ext.get("topic")


class GetAllSubscriptionGroupConfigRequestHeader(CommandCustomHeader):
    def __init__(self):
        pass

    def to_ext_fields(self) -> dict:
        return {}

    def from_ext_fields(self, ext: dict) -> None:
        pass


class GetSubscriptionGroupConfigRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.group: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"group": self.group})

    def from_ext_fields(self, ext: dict) -> None:
        self.group = ext.get("group")


class InterviewGetConsumerStatusRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.topic: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"consumerGroup": self.consumer_group, "topic": self.topic})

    def from_ext_fields(self, ext: dict) -> None:
        self.consumer_group = ext.get("consumerGroup")
        self.topic = ext.get("topic")


GetConsumerStatusRequestHeader = InterviewGetConsumerStatusRequestHeader


class GetTopicsByClusterRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.cluster: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"cluster": self.cluster})

    def from_ext_fields(self, ext: dict) -> None:
        self.cluster = ext.get("cluster")


class GetBrokerConfigResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.version: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"version": self.version})

    def from_ext_fields(self, ext: dict) -> None:
        self.version = ext.get("version")


class GetTopicConfigResponseHeader(CommandCustomHeader):
    def __init__(self):
        pass

    def to_ext_fields(self) -> dict:
        return {}

    def from_ext_fields(self, ext: dict) -> None:
        pass


class GetSubscriptionGroupResponseHeader(CommandCustomHeader):
    def __init__(self):
        pass

    def to_ext_fields(self) -> dict:
        return {}

    def from_ext_fields(self, ext: dict) -> None:
        pass


class GetTopicListResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.broker_addr: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"brokerAddr": self.broker_addr})

    def from_ext_fields(self, ext: dict) -> None:
        self.broker_addr = ext.get("brokerAddr")


# ---------------- namesrv ----------------

class RegisterBrokerRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.broker_name: Optional[str] = None
        self.broker_addr: Optional[str] = None
        self.cluster_name: Optional[str] = None
        self.ha_server_addr: Optional[str] = None
        self.broker_id: Optional[int] = None
        self.heartbeat_timeout_millis: Optional[int] = None
        self.enable_acting_master: Optional[bool] = None
        self.compressed: bool = False
        self.body_crc32: int = 0

    def to_ext_fields(self) -> dict:
        return _ext({
            "brokerName": self.broker_name,
            "brokerAddr": self.broker_addr,
            "clusterName": self.cluster_name,
            "haServerAddr": self.ha_server_addr,
            "brokerId": self.broker_id,
            "heartbeatTimeoutMillis": self.heartbeat_timeout_millis,
            "enableActingMaster": self.enable_acting_master,
            "compressed": self.compressed,
            "bodyCrc32": self.body_crc32,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.broker_name = ext.get("brokerName")
        self.broker_addr = ext.get("brokerAddr")
        self.cluster_name = ext.get("clusterName")
        self.ha_server_addr = ext.get("haServerAddr")
        self.broker_id = _i(ext.get("brokerId"))
        self.heartbeat_timeout_millis = _i(ext.get("heartbeatTimeoutMillis"))
        ea = ext.get("enableActingMaster")
        self.enable_acting_master = bool(ea) if ea is not None else None
        self.compressed = bool(ext.get("compressed", False)) if ext.get("compressed") is not None else False
        self.body_crc32 = int(ext.get("bodyCrc32", 0) or 0)


class RegisterBrokerResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.ha_server_addr: Optional[str] = None
        self.master_addr: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"haServerAddr": self.ha_server_addr, "masterAddr": self.master_addr})

    def from_ext_fields(self, ext: dict) -> None:
        self.ha_server_addr = ext.get("haServerAddr")
        self.master_addr = ext.get("masterAddr")


class UnRegisterBrokerRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.broker_name: Optional[str] = None
        self.broker_addr: Optional[str] = None
        self.cluster_name: Optional[str] = None
        self.broker_id: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "brokerName": self.broker_name,
            "brokerAddr": self.broker_addr,
            "clusterName": self.cluster_name,
            "brokerId": self.broker_id,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.broker_name = ext.get("brokerName")
        self.broker_addr = ext.get("brokerAddr")
        self.cluster_name = ext.get("clusterName")
        self.broker_id = _i(ext.get("brokerId"))


class GetRouteInfoRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None
        self.accept_standard_json_only: Optional[bool] = None

    def to_ext_fields(self) -> dict:
        return _ext({"topic": self.topic, "acceptStandardJsonOnly": self.accept_standard_json_only})

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        v = ext.get("acceptStandardJsonOnly")
        self.accept_standard_json_only = bool(v) if v is not None else None


class PutKVConfigRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.namespace: Optional[str] = None
        self.key: Optional[str] = None
        self.value: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"namespace": self.namespace, "key": self.key, "value": self.value})

    def from_ext_fields(self, ext: dict) -> None:
        self.namespace = ext.get("namespace")
        self.key = ext.get("key")
        self.value = ext.get("value")


class GetKVConfigRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.namespace: Optional[str] = None
        self.key: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"namespace": self.namespace, "key": self.key})

    def from_ext_fields(self, ext: dict) -> None:
        self.namespace = ext.get("namespace")
        self.key = ext.get("key")


class GetKVConfigResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.value: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"value": self.value})

    def from_ext_fields(self, ext: dict) -> None:
        self.value = ext.get("value")


class DeleteKVConfigRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.namespace: Optional[str] = None
        self.key: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"namespace": self.namespace, "key": self.key})

    def from_ext_fields(self, ext: dict) -> None:
        self.namespace = ext.get("namespace")
        self.key = ext.get("key")


class GetKVListByNamespaceRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.namespace: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"namespace": self.namespace})

    def from_ext_fields(self, ext: dict) -> None:
        self.namespace = ext.get("namespace")


class RegisterTopicRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"topic": self.topic})

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")


class RegisterOrderTopicRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None
        self.order_topic_string: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"topic": self.topic, "orderTopicString": self.order_topic_string})

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.order_topic_string = ext.get("orderTopicString")


class DeleteTopicFromNamesrvRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.topic: Optional[str] = None
        self.cluster_name: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"topic": self.topic, "clusterName": self.cluster_name})

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.cluster_name = ext.get("clusterName")


class GetBrokerMemberGroupRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.cluster_name: Optional[str] = None
        self.broker_name: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"clusterName": self.cluster_name, "brokerName": self.broker_name})

    def from_ext_fields(self, ext: dict) -> None:
        self.cluster_name = ext.get("clusterName")
        self.broker_name = ext.get("brokerName")


class WipeWritePermOfBrokerRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.broker_name: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"brokerName": self.broker_name})

    def from_ext_fields(self, ext: dict) -> None:
        self.broker_name = ext.get("brokerName")


class WipeWritePermOfBrokerResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.wipe_topic_count: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"wipeTopicCount": self.wipe_topic_count})

    def from_ext_fields(self, ext: dict) -> None:
        v = ext.get("wipeTopicCount")
        self.wipe_topic_count = _i(v)


class AddWritePermOfBrokerRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.broker_name: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"brokerName": self.broker_name})

    def from_ext_fields(self, ext: dict) -> None:
        self.broker_name = ext.get("brokerName")


class AddWritePermOfBrokerResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.add_topic_count: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({"addTopicCount": self.add_topic_count})

    def from_ext_fields(self, ext: dict) -> None:
        v = ext.get("addTopicCount")
        self.add_topic_count = _i(v)


class BrokerHeartbeatRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.cluster_name: Optional[str] = None
        self.broker_addr: Optional[str] = None
        self.broker_name: Optional[str] = None
        self.broker_id: Optional[int] = None
        self.epoch: Optional[int] = None
        self.max_offset: Optional[int] = None
        self.confirm_offset: Optional[int] = None
        self.heartbeat_timeout_mills: Optional[int] = None
        self.election_priority: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "clusterName": self.cluster_name,
            "brokerAddr": self.broker_addr,
            "brokerName": self.broker_name,
            "brokerId": self.broker_id,
            "epoch": self.epoch,
            "maxOffset": self.max_offset,
            "confirmOffset": self.confirm_offset,
            "heartbeatTimeoutMills": self.heartbeat_timeout_mills,
            "electionPriority": self.election_priority,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.cluster_name = ext.get("clusterName")
        self.broker_addr = ext.get("brokerAddr")
        self.broker_name = ext.get("brokerName")
        self.broker_id = _i(ext.get("brokerId"))
        self.epoch = _i(ext.get("epoch"))
        self.max_offset = _i(ext.get("maxOffset"))
        self.confirm_offset = _i(ext.get("confirmOffset"))
        self.heartbeat_timeout_mills = _i(ext.get("heartbeatTimeoutMills"))
        self.election_priority = _i(ext.get("electionPriority"))


class QueryDataVersionRequestHeader(CommandCustomHeader):
    def __init__(self):
        self.broker_name: Optional[str] = None
        self.broker_addr: Optional[str] = None
        self.cluster_name: Optional[str] = None
        self.broker_id: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "brokerName": self.broker_name,
            "brokerAddr": self.broker_addr,
            "clusterName": self.cluster_name,
            "brokerId": self.broker_id,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.broker_name = ext.get("brokerName")
        self.broker_addr = ext.get("brokerAddr")
        self.cluster_name = ext.get("clusterName")
        self.broker_id = _i(ext.get("brokerId"))


class QueryDataVersionResponseHeader(CommandCustomHeader):
    def __init__(self):
        self.changed: Optional[bool] = None

    def to_ext_fields(self) -> dict:
        return _ext({"changed": self.changed})

    def from_ext_fields(self, ext: dict) -> None:
        v = ext.get("changed")
        self.changed = bool(v) if v is not None else None