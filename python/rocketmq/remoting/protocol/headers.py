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
    """过滤 None，并把 bool 规范成 Java ``Boolean.toString`` 的小写形式。

    为什么必须显式转换：``RemotingCommand`` 落 ext_fields 时统一走 ``str(v)``，
    Python 的 ``str(False)`` 是 ``"False"``，而 Java 写的是 ``"false"``。
    broker 侧 ``Boolean.parseBoolean`` 大小写不敏感，所以功能上不出错，但线上
    报文会与 Java 客户端不一致（对比抓包/单测断言时很别扭）。
    这里统一成小写，与 C++ 侧 ``putOptBool`` 完全一致。
    """
    out = {}
    for k, v in fields.items():
        if v is None:
            continue
        out[k] = ("true" if v else "false") if isinstance(v, bool) else v
    return out


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


class ReplyMessageRequestHeader(CommandCustomHeader):
    """broker → 请求方 的 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 请求头。

    对应 Java ``org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader``
    （字段与 ``SendMessageRequestHeader`` 高度重合，但多了 bornHost/storeHost/storeTimestamp，
    broker 的 ``ReplyMessageProcessor#pushReplyMessage`` 就是用它拼出来的）。
    请求方收到后据此 + body 还原出真正的应答 MessageExt。
    """

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
        self.born_host: Optional[str] = None
        self.store_host: Optional[str] = None
        self.store_timestamp: Optional[int] = None

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
            "bornHost": self.born_host,
            "storeHost": self.store_host,
            "storeTimestamp": self.store_timestamp,
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
        self.born_host = ext.get("bornHost")
        self.store_host = ext.get("storeHost")
        self.store_timestamp = _l(ext.get("storeTimestamp"))


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
    """对应 org.apache.rocketmq.remoting.protocol.header.QueryMessageRequestHeader。

    ``index_type`` 取 MessageConst.INDEX_KEY_TYPE("K") / INDEX_UNIQUE_TYPE("U") /
    INDEX_TAG_TYPE("T")；broker 侧为空时默认按 "K"（普通 key 索引）查。
    """

    def __init__(self):
        self.topic: Optional[str] = None
        self.key: Optional[str] = None
        self.max_num: Optional[int] = None
        self.begin_timestamp: Optional[int] = None
        self.end_timestamp: Optional[int] = None
        self.index_type: Optional[str] = None
        self.last_key: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "topic": self.topic, "key": self.key, "maxNum": self.max_num,
            "beginTimestamp": self.begin_timestamp, "endTimestamp": self.end_timestamp,
            "indexType": self.index_type, "lastKey": self.last_key,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.key = ext.get("key")
        self.max_num = _i(ext.get("maxNum"))
        self.begin_timestamp = _l(ext.get("beginTimestamp"))
        self.end_timestamp = _l(ext.get("endTimestamp"))
        self.index_type = ext.get("indexType")
        self.last_key = ext.get("lastKey")


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
    """对应 org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader。

    ⚠ 继承 RpcRequestHeader 的 brokerName 字段在 Java 里**反射名是 ``bname``**
    （setter 为 setBrokerName，但字段声明名是 bname）。写错键 broker 会静默丢字段。
    字段名严格与 Java 一致：topic / producerGroup / tranStateTableOffset /
    commitLogOffset / commitOrRollback / fromTransactionCheck / msgId /
    transactionId / bname。
    """

    def __init__(self):
        self.topic: Optional[str] = None
        self.producer_group: Optional[str] = None
        self.tran_state_table_offset: Optional[int] = None
        self.commit_log_offset: Optional[int] = None
        self.commit_or_rollback: Optional[int] = None
        self.from_transaction_check: Optional[bool] = None
        self.msg_id: Optional[str] = None
        self.transaction_id: Optional[str] = None
        self.bname: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "topic": self.topic,
            "producerGroup": self.producer_group,
            "tranStateTableOffset": self.tran_state_table_offset,
            "commitLogOffset": self.commit_log_offset,
            "commitOrRollback": self.commit_or_rollback,
            "fromTransactionCheck": self.from_transaction_check,
            "msgId": self.msg_id,
            "transactionId": self.transaction_id,
            "bname": self.bname,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.producer_group = ext.get("producerGroup")
        self.tran_state_table_offset = _l(ext.get("tranStateTableOffset"))
        self.commit_log_offset = _l(ext.get("commitLogOffset"))
        self.commit_or_rollback = _i(ext.get("commitOrRollback"))
        self.from_transaction_check = _b(ext.get("fromTransactionCheck"))
        self.msg_id = ext.get("msgId")
        self.transaction_id = ext.get("transactionId")
        self.bname = ext.get("bname")


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
    """对应 org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader。

    ⚠ 同样继承 RpcRequestHeader：brokerName 反射名是 ``bname``。字段名严格与 Java
    一致：topic / tranStateTableOffset / commitLogOffset / msgId / transactionId /
    offsetMsgId / bname。
    """

    def __init__(self):
        self.topic: Optional[str] = None
        self.tran_state_table_offset: Optional[int] = None
        self.commit_log_offset: Optional[int] = None
        self.msg_id: Optional[str] = None
        self.transaction_id: Optional[str] = None
        self.offset_msg_id: Optional[str] = None
        self.bname: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "topic": self.topic,
            "tranStateTableOffset": self.tran_state_table_offset,
            "commitLogOffset": self.commit_log_offset,
            "msgId": self.msg_id,
            "transactionId": self.transaction_id,
            "offsetMsgId": self.offset_msg_id,
            "bname": self.bname,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.tran_state_table_offset = _l(ext.get("tranStateTableOffset"))
        self.commit_log_offset = _l(ext.get("commitLogOffset"))
        self.msg_id = ext.get("msgId")
        self.transaction_id = ext.get("transactionId")
        self.offset_msg_id = ext.get("offsetMsgId")
        self.bname = ext.get("bname")


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
    """对应 org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader。

    ⚠ broker 的 checkFields() 会把 ``topicFilterType`` 解析成枚举，**为空直接抛
    RemotingCommandException("topicFilterType = [null] value invalid")**，
    所以哪怕只想建普通 topic，也必须显式下发 topicFilterType。
    """

    def __init__(self):
        self.topic: Optional[str] = None
        self.default_topic: Optional[str] = None
        self.read_queue_nums: Optional[int] = None
        self.write_queue_nums: Optional[int] = None
        self.perm: Optional[int] = None
        self.topic_filter_type: Optional[str] = None
        self.topic_sys_flag: Optional[int] = None
        self.order: Optional[bool] = None
        # AttributeParser.parseToString 格式："k1=v1,k2" （值为空时只留 key）
        self.attributes: Optional[str] = None
        self.force: Optional[bool] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "topic": self.topic, "defaultTopic": self.default_topic,
            "readQueueNums": self.read_queue_nums, "writeQueueNums": self.write_queue_nums,
            "perm": self.perm, "topicFilterType": self.topic_filter_type,
            "topicSysFlag": self.topic_sys_flag, "order": self.order,
            "attributes": self.attributes, "force": self.force,
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


# ---------------- POP 模式（5.x 轻量消费） ----------------
#
# ext key 必须逐字等于 Java 侧的字段名：broker 用 fastjson2 按 Java 属性名反序列化，
# 错一个字母就**静默丢字段**（不报错、行为静默退化），所以下面每个 key 都对着
# Java `remoting/.../protocol/header/Pop*Header.java` 抄。
#
# 另外注意 `order` / `suspend` 这两个字段 Java 用的是**非空** Boolean/boolean
# （`Boolean order = Boolean.FALSE`、`private boolean suspend = false`），
# `encodeHeader` 只会跳过 null，所以它们**总是**出现在报文里 —— 这里也不做 None 过滤。


class PopMessageRequestHeader(CommandCustomHeader):
    """Java ``PopMessageRequestHeader``（RequestCode.POP_MESSAGE = 200050）。"""

    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.topic: Optional[str] = None
        self.queue_id: Optional[int] = None
        self.max_msg_nums: Optional[int] = None
        self.invisible_time: Optional[int] = None
        self.poll_time: Optional[int] = None
        # bornTime 必须是**当前毫秒时间戳**：broker 校验
        # `now - bornTime - pollTime > 500` 就直接回 POLLING_TIMEOUT(210)。
        self.born_time: Optional[int] = None
        # 0 = MIN（从最小位点开始，能消费历史），1 = MAX（只拿新消息）
        self.init_mode: Optional[int] = None
        self.exp_type: Optional[str] = None
        self.exp: Optional[str] = None
        self.order: bool = False
        self.attempt_id: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "consumerGroup": self.consumer_group,
            "topic": self.topic,
            "queueId": self.queue_id,
            "maxMsgNums": self.max_msg_nums,
            "invisibleTime": self.invisible_time,
            "pollTime": self.poll_time,
            "bornTime": self.born_time,
            "initMode": self.init_mode,
            "expType": self.exp_type,
            "exp": self.exp,
            "order": self.order,
            "attemptId": self.attempt_id,
        })


class PopMessageResponseHeader(CommandCustomHeader):
    """Java ``PopMessageResponseHeader``。

    ``start_offset_info`` / ``msg_offset_info`` / ``order_count_info`` 是编码过的
    字符串（空格做字段分隔、分号做队列分隔），用 ``extra_info`` 模块解析。
    """

    def __init__(self):
        self.pop_time: Optional[int] = None
        self.invisible_time: Optional[int] = None
        self.revive_qid: Optional[int] = None
        self.rest_num: Optional[int] = None
        self.start_offset_info: Optional[str] = None
        self.msg_offset_info: Optional[str] = None
        self.order_count_info: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "popTime": self.pop_time,
            "invisibleTime": self.invisible_time,
            "reviveQid": self.revive_qid,
            "restNum": self.rest_num,
            "startOffsetInfo": self.start_offset_info,
            "msgOffsetInfo": self.msg_offset_info,
            "orderCountInfo": self.order_count_info,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.pop_time = _l(ext.get("popTime"))
        self.invisible_time = _l(ext.get("invisibleTime"))
        self.revive_qid = _i(ext.get("reviveQid"))
        self.rest_num = _l(ext.get("restNum"))
        self.start_offset_info = ext.get("startOffsetInfo")
        self.msg_offset_info = ext.get("msgOffsetInfo")
        self.order_count_info = ext.get("orderCountInfo")


class AckMessageRequestHeader(CommandCustomHeader):
    """Java ``AckMessageRequestHeader``（RequestCode.ACK_MESSAGE = 200051）。

    ``offset`` 是 **consumeQueue offset**（即 CK 串第 8 段 / msgQueueOffset），
    不是 commitlog offset —— 传错会被 broker 回 NO_MESSAGE。
    """

    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.topic: Optional[str] = None
        self.queue_id: Optional[int] = None
        self.extra_info: Optional[str] = None
        self.offset: Optional[int] = None
        self.lite_topic: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "consumerGroup": self.consumer_group,
            "topic": self.topic,
            "queueId": self.queue_id,
            "extraInfo": self.extra_info,
            "offset": self.offset,
            "liteTopic": self.lite_topic,
        })


class ChangeInvisibleTimeRequestHeader(CommandCustomHeader):
    """Java ``ChangeInvisibleTimeRequestHeader``（CHANGE_MESSAGE_INVISIBLETIME = 200053）。"""

    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.topic: Optional[str] = None
        self.queue_id: Optional[int] = None
        self.extra_info: Optional[str] = None
        self.offset: Optional[int] = None
        self.invisible_time: Optional[int] = None
        self.lite_topic: Optional[str] = None
        self.suspend: bool = False

    def to_ext_fields(self) -> dict:
        return _ext({
            "consumerGroup": self.consumer_group,
            "topic": self.topic,
            "queueId": self.queue_id,
            "extraInfo": self.extra_info,
            "offset": self.offset,
            "invisibleTime": self.invisible_time,
            "liteTopic": self.lite_topic,
            "suspend": self.suspend,
        })


class ChangeInvisibleTimeResponseHeader(CommandCustomHeader):
    """Java ``ChangeInvisibleTimeResponseHeader``。

    返回的是**新的** ``invisible_time`` / ``pop_time`` / ``revive_qid``；
    客户端要用它们重建 extraInfo 供后续 ACK 使用。
    """

    def __init__(self):
        self.pop_time: Optional[int] = None
        self.invisible_time: Optional[int] = None
        self.revive_qid: Optional[int] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "popTime": self.pop_time,
            "invisibleTime": self.invisible_time,
            "reviveQid": self.revive_qid,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.pop_time = _l(ext.get("popTime"))
        self.invisible_time = _l(ext.get("invisibleTime"))
        self.revive_qid = _i(ext.get("reviveQid"))
