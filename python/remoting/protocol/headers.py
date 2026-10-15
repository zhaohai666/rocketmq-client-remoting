# -*- coding: utf-8 -*-
"""All request/response headers (corresponding to the headers of the protocol's
rocketmq.remoting.protocol.header.* packages).

Every header provides to_ext_fields() (emitting only the non-None fields, matching the
serialization behaviour of makeCustomHeaderToNet on the broker side)
and from_ext_fields() (mapping ext_fields back to fields).
"""
from __future__ import annotations

from typing import Optional

from common.boundary_type import BoundaryType

# ---------------- base class ----------------


class CommandCustomHeader:
    def to_ext_fields(self) -> dict:
        raise NotImplementedError

    def check_fields(self) -> None:
        pass


def _ext(fields: dict) -> dict:
    """Filter out None and normalize bool into the lowercase on-wire form
    ``"false"``/``"true"``.

    Why the explicit conversion is necessary: when ``RemotingCommand`` fills
    ext_fields it uniformly goes through ``str(v)``, and Python's ``str(False)`` is
    ``"False"``, while the broker-side ``Boolean.parseBoolean`` expects the lowercase
    on-wire text ``"false"``. It is case-insensitive and functionally harmless, but the
    messages on the wire must consistently use lowercase (otherwise packet captures and
    unit-test assertions look odd).
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


# ---------------- send message ----------------


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
    """Short field name encoding (producerGroup->a ...); the on-wire key names must match
    the protocol's V2 header verbatim."""

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
        # in the protocol's V2 header the short name of brokerName is simply n
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
    """The request header of the broker -> requester PUSH_REPLY_MESSAGE_TO_CLIENT(326).

    It corresponds to the protocol header ``ReplyMessageRequestHeader`` (its fields
    overlap heavily with ``SendMessageRequestHeader``, but it adds
    bornHost/storeHost/storeTimestamp, which is what the broker's
    ``ReplyMessageProcessor#pushReplyMessage`` uses to assemble the message). Upon
    receipt, the requester reconstructs the real reply MessageExt from it plus the
    body.
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
        # Only for timer/delay messages: the broker's
        # SendMessageProcessor#attachRecallHandle attaches it only when it sees
        # TIMER_OUT_MS + REAL_TOPIC; it is always None for ordinary messages.
        self.recall_handle: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "msgId": self.msg_id,
            "queueId": self.queue_id,
            "queueOffset": self.queue_offset,
            "transactionId": self.transaction_id,
            "batchUniqId": self.batch_uniq_id,
            "recallHandle": self.recall_handle,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.msg_id = ext.get("msgId")
        self.queue_id = _i(ext.get("queueId"))
        self.queue_offset = _l(ext.get("queueOffset"))
        self.transaction_id = ext.get("transactionId")
        self.batch_uniq_id = ext.get("batchUniqId")
        self.recall_handle = ext.get("recallHandle")


class RecallMessageRequestHeader(CommandCustomHeader):
    """Corresponds to the protocol header RecallMessageRequestHeader.

    CAUTION: this header inherits the protocol's ``TopicRequestHeader`` ->
    ``RpcRequestHeader``, and the **on-wire key name** of the parent field ``bname``
    is literally ``bname`` (not brokerName): writing ``brokerName`` makes the broker
    silently drop the field. The broker's RecallMessageProcessor only really uses
    topic/producerGroup/recallHandle, and the brokerName validated inside the handle is
    also extracted from the handle, but the key name in the message must still be
    ``bname`` verbatim.
    """

    def __init__(self):
        self.producer_group: Optional[str] = None
        self.topic: Optional[str] = None
        self.recall_handle: Optional[str] = None
        self.bname: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "producerGroup": self.producer_group,
            "topic": self.topic,
            "recallHandle": self.recall_handle,
            "bname": self.bname,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.producer_group = ext.get("producerGroup")
        self.topic = ext.get("topic")
        self.recall_handle = ext.get("recallHandle")
        self.bname = ext.get("bname")


class RecallMessageResponseHeader(CommandCustomHeader):
    """Corresponds to the protocol header RecallMessageResponseHeader: it has a single
    field ``msgId``."""

    def __init__(self):
        self.msg_id: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"msgId": self.msg_id})

    def from_ext_fields(self, ext: dict) -> None:
        self.msg_id = ext.get("msgId")


# ---------------- pull message ----------------


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


# ---------------- offset ----------------


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
        # In the protocol this field is @CFNullable: when it is None the **key is not
        # written at all** (the deprecated 5-parameter searchOffset shape simply has
        # no timestamp key).
        self.boundary_type: Optional[BoundaryType] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "topic": self.topic,
            "queueId": self.queue_id,
            "timestamp": self.timestamp,
            # The on-wire text is the enum name in upper case (Enum.toString() in the
            # broker-side makeCustomHeaderToNet), not the lowercase name from
            # BoundaryType.getName().
            "boundaryType": self.boundary_type.value if self.boundary_type is not None else None,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.queue_id = _i(ext.get("queueId"))
        self.timestamp = _l(ext.get("timestamp"))
        value = ext.get("boundaryType")
        # missing key => None (the broker side falls back to LOWER by itself for a
        # default boundaryType);
        # key present but value unknown => following the lenient semantics of
        # BoundaryType.get_type, return LOWER.
        self.boundary_type = BoundaryType.get_type(value) if value is not None else None


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


# ---------------- query message ----------------


class QueryMessageRequestHeader(CommandCustomHeader):
    pass

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


# ---------------- heartbeat / unregister ----------------


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


# ---------------- consumer management ----------------


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


class GetConsumerStatusRequestHeader(CommandCustomHeader):
    """The request header of GET_CONSUMER_STATUS_FROM_CLIENT(221)."""

    def __init__(self):
        self.topic: Optional[str] = None
        self.group: Optional[str] = None
        self.client_addr: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({"topic": self.topic, "group": self.group,
                     "clientAddr": self.client_addr})

    def from_ext_fields(self, ext: dict) -> None:
        self.topic = ext.get("topic")
        self.group = ext.get("group")
        self.client_addr = ext.get("clientAddr")


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
    pass

    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.client_id: Optional[str] = None
        self.msg_id: Optional[str] = None
        self.broker_name: Optional[str] = None
        self.topic: Optional[str] = None

    def to_ext_fields(self) -> dict:
        return _ext({
            "consumerGroup": self.consumer_group, "clientId": self.client_id,
            "msgId": self.msg_id, "brokerName": self.broker_name,
            "topic": self.topic,
        })

    def from_ext_fields(self, ext: dict) -> None:
        self.consumer_group = ext.get("consumerGroup")
        self.client_id = ext.get("clientId")
        self.msg_id = ext.get("msgId")
        self.broker_name = ext.get("brokerName")
        self.topic = ext.get("topic")


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


# ---------------- lock ----------------


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


# ---------------- transaction ----------------


class EndTransactionRequestHeader(CommandCustomHeader):
    """Corresponds to the protocol header EndTransactionRequestHeader.

    CAUTION: the brokerName field inherited from RpcRequestHeader has an **on-wire key
    name of ``bname``** (the setter is setBrokerName, but the protocol declares the field
    as bname). A wrong key makes the broker silently drop the field.
    The field names strictly follow the protocol: topic / producerGroup /
    tranStateTableOffset / commitLogOffset / commitOrRollback / fromTransactionCheck /
    msgId / transactionId / bname.
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
    """Corresponds to the protocol header CheckTransactionStateRequestHeader.

    CAUTION: it also inherits RpcRequestHeader, so the on-wire key name of brokerName is
    ``bname``. The field names strictly follow the protocol: topic /
    tranStateTableOffset / commitLogOffset / msgId / transactionId / offsetMsgId / bname.
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


# ---------------- admin / query ----------------


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
    pass

    def __init__(self):
        self.topic: Optional[str] = None
        self.default_topic: Optional[str] = None
        self.read_queue_nums: Optional[int] = None
        self.write_queue_nums: Optional[int] = None
        self.perm: Optional[int] = None
        self.topic_filter_type: Optional[str] = None
        self.topic_sys_flag: Optional[int] = None
        self.order: Optional[bool] = None
        # AttributeParser.parseToString format: "k1=v1,k2" (an empty value keeps only the key)
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


# ---------------- POP mode (5.x lightweight consumption) ----------------
#
# An ext key must equal the protocol field name verbatim: the broker deserializes it
# with fastjson using the protocol property names, and a single wrong letter
# **silently drops the field** (no error, the behaviour just degrades silently), so
# every key below was checked verbatim against the field declarations of the Pop*Header
# family in the protocol.
#
# Also note that `order` / `suspend` are **non-null** Boolean/boolean fields in the
# protocol (`order = Boolean.FALSE`, `suspend = false`), and `encodeHeader` only skips
# nulls, so they **always** appear in the message -- so no None filtering is done here.


class PopMessageRequestHeader(CommandCustomHeader):
    """The protocol header ``PopMessageRequestHeader`` (RequestCode.POP_MESSAGE = 200050)."""

    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.topic: Optional[str] = None
        self.queue_id: Optional[int] = None
        self.max_msg_nums: Optional[int] = None
        self.invisible_time: Optional[int] = None
        self.poll_time: Optional[int] = None
        # bornTime must be the **current millisecond timestamp**: the broker replies with
        # POLLING_TIMEOUT(210) straight away once it checks
        # `now - bornTime - pollTime > 500`.
        self.born_time: Optional[int] = None
        # 0 = MIN (start from the smallest offset, historical messages can be
        # consumed), 1 = MAX (only new messages)
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
    """The protocol header ``PopMessageResponseHeader``.

    ``start_offset_info`` / ``msg_offset_info`` / ``order_count_info`` are encoded
    strings (a space separates the fields, a semicolon separates the queues); parse
    them with the ``extra_info`` module.
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
    """The protocol header ``AckMessageRequestHeader`` (RequestCode.ACK_MESSAGE = 200051).

    ``offset`` is the **consumeQueue offset** (that is, the 8th segment of the CK
    string / msgQueueOffset), not the commitlog offset -- passing the wrong one makes
    the broker reply with NO_MESSAGE.
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
    """The protocol header ``ChangeInvisibleTimeRequestHeader`` (CHANGE_MESSAGE_INVISIBLETIME = 200053)."""

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
    """The protocol header ``ChangeInvisibleTimeResponseHeader``.

    What it returns are the **new** ``invisible_time`` / ``pop_time`` / ``revive_qid``;
    the client uses them to rebuild extraInfo for subsequent ACKs.
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
