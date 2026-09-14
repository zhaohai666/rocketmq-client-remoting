# -*- coding: utf-8 -*-
"""管理端专用响应体（对应 org.apache.rocketmq.remoting.protocol.admin.* 与 body.* 中的管理类）：

TopicStatsTable / TopicOffset / ConsumeStats / OffsetWrapper /
TopicConfigSerializeWrapper / ConsumeQueueData / QueryConsumeQueueResponseBody。

⚠ 关键坑：这几个类的 Map 键是 **MessageQueue**，fastjson2 会把键直接内联成 JSON 对象，
产出**非法 JSON**，例如：
    {"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}:{...}}}
所以必须用 serialize.fastjson_loads 解析，再把字符串键还原成 MessageQueue。
字段名与结构均由 Java 探针实测确认。
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from ...common.message import MessageQueue
from ...common.topic_config import TopicConfig
from . import serialize
from .serialize import RemotingSerializable, decode_message_queue_key


def parse_message_queue_key(key: str) -> Optional[MessageQueue]:
    """把 fastjson2 写出的 MessageQueue 内联对象键还原成 MessageQueue。

    返回 None 表示这个键不是内联对象（例如 ``"0"``、``"G1"`` 这类普通字符串键）。
    """
    d = decode_message_queue_key(key)
    if d is None:
        return None
    return MessageQueue(d.get("topic", ""), d.get("brokerName", ""), d.get("queueId", 0))


def message_queue_key(mq: MessageQueue) -> str:
    """把 MessageQueue 序列化成 fastjson2 风格的内联对象键（键按字母序）。"""
    return '{"brokerName":"%s","queueId":%d,"topic":"%s"}' % (
        mq.broker_name, mq.queue_id, mq.topic)


def decode_message_queue_map(raw: Optional[dict]) -> Dict[MessageQueue, Any]:
    """通用：解析以 MessageQueue 为键的 map（fastjson2 非字符串键）。"""
    result: Dict[MessageQueue, Any] = {}
    for k, v in (raw or {}).items():
        mq = parse_message_queue_key(k)
        if mq is not None:
            result[mq] = v
    return result


# ---------------------------------------------------------------- TopicStatsTable
class TopicOffset:
    """对应 org.apache.rocketmq.remoting.protocol.admin.TopicOffset。"""

    def __init__(self, min_offset: int = 0, max_offset: int = 0,
                 last_update_timestamp: int = 0):
        self.min_offset = min_offset
        self.max_offset = max_offset
        self.last_update_timestamp = last_update_timestamp

    def to_dict(self) -> dict:
        return {
            "minOffset": self.min_offset,
            "maxOffset": self.max_offset,
            "lastUpdateTimestamp": self.last_update_timestamp,
        }

    @staticmethod
    def from_dict(d: dict) -> "TopicOffset":
        return TopicOffset(
            min_offset=d.get("minOffset", 0),
            max_offset=d.get("maxOffset", 0),
            last_update_timestamp=d.get("lastUpdateTimestamp", 0),
        )

    def __repr__(self):
        return "TopicOffset[min=%d, max=%d, ts=%d]" % (
            self.min_offset, self.max_offset, self.last_update_timestamp)


class TopicStatsTable:
    """对应 org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable。

    探针输出：{"offsetTable":{<MessageQueue>:{...}},"topicPutTps":0.0}
    """

    def __init__(self):
        self.offset_table: Dict[MessageQueue, TopicOffset] = {}
        self.topic_put_tps: float = 0.0

    def to_dict(self) -> dict:
        return {
            "offsetTable": {message_queue_key(k): v.to_dict() for k, v in self.offset_table.items()},
            "topicPutTps": self.topic_put_tps,
        }

    @staticmethod
    def from_dict(d: dict) -> "TopicStatsTable":
        t = TopicStatsTable()
        for mq, v in decode_message_queue_map(d.get("offsetTable")).items():
            t.offset_table[mq] = TopicOffset.from_dict(v)
        t.topic_put_tps = d.get("topicPutTps", 0.0)
        return t

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "TopicStatsTable":
        return TopicStatsTable.from_dict(serialize.fastjson_loads(data.decode("utf-8")))


# ---------------------------------------------------------------- ConsumeStats
class OffsetWrapper:
    """对应 org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper。"""

    def __init__(self, broker_offset: int = 0, consumer_offset: int = 0,
                 last_timestamp: int = 0, pull_offset: int = 0):
        self.broker_offset = broker_offset
        self.consumer_offset = consumer_offset
        self.last_timestamp = last_timestamp
        self.pull_offset = pull_offset

    @property
    def lag(self) -> int:
        """与 Java OffsetWrapper.getLag() 一致：brokerOffset - consumerOffset。"""
        return self.broker_offset - self.consumer_offset

    def to_dict(self) -> dict:
        return {
            "brokerOffset": self.broker_offset,
            "consumerOffset": self.consumer_offset,
            "lastTimestamp": self.last_timestamp,
            "pullOffset": self.pull_offset,
        }

    @staticmethod
    def from_dict(d: dict) -> "OffsetWrapper":
        return OffsetWrapper(
            broker_offset=d.get("brokerOffset", 0),
            consumer_offset=d.get("consumerOffset", 0),
            last_timestamp=d.get("lastTimestamp", 0),
            pull_offset=d.get("pullOffset", 0),
        )

    def __repr__(self):
        return "OffsetWrapper[broker=%d, consumer=%d, lag=%d]" % (
            self.broker_offset, self.consumer_offset, self.lag)


class ConsumeStats:
    """对应 org.apache.rocketmq.remoting.protocol.admin.ConsumeStats。

    探针输出：{"consumeTps":1.5,"offsetTable":{<MessageQueue>:<OffsetWrapper>}}
    """

    def __init__(self):
        self.offset_table: Dict[MessageQueue, OffsetWrapper] = {}
        self.consume_tps: float = 0.0

    @property
    def total_lag(self) -> int:
        return sum(ow.lag for ow in self.offset_table.values())

    def to_dict(self) -> dict:
        return {
            "offsetTable": {message_queue_key(k): v.to_dict() for k, v in self.offset_table.items()},
            "consumeTps": self.consume_tps,
        }

    @staticmethod
    def from_dict(d: dict) -> "ConsumeStats":
        cs = ConsumeStats()
        for mq, v in decode_message_queue_map(d.get("offsetTable")).items():
            cs.offset_table[mq] = OffsetWrapper.from_dict(v)
        cs.consume_tps = d.get("consumeTps", 0.0)
        return cs

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "ConsumeStats":
        return ConsumeStats.from_dict(serialize.fastjson_loads(data.decode("utf-8")))


# ---------------------------------------------------------------- TopicConfigSerializeWrapper
class TopicConfigSerializeWrapper:
    """对应 org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper。

    探针输出：{"dataVersion":{...},"topicConfigTable":{"Topic":{"attributes":{},...}}}
    """

    def __init__(self):
        self.topic_config_table: Dict[str, TopicConfig] = {}
        self.data_version: Dict[str, Any] = {}

    def to_dict(self) -> dict:
        return {
            "dataVersion": self.data_version,
            "topicConfigTable": {k: v.to_dict() for k, v in self.topic_config_table.items()},
        }

    @staticmethod
    def from_dict(d: dict) -> "TopicConfigSerializeWrapper":
        w = TopicConfigSerializeWrapper()
        w.topic_config_table = {
            k: TopicConfig.from_dict(v)
            for k, v in (d.get("topicConfigTable") or {}).items()}
        w.data_version = dict(d.get("dataVersion") or {})
        return w

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "TopicConfigSerializeWrapper":
        return TopicConfigSerializeWrapper.from_dict(serialize.fastjson_loads(data.decode("utf-8")))


# ---------------------------------------------------------------- QueryConsumeQueue
class ConsumeQueueData:
    """对应 org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData。

    探针/源码字段：physicOffset, physicSize, tagsCode, extendDataJson, bitMap, eval, msg
    """

    def __init__(self, physic_offset: int = 0, physic_size: int = 0, tags_code: int = 0,
                 extend_data_json: Optional[str] = None, bit_map: Optional[str] = None,
                 eval_: bool = False, msg: Optional[str] = None):
        self.physic_offset = physic_offset
        self.physic_size = physic_size
        self.tags_code = tags_code
        self.extend_data_json = extend_data_json
        self.bit_map = bit_map
        self.eval = eval_
        self.msg = msg

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {
            "physicOffset": self.physic_offset,
            "physicSize": self.physic_size,
            "tagsCode": self.tags_code,
            "eval": self.eval,
            "bitMap": self.bit_map,
        }
        # 同 Java：null 字段不序列化
        if self.extend_data_json is not None:
            d["extendDataJson"] = self.extend_data_json
        if self.msg is not None:
            d["msg"] = self.msg
        return d

    @staticmethod
    def from_dict(d: dict) -> "ConsumeQueueData":
        return ConsumeQueueData(
            physic_offset=d.get("physicOffset", 0),
            physic_size=d.get("physicSize", 0),
            tags_code=d.get("tagsCode", 0),
            extend_data_json=d.get("extendDataJson"),
            bit_map=d.get("bitMap"),
            eval_=d.get("eval", False),
            msg=d.get("msg"),
        )


class QueryConsumeQueueResponseBody:
    """对应 org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody。

    探针输出：{"filterData":"*","maxQueueIndex":88,"minQueueIndex":1,"subscriptionData":{...}}
    （queueData 为 null 时不出现）
    """

    def __init__(self):
        self.subscription_data: Optional[dict] = None
        self.filter_data: Optional[str] = None
        self.queue_data: Optional[List[ConsumeQueueData]] = None
        self.max_queue_index: int = 0
        self.min_queue_index: int = 0

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {
            "maxQueueIndex": self.max_queue_index,
            "minQueueIndex": self.min_queue_index,
        }
        if self.subscription_data is not None:
            d["subscriptionData"] = self.subscription_data
        if self.filter_data is not None:
            d["filterData"] = self.filter_data
        if self.queue_data is not None:
            d["queueData"] = [q.to_dict() for q in self.queue_data]
        return d

    @staticmethod
    def from_dict(d: dict) -> "QueryConsumeQueueResponseBody":
        b = QueryConsumeQueueResponseBody()
        b.subscription_data = d.get("subscriptionData")
        b.filter_data = d.get("filterData")
        raw = d.get("queueData")
        b.queue_data = [ConsumeQueueData.from_dict(x) for x in raw] if raw else None
        b.max_queue_index = d.get("maxQueueIndex", 0)
        b.min_queue_index = d.get("minQueueIndex", 0)
        return b

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "QueryConsumeQueueResponseBody":
        return QueryConsumeQueueResponseBody.from_dict(
            serialize.fastjson_loads(data.decode("utf-8")))


__all__ = ["TopicOffset", "TopicStatsTable", "OffsetWrapper", "ConsumeStats",
           "TopicConfigSerializeWrapper", "ConsumeQueueData",
           "QueryConsumeQueueResponseBody"]
