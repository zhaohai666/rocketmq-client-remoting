# -*- coding: utf-8 -*-
"""公共 body（对应 org.apache.rocketmq.remoting.protocol.body.* 常用部分）。"""
from __future__ import annotations

from typing import Dict, List, Optional

from ..protocol.serialize import RemotingSerializable, fastjson_loads
# MessageQueue 作为 map 键的解析/序列化（fastjson2 非字符串键）+ 共用 DTO
from .admin_body import decode_message_queue_map, message_queue_key  # noqa: F401


class KVTable:
    def __init__(self):
        self.table: Dict[str, str] = {}

    def to_dict(self) -> dict:
        return {"table": self.table}

    @staticmethod
    def from_dict(d: dict) -> "KVTable":
        kv = KVTable()
        kv.table = d.get("table") or {}
        return kv

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "KVTable":
        return KVTable.from_dict(RemotingSerializable.decode_json(data))


class TopicList:
    def __init__(self):
        self.topic_list: List[str] = []
        self.broker_addr: Optional[str] = None

    def get_topic_list(self) -> List[str]:
        return list(self.topic_list)

    def to_dict(self) -> dict:
        d = {"topicList": self.topic_list}
        if self.broker_addr is not None:
            d["brokerAddr"] = self.broker_addr
        return d

    @staticmethod
    def from_dict(d: dict) -> "TopicList":
        tl = TopicList()
        tl.topic_list = list(d.get("topicList") or [])
        tl.broker_addr = d.get("brokerAddr")
        return tl

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "TopicList":
        return TopicList.from_dict(RemotingSerializable.decode_json(data))


class LockBatchRequestBody:
    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.client_id: Optional[str] = None
        self.mq_set: List[dict] = []

    def to_dict(self) -> dict:
        return {
            "consumerGroup": self.consumer_group,
            "clientId": self.client_id,
            "mqSet": self.mq_set,
        }

    @staticmethod
    def from_dict(d: dict) -> "LockBatchRequestBody":
        b = LockBatchRequestBody()
        b.consumer_group = d.get("consumerGroup")
        b.client_id = d.get("clientId")
        b.mq_set = list(d.get("mqSet") or [])
        return b

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "LockBatchRequestBody":
        return LockBatchRequestBody.from_dict(RemotingSerializable.decode_json(data))


class LockBatchResponseBody:
    def __init__(self):
        self.lock_ok_mq_set: List[dict] = []

    def to_dict(self) -> dict:
        return {"lockOKMQSet": self.lock_ok_mq_set}

    @staticmethod
    def from_dict(d: dict) -> "LockBatchResponseBody":
        b = LockBatchResponseBody()
        b.lock_ok_mq_set = list(d.get("lockOKMQSet") or [])
        return b

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "LockBatchResponseBody":
        return LockBatchResponseBody.from_dict(RemotingSerializable.decode_json(data))


class UnlockBatchRequestBody:
    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.client_id: Optional[str] = None
        self.mq_set: List[dict] = []

    def to_dict(self) -> dict:
        return {
            "consumerGroup": self.consumer_group,
            "clientId": self.client_id,
            "mqSet": self.mq_set,
        }

    @staticmethod
    def from_dict(d: dict) -> "UnlockBatchRequestBody":
        b = UnlockBatchRequestBody()
        b.consumer_group = d.get("consumerGroup")
        b.client_id = d.get("clientId")
        b.mq_set = list(d.get("mqSet") or [])
        return b

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "UnlockBatchRequestBody":
        return UnlockBatchRequestBody.from_dict(RemotingSerializable.decode_json(data))


class GetConsumerListByGroupResponseBody:
    def __init__(self):
        self.consumer_id_list: List[str] = []

    def to_dict(self) -> dict:
        return {"consumerIdList": self.consumer_id_list}

    @staticmethod
    def from_dict(d: dict) -> "GetConsumerListByGroupResponseBody":
        b = GetConsumerListByGroupResponseBody()
        b.consumer_id_list = list(d.get("consumerIdList") or [])
        return b

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "GetConsumerListByGroupResponseBody":
        return GetConsumerListByGroupResponseBody.from_dict(RemotingSerializable.decode_json(data))


class ClusterInfo:
    def __init__(self):
        self.broker_addr_table: Dict[str, Dict[int, str]] = {}
        self.cluster_addr_table: Dict[str, List[str]] = {}

    def get_broker_addrs(self) -> List[str]:
        """收集所有 broker 的地址（去重，按 broker 名称、brokerId 顺序）。"""
        addrs: List[str] = []
        seen = set()
        for broker_name in sorted(self.broker_addr_table.keys()):
            for broker_id in sorted(self.broker_addr_table[broker_name].keys()):
                addr = self.broker_addr_table[broker_name][broker_id]
                if addr and addr not in seen:
                    seen.add(addr)
                    addrs.append(addr)
        return addrs

    def to_dict(self) -> dict:
        return {
            "brokerAddrTable": {
                k: {
                    "cluster": "",
                    "brokerName": k,
                    "brokerAddrs": {str(kk): v for kk, v in vv.items()},
                    "enableActingMaster": False,
                } for k, vv in self.broker_addr_table.items()
            },
            "clusterAddrTable": self.cluster_addr_table,
        }

    @staticmethod
    def from_dict(d: dict) -> "ClusterInfo":
        ci = ClusterInfo()
        # 真实 RocketMQ 的 brokerAddrTable[name] 是一个 BrokerData 对象，
        # 真正的 {brokerId: addr} 映射在其中的 brokerAddrs 字段下。
        ci.broker_addr_table = {
            k: {int(kk): v for kk, v in (vv.get("brokerAddrs") or {}).items()}
            for k, vv in (d.get("brokerAddrTable") or {}).items()
        }
        ci.cluster_addr_table = d.get("clusterAddrTable") or {}
        return ci

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "ClusterInfo":
        return ClusterInfo.from_dict(RemotingSerializable.decode_json(data))


class ConsumerRunningInfo:
    def __init__(self):
        self.properties: Dict[str, str] = {}
        self.subscription_set: List[dict] = []
        self.mq_table: Dict[str, dict] = {}
        self.jstack: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "properties": self.properties,
            "subscriptionSet": self.subscription_set,
            "mqTable": self.mq_table,
            "jstack": self.jstack,
        }

    @staticmethod
    def from_dict(d: dict) -> "ConsumerRunningInfo":
        ri = ConsumerRunningInfo()
        ri.properties = d.get("properties") or {}
        ri.subscription_set = list(d.get("subscriptionSet") or [])
        ri.mq_table = d.get("mqTable") or {}
        ri.jstack = d.get("jstack")
        return ri

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "ConsumerRunningInfo":
        return ConsumerRunningInfo.from_dict(RemotingSerializable.decode_json(data))


class Connection:
    def __init__(self):
        self.client_id: Optional[str] = None
        self.client_addr: Optional[str] = None
        self.language: Optional[str] = None
        self.version: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "clientId": self.client_id,
            "clientAddr": self.client_addr,
            "language": self.language,
            "version": self.version,
        }

    @staticmethod
    def from_dict(d: dict) -> "Connection":
        c = Connection()
        c.client_id = d.get("clientId")
        c.client_addr = d.get("clientAddr")
        c.language = d.get("language")
        c.version = d.get("version")
        return c


class ConsumerConnection:
    def __init__(self):
        self.connection_set: List[Connection] = []
        self.subscription_table: Dict[str, dict] = {}
        self.consume_type: Optional[str] = None
        self.message_model: Optional[str] = None
        self.consume_from_where: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "connectionSet": [c.to_dict() for c in self.connection_set],
            "subscriptionTable": self.subscription_table,
            "consumeType": self.consume_type,
            "messageModel": self.message_model,
            "consumeFromWhere": self.consume_from_where,
        }

    @staticmethod
    def from_dict(d: dict) -> "ConsumerConnection":
        cc = ConsumerConnection()
        cc.connection_set = [Connection.from_dict(c) for c in (d.get("connectionSet") or [])]
        cc.subscription_table = d.get("subscriptionTable") or {}
        cc.consume_type = d.get("consumeType")
        cc.message_model = d.get("messageModel")
        cc.consume_from_where = d.get("consumeFromWhere")
        return cc

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "ConsumerConnection":
        return ConsumerConnection.from_dict(RemotingSerializable.decode_json(data))


class ProducerConnection:
    def __init__(self):
        self.connection_set: List[Connection] = []

    def to_dict(self) -> dict:
        return {"connectionSet": [c.to_dict() for c in self.connection_set]}

    @staticmethod
    def from_dict(d: dict) -> "ProducerConnection":
        pc = ProducerConnection()
        pc.connection_set = [Connection.from_dict(c) for c in (d.get("connectionSet") or [])]
        return pc

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "ProducerConnection":
        return ProducerConnection.from_dict(RemotingSerializable.decode_json(data))


class QueryConsumeTimeSpanBody:
    def __init__(self):
        self.consume_time_span_set: List[dict] = []

    def to_dict(self) -> dict:
        return {"consumeTimeSpanSet": self.consume_time_span_set}

    @staticmethod
    def from_dict(d: dict) -> "QueryConsumeTimeSpanBody":
        b = QueryConsumeTimeSpanBody()
        b.consume_time_span_set = list(d.get("consumeTimeSpanSet") or [])
        return b

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "QueryConsumeTimeSpanBody":
        return QueryConsumeTimeSpanBody.from_dict(RemotingSerializable.decode_json(data))


class ConsumeStatus:
    def __init__(self):
        self.pull_rt: float = 0
        self.pull_tps: float = 0
        self.consume_rt: float = 0
        self.consume_ok_tps: float = 0
        self.consume_failed_tps: float = 0
        self.consume_failed_msgs: int = 0

    def to_dict(self) -> dict:
        return {
            "pullRT": self.pull_rt,
            "pullTPS": self.pull_tps,
            "consumeRT": self.consume_rt,
            "consumeOKTPS": self.consume_ok_tps,
            "consumeFailedTPS": self.consume_failed_tps,
            "consumeFailedMsgs": self.consume_failed_msgs,
        }

    @staticmethod
    def from_dict(d: dict) -> "ConsumeStatus":
        cs = ConsumeStatus()
        cs.pull_rt = d.get("pullRT", 0)
        cs.pull_tps = d.get("pullTPS", 0)
        cs.consume_rt = d.get("consumeRT", 0)
        cs.consume_ok_tps = d.get("consumeOKTPS", 0)
        cs.consume_failed_tps = d.get("consumeFailedTPS", 0)
        cs.consume_failed_msgs = d.get("consumeFailedMsgs", 0)
        return cs


class ConsumeStatsList:
    def __init__(self):
        self.stats_list: List[dict] = []
        self.broker_addr: Optional[str] = None

    def to_dict(self) -> dict:
        d = {"statsList": self.stats_list}
        if self.broker_addr is not None:
            d["brokerAddr"] = self.broker_addr
        return d

    @staticmethod
    def from_dict(d: dict) -> "ConsumeStatsList":
        sl = ConsumeStatsList()
        sl.stats_list = list(d.get("statsList") or [])
        sl.broker_addr = d.get("brokerAddr")
        return sl

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "ConsumeStatsList":
        return ConsumeStatsList.from_dict(RemotingSerializable.decode_json(data))


class ResetOffsetBody:
    """对应 org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody。

    ⚠ Java 字段是 ``Map<MessageQueue, Long> offsetTable``，**不是** topic→queueId→offset
    的嵌套 map（早期 Python 实现写错了，真实 broker 响应解析不出来）。
    fastjson2 会把 MessageQueue 键内联成 JSON 对象，故用 admin_body 的键工具解析。
    """

    def __init__(self):
        self.offset_table: Dict[MessageQueue, int] = {}

    def to_dict(self) -> dict:
        return {"offsetTable": {message_queue_key(k): v for k, v in self.offset_table.items()}}

    @staticmethod
    def from_dict(d: dict) -> "ResetOffsetBody":
        b = ResetOffsetBody()
        for mq, v in decode_message_queue_map(d.get("offsetTable")).items():
            b.offset_table[mq] = int(v)
        return b

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "ResetOffsetBody":
        return ResetOffsetBody.from_dict(fastjson_loads(data.decode("utf-8")))