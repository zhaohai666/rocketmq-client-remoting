# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
from typing import Dict, List, Optional

from common.subscription_data import SubscriptionData
from remoting.protocol.serialize import RemotingSerializable, fastjson_loads
from remoting.protocol.bitset import BitSet
# Parsing/serialization of MessageQueue as a map key (fastjson2 non-string keys) + shared DTOs
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


class CheckClientRequestBody:
    """The protocol model ``CheckClientRequestBody``.

    It is only used by ``CHECK_CLIENT_CONFIG(46)``: the broker logs clientId/group and
    what it really validates is only the expressionType and subString of
    ``subscriptionData`` (on the broker side, in
    ``ClientManageProcessor.checkClientConfig``). The namespace field exists in the
    5.5.1 protocol but the sender never fills it, so the field is kept here without a
    value (fastjson does not serialize None).
    """

    def __init__(self):
        self.client_id: Optional[str] = None
        self.group: Optional[str] = None
        self.subscription_data: Optional[SubscriptionData] = None
        self.namespace: Optional[str] = None

    def to_dict(self) -> dict:
        d = {"clientId": self.client_id, "group": self.group}
        if self.subscription_data is not None:
            d["subscriptionData"] = self.subscription_data.to_dict()
        if self.namespace is not None:
            d["namespace"] = self.namespace
        return d

    @staticmethod
    def from_dict(d: dict) -> "CheckClientRequestBody":
        b = CheckClientRequestBody()
        b.client_id = d.get("clientId")
        b.group = d.get("group")
        b.namespace = d.get("namespace")
        sd = d.get("subscriptionData")
        if sd:
            sub = SubscriptionData(sd.get("topic"), sd.get("subString"))
            sub.class_filter_mode = bool(sd.get("classFilterMode", False))
            sub.tags_set = set(sd.get("tagsSet") or [])
            sub.code_set = set(sd.get("codeSet") or [])
            sub.sub_version = int(sd.get("subVersion", 0))
            sub.expression_type = sd.get("expressionType", "TAG")
            b.subscription_data = sub
        return b

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "CheckClientRequestBody":
        return CheckClientRequestBody.from_dict(RemotingSerializable.decode_json(data))


class ClusterInfo:
    def __init__(self):
        self.broker_addr_table: Dict[str, Dict[int, str]] = {}
        self.cluster_addr_table: Dict[str, List[str]] = {}

    def get_broker_addrs(self) -> List[str]:
        """Collect the addresses of all brokers (deduplicated, ordered by broker name then brokerId)."""
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
        # In real RocketMQ, brokerAddrTable[name] is a BrokerData object, and the actual
        # {brokerId: addr} mapping lives in its brokerAddrs field.
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
    """The protocol model ``ConsumerRunningInfo``.

    Both ends use it: the **admin side** parses the running info aggregated by the
    broker; the **client side** encodes its own running info when replying to
    GET_CONSUMER_RUNNING_INFO(307).

    CAUTION: the keys of ``mqTable`` / ``mqPopTable`` are ``MessageQueue``, and the
    broker-side fastjson inlines them as JSON objects
    (``{{"brokerName":...,"queueId":...,"topic":...}:{...}}``) -- that is not valid JSON,
    but the broker can read it back thanks to fastjson's lenient parsing, so it must be
    handled with ``message_queue_key`` / ``decode_message_queue_map`` and cannot be
    treated as ordinary string keys.
    """

    PROP_NAMESERVER_ADDR = "PROP_NAMESERVER_ADDR"
    PROP_THREADPOOL_CORE_SIZE = "PROP_THREADPOOL_CORE_SIZE"
    PROP_CONSUME_ORDERLY = "PROP_CONSUMEORDERLY"   # the protocol constant name has no underscore (spelled that way)
    PROP_CONSUME_TYPE = "PROP_CONSUME_TYPE"
    PROP_CLIENT_VERSION = "PROP_CLIENT_VERSION"
    PROP_CONSUMER_START_TIMESTAMP = "PROP_CONSUMER_START_TIMESTAMP"

    def __init__(self):
        self.properties: Dict[str, str] = {}
        self.subscription_set: List[dict] = []
        self.mq_table: Dict[MessageQueue, dict] = {}
        self.mq_pop_table: Dict[MessageQueue, dict] = {}
        self.status_table: Dict[str, dict] = {}
        self.user_consumer_info: Dict[str, str] = {}
        self.jstack: Optional[str] = None

    def to_dict(self) -> dict:
        d = {
            "properties": self.properties,
            "subscriptionSet": self.subscription_set,
            "mqTable": {message_queue_key(k): v for k, v in self.mq_table.items()},
            "mqPopTable": {message_queue_key(k): v for k, v in self.mq_pop_table.items()},
            "statusTable": self.status_table,
            "userConsumerInfo": self.user_consumer_info,
            "jstack": self.jstack,
        }
        return d

    @staticmethod
    def from_dict(d: dict) -> "ConsumerRunningInfo":
        ri = ConsumerRunningInfo()
        ri.properties = d.get("properties") or {}
        ri.subscription_set = list(d.get("subscriptionSet") or [])
        ri.mq_table = decode_message_queue_map(d.get("mqTable"))
        ri.mq_pop_table = decode_message_queue_map(d.get("mqPopTable"))
        ri.status_table = d.get("statusTable") or {}
        ri.user_consumer_info = d.get("userConsumerInfo") or {}
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

    @staticmethod
    def decode(data: bytes) -> "QueryConsumeTimeSpanBody":
        return QueryConsumeTimeSpanBody.from_dict(RemotingSerializable.decode_json(data))

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
    """The protocol model `body.ConsumeStatsList` (the `GET_BROKER_CONSUME_STATS` response).

    CAUTION: the JSON key is the protocol field name `consumeStatsList`, not
    `statsList`: with the wrong key name a real response parses into an empty list,
    which looks like "this broker has no backlog".
    `brokerAddr` is a String field in the protocol, serialized with NON_NULL, so when
    it is None the whole key disappears; `totalDiff` / `totalInflightDiff` are `long`
    primitive fields and are always present.
    """

    def __init__(self):
        self.stats_list: List[dict] = []
        self.broker_addr: Optional[str] = None
        self.total_diff: int = 0
        self.total_inflight_diff: int = 0

    def to_dict(self) -> dict:
        d = {"consumeStatsList": self.stats_list}
        if self.broker_addr is not None:
            d["brokerAddr"] = self.broker_addr
        d["totalDiff"] = self.total_diff
        d["totalInflightDiff"] = self.total_inflight_diff
        return d

    @staticmethod
    def from_dict(d: dict) -> "ConsumeStatsList":
        sl = ConsumeStatsList()
        sl.stats_list = list(d.get("consumeStatsList") or [])
        sl.broker_addr = d.get("brokerAddr")
        sl.total_diff = int(d.get("totalDiff") or 0)
        sl.total_inflight_diff = int(d.get("totalInflightDiff") or 0)
        return sl

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "ConsumeStatsList":
        return ConsumeStatsList.from_dict(RemotingSerializable.decode_json(data))


class ResetOffsetBody:
    """The protocol model ``ResetOffsetBody``.

    CAUTION: the protocol field is ``Map<MessageQueue, Long> offsetTable``, **not** a
    nested topic->queueId->offset map (a real broker response that fails to parse has
    exactly this shape). The broker-side fastjson inlines MessageQueue keys as JSON
    objects, so the key helpers from admin_body are used to parse them.
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


class MessageQueueForC:
    pass

    def __init__(self, topic: str = "", broker_name: str = "", queue_id: int = 0,
                 offset: int = 0):
        self.topic = topic
        self.broker_name = broker_name
        self.queue_id = queue_id
        self.offset = offset

    def to_dict(self) -> dict:
        return {"topic": self.topic, "brokerName": self.broker_name,
                "queueId": self.queue_id, "offset": self.offset}

    @staticmethod
    def from_dict(d: dict) -> "MessageQueueForC":
        return MessageQueueForC(
            topic=d.get("topic") or "",
            broker_name=d.get("brokerName") or "",
            queue_id=int(d.get("queueId") or 0),
            offset=int(d.get("offset") or 0),
        )


class ResetOffsetBodyForC:
    """The protocol model ``ResetOffsetBodyForC``: ``offsetTable`` is an
    **array** (each entry carries its own offset), not a map.

    The broker only pushes this body when the language of the 222
    (RESET_CONSUMER_OFFSET) initiator is CPP (``Broker2Client.resetOffset``), so it
    must be parseable -- failing to parse one that arrived means silently dropping the
    whole reset.
    """

    def __init__(self):
        self.offset_table: List[MessageQueueForC] = []

    def to_dict(self) -> dict:
        return {"offsetTable": [e.to_dict() for e in self.offset_table]}

    @staticmethod
    def from_dict(d: dict) -> "ResetOffsetBodyForC":
        b = ResetOffsetBodyForC()
        for item in d.get("offsetTable") or []:
            if isinstance(item, dict):
                b.offset_table.append(MessageQueueForC.from_dict(item))
        return b

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "ResetOffsetBodyForC":
        return ResetOffsetBodyForC.from_dict(fastjson_loads(data.decode("utf-8")))


# ---------------------------------------------------------------- 42 GET_CONSUMER_STATUS_FROM_CLIENT
class GetConsumerStatusBody:
    pass

    def __init__(self):
        self.message_queue_table: Dict[MessageQueue, int] = {}
        self.consumer_table: Dict[str, Dict[MessageQueue, int]] = {}

    def to_dict(self) -> dict:
        return {
            "messageQueueTable": {message_queue_key(k): v
                                  for k, v in self.message_queue_table.items()},
            "consumerTable": {cid: {message_queue_key(k): v for k, v in tbl.items()}
                              for cid, tbl in self.consumer_table.items()},
        }

    @staticmethod
    def from_dict(d: dict) -> "GetConsumerStatusBody":
        b = GetConsumerStatusBody()
        b.message_queue_table = {mq: int(v) for mq, v in
                                 decode_message_queue_map(d.get("messageQueueTable")).items()}
        for cid, tbl in (d.get("consumerTable") or {}).items():
            b.consumer_table[cid] = {mq: int(v) for mq, v in
                                     decode_message_queue_map(tbl).items()}
        return b

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "GetConsumerStatusBody":
        return GetConsumerStatusBody.from_dict(fastjson_loads(data.decode("utf-8")))


# ---------------------------------------------------------------- 307 / 309
class ProcessQueueInfo:
    pass

    def __init__(self):
        self.commit_offset: int = 0
        self.cached_msg_min_offset: int = 0
        self.cached_msg_max_offset: int = 0
        self.cached_msg_count: int = 0
        self.cached_msg_size_in_mib: int = 0
        self.transaction_msg_min_offset: int = 0
        self.transaction_msg_max_offset: int = 0
        self.transaction_msg_count: int = 0
        self.locked: bool = False
        self.try_unlock_times: int = 0
        self.last_lock_timestamp: int = 0
        self.droped: bool = False
        self.last_pull_timestamp: int = 0
        self.last_consume_timestamp: int = 0

    def to_dict(self) -> dict:
        return {
            "commitOffset": self.commit_offset,
            "cachedMsgMinOffset": self.cached_msg_min_offset,
            "cachedMsgMaxOffset": self.cached_msg_max_offset,
            "cachedMsgCount": self.cached_msg_count,
            "cachedMsgSizeInMiB": self.cached_msg_size_in_mib,
            "transactionMsgMinOffset": self.transaction_msg_min_offset,
            "transactionMsgMaxOffset": self.transaction_msg_max_offset,
            "transactionMsgCount": self.transaction_msg_count,
            "locked": self.locked,
            "tryUnlockTimes": self.try_unlock_times,
            "lastLockTimestamp": self.last_lock_timestamp,
            "droped": self.droped,
            "lastPullTimestamp": self.last_pull_timestamp,
            "lastConsumeTimestamp": self.last_consume_timestamp,
        }

    @staticmethod
    def from_dict(d: dict) -> "ProcessQueueInfo":
        p = ProcessQueueInfo()
        p.commit_offset = int(d.get("commitOffset", 0))
        p.cached_msg_min_offset = int(d.get("cachedMsgMinOffset", 0))
        p.cached_msg_max_offset = int(d.get("cachedMsgMaxOffset", 0))
        p.cached_msg_count = int(d.get("cachedMsgCount", 0))
        p.cached_msg_size_in_mib = int(d.get("cachedMsgSizeInMiB", 0))
        p.transaction_msg_min_offset = int(d.get("transactionMsgMinOffset", 0))
        p.transaction_msg_max_offset = int(d.get("transactionMsgMaxOffset", 0))
        p.transaction_msg_count = int(d.get("transactionMsgCount", 0))
        p.locked = bool(d.get("locked", False))
        p.try_unlock_times = int(d.get("tryUnlockTimes", 0))
        p.last_lock_timestamp = int(d.get("lastLockTimestamp", 0))
        p.droped = bool(d.get("droped", False))
        p.last_pull_timestamp = int(d.get("lastPullTimestamp", 0))
        p.last_consume_timestamp = int(d.get("lastConsumeTimestamp", 0))
        return p

    def __repr__(self):
        return ("ProcessQueueInfo[commitOffset=%d, cached=%d, droped=%s]"
                % (self.commit_offset, self.cached_msg_count, self.droped))


class CMResult:
    pass

    CR_SUCCESS = "CR_SUCCESS"
    CR_LATER = "CR_LATER"
    CR_ROLLBACK = "CR_ROLLBACK"
    CR_COMMIT = "CR_COMMIT"
    CR_THROW_EXCEPTION = "CR_THROW_EXCEPTION"
    CR_RETURN_NULL = "CR_RETURN_NULL"


class ConsumeMessageDirectlyResult:
    pass

    def __init__(self):
        self.order: bool = False
        self.auto_commit: bool = True
        self.consume_result: Optional[str] = None
        self.remark: Optional[str] = None
        self.spent_time_mills: int = 0

    def to_dict(self) -> dict:
        d = {
            "order": self.order,
            "autoCommit": self.auto_commit,
            "consumeResult": self.consume_result,
            "remark": self.remark,
            "spentTimeMills": self.spent_time_mills,
        }
        return d

    @staticmethod
    def from_dict(d: dict) -> "ConsumeMessageDirectlyResult":
        r = ConsumeMessageDirectlyResult()
        r.order = bool(d.get("order", False))
        r.auto_commit = bool(d.get("autoCommit", True))
        r.consume_result = d.get("consumeResult")
        r.remark = d.get("remark")
        r.spent_time_mills = int(d.get("spentTimeMills", 0))
        return r

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "ConsumeMessageDirectlyResult":
        return ConsumeMessageDirectlyResult.from_dict(fastjson_loads(data.decode("utf-8")))


# ---------------------------------------------------------------- POP batch acknowledge
#: compact key -> long key accepted when reading a ``BatchAck`` entry. The compact family
#: is what goes on the wire; the long spellings only appear in payloads produced elsewhere.
_BATCH_ACK_KEY_ALIASES = {
    "c": "consumerGroup",
    "t": "topic",
    "r": "retry",
    "so": "startOffset",
    "q": "queueId",
    "rq": "reviveQueueId",
    "pt": "popTime",
    "it": "invisibleTime",
    "b": "bitSet",
}


class BatchAck:
    """One entry of the batch acknowledge body: every acknowledged offset of a single
    pop of a single queue.

    ``start_offset`` is the checkpoint offset of that pop and ``bit_set`` marks the
    acknowledged offsets **relative to it**: bit ``i`` stands for consume-queue offset
    ``start_offset + i``, which the broker expands back into absolute offsets.

    The JSON keys are the compact family ``c t r so q rq pt it b``, and ``b`` is the
    base64 of the bit set bytes (little-endian, trailing zero bytes dropped).
    """

    def __init__(self):
        self.consumer_group: Optional[str] = None
        self.topic: Optional[str] = None
        self.retry: str = "0"          # "1"/"2" when the message was revived from a retry topic
        self.start_offset: int = 0
        self.queue_id: int = 0
        self.revive_queue_id: int = 0
        self.pop_time: int = 0
        self.invisible_time: int = 0
        self.bit_set: BitSet = BitSet()

    def acked_offsets(self) -> List[int]:
        """The absolute consume-queue offsets this entry acknowledges."""
        return [self.start_offset + index for index in self.bit_set.set_indexes()]

    def to_dict(self) -> dict:
        return {
            "c": self.consumer_group,
            "t": self.topic,
            "r": self.retry,
            "so": self.start_offset,
            "q": self.queue_id,
            "rq": self.revive_queue_id,
            "pt": self.pop_time,
            "it": self.invisible_time,
            "b": base64.b64encode(self.bit_set.to_bytes()).decode("ascii"),
        }

    @staticmethod
    def from_dict(d: dict) -> "BatchAck":
        def pick(compact: str) -> Optional[object]:
            if compact in d:
                return d[compact]
            return d.get(_BATCH_ACK_KEY_ALIASES[compact])

        ack = BatchAck()
        ack.consumer_group = pick("c")
        ack.topic = pick("t")
        retry = pick("r")
        ack.retry = str(retry) if retry is not None else "0"
        ack.start_offset = int(pick("so") or 0)
        ack.queue_id = int(pick("q") or 0)
        ack.revive_queue_id = int(pick("rq") or 0)
        ack.pop_time = int(pick("pt") or 0)
        ack.invisible_time = int(pick("it") or 0)
        raw = pick("b")
        ack.bit_set = BitSet.from_bytes(base64.b64decode(raw)) if raw else BitSet()
        return ack

    def __repr__(self):
        return ("BatchAck[consumerGroup=%s, topic=%s, retry=%s, startOffset=%d, queueId=%d, "
                "reviveQueueId=%d, popTime=%d, invisibleTime=%d, bits=%r]"
                % (self.consumer_group, self.topic, self.retry, self.start_offset,
                   self.queue_id, self.revive_queue_id, self.pop_time,
                   self.invisible_time, self.bit_set.set_indexes()))


class BatchAckMessageRequestBody:
    """The body of ``BATCH_ACK_MESSAGE(200151)``: one broker plus the merged entries.

    The request itself carries **no custom header** -- group, topic, queue, offsets,
    popTime and invisibleTime all travel inside these entries.
    """

    def __init__(self):
        self.broker_name: Optional[str] = None
        self.acks: List[BatchAck] = []

    @staticmethod
    def from_extra_infos(consumer_group: str, topic: str,
                         extra_info_list: List[str]) -> "BatchAckMessageRequestBody":
        """Merge POP checkpoints into body entries, one per distinct pop.

        Two checkpoints belong to the same pop -- and therefore to the same entry -- when
        their retry flag, queueId, checkpoint offset **and** popTime all match; that
        quadruple is the merge key. The first checkpoint of the list decides the body's
        broker name, so a list spanning brokers must be split by the caller.
        """
        from remoting.protocol import extra_info as extra

        broker_name: Optional[str] = None
        merged: Dict[str, BatchAck] = {}
        for info in extra_info_list:
            seg = extra.split(info)
            if broker_name is None:
                broker_name = extra.get_broker_name(seg)
            retry = extra.get_retry(seg)
            queue_id = extra.get_queue_id(seg)
            ck_queue_offset = extra.get_ck_queue_offset(seg)
            pop_time = extra.get_pop_time(seg)
            merge_key = "%s@%d@%d@%d" % (retry, queue_id, ck_queue_offset, pop_time)
            ack = merged.get(merge_key)
            if ack is None:
                ack = BatchAck()
                ack.consumer_group = consumer_group
                ack.topic = topic
                ack.retry = retry
                ack.start_offset = ck_queue_offset
                ack.queue_id = queue_id
                ack.revive_queue_id = extra.get_revive_qid(seg)
                ack.pop_time = pop_time
                ack.invisible_time = extra.get_invisible_time(seg)
                merged[merge_key] = ack
            # the offset is relative to the checkpoint of this very pop
            ack.bit_set.set_bit(extra.get_queue_offset(seg) - ck_queue_offset)

        body = BatchAckMessageRequestBody()
        body.broker_name = broker_name
        body.acks = list(merged.values())
        return body

    def to_dict(self) -> dict:
        return {"brokerName": self.broker_name,
                "acks": [a.to_dict() for a in self.acks]}

    @staticmethod
    def from_dict(d: dict) -> "BatchAckMessageRequestBody":
        b = BatchAckMessageRequestBody()
        b.broker_name = d.get("brokerName")
        b.acks = [BatchAck.from_dict(a) for a in (d.get("acks") or [])]
        return b

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "BatchAckMessageRequestBody":
        return BatchAckMessageRequestBody.from_dict(RemotingSerializable.decode_json(data))
