# -*- coding: utf-8 -*-
"""公共 body（对应 org.apache.rocketmq.remoting.protocol.body.* 常用部分）。"""
from __future__ import annotations

from typing import Dict, List, Optional

from ...common.subscription_data import SubscriptionData
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


class CheckClientRequestBody:
    """对应 org.apache.rocketmq.remoting.protocol.body.CheckClientRequestBody。

    只被 ``CHECK_CLIENT_CONFIG(46)`` 用到：broker 拿 clientId/group 记日志，真正被校验的
    只有 ``subscriptionData`` 的 expressionType 与 subString（Java
    ``ClientManageProcessor.checkClientConfig``）。namespace 字段 Java 5.5.1 里存在但
    发送端不填，这里同样保留字段而不写值（fastjson 不序列化 None）。
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
    """对应 org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo。

    两端都用得到：**admin 侧**解析 broker 汇总的运行信息；**客户端侧**在应答
    GET_CONSUMER_RUNNING_INFO(307) 时编码自己的运行信息。

    ⚠ ``mqTable`` / ``mqPopTable`` 的键是 ``MessageQueue``，fastjson2 会把它内联成
    JSON 对象（``{{"brokerName":...,"queueId":...,"topic":...}:{...}}``）——这不是
    合法 JSON，但 Java 的 fastjson2 能读回来，所以必须用 ``message_queue_key`` /
    ``decode_message_queue_map`` 处理，不能当普通字符串键。
    """

    PROP_NAMESERVER_ADDR = "PROP_NAMESERVER_ADDR"
    PROP_THREADPOOL_CORE_SIZE = "PROP_THREADPOOL_CORE_SIZE"
    PROP_CONSUME_ORDERLY = "PROP_CONSUMEORDERLY"   # 注意 Java 常量名没有下划线
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
    """对应 Java `body.ConsumeStatsList`（`GET_BROKER_CONSUME_STATS` 响应）。

    ⚠ JSON 键是 Java 字段名 `consumeStatsList`，不是 `statsList`：写错键名时
    真机响应会解析成空列表，看着像「这个 broker 没有积压」。
    `brokerAddr` 是 Java 的 String 字段，走 NON_NULL 序列化所以 None 时整键消失；
    `totalDiff` / `totalInflightDiff` 是 `long` 原语字段，恒在。
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


# ---------------------------------------------------------------- 42 GET_CONSUMER_STATUS_FROM_CLIENT
class GetConsumerStatusBody:
    """对应 org.apache.rocketmq.remoting.protocol.body.GetConsumerStatusBody。

    两个 map 的键都是 ``MessageQueue``（fastjson2 内联对象键）。
    """

    def __init__(self):
        self.message_queue_table: Dict[MessageQueue, int] = {}
        # 已废弃的字段：Java 仍保留（consumerTable: clientId → 位点表）
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
    """对应 org.apache.rocketmq.remoting.protocol.body.ProcessQueueInfo。"""

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
        self.droped: bool = False          # Java 字段名就是 droped（拼写如此）
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
    """对应 org.apache.rocketmq.remoting.protocol.body.CMResult。"""

    CR_SUCCESS = "CR_SUCCESS"
    CR_LATER = "CR_LATER"
    CR_ROLLBACK = "CR_ROLLBACK"
    CR_COMMIT = "CR_COMMIT"
    CR_THROW_EXCEPTION = "CR_THROW_EXCEPTION"
    CR_RETURN_NULL = "CR_RETURN_NULL"


class ConsumeMessageDirectlyResult:
    """对应 org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult。

    这是 309 的应答 body，字段全是标量 —— 是四个 body 里唯一不需要处理
    MessageQueue 内联键的一个。
    """

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