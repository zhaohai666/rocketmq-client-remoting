# -*- coding: utf-8 -*-
"""Response bodies used only by the administrative APIs (the admin.* classes in the
protocol and the management classes in body.*):

TopicStatsTable / TopicOffset / ConsumeStats / OffsetWrapper /
TopicConfigSerializeWrapper / ConsumeQueueData / QueryConsumeQueueResponseBody /
UserInfo / AclInfo (with AclPolicy and AclEntry).

CAUTION: a key trap -- the Map keys of these classes are **MessageQueue**, and when
the broker side serializes them with fastjson it inlines the keys directly as JSON
objects, producing **invalid JSON**, for example:
    {"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}:{...}}}
So they must be parsed with serialize.fastjson_loads, and the string keys converted
back into MessageQueue afterwards. The field names and structure were confirmed
against real responses from a 5.5.1 broker.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from common.message import MessageQueue
from common.topic_config import TopicConfig
from . import serialize
from .serialize import RemotingSerializable, decode_message_queue_key


def parse_message_queue_key(key: str) -> Optional[MessageQueue]:
    """Turn a MessageQueue key inlined by fastjson2 back into a MessageQueue.

    Returning None means the key is not an inline object (for example an ordinary
    string key such as ``"0"`` or ``"G1"``).
    """
    d = decode_message_queue_key(key)
    if d is None:
        return None
    return MessageQueue(d.get("topic", ""), d.get("brokerName", ""), d.get("queueId", 0))


def message_queue_key(mq: MessageQueue) -> str:
    """Serialize a MessageQueue into a fastjson2-style inline object key (keys in
    alphabetical order)."""
    return '{"brokerName":"%s","queueId":%d,"topic":"%s"}' % (
        mq.broker_name, mq.queue_id, mq.topic)


def decode_message_queue_map(raw: Optional[dict]) -> Dict[MessageQueue, Any]:
    """Generic helper: parse a map keyed by MessageQueue (fastjson2 non-string keys)."""
    result: Dict[MessageQueue, Any] = {}
    for k, v in (raw or {}).items():
        mq = parse_message_queue_key(k)
        if mq is not None:
            result[mq] = v
    return result


# ---------------------------------------------------------------- TopicStatsTable
class TopicOffset:
    pass

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
    pass

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
    pass

    def __init__(self, broker_offset: int = 0, consumer_offset: int = 0,
                 last_timestamp: int = 0, pull_offset: int = 0):
        self.broker_offset = broker_offset
        self.consumer_offset = consumer_offset
        self.last_timestamp = last_timestamp
        self.pull_offset = pull_offset

    @property
    def lag(self) -> int:
        """Definition of lag: brokerOffset - consumerOffset."""
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
    pass

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
    pass

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
    pass

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
        # fastjson NON_NULL semantics: null fields are not serialized
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
    pass

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


# ---------------------------------------------------------------- Auth user / ACL management
class UserType:
    """User type as the auth module spells it on the wire."""
    SUPER = "Super"
    NORMAL = "Normal"


class UserStatus:
    """User status as the auth module spells it on the wire (lowercase)."""
    ENABLE = "enable"
    DISABLE = "disable"


class PolicyType:
    """Policy type as the auth module spells it on the wire."""
    CUSTOM = "Custom"
    DEFAULT = "Default"


class Decision:
    """Entry decision as the auth module spells it on the wire."""
    ALLOW = "Allow"
    DENY = "Deny"


class UserInfo:
    """An auth user record: the body of createUser / updateUser, and the reply of
    getUser / listUser."""

    def __init__(self, username: Optional[str] = None, password: Optional[str] = None,
                 user_type: Optional[str] = None, user_status: Optional[str] = None):
        self.username = username
        self.password = password
        self.user_type = user_type
        self.user_status = user_status

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {}
        # Wire contract: an unset field is absent from the JSON, never emitted as null --
        # a null value and a missing key are not the same request.
        if self.username is not None:
            d["username"] = self.username
        if self.password is not None:
            d["password"] = self.password
        if self.user_type is not None:
            d["userType"] = self.user_type
        if self.user_status is not None:
            d["userStatus"] = self.user_status
        return d

    @staticmethod
    def from_dict(d: dict) -> "UserInfo":
        d = d or {}
        return UserInfo(
            username=d.get("username"),
            password=d.get("password"),
            user_type=d.get("userType"),
            user_status=d.get("userStatus"),
        )

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: Optional[bytes]) -> Optional["UserInfo"]:
        if not data:
            return None
        obj = serialize.fastjson_loads(data.decode("utf-8"))
        return UserInfo.from_dict(obj if isinstance(obj, dict) else {})

    @staticmethod
    def decode_list(data: Optional[bytes]) -> List["UserInfo"]:
        if not data:
            return []
        obj = serialize.fastjson_loads(data.decode("utf-8"))
        if not isinstance(obj, list):
            return []
        return [UserInfo.from_dict(x) for x in obj if isinstance(x, dict)]

    def __repr__(self):
        return "UserInfo[username=%s, userType=%s, userStatus=%s]" % (
            self.username, self.user_type, self.user_status)


class AclEntry:
    """One resource of a policy, with the actions / source ips allowed or denied on it."""

    def __init__(self, resource: Optional[str] = None, actions: Optional[List[str]] = None,
                 source_ips: Optional[List[str]] = None, decision: Optional[str] = None):
        self.resource = resource
        self.actions = actions
        self.source_ips = source_ips
        self.decision = decision

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {}
        if self.resource is not None:
            d["resource"] = self.resource
        if self.actions is not None:
            d["actions"] = list(self.actions)
        if self.source_ips is not None:
            d["sourceIps"] = list(self.source_ips)
        if self.decision is not None:
            d["decision"] = self.decision
        return d

    @staticmethod
    def from_dict(d: dict) -> "AclEntry":
        d = d or {}
        return AclEntry(
            resource=d.get("resource"),
            actions=list(d["actions"]) if d.get("actions") is not None else None,
            source_ips=list(d["sourceIps"]) if d.get("sourceIps") is not None else None,
            decision=d.get("decision"),
        )

    def __repr__(self):
        return "AclEntry[resource=%s, actions=%s, sourceIps=%s, decision=%s]" % (
            self.resource, self.actions, self.source_ips, self.decision)


class AclPolicy:
    """A named group of ACL entries; ``policy_type`` stays unset for a caller-built policy."""

    def __init__(self, policy_type: Optional[str] = None,
                 entries: Optional[List[AclEntry]] = None):
        self.policy_type = policy_type
        self.entries = entries if entries is not None else []

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {}
        if self.policy_type is not None:
            d["policyType"] = self.policy_type
        d["entries"] = [e.to_dict() for e in self.entries]
        return d

    @staticmethod
    def from_dict(d: dict) -> "AclPolicy":
        d = d or {}
        raw = d.get("entries")
        return AclPolicy(
            policy_type=d.get("policyType"),
            entries=[AclEntry.from_dict(x) for x in raw if isinstance(x, dict)]
            if isinstance(raw, list) else [],
        )

    def __repr__(self):
        return "AclPolicy[policyType=%s, entries=%r]" % (self.policy_type, self.entries)


class AclInfo:
    """The ACL record of one subject: the body of createAcl / updateAcl, and the reply of
    getAcl / listAcl."""

    def __init__(self, subject: Optional[str] = None,
                 policies: Optional[List[AclPolicy]] = None):
        self.subject = subject
        self.policies = policies if policies is not None else []

    @staticmethod
    def for_resources(subject: Optional[str], resources: Union[str, List[str], None],
                      actions: Optional[List[str]] = None,
                      source_ips: Optional[List[str]] = None,
                      decision: Optional[str] = None) -> "AclInfo":
        """Build the single-policy shape the convenience form describes: no policy type,
        one entry per resource, all repeating the same actions / source ips / decision."""
        if resources is None:
            names: List[str] = []
        elif isinstance(resources, str):
            names = [resources]
        else:
            names = list(resources)
        entries = [AclEntry(resource=r, actions=None if actions is None else list(actions),
                            source_ips=None if source_ips is None else list(source_ips),
                            decision=decision) for r in names]
        return AclInfo(subject=subject, policies=[AclPolicy(entries=entries)])

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {}
        if self.subject is not None:
            d["subject"] = self.subject
        d["policies"] = [p.to_dict() for p in self.policies]
        return d

    @staticmethod
    def from_dict(d: dict) -> "AclInfo":
        d = d or {}
        raw = d.get("policies")
        return AclInfo(
            subject=d.get("subject"),
            policies=[AclPolicy.from_dict(x) for x in raw if isinstance(x, dict)]
            if isinstance(raw, list) else [],
        )

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: Optional[bytes]) -> Optional["AclInfo"]:
        if not data:
            return None
        obj = serialize.fastjson_loads(data.decode("utf-8"))
        return AclInfo.from_dict(obj if isinstance(obj, dict) else {})

    @staticmethod
    def decode_list(data: Optional[bytes]) -> List["AclInfo"]:
        if not data:
            return []
        obj = serialize.fastjson_loads(data.decode("utf-8"))
        if not isinstance(obj, list):
            return []
        return [AclInfo.from_dict(x) for x in obj if isinstance(x, dict)]

    def __repr__(self):
        return "AclInfo[subject=%s, policies=%r]" % (self.subject, self.policies)


__all__ = ["TopicOffset", "TopicStatsTable", "OffsetWrapper", "ConsumeStats",
           "TopicConfigSerializeWrapper", "ConsumeQueueData",
           "QueryConsumeQueueResponseBody", "UserType", "UserStatus", "PolicyType",
           "Decision", "UserInfo", "AclEntry", "AclPolicy", "AclInfo",
           "ReplicaIdentity", "ReplicasInfo", "BrokerReplicasInfo",
           "CheckRocksdbCqWriteStatus", "CheckRocksdbCqWriteResult",
           "ClientGroup", "LiteLagInfo", "GetBrokerLiteInfoResponseBody",
           "GetParentTopicInfoResponseBody", "GetLiteTopicInfoResponseBody",
           "GetLiteClientInfoResponseBody", "GetLiteGroupInfoResponseBody",
           "BrokerOperatorResult", "AdminToolsResultCodeEnum", "AdminToolResult"]


# ---------------------------------------------------------------- Controller sync state
class ReplicaIdentity:
    """One replica of a controller-managed broker (BrokerReplicasInfo.ReplicaIdentity)."""

    def __init__(self, broker_name: Optional[str] = None, broker_id: Optional[int] = None,
                 broker_address: Optional[str] = None, alive: Optional[bool] = None):
        self.broker_name = broker_name
        self.broker_id = broker_id
        self.broker_address = broker_address
        self.alive = alive

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {}
        if self.broker_name is not None:
            d["brokerName"] = self.broker_name
        if self.broker_id is not None:
            d["brokerId"] = self.broker_id
        if self.broker_address is not None:
            d["brokerAddress"] = self.broker_address
        if self.alive is not None:
            d["alive"] = self.alive
        return d

    @staticmethod
    def from_dict(d: dict) -> "ReplicaIdentity":
        d = d or {}
        return ReplicaIdentity(
            broker_name=d.get("brokerName"),
            broker_id=d.get("brokerId"),
            broker_address=d.get("brokerAddress"),
            alive=d.get("alive"),
        )

    def __repr__(self):
        return "ReplicaIdentity[brokerName=%s, brokerId=%s, brokerAddress=%s, alive=%s]" % (
            self.broker_name, self.broker_id, self.broker_address, self.alive)


class ReplicasInfo:
    """The controller view of one broker: master and the in-sync / not-in-sync sets."""

    def __init__(self, master_broker_id: Optional[int] = None,
                 master_address: Optional[str] = None,
                 master_epoch: Optional[int] = None,
                 sync_state_set_epoch: Optional[int] = None,
                 in_sync_replicas: Optional[List[ReplicaIdentity]] = None,
                 not_in_sync_replicas: Optional[List[ReplicaIdentity]] = None):
        self.master_broker_id = master_broker_id
        self.master_address = master_address
        self.master_epoch = master_epoch
        self.sync_state_set_epoch = sync_state_set_epoch
        self.in_sync_replicas = in_sync_replicas
        self.not_in_sync_replicas = not_in_sync_replicas

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {}
        if self.master_broker_id is not None:
            d["masterBrokerId"] = self.master_broker_id
        if self.master_address is not None:
            d["masterAddress"] = self.master_address
        if self.master_epoch is not None:
            d["masterEpoch"] = self.master_epoch
        if self.sync_state_set_epoch is not None:
            d["syncStateSetEpoch"] = self.sync_state_set_epoch
        if self.in_sync_replicas is not None:
            d["inSyncReplicas"] = [r.to_dict() for r in self.in_sync_replicas]
        if self.not_in_sync_replicas is not None:
            d["notInSyncReplicas"] = [r.to_dict() for r in self.not_in_sync_replicas]
        return d

    @staticmethod
    def from_dict(d: dict) -> "ReplicasInfo":
        d = d or {}
        in_sync = d.get("inSyncReplicas")
        not_in_sync = d.get("notInSyncReplicas")
        return ReplicasInfo(
            master_broker_id=d.get("masterBrokerId"),
            master_address=d.get("masterAddress"),
            master_epoch=d.get("masterEpoch"),
            sync_state_set_epoch=d.get("syncStateSetEpoch"),
            in_sync_replicas=[ReplicaIdentity.from_dict(x) for x in in_sync]
            if isinstance(in_sync, list) else None,
            not_in_sync_replicas=[ReplicaIdentity.from_dict(x) for x in not_in_sync]
            if isinstance(not_in_sync, list) else None,
        )

    def __repr__(self):
        return "ReplicasInfo[master=%s:%s, inSync=%d, notInSync=%d]" % (
            self.master_address, self.master_broker_id,
            len(self.in_sync_replicas or []), len(self.not_in_sync_replicas or []))


class BrokerReplicasInfo:
    """The reply body of CONTROLLER_GET_SYNC_STATE_DATA (1006): per-broker replica sets."""

    def __init__(self):
        self.replicas_info_table: Dict[str, ReplicasInfo] = {}

    def add_replica_info(self, broker_name: str, replicas_info: ReplicasInfo) -> None:
        self.replicas_info_table[broker_name] = replicas_info

    def to_dict(self) -> dict:
        return {"replicasInfoTable": {k: v.to_dict()
                                      for k, v in self.replicas_info_table.items()}}

    @staticmethod
    def from_dict(d: dict) -> "BrokerReplicasInfo":
        b = BrokerReplicasInfo()
        for name, info in (d.get("replicasInfoTable") or {}).items():
            b.replicas_info_table[name] = ReplicasInfo.from_dict(info)
        return b

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: Optional[bytes]) -> "BrokerReplicasInfo":
        if not data:
            return BrokerReplicasInfo()
        return BrokerReplicasInfo.from_dict(serialize.fastjson_loads(data.decode("utf-8")))


# ---------------------------------------------------------------- RocksDB cold queue
class CheckRocksdbCqWriteStatus:
    """The CheckRocksdbCqWriteResult.CheckStatus enum values."""
    CHECK_OK = 0
    CHECK_NOT_OK = 1
    CHECK_IN_PROGRESS = 2
    CHECK_ERROR = 3


class CheckRocksdbCqWriteResult:
    """The reply body of CHECK_ROCKSDB_CQ_WRITE_PROGRESS (354)."""

    def __init__(self, check_result: Optional[str] = None, check_status: int = 0):
        self.check_result = check_result
        self.check_status = check_status

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {"checkStatus": self.check_status}
        if self.check_result is not None:
            d["checkResult"] = self.check_result
        return d

    @staticmethod
    def from_dict(d: dict) -> "CheckRocksdbCqWriteResult":
        d = d or {}
        return CheckRocksdbCqWriteResult(
            check_result=d.get("checkResult"),
            check_status=d.get("checkStatus", 0),
        )

    @staticmethod
    def decode(data: Optional[bytes]) -> "CheckRocksdbCqWriteResult":
        if not data:
            return CheckRocksdbCqWriteResult()
        return CheckRocksdbCqWriteResult.from_dict(
            serialize.fastjson_loads(data.decode("utf-8")))

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    def __repr__(self):
        return "CheckRocksdbCqWriteResult[checkResult=%s, checkStatus=%d]" % (
            self.check_result, self.check_status)


# ---------------------------------------------------------------- Lite topic admin
class ClientGroup:
    """A (clientId, group) pair of the lite topic subscriber set."""

    def __init__(self, client_id: Optional[str] = None, group: Optional[str] = None):
        self.client_id = client_id
        self.group = group

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {}
        if self.client_id is not None:
            d["clientId"] = self.client_id
        if self.group is not None:
            d["group"] = self.group
        return d

    @staticmethod
    def from_dict(d: dict) -> "ClientGroup":
        d = d or {}
        return ClientGroup(client_id=d.get("clientId"), group=d.get("group"))

    def __eq__(self, other):
        return (isinstance(other, ClientGroup)
                and self.client_id == other.client_id and self.group == other.group)

    def __hash__(self):
        return hash((self.client_id, self.group))

    def __repr__(self):
        return "ClientGroup{clientId='%s', group='%s'}" % (self.client_id, self.group)


class LiteLagInfo:
    """One lite topic lag entry of GetLiteGroupInfoResponseBody."""

    def __init__(self, lite_topic: Optional[str] = None, lag_count: int = 0,
                 earliest_unconsumed_timestamp: int = -1):
        self.lite_topic = lite_topic
        self.lag_count = lag_count
        self.earliest_unconsumed_timestamp = earliest_unconsumed_timestamp

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {
            "lagCount": self.lag_count,
            "earliestUnconsumedTimestamp": self.earliest_unconsumed_timestamp,
        }
        if self.lite_topic is not None:
            d["liteTopic"] = self.lite_topic
        return d

    @staticmethod
    def from_dict(d: dict) -> "LiteLagInfo":
        d = d or {}
        return LiteLagInfo(
            lite_topic=d.get("liteTopic"),
            lag_count=d.get("lagCount", 0),
            earliest_unconsumed_timestamp=d.get("earliestUnconsumedTimestamp", -1),
        )

    def __repr__(self):
        return "LiteLagInfo[liteTopic=%s, lagCount=%d]" % (self.lite_topic, self.lag_count)

    def __eq__(self, other):
        return (isinstance(other, LiteLagInfo)
                and self.lite_topic == other.lite_topic
                and self.lag_count == other.lag_count
                and self.earliest_unconsumed_timestamp == other.earliest_unconsumed_timestamp)

    def __hash__(self):
        return hash((self.lite_topic, self.lag_count, self.earliest_unconsumed_timestamp))


class GetBrokerLiteInfoResponseBody:
    """The reply body of GET_BROKER_LITE_INFO (200074)."""

    def __init__(self):
        self.store_type: Optional[str] = None
        self.max_lmq_num: int = 0
        self.current_lmq_num: int = 0
        self.lite_subscription_count: int = 0
        self.order_info_count: int = 0
        self.cq_table_size: int = 0
        self.offset_table_size: int = 0
        self.event_map_size: int = 0
        self.topic_meta: Dict[str, int] = {}
        self.group_meta: Dict[str, set] = {}

    def to_dict(self) -> dict:
        return {
            "storeType": self.store_type,
            "maxLmqNum": self.max_lmq_num,
            "currentLmqNum": self.current_lmq_num,
            "liteSubscriptionCount": self.lite_subscription_count,
            "orderInfoCount": self.order_info_count,
            "cqTableSize": self.cq_table_size,
            "offsetTableSize": self.offset_table_size,
            "eventMapSize": self.event_map_size,
            "topicMeta": self.topic_meta,
            "groupMeta": {k: sorted(v) for k, v in self.group_meta.items()},
        }

    @staticmethod
    def from_dict(d: dict) -> "GetBrokerLiteInfoResponseBody":
        d = d or {}
        b = GetBrokerLiteInfoResponseBody()
        b.store_type = d.get("storeType")
        b.max_lmq_num = d.get("maxLmqNum", 0)
        b.current_lmq_num = d.get("currentLmqNum", 0)
        b.lite_subscription_count = d.get("liteSubscriptionCount", 0)
        b.order_info_count = d.get("orderInfoCount", 0)
        b.cq_table_size = d.get("cqTableSize", 0)
        b.offset_table_size = d.get("offsetTableSize", 0)
        b.event_map_size = d.get("eventMapSize", 0)
        b.topic_meta = dict(d.get("topicMeta") or {})
        b.group_meta = {k: set(v) for k, v in (d.get("groupMeta") or {}).items()}
        return b

    @staticmethod
    def decode(data: Optional[bytes]) -> Optional["GetBrokerLiteInfoResponseBody"]:
        if not data:
            return None
        return GetBrokerLiteInfoResponseBody.from_dict(
            serialize.fastjson_loads(data.decode("utf-8")))

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())


class GetParentTopicInfoResponseBody:
    """The reply body of GET_PARENT_TOPIC_INFO (200075)."""

    def __init__(self):
        self.topic: Optional[str] = None
        self.ttl: int = 0
        self.groups: set = set()
        self.lmq_num: int = 0
        self.lite_topic_count: int = 0

    def to_dict(self) -> dict:
        return {
            "topic": self.topic,
            "ttl": self.ttl,
            "groups": sorted(self.groups),
            "lmqNum": self.lmq_num,
            "liteTopicCount": self.lite_topic_count,
        }

    @staticmethod
    def from_dict(d: dict) -> "GetParentTopicInfoResponseBody":
        d = d or {}
        b = GetParentTopicInfoResponseBody()
        b.topic = d.get("topic")
        b.ttl = d.get("ttl", 0)
        b.groups = set(d.get("groups") or [])
        b.lmq_num = d.get("lmqNum", 0)
        b.lite_topic_count = d.get("liteTopicCount", 0)
        return b

    @staticmethod
    def decode(data: Optional[bytes]) -> Optional["GetParentTopicInfoResponseBody"]:
        if not data:
            return None
        return GetParentTopicInfoResponseBody.from_dict(
            serialize.fastjson_loads(data.decode("utf-8")))

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())


class GetLiteTopicInfoResponseBody:
    """The reply body of GET_LITE_TOPIC_INFO (200076)."""

    def __init__(self):
        self.parent_topic: Optional[str] = None
        self.lite_topic: Optional[str] = None
        self.subscriber: set = set()
        self.topic_offset: Optional[TopicOffset] = None
        self.sharding_to_broker: bool = False

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {
            "shardingToBroker": self.sharding_to_broker,
        }
        if self.parent_topic is not None:
            d["parentTopic"] = self.parent_topic
        if self.lite_topic is not None:
            d["liteTopic"] = self.lite_topic
        d["subscriber"] = [c.to_dict() for c in self.subscriber]
        if self.topic_offset is not None:
            d["topicOffset"] = self.topic_offset.to_dict()
        return d

    @staticmethod
    def from_dict(d: dict) -> "GetLiteTopicInfoResponseBody":
        d = d or {}
        b = GetLiteTopicInfoResponseBody()
        b.parent_topic = d.get("parentTopic")
        b.lite_topic = d.get("liteTopic")
        raw_subs = d.get("subscriber")
        b.subscriber = {ClientGroup.from_dict(x) for x in raw_subs} \
            if isinstance(raw_subs, list) else set()
        offset = d.get("topicOffset")
        b.topic_offset = TopicOffset.from_dict(offset) if isinstance(offset, dict) else None
        b.sharding_to_broker = d.get("shardingToBroker", False)
        return b

    @staticmethod
    def decode(data: Optional[bytes]) -> Optional["GetLiteTopicInfoResponseBody"]:
        if not data:
            return None
        return GetLiteTopicInfoResponseBody.from_dict(
            serialize.fastjson_loads(data.decode("utf-8")))

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())


class GetLiteClientInfoResponseBody:
    """The reply body of GET_LITE_CLIENT_INFO (200077)."""

    def __init__(self):
        self.parent_topic: Optional[str] = None
        self.group: Optional[str] = None
        self.client_id: Optional[str] = None
        self.last_access_time: int = 0
        self.last_consume_time: int = 0
        self.lite_topic_count: int = 0
        self.lite_topic_set: set = set()

    def to_dict(self) -> dict:
        return {
            "parentTopic": self.parent_topic,
            "group": self.group,
            "clientId": self.client_id,
            "lastAccessTime": self.last_access_time,
            "lastConsumeTime": self.last_consume_time,
            "liteTopicCount": self.lite_topic_count,
            "liteTopicSet": sorted(self.lite_topic_set),
        }

    @staticmethod
    def from_dict(d: dict) -> "GetLiteClientInfoResponseBody":
        d = d or {}
        b = GetLiteClientInfoResponseBody()
        b.parent_topic = d.get("parentTopic")
        b.group = d.get("group")
        b.client_id = d.get("clientId")
        b.last_access_time = d.get("lastAccessTime", 0)
        b.last_consume_time = d.get("lastConsumeTime", 0)
        b.lite_topic_count = d.get("liteTopicCount", 0)
        b.lite_topic_set = set(d.get("liteTopicSet") or [])
        return b

    @staticmethod
    def decode(data: Optional[bytes]) -> Optional["GetLiteClientInfoResponseBody"]:
        if not data:
            return None
        return GetLiteClientInfoResponseBody.from_dict(
            serialize.fastjson_loads(data.decode("utf-8")))

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())


class GetLiteGroupInfoResponseBody:
    """The reply body of GET_LITE_GROUP_INFO (200078)."""

    def __init__(self):
        self.group: Optional[str] = None
        self.parent_topic: Optional[str] = None
        self.lite_topic: Optional[str] = None
        self.earliest_unconsumed_timestamp: int = -1
        self.total_lag_count: int = 0
        self.lite_topic_offset_wrapper: Optional[OffsetWrapper] = None
        self.lag_count_top_k: List[LiteLagInfo] = []
        self.lag_timestamp_top_k: List[LiteLagInfo] = []

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {
            "earliestUnconsumedTimestamp": self.earliest_unconsumed_timestamp,
            "totalLagCount": self.total_lag_count,
            "lagCountTopK": [x.to_dict() for x in self.lag_count_top_k],
            "lagTimestampTopK": [x.to_dict() for x in self.lag_timestamp_top_k],
        }
        if self.group is not None:
            d["group"] = self.group
        if self.parent_topic is not None:
            d["parentTopic"] = self.parent_topic
        if self.lite_topic is not None:
            d["liteTopic"] = self.lite_topic
        if self.lite_topic_offset_wrapper is not None:
            d["liteTopicOffsetWrapper"] = self.lite_topic_offset_wrapper.to_dict()
        return d

    @staticmethod
    def from_dict(d: dict) -> "GetLiteGroupInfoResponseBody":
        d = d or {}
        b = GetLiteGroupInfoResponseBody()
        b.group = d.get("group")
        b.parent_topic = d.get("parentTopic")
        b.lite_topic = d.get("liteTopic")
        b.earliest_unconsumed_timestamp = d.get("earliestUnconsumedTimestamp", -1)
        b.total_lag_count = d.get("totalLagCount", 0)
        wrapper = d.get("liteTopicOffsetWrapper")
        b.lite_topic_offset_wrapper = (
            OffsetWrapper.from_dict(wrapper) if isinstance(wrapper, dict) else None)
        for attr, key in (("lag_count_top_k", "lagCountTopK"),
                          ("lag_timestamp_top_k", "lagTimestampTopK")):
            raw = d.get(key)
            setattr(b, attr, [LiteLagInfo.from_dict(x) for x in raw]
                    if isinstance(raw, list) else [])
        return b

    @staticmethod
    def decode(data: Optional[bytes]) -> Optional["GetLiteGroupInfoResponseBody"]:
        if not data:
            return None
        return GetLiteGroupInfoResponseBody.from_dict(
            serialize.fastjson_loads(data.decode("utf-8")))

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())


# ---------------------------------------------------------------- Fan-out tool results
class BrokerOperatorResult:
    """The per-broker outcome lists carried by the fan-out tools that mutate brokers."""

    def __init__(self, success_list: Optional[List[str]] = None,
                 failure_list: Optional[List[str]] = None):
        self.success_list: List[str] = success_list if success_list is not None else []
        self.failure_list: List[str] = failure_list if failure_list is not None else []

    def to_dict(self) -> dict:
        return {"successList": list(self.success_list),
                "failureList": list(self.failure_list)}

    @staticmethod
    def from_dict(d: dict) -> "BrokerOperatorResult":
        d = d or {}
        return BrokerOperatorResult(
            success_list=list(d.get("successList") or []),
            failure_list=list(d.get("failureList") or []),
        )

    def __repr__(self):
        return "BrokerOperatorResult{successList=%s, failureList=%s}" % (
            self.success_list, self.failure_list)


class AdminToolsResultCodeEnum:
    """Result codes of the concurrent admin tools."""
    SUCCESS = 200
    REMOTING_ERROR = -1001
    MQ_BROKER_ERROR = -1002
    MQ_CLIENT_ERROR = -1003
    INTERRUPT_ERROR = -1004
    TOPIC_ROUTE_INFO_NOT_EXIST = -2001
    CONSUMER_NOT_ONLINE = -2002
    BROADCAST_CONSUMPTION = -2003


class AdminToolResult:
    """The envelope of the concurrent admin tools: a success flag with a result code,
    an error message and the payload (None on failures that carry no data)."""

    def __init__(self, success: bool, code: int, error_msg: Optional[str] = None,
                 data=None):
        self.success = success
        self.code = code
        self.error_msg = error_msg
        self.data = data

    @staticmethod
    def success(data=None) -> "AdminToolResult":
        return AdminToolResult(True, AdminToolsResultCodeEnum.SUCCESS, "success", data)

    @staticmethod
    def failure(code_enum: int, error_msg: Optional[str], data=None) -> "AdminToolResult":
        return AdminToolResult(False, code_enum, error_msg, data)

    def to_dict(self) -> dict:
        d: Dict[str, Any] = {"success": self.success, "code": self.code}
        if self.error_msg is not None:
            d["errorMsg"] = self.error_msg
        if self.data is not None:
            d["data"] = self.data.to_dict() if hasattr(self.data, "to_dict") else self.data
        return d

    def __repr__(self):
        return "AdminToolResult[success=%s, code=%d, errorMsg=%s, data=%s]" % (
            self.success, self.code, self.error_msg, self.data)
