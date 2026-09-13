# -*- coding: utf-8 -*-
"""心跳数据（对应 org.apache.rocketmq.remoting.protocol.heartbeat.*）。"""
from __future__ import annotations

import time
from typing import List, Optional, Set

from ...common.subscription_data import SubscriptionData


class ConsumeType:
    CONSUME_ACTIVELY = "CONSUME_ACTIVELY"
    CONSUME_PASSIVELY = "CONSUME_PASSIVELY"


class MessageModel:
    CLUSTERING = "CLUSTERING"
    BROADCASTING = "BROADCASTING"


class ConsumeFromWhere:
    CONSUME_FROM_LAST_OFFSET = "CONSUME_FROM_LAST_OFFSET"
    CONSUME_FROM_FIRST_OFFSET = "CONSUME_FROM_FIRST_OFFSET"
    CONSUME_FROM_TIMESTAMP = "CONSUME_FROM_TIMESTAMP"


class ProducerData:
    def __init__(self, group_name: str = ""):
        self.group_name = group_name

    def to_dict(self) -> dict:
        return {"groupName": self.group_name}

    @staticmethod
    def from_dict(d: dict) -> "ProducerData":
        return ProducerData(d.get("groupName", ""))

    def __eq__(self, other):
        return isinstance(other, ProducerData) and self.group_name == other.group_name

    def __hash__(self):
        return hash(self.group_name)

    def __repr__(self):
        return "ProducerData [groupName=%s]" % self.group_name


class ConsumerData:
    def __init__(self, group_name: str = "", consume_type: str = ConsumeType.CONSUME_PASSIVELY,
                 message_model: str = MessageModel.CLUSTERING, consume_from_where: str = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET):
        self.group_name = group_name
        self.consume_type = consume_type
        self.message_model = message_model
        self.consume_from_where = consume_from_where
        self.subscription_data_set: Set[SubscriptionData] = set()
        self.consume_timestamp = int(time.time() * 1000)
        self.unit_mode = False
        self.max_reconsume_times = 0

    def to_dict(self) -> dict:
        return {
            "groupName": self.group_name,
            "consumeType": self.consume_type,
            "messageModel": self.message_model,
            "consumeFromWhere": self.consume_from_where,
            "subscriptionDataSet": [s.__dict__ for s in self.subscription_data_set],
            "consumeTimestamp": self.consume_timestamp,
            "unitMode": self.unit_mode,
            "maxReconsumeTimes": self.max_reconsume_times,
        }

    @staticmethod
    def from_dict(d: dict) -> "ConsumerData":
        cd = ConsumerData(d.get("groupName", ""), d.get("consumeType", ConsumeType.CONSUME_PASSIVELY),
                          d.get("messageModel", MessageModel.CLUSTERING),
                          d.get("consumeFromWhere", ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET))
        cd.consume_timestamp = int(d.get("consumeTimestamp", 0))
        cd.unit_mode = d.get("unitMode", False) or False
        cd.max_reconsume_times = int(d.get("maxReconsumeTimes", 0))
        for sd in d.get("subscriptionDataSet") or []:
            sub = SubscriptionData(sd.get("topic"), sd.get("subString"))
            sub.sub_version = int(sd.get("subVersion", 0))
            sub.expression_type = sd.get("expressionType", "TAG")
            sub.tags_set = set(sd.get("tagsSet") or [])
            sub.code_set = set(int(v) for v in (sd.get("codeSet") or []))
            cd.subscription_data_set.add(sub)
        return cd

    def __repr__(self):
        return "ConsumerData [groupName=%s, consumeType=%s, messageModel=%s, consumeFromWhere=%s]" % (
            self.group_name, self.consume_type, self.message_model, self.consume_from_where)


class HeartbeatData:
    def __init__(self, client_id: str = ""):
        self.client_id = client_id
        self.producer_data_set: Set[ProducerData] = set()
        self.consumer_data_set: Set[ConsumerData] = set()

    def to_dict(self) -> dict:
        return {
            "clientID": self.client_id,
            "producerDataSet": [p.to_dict() for p in self.producer_data_set],
            "consumerDataSet": [c.to_dict() for c in self.consumer_data_set],
        }

    @staticmethod
    def from_dict(d: dict) -> "HeartbeatData":
        hb = HeartbeatData(d.get("clientID", ""))
        for p in d.get("producerDataSet") or []:
            hb.producer_data_set.add(ProducerData.from_dict(p))
        for c in d.get("consumerDataSet") or []:
            hb.consumer_data_set.add(ConsumerData.from_dict(c))
        return hb

    def encode(self) -> bytes:
        from .serialize import RemotingSerializable
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "HeartbeatData":
        from .serialize import RemotingSerializable
        return HeartbeatData.from_dict(RemotingSerializable.decode_json(data))