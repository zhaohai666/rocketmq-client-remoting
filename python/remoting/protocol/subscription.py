# -*- coding: utf-8 -*-
"""The models related to subscription groups, corresponding to the protocol's subscription package:
SubscriptionGroupConfig / GroupRetryPolicy / SimpleSubscriptionData.

The field names and default values follow the real serialized output of a 5.x broker (``JSON.toJSONString(new SubscriptionGroupConfig())``):
{"attributes":{},"brokerId":0,"consumeBroadcastEnable":true,"consumeEnable":true,
 "consumeFromMinEnable":true,"consumeMessageOrderly":false,"consumeTimeoutMinute":15,
 "groupName":"MyGroup","groupRetryPolicy":{"type":"CUSTOMIZED"},"groupSysFlag":0,
 "notifyConsumerIdsChangedEnable":true,"retryMaxTimes":16,"retryQueueNums":1,
 "whichBrokerWhenConsumeSlowly":1}
Note that the broker-side fastjson **skips null fields** (when ``subscriptionDataSet`` is null the
whole key does not appear), so from_dict always uses ``.get(k, default)``.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from remoting.protocol.serialize import RemotingSerializable

# MixAll.MASTER_ID
MASTER_ID = 0


class GroupRetryPolicyType:
    EXPONENTIAL = "EXPONENTIAL"
    CUSTOMIZED = "CUSTOMIZED"


class GroupRetryPolicy:
    """A simplified version: it keeps only type and the raw dicts of the two sub-policies (the broker side does not serialize them when null)."""

    def __init__(self, policy_type: str = GroupRetryPolicyType.CUSTOMIZED,
                 exponential_retry_policy: Optional[dict] = None,
                 customized_retry_policy: Optional[dict] = None):
        self.type = policy_type
        self.exponential_retry_policy = exponential_retry_policy
        self.customized_retry_policy = customized_retry_policy

    def to_dict(self) -> dict:
        d = {"type": self.type}
        # The protocol defaults to type=CUSTOMIZED with customizedRetryPolicy null (the field has
        # a default instance, but the broker-side fastjson only serializes non-null values), so the
        # measured output contains only type; that is why it is likewise only emitted when explicitly set.
        if self.exponential_retry_policy is not None:
            d["exponentialRetryPolicy"] = self.exponential_retry_policy
        if self.customized_retry_policy is not None:
            d["customizedRetryPolicy"] = self.customized_retry_policy
        return d

    @staticmethod
    def from_dict(d: Optional[dict]) -> "GroupRetryPolicy":
        d = d or {}
        return GroupRetryPolicy(
            policy_type=d.get("type", GroupRetryPolicyType.CUSTOMIZED),
            exponential_retry_policy=d.get("exponentialRetryPolicy"),
            customized_retry_policy=d.get("customizedRetryPolicy"),
        )


class SimpleSubscriptionData:
    """The protocol model ``SimpleSubscriptionData``."""

    def __init__(self, topic: str = "", expression_type: str = "TAG",
                 expression: str = "*", version: int = 0):
        self.topic = topic
        self.expression_type = expression_type
        self.expression = expression
        self.version = version

    def to_dict(self) -> dict:
        return {
            "topic": self.topic,
            "expressionType": self.expression_type,
            "expression": self.expression,
            "version": self.version,
        }

    @staticmethod
    def from_dict(d: dict) -> "SimpleSubscriptionData":
        return SimpleSubscriptionData(
            topic=d.get("topic", ""),
            expression_type=d.get("expressionType", "TAG"),
            expression=d.get("expression", "*"),
            version=d.get("version", 0),
        )


class SubscriptionGroupConfig:
    """The protocol model ``SubscriptionGroupConfig``."""

    def __init__(self, group_name: str = ""):
        self.group_name = group_name
        self.consume_enable = True
        self.consume_from_min_enable = True
        self.consume_broadcast_enable = True
        self.consume_message_orderly = False
        self.retry_queue_nums = 1
        self.retry_max_times = 16
        self.group_retry_policy = GroupRetryPolicy()
        self.broker_id = MASTER_ID
        self.which_broker_when_consume_slowly = 1
        self.notify_consumer_ids_changed_enable = True
        self.group_sys_flag = 0
        self.consume_timeout_minute = 15
        self.subscription_data_set: Optional[List[SimpleSubscriptionData]] = None
        self.attributes: Dict[str, str] = {}

    def to_dict(self) -> dict:
        d = {
            "groupName": self.group_name,
            "consumeEnable": self.consume_enable,
            "consumeFromMinEnable": self.consume_from_min_enable,
            "consumeBroadcastEnable": self.consume_broadcast_enable,
            "consumeMessageOrderly": self.consume_message_orderly,
            "retryQueueNums": self.retry_queue_nums,
            "retryMaxTimes": self.retry_max_times,
            "groupRetryPolicy": self.group_retry_policy.to_dict(),
            "brokerId": self.broker_id,
            "whichBrokerWhenConsumeSlowly": self.which_broker_when_consume_slowly,
            "notifyConsumerIdsChangedEnable": self.notify_consumer_ids_changed_enable,
            "groupSysFlag": self.group_sys_flag,
            "consumeTimeoutMinute": self.consume_timeout_minute,
            "attributes": self.attributes,
        }
        # fastjson2 skips null by default
        if self.subscription_data_set is not None:
            d["subscriptionDataSet"] = [s.to_dict() for s in self.subscription_data_set]
        return d

    @staticmethod
    def from_dict(d: dict) -> "SubscriptionGroupConfig":
        cfg = SubscriptionGroupConfig(d.get("groupName", ""))
        cfg.consume_enable = d.get("consumeEnable", True)
        cfg.consume_from_min_enable = d.get("consumeFromMinEnable", True)
        cfg.consume_broadcast_enable = d.get("consumeBroadcastEnable", True)
        cfg.consume_message_orderly = d.get("consumeMessageOrderly", False)
        cfg.retry_queue_nums = d.get("retryQueueNums", 1)
        cfg.retry_max_times = d.get("retryMaxTimes", 16)
        cfg.group_retry_policy = GroupRetryPolicy.from_dict(d.get("groupRetryPolicy"))
        cfg.broker_id = d.get("brokerId", MASTER_ID)
        cfg.which_broker_when_consume_slowly = d.get("whichBrokerWhenConsumeSlowly", 1)
        cfg.notify_consumer_ids_changed_enable = d.get("notifyConsumerIdsChangedEnable", True)
        cfg.group_sys_flag = d.get("groupSysFlag", 0)
        cfg.consume_timeout_minute = d.get("consumeTimeoutMinute", 15)
        raw_sub = d.get("subscriptionDataSet")
        cfg.subscription_data_set = (
            [SimpleSubscriptionData.from_dict(x) for x in raw_sub] if raw_sub else None)
        cfg.attributes = dict(d.get("attributes") or {})
        return cfg

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "SubscriptionGroupConfig":
        return SubscriptionGroupConfig.from_dict(RemotingSerializable.decode_json(data))

    def __repr__(self):
        return "SubscriptionGroupConfig[groupName=%s, retryQueueNums=%d, brokerId=%d]" % (
            self.group_name, self.retry_queue_nums, self.broker_id)


class SubscriptionGroupWrapper:
    """The protocol model ``SubscriptionGroupWrapper``.

    Measured broker output:{"dataVersion":{...},"forbiddenTable":{},"subscriptionGroupTable":{...}}
    """

    def __init__(self):
        self.subscription_group_table: Dict[str, SubscriptionGroupConfig] = {}
        self.forbidden_table: Dict[str, Any] = {}
        self.data_version: Dict[str, Any] = {}

    def to_dict(self) -> dict:
        return {
            "dataVersion": self.data_version,
            "forbiddenTable": self.forbidden_table,
            "subscriptionGroupTable": {k: v.to_dict() for k, v in self.subscription_group_table.items()},
        }

    @staticmethod
    def from_dict(d: dict) -> "SubscriptionGroupWrapper":
        w = SubscriptionGroupWrapper()
        table = d.get("subscriptionGroupTable") or {}
        w.subscription_group_table = {
            k: SubscriptionGroupConfig.from_dict(v) for k, v in table.items()}
        w.forbidden_table = dict(d.get("forbiddenTable") or {})
        w.data_version = dict(d.get("dataVersion") or {})
        return w

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "SubscriptionGroupWrapper":
        return SubscriptionGroupWrapper.from_dict(RemotingSerializable.decode_json(data))


__all__ = ["SubscriptionGroupConfig", "SubscriptionGroupWrapper",
           "GroupRetryPolicy", "GroupRetryPolicyType", "SimpleSubscriptionData",
           "MASTER_ID"]
