# -*- coding: utf-8 -*-
"""TopicConfig（对应 org.apache.rocketmq.common.TopicConfig）。

字段与默认值以 Java 5.x 为准（探针实测，勿凭记忆改）：
``new TopicConfig("t")`` → readQueueNums=16, writeQueueNums=16, perm=6,
topicFilterType=SINGLE_TAG, topicSysFlag=0, order=false, attributes={}。
``attributes`` **会被序列化**（Java 的 ``getAttributes()`` 没有 serialize=false）。
"""
from __future__ import annotations

import json
from typing import Dict

# org.apache.rocketmq.common.TopicConfig.defaultReadQueueNums / defaultWriteQueueNums
DEFAULT_READ_QUEUE_NUMS = 16
DEFAULT_WRITE_QUEUE_NUMS = 16
# PermName.PERM_READ | PermName.PERM_WRITE
DEFAULT_PERM = 6


class TopicFilterType:
    SINGLE_TAG = "SINGLE_TAG"
    MULTI_TAG = "MULTI_TAG"


class TopicConfig:
    def __init__(self, topic_name: str = "",
                 read_queue_nums: int = DEFAULT_READ_QUEUE_NUMS,
                 write_queue_nums: int = DEFAULT_WRITE_QUEUE_NUMS,
                 perm: int = DEFAULT_PERM,
                 topic_filter_type: str = TopicFilterType.SINGLE_TAG,
                 topic_sys_flag: int = 0,
                 order: bool = False,
                 attributes: Dict[str, str] = None):
        self.topic_name = topic_name
        self.read_queue_nums = read_queue_nums
        self.write_queue_nums = write_queue_nums
        self.perm = perm
        self.topic_filter_type = topic_filter_type
        self.topic_sys_flag = topic_sys_flag
        self.order = order
        self.attributes: Dict[str, str] = dict(attributes or {})

    def encode(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    def to_dict(self) -> dict:
        return {
            "topicName": self.topic_name,
            "readQueueNums": self.read_queue_nums,
            "writeQueueNums": self.write_queue_nums,
            "perm": self.perm,
            "topicFilterType": self.topic_filter_type,
            "topicSysFlag": self.topic_sys_flag,
            "order": self.order,
            "attributes": self.attributes,
        }

    @staticmethod
    def from_dict(data: dict) -> "TopicConfig":
        return TopicConfig(
            topic_name=data.get("topicName", ""),
            read_queue_nums=data.get("readQueueNums", DEFAULT_READ_QUEUE_NUMS),
            write_queue_nums=data.get("writeQueueNums", DEFAULT_WRITE_QUEUE_NUMS),
            perm=data.get("perm", DEFAULT_PERM),
            topic_filter_type=data.get("topicFilterType", TopicFilterType.SINGLE_TAG),
            topic_sys_flag=data.get("topicSysFlag", 0),
            order=data.get("order", False),
            attributes=dict(data.get("attributes") or {}),
        )

    @staticmethod
    def decode(json_str: str) -> "TopicConfig":
        return TopicConfig.from_dict(json.loads(json_str))

    def __repr__(self):
        return "TopicConfig[topicName=%s, readQueueNums=%d, writeQueueNums=%d, perm=%s]" % (
            self.topic_name, self.read_queue_nums, self.write_queue_nums,
            PermName.perm_to_string(self.perm))


from .sysflag import PermName  # noqa: E402

__all__ = ["TopicConfig", "TopicFilterType",
           "DEFAULT_READ_QUEUE_NUMS", "DEFAULT_WRITE_QUEUE_NUMS", "DEFAULT_PERM"]
