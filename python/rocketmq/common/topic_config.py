# -*- coding: utf-8 -*-
"""TopicConfig（对应 org.apache.rocketmq.common.TopicConfig）。"""
from __future__ import annotations

import json
from typing import Dict, Optional


class TopicFilterType:
    SINGLE_TAG = "SINGLE_TAG"
    MULTI_TAG = "MULTI_TAG"


class TopicConfig:
    def __init__(self, topic_name: str = "", read_queue_nums: int = 4,
                 write_queue_nums: int = 4, perm: int = 6):  # 默认 RW=6
        self.topic_name = topic_name
        self.read_queue_nums = read_queue_nums
        self.write_queue_nums = write_queue_nums
        self.perm = perm
        self.topic_filter_type = TopicFilterType.MULTI_TAG
        self.topic_sys_flag = 0
        self.order = False
        self.attributes: Dict[str, str] = {}

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
        }

    @staticmethod
    def decode(json_str: str) -> "TopicConfig":
        data = json.loads(json_str)
        cfg = TopicConfig(data.get("topicName", ""))
        cfg.read_queue_nums = data.get("readQueueNums", 4)
        cfg.write_queue_nums = data.get("writeQueueNums", 4)
        cfg.perm = data.get("perm", 6)
        cfg.topic_filter_type = data.get("topicFilterType", TopicFilterType.MULTI_TAG)
        cfg.topic_sys_flag = data.get("topicSysFlag", 0)
        cfg.order = data.get("order", False)
        return cfg

    @staticmethod
    def from_dict(data: dict) -> "TopicConfig":
        cfg = TopicConfig(data.get("topicName", ""))
        cfg.read_queue_nums = data.get("readQueueNums", 4)
        cfg.write_queue_nums = data.get("writeQueueNums", 4)
        cfg.perm = data.get("perm", 6)
        cfg.topic_filter_type = data.get("topicFilterType", TopicFilterType.MULTI_TAG)
        cfg.topic_sys_flag = data.get("topicSysFlag", 0)
        cfg.order = data.get("order", False)
        return cfg

    def __repr__(self):
        return "TopicConfig[topicName=%s, readQueueNums=%d, writeQueueNums=%d, perm=%s]" % (
            self.topic_name, self.read_queue_nums, self.write_queue_nums, PermName.perm_to_string(self.perm))


from .sysflag import PermName  # noqa: E402