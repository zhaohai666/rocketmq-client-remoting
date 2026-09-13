# -*- coding: utf-8 -*-
"""SubscriptionData（对应 org.apache.rocketmq.common.filter.SubscriptionData 与 remoting 的 SubscriptionData）。"""
from __future__ import annotations

import time
from typing import List, Optional, Set


class ExpressionType:
    TAG = "TAG"
    SQL92 = "SQL92"
    CLASS_FILTER = "CLASS_FILTER"


class SubscriptionData:
    def __init__(self, topic: Optional[str] = None, sub_string: Optional[str] = None):
        self.class_filter_mode = False
        self.topic = topic
        self.sub_string = sub_string
        self.tags_set: Set[str] = set()
        self.code_set: Set[int] = set()
        self.sub_version = int(time.time() * 1000)
        self.expression_type = ExpressionType.TAG
        self.filter_class_source: Optional[str] = None

    def get_topic(self) -> Optional[str]:
        return self.topic

    def set_topic(self, topic: str) -> None:
        self.topic = topic

    def get_sub_string(self) -> Optional[str]:
        return self.sub_string

    def set_sub_string(self, sub_string: Optional[str]) -> None:
        self.sub_string = sub_string

    def get_tags_set(self) -> Set[str]:
        return self.tags_set

    def set_tags_set(self, tags_set: Set[str]) -> None:
        self.tags_set = tags_set

    def get_code_set(self) -> Set[int]:
        return self.code_set

    def get_sub_version(self) -> int:
        return self.sub_version

    def set_sub_version(self, sub_version: int) -> None:
        self.sub_version = sub_version

    def get_expression_type(self) -> str:
        return self.expression_type

    def set_expression_type(self, expression_type: str) -> None:
        self.expression_type = expression_type

    def get_filter_class_source(self) -> Optional[str]:
        return self.filter_class_source

    def set_filter_class_source(self, source: Optional[str]) -> None:
        self.filter_class_source = source

    def is_class_filter_mode(self) -> bool:
        return self.class_filter_mode

    def set_class_filter_mode(self, mode: bool) -> None:
        self.class_filter_mode = mode

    def __eq__(self, other) -> bool:
        if not isinstance(other, SubscriptionData):
            return False
        if self.class_filter_mode != other.class_filter_mode:
            return False
        if self.topic != other.topic:
            return False
        if self.sub_string != other.sub_string:
            return False
        if self.expression_type != other.expression_type:
            return False
        if self.tags_set != other.tags_set:
            return False
        if self.code_set != other.code_set:
            return False
        return True

    def __repr__(self):
        return "SubscriptionData [topic=%s, subString=%s, tagsSet=%s]" % (
            self.topic, self.sub_string, self.tags_set)


class FilterAPI:
    @staticmethod
    def build_subscription_data(topic: str, sub_string: str) -> SubscriptionData:
        sub = SubscriptionData(topic=topic, sub_string=sub_string)
        if sub_string is None or sub_string == "*" or not sub_string.strip():
            sub.tags_set.add("*")
        else:
            for tag in [t.strip() for t in sub_string.split("||")]:
                if tag != "":
                    sub.tags_set.add(tag)
        return sub

    @staticmethod
    def build(topic: str, selector):
        """根据 MessageSelector/expression 构建 SubscriptionData（简化）。"""
        sub_string = None
        expression_type = ExpressionType.TAG
        if selector is not None:
            expression_type = getattr(selector, "type", ExpressionType.TAG)
            sub_string = getattr(selector, "expression", None)
        return FilterAPI.build_subscription_data(topic, sub_string) if sub_string else None