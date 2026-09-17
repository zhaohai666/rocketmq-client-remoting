# -*- coding: utf-8 -*-
"""SubscriptionData（对应 org.apache.rocketmq.common.filter.SubscriptionData 与 remoting 的 SubscriptionData）。"""
from __future__ import annotations

import time
from typing import List, Optional, Set

from .util_all import java_string_hash


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

    def __hash__(self) -> int:
        # 必须与 __eq__ 配套：Python 定义 __eq__ 后默认 __hash__ 变 None，
        # SubscriptionData 一旦不可哈希，ConsumerData.subscription_data_set.add()
        # 会直接抛 TypeError —— 心跳就发不出去（broker 端也就看不到消费者，
        # rebalance 查不到消费者列表 → 队列分不下来）。
        return hash((self.class_filter_mode, self.topic, self.sub_string,
                     self.expression_type,
                     frozenset(self.tags_set or ()),
                     frozenset(self.code_set or ())))

    def to_dict(self) -> dict:
        """Java 字段名（camelCase），tagsSet/codeSet 转 list 才能 JSON 序列化。

        注意 filterClassSource 在 Java 里是 @JSONField(serialize=false) —— **不序列化**。
        """
        return {
            "classFilterMode": self.class_filter_mode,
            "topic": self.topic,
            "subString": self.sub_string,
            "tagsSet": sorted(self.tags_set or ()),
            "codeSet": sorted(self.code_set or ()),
            "subVersion": self.sub_version,
            "expressionType": self.expression_type,
        }

    def __repr__(self):
        return "SubscriptionData [topic=%s, subString=%s, tagsSet=%s]" % (
            self.topic, self.sub_string, self.tags_set)


class FilterAPI:
    SUB_ALL = "*"

    @staticmethod
    def build_subscription_data(topic: str, sub_string: Optional[str]) -> SubscriptionData:
        """对齐 Java `FilterAPI.buildSubscriptionData`（探针实测向量见下）。

        Java 行为（`/tmp/subprobe/SubProbe.java` + `BlankProbe.java` + `EdgeProbe.java` 实测）：
          "*" / None / ""  → subString 归一为 "*"，**tagsSet 与 codeSet 都保持空**
          "TagA"           → tagsSet={TagA}, codeSet={2598919}
          "TagA||TagB"     → tagsSet={TagA,TagB}, codeSet={2598919,2598920}
          " TagA || TagB " → **subString 原样保留空格**，标签各自 trim
          "   "（纯空白）   → tagsSet 空、**subString 原样保留**（StringUtils.isEmpty 只认 null/""）
          "|||"            → tagsSet={|}, codeSet={124}（Java-split 只丢**末尾**空串）
          "||" / "||||"    → 抛 "subString split error"（Java-split 结果数组长度为 0）

        ⚠ 曾经的实现给 "*" 塞了 tagsSet={"*"}，并且从不填 codeSet —— 两处都是偏差：
        ① Java 里 tagsSet 非空是"客户端二次 tag 过滤"的开关（`PullAPIWrapper
        .processPullResult` 的 `!tagsSet.isEmpty()`），塞了 "*" 会让订阅全量时把
        所有正常 tag 的消息客户端自己过滤掉；② codeSet 是 broker 侧按 tag 哈希过滤的依据
        （`ExpressionMessageFilter.isMatchedByConsumeQueue` 走 `codeSet.contains`）。
        """
        sub = SubscriptionData(topic=topic, sub_string=sub_string)
        # Java: StringUtils.isEmpty(subString) || subString.equals("*") -> setSubString("*") 后直接 return
        # 注意是 isEmpty（只认 None/""）而非 isBlank —— 纯空白会走进下面的 split 分支。
        if sub_string is None or sub_string == "" or sub_string == FilterAPI.SUB_ALL:
            sub.sub_string = FilterAPI.SUB_ALL
            return sub
        # Java String.split("\\|\\|")：丢弃**末尾**空串
        parts = sub_string.split("||")
        while parts and parts[-1] == "":
            parts.pop()
        if not parts:
            # Java 这里是 throw new Exception("subString split error")
            raise ValueError("subString split error")
        for part in parts:
            tag = part.strip()
            if tag != "":
                sub.tags_set.add(tag)
                sub.code_set.add(java_string_hash(tag))
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