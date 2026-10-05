# -*- coding: utf-8 -*-
"""时间戳查位点的边界语义（对应 org.apache.rocketmq.common.BoundaryType）。

Java 锚点：

  * 枚举声明 ``LOWER("lower")`` / ``UPPER("upper")``（BoundaryType.java:23/:28），
    入网的值却是 ``Enum.toString()``（``RemotingCommand.makeCustomHeaderToNet``:430
    的 ``value.toString()``）—— 也就是**大写枚举名** ``LOWER`` / ``UPPER``。
    ``getName()`` 那个小写名只被 :41 的解析函数用，从不上报文。
  * ``getType(String)``:41 是 **宽松** 解析：只有 ``"upper".equalsIgnoreCase(name)``
    才回 UPPER，其余（null / 空串 / 未知值 / 小写 "lower"）一律 LOWER。broker 侧
    ``RemotingCommand`` 解 header 时就是拿它做 ``BoundaryType`` 字段的转换，所以
    客户端发什么大小写都能被认出来，但报文必须与 Java 客户端逐字节一致。

broker 侧语义（ConsumeQueue.binarySearchInQueueByTime:261-270 / :392-404）：

  * 时间戳大于队列里最后一条的 storeTime ⇒ LOWER = maxOffset（下一个可写位点），
    UPPER = maxOffset - 1（最后一条自己的位点）；
  * 时间戳小于最老一条 ⇒ LOWER = 0，UPPER = 0；
  * 命中 ⇒ LOWER = 共享该 storeTime 的第一条，UPPER = 最后一条；
  * 落在空档 ⇒ LOWER = 下一条（右边界），UPPER = 上一条（左边界）。
"""
from __future__ import annotations

from enum import Enum


class BoundaryType(Enum):
    """对应 Java ``BoundaryType``；``.value`` 就是 Java ``toString()`` 的入网文本。"""

    LOWER = "LOWER"
    UPPER = "UPPER"

    @property
    def lowercase_name(self) -> str:
        """Java ``BoundaryType.getName()``（"lower"/"upper"），只用于比对与日志。"""
        return self.value.lower()

    @staticmethod
    def get_type(name) -> "BoundaryType":
        """Java ``BoundaryType.getType(String)``：非 "upper"（大小写不敏感）一律 LOWER。"""
        if isinstance(name, str) and name.lower() == BoundaryType.UPPER.lowercase_name:
            return BoundaryType.UPPER
        return BoundaryType.LOWER


__all__ = ["BoundaryType"]
