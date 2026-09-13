# -*- coding: utf-8 -*-
"""协议常量与 Java 源码对齐的回归守卫。

若环境变量 ``ROCKETMQ_JAVA_SRC`` 指向 Java 的 ``rocketmq/remoting/src/main/java``，
则逐条比对 RequestCode / ResponseCode / LanguageCode 的取值，防止 Python 侧漂移；
未配置时整体跳过（不依赖 Java 源码也能跑完其余测试）。
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

from rocketmq.remoting.protocol.codes import LanguageCode, RequestCode, ResponseCode, SerializeType

JAVA_SRC = os.environ.get("ROCKETMQ_JAVA_SRC")

FIELD = re.compile(r"public\s+static\s+final\s+int\s+([A-Z0-9_]+)\s*=\s*(-?\d+)\s*;")
ENUM_ENTRY = re.compile(r"\s*([A-Z][A-Z0-9_]*)\(\s*\(?byte\)?\s*(-?\d+)\s*\)")

pytestmark = pytest.mark.skipif(
    not JAVA_SRC, reason="未设置 ROCKETMQ_JAVA_SRC，跳过与 Java 源码的常量比对")


def _java_dir() -> Path:
    p = Path(JAVA_SRC)
    if p.is_dir() and (p / "RequestCode.java").exists():
        return p
    candidate = p / "remoting" / "src" / "main" / "java" / "org" / "apache" / "rocketmq" / "remoting" / "protocol"
    return candidate


def _java_int_constants(filename: str) -> dict:
    text = (Path(_java_dir()) / filename).read_text(encoding="utf-8")
    return {k: int(v) for k, v in FIELD.findall(text)}


def test_request_code_matches_java():
    if not (Path(_java_dir()) / "RequestCode.java").exists():
        pytest.skip("未找到 Java RequestCode.java")
    java = _java_int_constants("RequestCode.java")
    bad = {k: (v, getattr(RequestCode, k, None)) for k, v in java.items()
           if getattr(RequestCode, k, None) != v}
    assert not bad, "RequestCode 与 Java 不一致：%s" % bad


def test_response_code_matches_java():
    if not (Path(_java_dir()) / "ResponseCode.java").exists():
        pytest.skip("未找到 Java ResponseCode.java")
    java = _java_int_constants("ResponseCode.java")
    bad = {k: (v, getattr(ResponseCode, k, None)) for k, v in java.items()
           if getattr(ResponseCode, k, None) != v}
    assert not bad, "ResponseCode 与 Java 不一致：%s" % bad


def test_language_code_matches_java():
    path = Path(_java_dir()) / "LanguageCode.java"
    if not path.exists():
        pytest.skip("未找到 Java LanguageCode.java")
    bad = []
    for line in path.read_text(encoding="utf-8").splitlines():
        m = ENUM_ENTRY.match(line)
        if not m:
            continue
        name, value = m.group(1), int(m.group(2))
        if getattr(LanguageCode, name, None) != value:
            bad.append((name, value, getattr(LanguageCode, name, None)))
    assert not bad, "LanguageCode 与 Java 不一致：%s" % bad


def test_serialize_type_values():
    assert (SerializeType.JSON, SerializeType.ROCKETMQ) == (0, 1)
