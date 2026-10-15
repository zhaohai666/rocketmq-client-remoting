# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

from remoting.protocol.codes import LanguageCode, RequestCode, ResponseCode, SerializeType

SERVER_SRC = os.environ.get("ROCKETMQ_SERVER_SRC")

FIELD = re.compile(r"public\s+static\s+final\s+int\s+([A-Z0-9_]+)\s*=\s*(-?\d+)\s*;")
ENUM_ENTRY = re.compile(r"\s*([A-Z][A-Z0-9_]*)\(\s*\(?byte\)?\s*(-?\d+)\s*\)")

pytestmark = pytest.mark.skipif(
    not SERVER_SRC, reason="ROCKETMQ_SERVER_SRC is not set, skipping the constant comparison against the server protocol source")


def _reference_src_dir() -> Path:
    p = Path(SERVER_SRC)
    if p.is_dir() and (p / "RequestCode.java").exists():
        return p
    candidate = p / "remoting" / "src" / "main" / "java" / "org" / "apache" / "rocketmq" / "remoting" / "protocol"
    return candidate


def _reference_int_constants(filename: str) -> dict:
    text = (Path(_reference_src_dir()) / filename).read_text(encoding="utf-8")
    return {k: int(v) for k, v in FIELD.findall(text)}


def test_request_code_matches_the_reference_source():
    if not (Path(_reference_src_dir()) / "RequestCode.java").exists():
        pytest.skip("the server protocol's RequestCode definition was not found")
    reference = _reference_int_constants("RequestCode.java")
    bad = {k: (v, getattr(RequestCode, k, None)) for k, v in reference.items()
           if getattr(RequestCode, k, None) != v}
    assert not bad, "RequestCode does not match the server protocol source: %s" % bad


def test_response_code_matches_the_reference_source():
    if not (Path(_reference_src_dir()) / "ResponseCode.java").exists():
        pytest.skip("the server protocol's ResponseCode definition was not found")
    reference = _reference_int_constants("ResponseCode.java")
    bad = {k: (v, getattr(ResponseCode, k, None)) for k, v in reference.items()
           if getattr(ResponseCode, k, None) != v}
    assert not bad, "ResponseCode does not match the server protocol source: %s" % bad


def test_language_code_matches_the_reference_source():
    path = Path(_reference_src_dir()) / "LanguageCode.java"
    if not path.exists():
        pytest.skip("the server protocol's LanguageCode definition was not found")
    bad = []
    for line in path.read_text(encoding="utf-8").splitlines():
        m = ENUM_ENTRY.match(line)
        if not m:
            continue
        name, value = m.group(1), int(m.group(2))
        if getattr(LanguageCode, name, None) != value:
            bad.append((name, value, getattr(LanguageCode, name, None)))
    assert not bad, "LanguageCode does not match the server protocol source: %s" % bad


def test_serialize_type_values():
    assert (SerializeType.JSON, SerializeType.ROCKETMQ) == (0, 1)
