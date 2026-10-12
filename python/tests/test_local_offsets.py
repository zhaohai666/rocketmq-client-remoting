import json
import os

import pytest

from client.consumer import (_build_local_offsets_json,
                                       _parse_local_offsets_json,
                                       DefaultMQPushConsumer)
from common.message import MessageQueue

# 真实 fastjson2 2.0.59 + rocketmq-client 5.5.0 的输出（含 pretty 版）
FASTJSON_OFFSETS_COMPACT = ('{"offsetTable":{{"brokerName":"broker-a","queueId":1,"topic":"Tt"}:9,'
                '{"brokerName":"broker-a","queueId":0,"topic":"Tt"}:7}}')
FASTJSON_OFFSETS_PRETTY = ('{\n\t"offsetTable":{\n\t\t{"brokerName":"broker-a","queueId":0,'
               '"topic":"Tt"}:7\n\t}\n}')


def _mq_map():
    return {"%s%s%d" % ("Tt", "broker-a", i): MessageQueue("Tt", "broker-a", i)
            for i in range(3)}


def test_build_uses_the_object_as_key_format():
    text = _build_local_offsets_json(
        {"Ttbroker-a0": 7, "Ttbroker-a1": 9}, _mq_map())
    assert text.startswith('{"offsetTable":{')
    # 字段序 brokerName/queueId/topic（fastjson2 字母序），每个 key 是内嵌对象
    assert '"brokerName":"broker-a","queueId":0,"topic":"Tt"}:7' in text
    assert '"brokerName":"broker-a","queueId":1,"topic":"Tt"}:9' in text


def test_build_skips_entries_without_queue_info():
    text = _build_local_offsets_json({"Ttbroker-a0": 7, "Orphan_b0": 3}, _mq_map())
    assert "Orphan" not in text


def test_parse_fastjson_compact_and_pretty():
    assert _parse_local_offsets_json(FASTJSON_OFFSETS_COMPACT) == {
        "Ttbroker-a1": 9, "Ttbroker-a0": 7}
    assert _parse_local_offsets_json(FASTJSON_OFFSETS_PRETTY) == {"Ttbroker-a0": 7}


def test_parse_accepts_legacy_flat_map():
    assert _parse_local_offsets_json('{"Ttbroker-a0":5}') == {"Ttbroker-a0": 5}


def test_parse_rejects_garbage():
    assert _parse_local_offsets_json('{"offsetTable":{{{') is None
    assert _parse_local_offsets_json('{"offsetTable":') is None
    assert _parse_local_offsets_json('') is None


def test_roundtrip_built_text():
    text = _build_local_offsets_json(
        {"Ttbroker-a0": 7, "Ttbroker-a1": 9, "Ttbroker-a2": 11}, _mq_map())
    assert _parse_local_offsets_json(text) == {
        "Ttbroker-a0": 7, "Ttbroker-a1": 9, "Ttbroker-a2": 11}


@pytest.fixture()
def offset_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    return tmp_path


def _consumer_with_offsets(home):
    c = DefaultMQPushConsumer("g_local_offsets_test")
    c._mq_map.update(_mq_map())
    with c._lock:
        c._consume_offsets.update({"Ttbroker-a0": 7, "Ttbroker-a1": 9,
                                   "Ttbroker-a2": 11})
    return c


def test_save_writes_the_object_as_key_format_and_rolls_bak(offset_home):
    c = _consumer_with_offsets(offset_home)
    c._save_local_offsets()
    path = c._local_offset_path()
    raw = open(path, encoding="utf-8").read()
    assert raw.startswith('{"offsetTable":{')
    assert not os.path.exists(path + ".bak")

    with c._lock:
        c._consume_offsets["Ttbroker-a0"] = 99
    c._save_local_offsets()
    assert open(path + ".bak", encoding="utf-8").read() == raw


def test_load_main_then_bak_fallback(offset_home):
    c = _consumer_with_offsets(offset_home)
    c._save_local_offsets()
    path = c._local_offset_path()
    with c._lock:
        c._consume_offsets["Ttbroker-a0"] = 99
    c._save_local_offsets()
    assert c._load_local_offsets()["Ttbroker-a0"] == 99

    # 主文件缺失 → .bak（上一代）
    os.remove(path)
    assert c._load_local_offsets()["Ttbroker-a0"] == 7

    # .bak 也缺失 → 空（按首次启动处理）
    os.remove(path + ".bak")
    assert c._load_local_offsets() == {}


def test_load_reads_a_fastjson_written_file(offset_home):
    c = _consumer_with_offsets(offset_home)
    path = c._local_offset_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(FASTJSON_OFFSETS_COMPACT)
    assert c._load_local_offsets() == {"Ttbroker-a1": 9, "Ttbroker-a0": 7}
