# 本地位点文件格式对齐 Java LocalFileOffsetStore 的回归守卫。
#
# Java 侧 fastjson2 序列化 Map<MessageQueue, AtomicLong> 时把 MessageQueue 对象直接
# 当 JSON key 写出（用真实 jar 跑 OffsetSerializeWrapper.toJson 实测）：
#   {"offsetTable":{{"brokerName":"broker-a","queueId":1,"topic":"Tt"}:9,...}}
# 严格 JSON 非法，但 fastjson2 自产自销能读回。四端按同一格式落盘，Java 消费者
# 与本端消费者共用同一份 ~/.rocketmq_offsets/<clientId>/<group>/offsets.json 时互认。
import json
import os

import pytest

from rocketmq.client.consumer import (_build_local_offsets_json,
                                       _parse_local_offsets_json,
                                       DefaultMQPushConsumer)
from rocketmq.common.message import MessageQueue

# 真实 fastjson2 2.0.59 + rocketmq-client 5.5.0 的输出（含 pretty 版）
JAVA_COMPACT = ('{"offsetTable":{{"brokerName":"broker-a","queueId":1,"topic":"Tt"}:9,'
                '{"brokerName":"broker-a","queueId":0,"topic":"Tt"}:7}}')
JAVA_PRETTY = ('{\n\t"offsetTable":{\n\t\t{"brokerName":"broker-a","queueId":0,'
               '"topic":"Tt"}:7\n\t}\n}')


def _mq_map():
    return {"%s%s%d" % ("Tt", "broker-a", i): MessageQueue("Tt", "broker-a", i)
            for i in range(3)}


def test_build_uses_java_object_as_key_format():
    text = _build_local_offsets_json(
        {"Ttbroker-a0": 7, "Ttbroker-a1": 9}, _mq_map())
    assert text.startswith('{"offsetTable":{')
    # 字段序 brokerName/queueId/topic（fastjson2 字母序），每个 key 是内嵌对象
    assert '"brokerName":"broker-a","queueId":0,"topic":"Tt"}:7' in text
    assert '"brokerName":"broker-a","queueId":1,"topic":"Tt"}:9' in text


def test_build_skips_entries_without_queue_info():
    text = _build_local_offsets_json({"Ttbroker-a0": 7, "Orphan_b0": 3}, _mq_map())
    assert "Orphan" not in text


def test_parse_java_format_compact_and_pretty():
    assert _parse_local_offsets_json(JAVA_COMPACT) == {
        "Ttbroker-a1": 9, "Ttbroker-a0": 7}
    assert _parse_local_offsets_json(JAVA_PRETTY) == {"Ttbroker-a0": 7}


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


def test_save_writes_java_format_and_rolls_bak(offset_home):
    c = _consumer_with_offsets(offset_home)
    c._save_local_offsets()
    path = c._local_offset_path()
    raw = open(path, encoding="utf-8").read()
    assert raw.startswith('{"offsetTable":{')
    # 首次写不产生 .bak（Java string2File 只在已有旧内容时滚动）
    assert not os.path.exists(path + ".bak")

    with c._lock:
        c._consume_offsets["Ttbroker-a0"] = 99
    c._save_local_offsets()
    # .bak = 上一代内容（Java MixAll.string2File 语义）
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


def test_load_reads_java_written_file(offset_home):
    # Java 写出的文件本端要能读（跨端互认的核心）
    c = _consumer_with_offsets(offset_home)
    path = c._local_offset_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(JAVA_COMPACT)
    assert c._load_local_offsets() == {"Ttbroker-a1": 9, "Ttbroker-a0": 7}
