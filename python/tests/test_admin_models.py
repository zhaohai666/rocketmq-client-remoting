# -*- coding: utf-8 -*-
from __future__ import annotations

import json

import pytest

from common.message import MessageQueue
from common.topic_config import TopicConfig, TopicFilterType
from remoting.protocol.admin_body import (ConsumeStats, OffsetWrapper,
                                                   TopicConfigSerializeWrapper,
                                                   TopicStatsTable, TopicOffset)
from remoting.protocol.body import ResetOffsetBody
from remoting.protocol.serialize import (FastJsonDecodeError, fastjson_loads)
from remoting.protocol.subscription import (GroupRetryPolicy, SubscriptionGroupConfig,
                                                     SubscriptionGroupWrapper)


FASTJSON_TOPIC_STATS = ('{"offsetTable":{{"brokerName":"broker-a","queueId":3,'
                    '"topic":"MyTopic"}:{"lastUpdateTimestamp":1700000000000,'
                    '"maxOffset":500,"minOffset":0}},"topicPutTps":0.0}')

FASTJSON_CONSUME_STATS = ('{"consumeTps":1.5,"offsetTable":{{"brokerName":"broker-a",'
                      '"queueId":3,"topic":"MyTopic"}:{"brokerOffset":100,'
                      '"consumerOffset":90,"lastTimestamp":1700000000000,'
                      '"pullOffset":0}}}')

FASTJSON_TOPIC_CONFIG = ('{"attributes":{"+fileReservedTime":"72","+deleteWhen":"04"},'
                     '"order":false,"perm":6,"readQueueNums":16,'
                     '"topicFilterType":"SINGLE_TAG","topicName":"attr-topic",'
                     '"topicSysFlag":0,"writeQueueNums":16}')

FASTJSON_SUB_GROUP = ('{"attributes":{},"brokerId":0,"consumeBroadcastEnable":true,'
                  '"consumeEnable":true,"consumeFromMinEnable":true,'
                  '"consumeMessageOrderly":false,"consumeTimeoutMinute":15,'
                  '"groupName":"MyGroup","groupRetryPolicy":{"type":"CUSTOMIZED"},'
                  '"groupSysFlag":0,"notifyConsumerIdsChangedEnable":true,'
                  '"retryMaxTimes":16,"retryQueueNums":1,'
                  '"whichBrokerWhenConsumeSlowly":1}')


def test_fastjson_parses_object_keys():
    """核心：非字符串键的内联对象必须能解析（标准 json 会失败）。"""
    with pytest.raises(Exception):
        __import__("json").loads(FASTJSON_TOPIC_STATS)  # 证明确实是非法 JSON
    obj = fastjson_loads(FASTJSON_TOPIC_STATS)
    assert set(obj["offsetTable"].keys()) == {
        '{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}'}
    assert obj["offsetTable"]['{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}'
                              ]["maxOffset"] == 500


def test_fastjson_number_keys_still_supported():
    """BrokerData.brokerAddrs 的数字键（zhaohai666 5.x 会输出裸数字键）。"""
    obj = fastjson_loads('{"brokerAddrs":{0:"127.0.0.1:10911",1:"127.0.0.1:10912"}}')
    assert obj["brokerAddrs"]["0"] == "127.0.0.1:10911"
    assert obj["brokerAddrs"]["1"] == "127.0.0.1:10912"


def test_fastjson_edge_tokens():
    obj = fastjson_loads('{"a":NaN,"b":Infinity,"c":-Infinity,"d":null,"e":[1,2,3],'
                         '"f":"x\\ny","g":true}')
    assert obj["a"] != obj["a"]          # NaN
    assert obj["b"] == float("inf")
    assert obj["c"] == float("-inf")
    assert obj["d"] is None
    assert obj["e"] == [1, 2, 3]
    assert obj["f"] == "x\ny"
    assert obj["g"] is True


def test_fastjson_rejects_garbage():
    with pytest.raises(FastJsonDecodeError):
        fastjson_loads('{"a":}')


# ---------------------------------------------------------------- 管理端 DTO


def test_topic_stats_table_decode():
    t = TopicStatsTable.decode(FASTJSON_TOPIC_STATS.encode("utf-8"))
    assert len(t.offset_table) == 1
    mq = MessageQueue("MyTopic", "broker-a", 3)
    assert mq in t.offset_table
    assert t.offset_table[mq].max_offset == 500
    assert t.offset_table[mq].min_offset == 0
    assert t.topic_put_tps == 0.0


def test_topic_stats_table_round_trip():
    t = TopicStatsTable()
    t.offset_table[MessageQueue("T", "b", 7)] = TopicOffset(1, 9, 123)
    t.topic_put_tps = 2.5
    again = TopicStatsTable.decode(t.encode())
    assert again.offset_table[MessageQueue("T", "b", 7)].max_offset == 9
    assert again.offset_table[MessageQueue("T", "b", 7)].min_offset == 1
    assert again.offset_table[MessageQueue("T", "b", 7)].last_update_timestamp == 123
    assert again.topic_put_tps == 2.5


def test_consume_stats_decode_and_lag():
    cs = ConsumeStats.decode(FASTJSON_CONSUME_STATS.encode("utf-8"))
    mq = MessageQueue("MyTopic", "broker-a", 3)
    assert cs.consume_tps == 1.5
    assert cs.offset_table[mq].broker_offset == 100
    assert cs.offset_table[mq].consumer_offset == 90
    assert cs.offset_table[mq].lag == 10
    assert cs.total_lag == 10


def test_offset_wrapper_lag_is_broker_minus_consumer():
    pass
    assert OffsetWrapper(broker_offset=5, consumer_offset=9).lag == -4


def test_reset_offset_body_is_mq_keyed():
    pass
    raw = ('{"offsetTable":{{"brokerName":"b","queueId":0,"topic":"T"}:10,'
           '{"brokerName":"b","queueId":1,"topic":"T"}:20}}')
    body = ResetOffsetBody.decode(raw.encode("utf-8"))
    assert body.offset_table[MessageQueue("T", "b", 0)] == 10
    assert body.offset_table[MessageQueue("T", "b", 1)] == 20
    again = ResetOffsetBody.decode(body.encode())
    assert again.offset_table[MessageQueue("T", "b", 1)] == 20


def test_consume_stats_list_uses_the_server_field_name():
    pass
    from remoting.protocol.body import ConsumeStatsList

    raw = (b'{"consumeStatsList":[{"G_BROKER":[{"offsetTable":{}}]}],'
           b'"brokerAddr":"127.0.0.1:10911","totalDiff":7,"totalInflightDiff":2}')
    sl = ConsumeStatsList.decode(raw)
    assert len(sl.stats_list) == 1
    assert sl.broker_addr == "127.0.0.1:10911"
    assert sl.total_diff == 7
    assert sl.total_inflight_diff == 2

    # 旧键名的响应必须解析不出行，否则掩盖回归
    assert ConsumeStatsList.decode(b'{"statsList":[{"G":[{}]}]}').stats_list == []

    assert json.loads(ConsumeStatsList().encode()) == {
        "consumeStatsList": [], "totalDiff": 0, "totalInflightDiff": 0}


# ---------------------------------------------------------------- TopicConfig / 订阅组


def test_topic_config_server_side_defaults():
    pass
    cfg = TopicConfig("t")
    assert cfg.read_queue_nums == 16
    assert cfg.write_queue_nums == 16
    assert cfg.perm == 6
    assert cfg.topic_filter_type == TopicFilterType.SINGLE_TAG
    assert cfg.attributes == {}


def test_topic_config_serializes_attributes():
    cfg = TopicConfig.from_dict(fastjson_loads(FASTJSON_TOPIC_CONFIG))
    assert cfg.attributes["+deleteWhen"] == "04"
    assert cfg.attributes["+fileReservedTime"] == "72"
    out = cfg.to_dict()
    assert "attributes" in out and out["attributes"] == cfg.attributes


def test_topic_config_serialize_wrapper():
    raw = ('{"dataVersion":{"counter":0,"stateVersion":0,"timestamp":1},'
           '"topicConfigTable":{"MyTopic":' + FASTJSON_TOPIC_CONFIG + '}}')
    w = TopicConfigSerializeWrapper.decode(raw.encode("utf-8"))
    assert "MyTopic" in w.topic_config_table
    assert w.topic_config_table["MyTopic"].read_queue_nums == 16


def test_subscription_group_config_fields():
    cfg = SubscriptionGroupConfig.from_dict(fastjson_loads(FASTJSON_SUB_GROUP))
    assert cfg.group_name == "MyGroup"
    assert cfg.retry_queue_nums == 1
    assert cfg.retry_max_times == 16
    assert cfg.consume_timeout_minute == 15
    assert cfg.broker_id == 0
    assert cfg.group_retry_policy.type == "CUSTOMIZED"
    # fastjson2 跳过 null：subscriptionDataSet 不应出现
    assert config_has_no_null_sub_set(cfg)
    assert "subscriptionDataSet" not in cfg.to_dict()


def config_has_no_null_sub_set(cfg: SubscriptionGroupConfig) -> bool:
    return cfg.subscription_data_set is None


def test_subscription_group_wrapper_decode():
    raw = ('{"dataVersion":{"counter":0,"stateVersion":0,"timestamp":1},'
           '"forbiddenTable":{},"subscriptionGroupTable":{"G1":' + FASTJSON_SUB_GROUP + '}}')
    w = SubscriptionGroupWrapper.decode(raw.encode("utf-8"))
    assert w.subscription_group_table["G1"].group_name == "MyGroup"
    assert w.forbidden_table == {}


def test_subscription_group_round_trip():
    w = SubscriptionGroupWrapper()
    w.subscription_group_table["g"] = SubscriptionGroupConfig("g")
    w.subscription_group_table["g"].retry_max_times = 3
    w.subscription_group_table["g"].group_retry_policy = GroupRetryPolicy()
    again = SubscriptionGroupWrapper.decode(w.encode())
    assert again.subscription_group_table["g"].retry_max_times == 3


# ---------------------------------------------------------------- MixAll properties
def test_properties_text_round_trip_matches_the_server_format():
    pass
    from common.mix_all import MixAll
    text = "a = b\n  c : d  \nk=v\n#comment\n!c2\nempty=\ncont=first\\\n    second\n"
    props = MixAll.string2_properties(text)
    assert props["a"] == "b"
    assert props["c"] == "d  "
    assert props["k"] == "v"
    assert props["empty"] == ""
    assert props["cont"] == "firstsecond"
    assert "#comment" not in props

    round_trip = MixAll.string2_properties(MixAll.properties2_string(props))
    assert round_trip == props


def test_properties_whitespace_is_a_valid_separator():
    pass
    from common.mix_all import MixAll
    props = MixAll.string2_properties("a b\nonlykey\nk=v\n")
    assert props["a"] == "b", props
    assert props["onlykey"] == "", props
    assert props["k"] == "v", props
    # 空白分隔时，值里的 '=' 原样保留
    props2 = MixAll.string2_properties("a b=c=d\n")
    assert props2["a"] == "b=c=d", props2
    # 有 '=' 时 '=' 优先于空白成为分隔符（值里的空格保留）
    props3 = MixAll.string2_properties("messageDelayLevel=1s 5s 10s\n")
    assert props3["messageDelayLevel"] == "1s 5s 10s", props3


def test_perm_name_is_valid_matches_the_server_rule():
    pass
    from common.sysflag import PermName
    assert PermName.is_valid(0) is True
    assert PermName.is_valid(7) is True
    assert PermName.is_valid(8) is False
    assert PermName.is_valid(-1) is False


def test_mix_all_group_helpers():
    from common.mix_all import MixAll
    assert MixAll.is_lmq("%LMQ%foo") is True
    assert MixAll.is_lmq("normal") is False
    assert MixAll.is_sys_consumer_group("CID_RMQ_SYS_x") is True
    assert MixAll.is_sys_consumer_group("normal") is False
    assert MixAll.is_predefined_group("TOOLS_CONSUMER") is True
    assert MixAll.is_predefined_group("my-group") is False


def test_unique_msg_query_flag_is_extfield_name():
    pass
    from common.mix_all import MixAll, QueryMsgType
    assert MixAll.UNIQUE_MSG_QUERY_FLAG == "_UNIQUE_KEY_QUERY"
    assert QueryMsgType.UNIQUE_KEY == 1


def test_message_const_index_types():
    """broker 的 indexType 取值：K / U / T。"""
    from common.message_const import MessageConst
    assert MessageConst.INDEX_KEY_TYPE == "K"
    assert MessageConst.INDEX_UNIQUE_TYPE == "U"
    assert MessageConst.INDEX_TAG_TYPE == "T"


def test_create_topic_header_sends_topic_filter_type():
    """broker 的 CreateTopicRequestHeader.checkFields 要求 topicFilterType 非空。"""
    from remoting.protocol.headers import CreateTopicRequestHeader
    h = CreateTopicRequestHeader()
    h.topic = "t"
    h.default_topic = "TBW102"
    h.read_queue_nums = 4
    h.write_queue_nums = 4
    h.perm = 6
    h.topic_filter_type = "SINGLE_TAG"
    h.topic_sys_flag = 0
    h.order = False
    h.attributes = ""
    h.force = False
    ext = h.to_ext_fields()
    assert ext["topicFilterType"] == "SINGLE_TAG"
    assert ext["attributes"] == ""
    assert ext["force"] == "false"


def test_query_message_header_index_type():
    from remoting.protocol.headers import QueryMessageRequestHeader
    h = QueryMessageRequestHeader()
    h.topic = "t"
    h.key = "k"
    h.max_num = 32
    h.begin_timestamp = 1
    h.end_timestamp = 2
    h.index_type = "U"
    ext = h.to_ext_fields()
    assert ext["indexType"] == "U"
    assert "lastKey" not in ext
