# -*- coding: utf-8 -*-
"""POP 模式（5.x 轻量消费）本地单测 —— 不需要集群。

覆盖三块最容易出错、且真机上很难定位的契约：

1. ``extra_info``（CK 串）编解码：**空格**做段分隔、Java ``String.split`` 的
   "丢弃末尾空串"语义、段数校验、重复 key 拒绝、retryFlag 判定。
2. 请求头的 extFields **键名逐字等于 Java 字段名** —— broker 用 fastjson2 按 Java
   属性名反序列化，错一个字母就**静默丢字段**（不报错、行为静默退化），
   所以这里逐键断言，把它当回归守卫。
3. ``POP_CK`` 反构：普通 topic 直连 POP 时 broker **不写** POP_CK 属性，
   必须由客户端用响应头的 startOffsetInfo/msgOffsetInfo 拼出 8 段 CK 串，
   否则后续 ACK 无从下手。这是整个 POP 实现最容易踩的坑。
"""
from __future__ import annotations

import pytest

from rocketmq.client.consumer_result import ChangeInvisibleTimeResult, PopResult, PopStatus
from rocketmq.client.exception import MQBrokerException
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.common.message import MessageExt
from rocketmq.common.message_const import MessageConst
from rocketmq.remoting.protocol import extra_info as ei
from rocketmq.remoting.protocol.codes import ResponseCode
from rocketmq.remoting.protocol.headers import (AckMessageRequestHeader,
                                                ChangeInvisibleTimeRequestHeader,
                                                ChangeInvisibleTimeResponseHeader,
                                                PopMessageRequestHeader,
                                                PopMessageResponseHeader)
from rocketmq.remoting.protocol.remoting_command import RemotingCommand

BROKER = "broker-a"
ADDR = "127.0.0.1:10911"
GROUP = "PopUnitTestGroup"
TOPIC = "PopUnitTestTopic"


# ---------------------------------------------------------------- 工具

def _resp(code: int, ext: dict = None, body: bytes = None, remark: str = "") -> RemotingCommand:
    r = RemotingCommand(code=code, remark=remark)
    r.ext_fields = dict(ext or {})
    r.body = body
    return r


class _FakeClient:
    """记录最后一次请求并返回预置响应的假 client。"""

    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, addr, request, timeout_millis=0):
        # 真实链路上 ext_fields 是在 encode 时从 custom_header 落下去的，
        # 这里显式触发一次，才能断言"线上报文里到底有哪些 key"。
        request.make_custom_header_to_net()
        self.requests.append((addr, request, timeout_millis))
        if len(self.responses) == 1:
            return self.responses[0]
        return self.responses.pop(0)


def _client(fake) -> MQClientInstance:
    inst = object.__new__(MQClientInstance)
    inst._invoke_sync = fake
    return inst


def _msg(topic: str = TOPIC, queue_id: int = 0, queue_offset: int = 0,
         props: dict = None) -> MessageExt:
    m = MessageExt(topic=topic, body=b"body")
    m.queue_id = queue_id
    m.queue_offset = queue_offset
    if props:
        m.properties.update(props)
    return m


def _pop_ext(token: int, pop_time: int = 1789613086027, invisible: int = 60000,
             revive_qid: int = 0) -> dict:
    return {
        "popTime": str(pop_time),
        "invisibleTime": str(invisible),
        "reviveQid": str(revive_qid),
        "restNum": "0",
        "startOffsetInfo": "0 0 0;0 3 0;0 2 0",
        "msgOffsetInfo": "0 0 %d;0 3 %d;0 2 %d" % (token, token, token),
        "orderCountInfo": "",
    }


# ---------------------------------------------------------------- 1. extra_info

class TestExtraInfoBuildSplit:
    def test_build_8_segments_joined_by_space(self):
        ck = ei.build_extra_info(0, 1789613086027, 60000, 0, TOPIC, BROKER, 3, 7)
        assert ck == "0 1789613086027 60000 0 0 broker-a 3 7"
        assert len(ei.split(ck)) == 8

    def test_build_7_segments_when_no_msg_queue_offset(self):
        ck = ei.build_extra_info(0, 1, 2, 3, TOPIC, BROKER, 4)
        assert ck == "0 1 2 3 0 broker-a 4"
        assert len(ei.split(ck)) == 7

    def test_split_drops_trailing_empty_like_java(self):
        # Java "1 2 3 ".split(" ") -> ["1","2","3"]；Python 原生会留一个 ""
        assert ei.split("1 2 3 ") == ["1", "2", "3"]
        assert ei.split("1 2 3  ") == ["1", "2", "3"]

    def test_split_none_rejected(self):
        with pytest.raises(ValueError):
            ei.split(None)

    def test_roundtrip(self):
        for msg_offset in (0, 7, 123456789):
            ck = ei.build_extra_info(11, 22, 33, 0, TOPIC, BROKER, 5, msg_offset)
            seg = ei.split(ck)
            assert ei.get_ck_queue_offset(seg) == 11
            assert ei.get_pop_time(seg) == 22
            assert ei.get_invisible_time(seg) == 33
            assert ei.get_revive_qid(seg) == 0
            assert ei.get_retry(seg) == ei.NORMAL_TOPIC
            assert ei.get_broker_name(seg) == BROKER
            assert ei.get_queue_id(seg) == 5
            assert ei.get_queue_offset(seg) == msg_offset


class TestExtraInfoGetters:
    def test_length_guards(self):
        seg7 = ei.split("0 1 2 3 0 broker-a 4")
        with pytest.raises(ValueError):
            ei.get_queue_offset(seg7)          # 需要第 8 段
        with pytest.raises(ValueError):
            ei.get_queue_id(["0", "1", "2", "3", "0", "b"])   # 需要 7 段
        with pytest.raises(ValueError):
            ei.get_ck_queue_offset([])

    def test_is_order_uses_revive_qid_999(self):
        assert ei.is_order(ei.split(ei.build_extra_info(0, 1, 2, 999, TOPIC, BROKER, 0)))
        assert not ei.is_order(ei.split(ei.build_extra_info(0, 1, 2, 0, TOPIC, BROKER, 0)))


class TestExtraInfoRetryTopic:
    def test_retry_flag_of_topic(self):
        assert ei.retry_of_topic(TOPIC) == ei.NORMAL_TOPIC
        assert ei.retry_of_topic("%%RETRY%%%s_%s" % (GROUP, TOPIC)) == ei.RETRY_TOPIC
        assert ei.retry_of_topic("%%RETRY%%%s+%s" % (GROUP, TOPIC)) == ei.RETRY_TOPIC_V2

    def test_v2_checked_before_v1_prefix(self):
        # V2 也带 %RETRY% 前缀，顺序判错就会被误判成 V1
        v2 = ei.build_pop_retry_topic_v2(TOPIC, GROUP)
        assert ei.is_pop_retry_topic_v2(v2)
        assert ei.retry_of_topic(v2) == ei.RETRY_TOPIC_V2

    def test_build_pop_retry_topics(self):
        assert ei.build_pop_retry_topic_v1(TOPIC, GROUP) == "%%RETRY%%%s_%s" % (GROUP, TOPIC)
        assert ei.build_pop_retry_topic_v2(TOPIC, GROUP) == "%%RETRY%%%s+%s" % (GROUP, TOPIC)
        # 默认（enableRetryTopicV2 关）走 V1
        assert ei.build_pop_retry_topic(TOPIC, GROUP) == ei.build_pop_retry_topic_v1(TOPIC, GROUP)

    def test_build_extra_info_marks_retry_messages(self):
        ck = ei.build_extra_info(0, 1, 2, 0, ei.build_pop_retry_topic_v1(TOPIC, GROUP), BROKER, 0)
        assert ei.get_retry(ei.split(ck)) == ei.RETRY_TOPIC

    def test_get_real_topic(self):
        assert ei.get_real_topic(TOPIC, GROUP, "0") == TOPIC
        assert ei.get_real_topic(TOPIC, GROUP, "1") == ei.build_pop_retry_topic_v1(TOPIC, GROUP)
        assert ei.get_real_topic(TOPIC, GROUP, "2") == ei.build_pop_retry_topic_v2(TOPIC, GROUP)
        with pytest.raises(ValueError):
            ei.get_real_topic(TOPIC, GROUP, "9")


class TestExtraInfoParse:
    def test_parse_start_offset_info(self):
        # 真机实测就是这种形状（空格分段、分号分队列）
        got = ei.parse_start_offset_info("0 0 0;0 3 0;0 2 0")
        assert got == {"0@0": 0, "0@3": 0, "0@2": 0}

    def test_parse_start_offset_info_single_queue(self):
        assert ei.parse_start_offset_info("0 3 5") == {"0@3": 5}

    def test_parse_empty_returns_none(self):
        assert ei.parse_start_offset_info("") is None
        assert ei.parse_start_offset_info(None) is None
        assert ei.parse_msg_offset_info("") is None

    def test_parse_start_offset_info_rejects_bad_segment_count(self):
        with pytest.raises(ValueError):
            ei.parse_start_offset_info("0 3")
        with pytest.raises(ValueError):
            ei.parse_start_offset_info("0 3 0 9")

    def test_parse_start_offset_info_rejects_duplicate(self):
        with pytest.raises(ValueError):
            ei.parse_start_offset_info("0 3 0;0 3 1")

    def test_parse_msg_offset_info(self):
        got = ei.parse_msg_offset_info("0 3 0,1,2;0 2 7")
        assert got == {"0@3": [0, 1, 2], "0@2": [7]}

    def test_parse_order_count_info(self):
        assert ei.parse_order_count_info("0 3 5") == {"0@3": 5}

    def test_parse_retry_queue_key(self):
        assert ei.parse_start_offset_info("1 3 0") == {"1@3": 0}

    def test_map_keys(self):
        assert ei.get_start_offset_info_map_key(TOPIC, 3) == "0@3"
        assert ei.get_queue_offset_key_value_key(3, 7) == "qo3%7"
        assert ei.get_queue_offset_map_key(TOPIC, 3, 7) == "0@qo3%7"
        # retry topic 的 key 带对应的 retryFlag
        assert ei.get_start_offset_info_map_key(ei.build_pop_retry_topic_v1(TOPIC, GROUP), 3) == "1@3"


# ---------------------------------------------------------------- 2. 请求/响应头

class TestPopRequestHeader:
    def test_ext_keys_exactly_match_java(self):
        h = PopMessageRequestHeader()
        h.consumer_group = GROUP
        h.topic = TOPIC
        h.queue_id = -1
        h.max_msg_nums = 32
        h.invisible_time = 60000
        h.poll_time = 0
        h.born_time = 1789613086027
        h.init_mode = 0
        h.exp_type = "TAG"
        h.exp = "*"
        h.attempt_id = "attempt-1"
        ext = h.to_ext_fields()
        assert set(ext.keys()) == {
            "consumerGroup", "topic", "queueId", "maxMsgNums", "invisibleTime",
            "pollTime", "bornTime", "initMode", "expType", "exp", "order", "attemptId",
        }
        assert ext["consumerGroup"] == GROUP
        assert ext["queueId"] == -1
        assert ext["bornTime"] == 1789613086027

    def test_order_always_serialized_as_lowercase(self):
        # Java 字段是 Boolean order = Boolean.FALSE（非 null），encodeHeader 不会跳过它
        h = PopMessageRequestHeader()
        h.topic = TOPIC
        ext = h.to_ext_fields()
        assert ext["order"] == "false"

    def test_unset_optionals_absent(self):
        h = PopMessageRequestHeader()
        h.topic = TOPIC
        ext = h.to_ext_fields()
        assert "consumerGroup" not in ext
        assert "exp" not in ext
        assert "attemptId" not in ext


class TestPopResponseHeader:
    def test_parse(self):
        h = PopMessageResponseHeader()
        h.from_ext_fields(_pop_ext(token=0))
        assert h.pop_time == 1789613086027
        assert h.invisible_time == 60000
        assert h.revive_qid == 0
        assert h.rest_num == 0
        assert h.start_offset_info == "0 0 0;0 3 0;0 2 0"
        assert h.msg_offset_info == "0 0 0;0 3 0;0 2 0"

    def test_parse_missing_fields_are_none(self):
        h = PopMessageResponseHeader()
        h.from_ext_fields({})
        assert h.pop_time is None
        assert h.start_offset_info is None


class TestAckAndChangeHeaders:
    def test_ack_ext_keys(self):
        h = AckMessageRequestHeader()
        h.consumer_group = GROUP
        h.topic = TOPIC
        h.queue_id = 3
        h.extra_info = "ck"
        h.offset = 7
        assert h.to_ext_fields() == {
            "consumerGroup": GROUP, "topic": TOPIC, "queueId": 3,
            "extraInfo": "ck", "offset": 7,
        }

    def test_change_invisible_ext_keys(self):
        h = ChangeInvisibleTimeRequestHeader()
        h.consumer_group = GROUP
        h.topic = TOPIC
        h.queue_id = 3
        h.extra_info = "ck"
        h.offset = 7
        h.invisible_time = 20000
        ext = h.to_ext_fields()
        assert set(ext.keys()) == {
            "consumerGroup", "topic", "queueId", "extraInfo", "offset",
            "invisibleTime", "suspend",
        }
        # Java 是 primitive boolean，所以总是出现，且是小写
        assert ext["suspend"] == "false"

    def test_change_invisible_response_parse(self):
        h = ChangeInvisibleTimeResponseHeader()
        h.from_ext_fields({"popTime": "111", "invisibleTime": "222", "reviveQid": "3"})
        assert (h.pop_time, h.invisible_time, h.revive_qid) == (111, 222, 3)


# ---------------------------------------------------------------- 3. pop_message

class TestPopMessageRequest:
    def test_born_time_filled_and_fields_passed(self, monkeypatch):
        fake = _FakeClient([_resp(ResponseCode.SUCCESS, _pop_ext(0), body=b"x")])
        monkeypatch.setattr("rocketmq.client.mq_client.decode_messages",
                            lambda body: [_msg(queue_offset=0)])
        res = _client(fake).pop_message(GROUP, TOPIC, queue_id=-1, init_mode=0,
                                        invisible_time=60000, poll_time=0,
                                        addr=ADDR, broker_name=BROKER)
        assert isinstance(res, PopResult) and res.status == PopStatus.FOUND
        _, req, _ = fake.requests[0]
        ext = req.ext_fields
        # bornTime 填 0 会让 broker 直接回 POLLING_TIMEOUT(210)，必须非 0
        assert int(ext["bornTime"]) > 0
        assert ext["pollTime"] == "0"
        assert ext["initMode"] == "0"
        assert ext["queueId"] == "-1"
        assert ext["invisibleTime"] == "60000"

    def test_passed_to_broker_addr(self):
        fake = _FakeClient([_resp(ResponseCode.POLLING_TIMEOUT, {})])
        _client(fake).pop_message(GROUP, TOPIC, addr=ADDR, broker_name=BROKER)
        assert fake.requests[0][0] == ADDR

    @pytest.mark.parametrize("code,status", [
        (ResponseCode.SUCCESS, PopStatus.FOUND),
        (ResponseCode.POLLING_FULL, PopStatus.POLLING_FULL),
        (ResponseCode.POLLING_TIMEOUT, PopStatus.POLLING_NOT_FOUND),
        (ResponseCode.PULL_NOT_FOUND, PopStatus.POLLING_NOT_FOUND),
    ])
    def test_status_mapping(self, code, status):
        fake = _FakeClient([_resp(code, {})])
        res = _client(fake).pop_message(GROUP, TOPIC, addr=ADDR, broker_name=BROKER)
        assert res.status == status
        assert res.msg_found_list == []

    def test_unknown_code_raises(self):
        fake = _FakeClient([_resp(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST,
                                  remark="subscription group not exist")])
        with pytest.raises(MQBrokerException) as e:
            _client(fake).pop_message(GROUP, TOPIC, addr=ADDR, broker_name=BROKER)
        assert "subscription group not exist" in str(e.value)

    def test_response_header_surfaces(self):
        fake = _FakeClient([_resp(ResponseCode.POLLING_TIMEOUT, _pop_ext(0))])
        res = _client(fake).pop_message(GROUP, TOPIC, addr=ADDR, broker_name=BROKER)
        assert res.pop_time == 1789613086027
        assert res.invisible_time == 60000
        assert res.start_offset_info == "0 0 0;0 3 0;0 2 0"


class TestStampPopCk:
    """POP_CK 反构 —— 普通 topic 直连 POP 时 broker 不写这个属性，必须客户端拼。"""

    def _header(self, start_info="0 0 0;0 3 0;0 2 0", msg_info="0 0 5;0 3 6;0 2 7"):
        h = PopMessageResponseHeader()
        h.pop_time, h.invisible_time, h.revive_qid = 1789613086027, 60000, 0
        h.start_offset_info = start_info
        h.msg_offset_info = msg_info
        return h

    def test_stamps_8_segment_ck(self):
        msgs = [_msg(queue_id=0, queue_offset=0),
                _msg(queue_id=3, queue_offset=0),
                _msg(queue_id=2, queue_offset=0)]
        _client(_FakeClient([]))._stamp_pop_ck(msgs, BROKER, self._header())
        for m in msgs:
            ck = m.properties[MessageConst.PROPERTY_POP_CK]
            seg = ei.split(ck)
            assert len(seg) == 8, "CK 必须是 8 段：%r" % ck
            assert ei.get_broker_name(seg) == BROKER
            assert ei.get_queue_id(seg) == m.queue_id
            # 第 7 段（msgQueueOffset）来自 msgOffsetInfo，不是消息自己的 queueOffset
            assert ei.get_queue_offset(seg) == {0: 5, 3: 6, 2: 7}[m.queue_id]

    def test_start_offset_from_start_offset_info(self):
        msgs = [_msg(queue_id=3, queue_offset=0)]
        _client(_FakeClient([]))._stamp_pop_ck(
            msgs, BROKER, self._header(start_info="0 3 99", msg_info="0 3 0"))
        seg = ei.split(msgs[0].properties[MessageConst.PROPERTY_POP_CK])
        assert ei.get_ck_queue_offset(seg) == 99

    def test_does_not_override_existing_pop_ck(self):
        # retry topic 弹回来的消息 broker 已经写好 POP_CK，绝不能覆盖
        existing = "1 1 2 0 1 broker-a 0 0"
        msgs = [_msg(queue_id=3, queue_offset=0,
                     props={MessageConst.PROPERTY_POP_CK: existing})]
        _client(_FakeClient([]))._stamp_pop_ck(msgs, BROKER, self._header())
        assert msgs[0].properties[MessageConst.PROPERTY_POP_CK] == existing

    def test_index_selects_right_offset_within_queue(self):
        # 同一队列多条：msgOffsetInfo 的列表按 queueOffset 排序后的下标取
        msgs = [_msg(queue_id=3, queue_offset=10),
                _msg(queue_id=3, queue_offset=11),
                _msg(queue_id=3, queue_offset=12)]
        _client(_FakeClient([]))._stamp_pop_ck(
            msgs, BROKER, self._header(start_info="0 3 0", msg_info="0 3 10,11,12"))
        got = [ei.get_queue_offset(ei.split(m.properties[MessageConst.PROPERTY_POP_CK]))
               for m in msgs]
        assert got == [10, 11, 12]

    def test_fallback_when_start_offset_info_missing(self):
        # Java 的 startOffsetInfo == null 分支：用自己的 queueOffset 当 ckOffset，
        # 再手工补一段凑成 8 段
        h = self._header()
        h.start_offset_info = None
        msgs = [_msg(queue_id=3, queue_offset=42)]
        _client(_FakeClient([]))._stamp_pop_ck(msgs, BROKER, h)
        seg = ei.split(msgs[0].properties[MessageConst.PROPERTY_POP_CK])
        assert len(seg) == 8
        assert ei.get_ck_queue_offset(seg) == 42
        assert ei.get_queue_offset(seg) == 42

    def test_first_pop_time_only_when_missing(self):
        msgs = [_msg(queue_id=0, queue_offset=0),
                _msg(queue_id=3, queue_offset=0,
                     props={MessageConst.PROPERTY_FIRST_POP_TIME: "111"})]
        _client(_FakeClient([]))._stamp_pop_ck(msgs, BROKER, self._header())
        assert msgs[0].properties[MessageConst.PROPERTY_FIRST_POP_TIME] == "1789613086027"
        assert msgs[1].properties[MessageConst.PROPERTY_FIRST_POP_TIME] == "111"


class TestPopMessagePostProcess:
    def test_broker_name_and_topic_stamped(self, monkeypatch):
        monkeypatch.setattr("rocketmq.client.mq_client.decode_messages",
                            lambda body: [_msg(queue_offset=0)])
        fake = _FakeClient([_resp(ResponseCode.SUCCESS, _pop_ext(0), body=b"x")])
        res = _client(fake).pop_message(GROUP, TOPIC, queue_id=0,
                                        addr=ADDR, broker_name=BROKER)
        # Java processPopResponse 收尾会统一盖 brokerName / topic
        assert res.msg_found_list[0].broker_name == BROKER
        assert res.msg_found_list[0].topic == TOPIC


# ---------------------------------------------------------------- 4. ack / change invisible

class TestAckMessage:
    def test_ext_keys_and_offset_passthrough(self):
        fake = _FakeClient([_resp(ResponseCode.SUCCESS, {})])
        ck = ei.build_extra_info(0, 1, 60000, 0, TOPIC, BROKER, 3, 7)
        code = _client(fake).ack_message(GROUP, TOPIC, 3, ck, 7,
                                         addr=ADDR, broker_name=BROKER)
        assert code == ResponseCode.SUCCESS
        _, req, _ = fake.requests[0]
        ext = req.ext_fields
        assert ext["extraInfo"] == ck
        # offset 必须是 consumeQueue offset（第 8 段），不是 commitlog offset
        assert ext["offset"] == "7"
        assert ext["queueId"] == "3"
        assert ext["consumerGroup"] == GROUP

    def test_error_code_returned_not_raised(self):
        fake = _FakeClient([_resp(ResponseCode.NO_MESSAGE, remark="no message")])
        code = _client(fake).ack_message(GROUP, TOPIC, 3, "0 1 2 0 0 broker-a 3 7", 7,
                                         addr=ADDR, broker_name=BROKER)
        assert code == ResponseCode.NO_MESSAGE

    def test_broker_name_taken_from_extra_info(self):
        # 不显式给 brokerName 时，从 CK 串第 6 段取（Java 也是这么找地址的）
        fake = _FakeClient([_resp(ResponseCode.SUCCESS, {})])
        ck = ei.build_extra_info(0, 1, 60000, 0, TOPIC, "broker-from-ck", 3, 7)
        _client(fake).ack_message(GROUP, TOPIC, 3, ck, 7, addr=ADDR)
        assert fake.requests[0][1].ext_fields["extraInfo"] == ck


class TestChangeInvisibleTime:
    def test_success_builds_new_8_segment_extra_info(self):
        fake = _FakeClient([_resp(ResponseCode.SUCCESS,
                                  {"popTime": "222", "invisibleTime": "30000", "reviveQid": "5"})])
        old = ei.build_extra_info(0, 1, 60000, 0, TOPIC, BROKER, 3, 7)
        res = _client(fake).change_invisible_time(GROUP, TOPIC, 3, old, 7, 30000,
                                                  addr=ADDR, broker_name=BROKER)
        assert isinstance(res, ChangeInvisibleTimeResult) and res.success
        assert res.pop_time == 222
        assert res.invisible_time == 30000
        assert res.revive_qid == 5
        # 新串要用**响应里的**新值重建，而不是旧串
        seg = ei.split(res.extra_info)
        assert len(seg) == 8
        assert ei.get_pop_time(seg) == 222
        assert ei.get_invisible_time(seg) == 30000
        assert ei.get_revive_qid(seg) == 5
        assert ei.get_queue_offset(seg) == 7

    def test_request_fields(self):
        fake = _FakeClient([_resp(ResponseCode.SUCCESS, {"popTime": "1",
                                                         "invisibleTime": "2",
                                                         "reviveQid": "3"})])
        old = ei.build_extra_info(0, 1, 60000, 0, TOPIC, BROKER, 3, 7)
        _client(fake).change_invisible_time(GROUP, TOPIC, 3, old, 7, 30000,
                                            addr=ADDR, broker_name=BROKER)
        ext = fake.requests[0][1].ext_fields
        assert ext["invisibleTime"] == "30000"
        assert ext["offset"] == "7"
        assert ext["extraInfo"] == old

    def test_failure_has_no_extra_info(self):
        fake = _FakeClient([_resp(ResponseCode.NO_MESSAGE, {"popTime": "0"})])
        res = _client(fake).change_invisible_time(GROUP, TOPIC, 3, "0 1 2 0 0 b 3 7", 7, 30000,
                                                  addr=ADDR, broker_name=BROKER)
        assert not res.success
        assert res.extra_info is None
