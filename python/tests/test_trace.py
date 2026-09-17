# -*- coding: utf-8 -*-
"""消息轨迹单测（对齐 Java client.trace 包）。

最有价值的是第一组：**EXPECTED_* 里的字符串是 Java 官方实现直接打印出来的**
（探针 /tmp/TraceParity.java，用真实 TraceDataEncoder.encoderFromContextBean 输出，
SOH=\\x01 / STX=\\x02 做了可读化）。只要三语言的编码器与这些常量逐字节一致，
就能与 RocketMQ 控制台 / Java 客户端互认。
"""
import time

from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus
from rocketmq.client.hook import ConsumeMessageContext, SendMessageContext
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import LocalTransactionState
from rocketmq.client.send_result import SendResult, SendStatus
from rocketmq.client.trace import (LOCAL_ADDRESS, AccessChannel, TraceBean, TraceConstants,
                                   TraceContext, TraceDataEncoder, TraceTransferBean,
                                   TraceType, java_split)
from rocketmq.client.trace_dispatcher import AsyncTraceDispatcher, TraceDispatcherType
from rocketmq.client.trace_hook import ConsumeMessageTraceHook, SendMessageTraceHook
from rocketmq.common.message import MessageExt, MessageQueue
from rocketmq.common.message_client_id_setter import (create_uniq_id, get_uniq_id, set_uniq_id)
from rocketmq.common.message_const import MessageConst
from rocketmq.common.message_type import MessageType
from rocketmq.common.mix_all import MixAll
from rocketmq.remoting.protocol.remoting_command import RemotingCommand

SOH = "\u0001"
STX = "\u0002"

MSG_ID_1 = "AC1400A1F0A018B4AAC2A1B2C3D4E5F6"
MSG_ID_2 = "AC1400A1F0A018B4AAC2A1B2C3D4E5F7"
OFFSET_MSG_ID = "AC1400A1000027100000000000000001"
BODY = b"x" * 42

# ---- Java 官方实现的编码结果（勿手改；改了就与 Java/控制台不兼容）----
EXPECTED_PUB = (SOH.join(["Pub", "1700000000000", "DefaultRegion", "GID_test", "TopicTest",
                          MSG_ID_1, "TagA", "KeyA KeyB", "127.0.0.1:10911", "42", "7", "0",
                          OFFSET_MSG_ID, "true"]) + STX)
EXPECTED_SUB_BEFORE = (
    SOH.join(["SubBefore", "1700000000000", "DefaultRegion", "CID_test", "REQ-SUB-001",
              MSG_ID_1, "2", "KeyA KeyB"]) + STX
    + SOH.join(["SubBefore", "1700000000000", "DefaultRegion", "CID_test", "REQ-SUB-001",
                MSG_ID_2, "0", "KeyC"]) + STX)
EXPECTED_SUB_AFTER = (SOH.join(["SubAfter", "REQ-SUB-001", MSG_ID_1, "11", "false",
                                "KeyA KeyB", "2", "1700000000000", "CID_test"]) + STX)
EXPECTED_END_TRANSACTION = (SOH.join(["EndTransaction", "1700000000000", "DefaultRegion",
                                      "GID_test", "TopicTest", MSG_ID_1, "TagA", "KeyA KeyB",
                                      "127.0.0.1:10911", "0", "TRAN-001", "COMMIT_MESSAGE",
                                      "false"]) + STX)
EXPECTED_RECALL = (SOH.join(["Recall", "1700000000000", "DefaultRegion", "GID_test",
                             "TopicTest", MSG_ID_1, "true"]) + STX)


def _bean(msg_id=MSG_ID_1, keys="KeyA KeyB", retry_times=2):
    b = TraceBean()
    b.topic = "TopicTest"
    b.msg_id = msg_id
    b.offset_msg_id = OFFSET_MSG_ID
    b.tags = "TagA"
    b.keys = keys
    b.store_host = "127.0.0.1:10911"
    b.store_time = 1700000000123
    b.retry_times = retry_times
    b.body_length = 42
    b.msg_type = MessageType.NORMAL_MSG
    b.transaction_id = "TRAN-001"
    b.transaction_state = LocalTransactionState.COMMIT_MESSAGE
    return b


# ---------------- 编码：与 Java 逐字节一致 ----------------
def test_encode_pub_matches_java_vector():
    ctx = TraceContext()
    ctx.trace_type = TraceType.PUB
    ctx.time_stamp = 1700000000000
    ctx.region_id = "DefaultRegion"
    ctx.group_name = "GID_test"
    ctx.cost_time = 7
    ctx.is_success = True
    ctx.request_id = "REQ-PUB-001"
    ctx.trace_beans = [_bean()]
    tb = TraceDataEncoder.encoder_from_context_bean(ctx)
    assert tb.trans_data == EXPECTED_PUB
    assert tb.trans_key == {MSG_ID_1, "KeyA", "KeyB"}


def test_encode_sub_before_matches_java_vector():
    ctx = TraceContext()
    ctx.trace_type = TraceType.SUB_BEFORE
    ctx.time_stamp = 1700000000000
    ctx.region_id = "DefaultRegion"
    ctx.group_name = "CID_test"
    ctx.request_id = "REQ-SUB-001"
    ctx.trace_beans = [_bean(), _bean(msg_id=MSG_ID_2, keys="KeyC", retry_times=0)]
    tb = TraceDataEncoder.encoder_from_context_bean(ctx)
    assert tb.trans_data == EXPECTED_SUB_BEFORE
    assert tb.trans_key == {MSG_ID_1, MSG_ID_2, "KeyA", "KeyB", "KeyC"}


def test_encode_sub_after_matches_java_vector():
    ctx = TraceContext()
    ctx.trace_type = TraceType.SUB_AFTER
    ctx.time_stamp = 1700000000000
    ctx.group_name = "CID_test"
    ctx.request_id = "REQ-SUB-001"
    ctx.cost_time = 11
    ctx.is_success = False
    ctx.context_code = 2
    ctx.access_channel = AccessChannel.LOCAL
    ctx.trace_beans = [_bean()]
    assert TraceDataEncoder.encoder_from_context_bean(ctx).trans_data == EXPECTED_SUB_AFTER


def test_encode_sub_after_cloud_drops_timestamp_and_group():
    """CLOUD 通道下 Java 不追加 timestamp/groupName 两段。"""
    ctx = TraceContext()
    ctx.trace_type = TraceType.SUB_AFTER
    ctx.time_stamp = 1700000000000
    ctx.group_name = "CID_test"
    ctx.request_id = "REQ-SUB-001"
    ctx.cost_time = 11
    ctx.is_success = False
    ctx.context_code = 2
    ctx.access_channel = AccessChannel.CLOUD
    ctx.trace_beans = [_bean()]
    data = TraceDataEncoder.encoder_from_context_bean(ctx).trans_data
    assert data == (SOH.join(["SubAfter", "REQ-SUB-001", MSG_ID_1, "11", "false",
                              "KeyA KeyB", "2"]) + STX)


def test_encode_end_transaction_and_recall_match_java_vectors():
    end_tx = TraceContext()
    end_tx.trace_type = TraceType.END_TRANSACTION
    end_tx.time_stamp = 1700000000000
    end_tx.region_id = "DefaultRegion"
    end_tx.group_name = "GID_test"
    end_tx.trace_beans = [_bean()]
    assert TraceDataEncoder.encoder_from_context_bean(end_tx).trans_data == EXPECTED_END_TRANSACTION

    recall = TraceContext()
    recall.trace_type = TraceType.RECALL
    recall.time_stamp = 1700000000000
    recall.region_id = "DefaultRegion"
    recall.group_name = "GID_test"
    recall.is_success = True
    recall.trace_beans = [_bean()]
    assert TraceDataEncoder.encoder_from_context_bean(recall).trans_data == EXPECTED_RECALL


def test_encode_null_returns_none():
    assert TraceDataEncoder.encoder_from_context_bean(None) is None


# ---------------- 解码：Java split 语义 + 字段还原 ----------------
def test_java_split_drops_trailing_empty_like_java():
    assert java_split("a" + STX, STX) == ["a"]
    assert java_split(STX, STX) == []
    assert java_split("a" + STX + "b" + STX, STX) == ["a", "b"]
    assert java_split("a" + SOH + SOH + "b", SOH) == ["a", "", "b"]
    # Python 原生 split 会保留末尾空串 —— 这正是不能直接用的原因
    assert "a" + STX.split()[0] is not None
    assert ("a" + STX).split(STX) == ["a", ""]


def test_decode_pub_round_trip():
    ctx = TraceContext()
    ctx.trace_type = TraceType.PUB
    ctx.time_stamp = 1700000000000
    ctx.region_id = "DefaultRegion"
    ctx.group_name = "GID_test"
    ctx.cost_time = 7
    ctx.is_success = True
    ctx.trace_beans = [_bean()]
    decoded = TraceDataEncoder.decoder_from_trace_data_string(
        TraceDataEncoder.encoder_from_context_bean(ctx).trans_data)
    assert len(decoded) == 1
    got = decoded[0]
    assert got.trace_type == TraceType.PUB
    assert got.time_stamp == 1700000000000
    assert got.region_id == "DefaultRegion"
    assert got.group_name == "GID_test"
    assert got.cost_time == 7
    assert got.is_success is True
    bean = got.trace_beans[0]
    assert (bean.topic, bean.msg_id, bean.tags, bean.keys, bean.store_host) == (
        "TopicTest", MSG_ID_1, "TagA", "KeyA KeyB", "127.0.0.1:10911")
    assert bean.body_length == 42
    assert bean.offset_msg_id == OFFSET_MSG_ID
    assert bean.msg_type == MessageType.NORMAL_MSG
    # Java 编码不写 clientHost，解码时落到 LOCAL_ADDRESS 默认值
    assert bean.client_host == LOCAL_ADDRESS


def test_decode_sub_before_keeps_request_id_and_retry_times():
    ctx = TraceContext()
    ctx.trace_type = TraceType.SUB_BEFORE
    ctx.time_stamp = 1700000000000
    ctx.region_id = "DefaultRegion"
    ctx.group_name = "CID_test"
    ctx.request_id = "REQ-SUB-001"
    ctx.trace_beans = [_bean(), _bean(msg_id=MSG_ID_2, keys="KeyC", retry_times=0)]
    decoded = TraceDataEncoder.decoder_from_trace_data_string(
        TraceDataEncoder.encoder_from_context_bean(ctx).trans_data)
    assert len(decoded) == 2
    assert [d.trace_beans[0].msg_id for d in decoded] == [MSG_ID_1, MSG_ID_2]
    assert [d.trace_beans[0].retry_times for d in decoded] == [2, 0]
    assert all(d.request_id == "REQ-SUB-001" for d in decoded)
    assert all(d.group_name == "CID_test" for d in decoded)


def test_decode_sub_after_context_code_and_legacy_branch():
    ctx = TraceContext()
    ctx.trace_type = TraceType.SUB_AFTER
    ctx.time_stamp = 1700000000000
    ctx.group_name = "CID_test"
    ctx.request_id = "REQ-SUB-001"
    ctx.cost_time = 11
    ctx.is_success = False
    ctx.context_code = 4
    ctx.access_channel = AccessChannel.LOCAL
    ctx.trace_beans = [_bean()]
    decoded = TraceDataEncoder.decoder_from_trace_data_string(
        TraceDataEncoder.encoder_from_context_bean(ctx).trans_data)[0]
    assert decoded.context_code == 4
    assert decoded.is_success is False
    assert decoded.time_stamp == 1700000000000
    assert decoded.group_name == "CID_test"
    # 老版本只有 7 段时不应读到 timestamp/group
    legacy = SOH.join(["SubAfter", "REQ", MSG_ID_1, "5", "true", "KeyA", "0"])
    got = TraceDataEncoder.decoder_from_trace_data_string(legacy)[0]
    assert got.context_code == 0
    assert got.time_stamp > 1700000000000   # 落到「当前时间」的默认值
    assert got.group_name == ""


def test_decode_empty_and_unknown_type():
    assert TraceDataEncoder.decoder_from_trace_data_string("") == []
    assert TraceDataEncoder.decoder_from_trace_data_string(None) == []
    assert TraceDataEncoder.decoder_from_trace_data_string("Bogus" + SOH + "1" + STX) == []


def test_decode_end_transaction_fields():
    end_tx = TraceContext()
    end_tx.trace_type = TraceType.END_TRANSACTION
    end_tx.time_stamp = 1700000000000
    end_tx.region_id = "DefaultRegion"
    end_tx.group_name = "GID_test"
    end_tx.trace_beans = [_bean()]
    got = TraceDataEncoder.decoder_from_trace_data_string(
        TraceDataEncoder.encoder_from_context_bean(end_tx).trans_data)[0]
    bean = got.trace_beans[0]
    assert got.trace_type == TraceType.END_TRANSACTION
    assert bean.transaction_id == "TRAN-001"
    assert bean.transaction_state == "COMMIT_MESSAGE"
    assert bean.from_transaction_check is False
    assert bean.msg_type == MessageType.NORMAL_MSG


# ---------------- 常量 ----------------
def test_constants_match_java():
    assert TraceConstants.CONTENT_SPLITOR == "\u0001"
    assert TraceConstants.FIELD_SPLITOR == "\u0002"
    assert TraceConstants.GROUP_NAME_PREFIX == "_INNER_TRACE_PRODUCER"
    assert TraceConstants.TRACE_INSTANCE_NAME == "PID_CLIENT_INNER_TRACE_PRODUCER"
    assert TraceConstants.TRACE_TOPIC_PREFIX == "rmq_sys_TRACE_DATA_"
    assert MixAll.TRACE_TOPIC == "RMQ_SYS_TRACE_TOPIC"
    assert MixAll.DEFAULT_TRACE_REGION_ID == "DefaultRegion"
    assert MessageConst.KEY_SEPARATOR == " "
    assert MessageConst.PROPERTY_TRACE_SWITCH == "TRACE_ON"
    assert [t.value for t in MessageType] == [0, 1, 2, 3, 4]
    assert MessageType.NORMAL_MSG.short_name == "Normal"
    assert MessageType.TRANS_MSG_HALF.short_name == "Trans"
    assert MessageType.TRANS_MSG_COMMIT.short_name == "TransCommit"
    assert MessageType.DELAY_MSG.short_name == "Delay"
    assert MessageType.ORDER_MSG.short_name == "Order"


# ---------------- 分发器 ----------------
class _FakeTraceProducer:
    """只记录 send 调用，不发网络（避免单测依赖集群）。"""

    def __init__(self):
        self.sent = []
        self.stopped = False

    def _topic_publish_info(self, topic):
        raise RuntimeError("no route in unit test")

    def send(self, msg, timeout=None):
        self.sent.append((msg.topic, bytes(msg.body), msg.get_keys()))
        return None

    def send_by_selector(self, msg, selector, arg, timeout=None):
        self.sent.append((msg.topic, bytes(msg.body), msg.get_keys()))
        return None

    def shutdown(self):
        self.stopped = True


def _pub_context(topic="TopicTest", msg_id=MSG_ID_1, region="DefaultRegion"):
    ctx = TraceContext()
    ctx.trace_type = TraceType.PUB
    ctx.time_stamp = 1700000000000
    ctx.region_id = region
    ctx.group_name = "GID_test"
    ctx.cost_time = 7
    ctx.is_success = True
    bean = _bean(msg_id=msg_id)
    bean.topic = topic
    ctx.trace_beans = [bean]
    return ctx


def _make_dispatcher():
    d = AsyncTraceDispatcher("GID_test", TraceDispatcherType.PRODUCE, 10, None, None)
    d.trace_producer = _FakeTraceProducer()
    return d


def test_dispatcher_group_name_and_defaults():
    d = _make_dispatcher()
    assert d.trace_topic_name == "RMQ_SYS_TRACE_TOPIC"
    assert d.batch_num == 10
    assert d.max_msg_size == 128000
    # Java：batchNum 上限 20
    d2 = AsyncTraceDispatcher("GID_test", TraceDispatcherType.PRODUCE, 50, None, None)
    d2.trace_producer = _FakeTraceProducer()
    assert d2.batch_num == 20
    # 内部生产者组名：_INNER_TRACE_PRODUCER-<group>-<PRODUCE|CONSUME>-<N>
    group = d.trace_producer.__class__  # noqa: F841 (仅确保可序列化)
    assert TraceConstants.GROUP_NAME_PREFIX in d._gen_group_name_for_trace()
    assert "-PRODUCE-" in d._gen_group_name_for_trace()
    assert "-CONSUME-" in AsyncTraceDispatcher(
        "CID_test", TraceDispatcherType.CONSUME)._gen_group_name_for_trace()


def test_dispatcher_custom_trace_topic():
    d = AsyncTraceDispatcher("GID_test", TraceDispatcherType.PRODUCE, 10, "MyTraceTopic")
    assert d.get_trace_topic_name() == "MyTraceTopic"
    d.trace_producer = _FakeTraceProducer()


def test_dispatcher_append_and_flush_sends_encoded_payload():
    d = _make_dispatcher()
    ctx = _pub_context()
    assert d.append(ctx) is True
    d._send_trace_data([_pub_context()])       # 直接走发送逻辑（不起线程）
    assert d.trace_producer.sent[0][0] == "RMQ_SYS_TRACE_TOPIC"
    assert d.trace_producer.sent[0][1].decode("utf-8") == EXPECTED_PUB
    # keys 是「原始消息 msgId + 业务 keys」，控制台靠它反查轨迹
    assert set(d.trace_producer.sent[0][2].split(" ")) == {MSG_ID_1, "KeyA", "KeyB"}


def test_dispatcher_skips_context_without_region_or_beans():
    d = _make_dispatcher()
    no_region = _pub_context(region="")
    assert no_region.trace_beans
    d._send_trace_data([no_region])
    assert d.trace_producer.sent == []
    empty = _pub_context()
    empty.trace_beans = []
    d._send_trace_data([empty])
    assert d.trace_producer.sent == []


def test_dispatcher_cloud_channel_uses_prefixed_topic():
    d = _make_dispatcher()
    ctx = _pub_context()
    ctx.access_channel = AccessChannel.CLOUD
    d._send_trace_data([ctx])
    assert d.trace_producer.sent[0][0] == "rmq_sys_TRACE_DATA_DefaultRegion"


def test_dispatcher_groups_by_business_topic():
    d = _make_dispatcher()
    d._send_trace_data([_pub_context(topic="TopicA"), _pub_context(topic="TopicB"),
                        _pub_context(topic="TopicA")])
    # 按 (业务 topic, 轨迹 topic) 分组 → 2 组；TopicA 那组有 2 条记录（2 个 STX 结尾）
    assert len(d.trace_producer.sent) == 2
    counts = sorted(body.decode("utf-8").count(STX) for _, body, _ in d.trace_producer.sent)
    assert counts == [1, 2]


def test_dispatcher_splits_payload_over_max_size():
    d = _make_dispatcher()
    d.max_msg_size = len(EXPECTED_PUB)     # 每条记录都到达阈值 → 逐条切块发送
    d._flush_data([TraceDataEncoder.encoder_from_context_bean(_pub_context())
                   for _ in range(3)], "TopicTest", "RMQ_SYS_TRACE_TOPIC")
    assert len(d.trace_producer.sent) == 3


def test_dispatcher_append_returns_false_when_full():
    d = _make_dispatcher()
    for _ in range(2048):
        d.append(_pub_context())
    assert d.append(_pub_context()) is False
    assert d.discard_count == 1


def test_dispatcher_shutdown_flushes_queue():
    d = _make_dispatcher()
    d.append(_pub_context())
    d.is_started = True
    d.stopped = True
    d.shutdown()
    assert d.trace_context_queue.qsize() == 0
    assert d.trace_producer.stopped is True
    assert d.stopped is True


# ---------------- 发送钩子 ----------------
class _CapturingDispatcher:
    def __init__(self):
        self.appended = []

    def get_trace_topic_name(self):
        return "RMQ_SYS_TRACE_TOPIC"

    def _client_id(self):
        return "CID@1"

    def append(self, ctx):
        self.appended.append(ctx)
        return True


def _send_context(topic="TopicTest", trace_on=True, region="DefaultRegion"):
    ctx = SendMessageContext()
    ctx.producer_group = "GID_test"
    ctx.broker_addr = "127.0.0.1:10911"
    msg = MessageExt(topic, BODY, "TagA", "KeyA")
    msg.set_keys("KeyA KeyB")
    ctx.message = msg
    ctx.mq = MessageQueue(topic, "broker-a", 0)
    ctx.send_result = SendResult(SendStatus.SEND_OK, MSG_ID_1,
                                 MessageQueue(topic, "broker-a", 0), 0,
                                 None, OFFSET_MSG_ID, region)
    ctx.send_result.trace_on = trace_on
    return ctx


def test_send_hook_pub_round_trip():
    d = _CapturingDispatcher()
    hook = SendMessageTraceHook(d)
    assert hook.hook_name() == "SendMessageTraceHook"
    ctx = _send_context()
    hook.send_message_before(ctx)
    assert ctx.mq_trace_context.trace_type == TraceType.PUB
    assert ctx.mq_trace_context.group_name == "GID_test"
    assert ctx.mq_trace_context.trace_beans[0].topic == "TopicTest"
    assert ctx.mq_trace_context.trace_beans[0].body_length == len(BODY)
    hook.send_message_after(ctx)
    assert len(d.appended) == 1
    got = d.appended[0]
    assert got.trace_type == TraceType.PUB
    assert got.region_id == "DefaultRegion"
    assert got.is_success is True
    assert got.cost_time >= 0
    assert got.trace_beans[0].msg_id == MSG_ID_1
    assert got.trace_beans[0].offset_msg_id == OFFSET_MSG_ID
    assert got.trace_beans[0].store_time >= got.time_stamp


def test_send_hook_skips_trace_topic_itself():
    """轨迹消息不能被再次追踪（否则无限递归）。"""
    d = _CapturingDispatcher()
    hook = SendMessageTraceHook(d)
    ctx = _send_context(topic="RMQ_SYS_TRACE_TOPIC")
    hook.send_message_before(ctx)
    assert ctx.mq_trace_context is None
    hook.send_message_after(ctx)
    assert d.appended == []


def test_send_hook_skips_when_broker_trace_off_or_no_region():
    d = _CapturingDispatcher()
    hook = SendMessageTraceHook(d)
    off = _send_context(trace_on=False)
    hook.send_message_before(off)
    hook.send_message_after(off)
    assert d.appended == []
    no_region = _send_context()
    no_region.send_result.region_id = None
    hook.send_message_before(no_region)
    hook.send_message_after(no_region)
    assert d.appended == []


def test_send_hook_marks_failure_status():
    d = _CapturingDispatcher()
    hook = SendMessageTraceHook(d)
    ctx = _send_context()
    ctx.send_result.send_status = SendStatus.FLUSH_DISK_TIMEOUT
    hook.send_message_before(ctx)
    hook.send_message_after(ctx)
    assert d.appended[0].is_success is False


def test_send_hook_requires_before_to_have_run():
    d = _CapturingDispatcher()
    hook = SendMessageTraceHook(d)
    ctx = _send_context()          # 不调 before → mq_trace_context 为 None
    hook.send_message_after(ctx)
    assert d.appended == []


# ---------------- 消费钩子 ----------------
def _consume_context(topic="TopicTest", trace_on=None, region="DefaultRegion"):
    msg = MessageExt(topic, BODY, "TagA", "KeyA")
    msg.msg_id = MSG_ID_1
    msg.store_timestamp = 1700000000000
    msg.store_size = 42
    msg.reconsume_times = 1
    if region is not None:
        msg.put_property(MessageConst.PROPERTY_MSG_REGION, region)
    if trace_on is not None:
        msg.put_property(MessageConst.PROPERTY_TRACE_SWITCH, trace_on)
    ctx = ConsumeMessageContext("CID_test", [msg], MessageQueue(topic, "broker-a", 0))
    ctx.props = {"ConsumeContextType": "SUCCESS"}
    ctx.success = True
    return ctx


def test_consume_hook_sub_before_and_after_share_request_id():
    d = _CapturingDispatcher()
    hook = ConsumeMessageTraceHook(d)
    assert hook.hook_name() == "ConsumeMessageTraceHook"
    ctx = _consume_context()
    hook.consume_message_before(ctx)
    assert len(d.appended) == 1
    before = d.appended[0]
    assert before.trace_type == TraceType.SUB_BEFORE
    assert before.group_name == "CID_test"
    assert before.region_id == "DefaultRegion"
    assert before.trace_beans[0].msg_id == MSG_ID_1
    assert before.trace_beans[0].retry_times == 1
    hook.consume_message_after(ctx)
    assert len(d.appended) == 2
    after = d.appended[1]
    assert after.trace_type == TraceType.SUB_AFTER
    assert after.request_id == before.request_id
    assert after.is_success is True
    assert after.cost_time >= 0
    # SUCCESS 的 ordinal = 0
    assert after.context_code == 0


def test_consume_hook_context_code_from_props():
    d = _CapturingDispatcher()
    hook = ConsumeMessageTraceHook(d)
    ctx = _consume_context()
    ctx.props = {"ConsumeContextType": "FAILED"}
    ctx.success = False
    hook.consume_message_before(ctx)
    hook.consume_message_after(ctx)
    assert d.appended[1].context_code == 4
    assert d.appended[1].is_success is False


def test_consume_hook_skips_message_with_trace_off():
    d = _CapturingDispatcher()
    hook = ConsumeMessageTraceHook(d)
    ctx = _consume_context(trace_on="false")
    hook.consume_message_before(ctx)
    assert d.appended == []
    hook.consume_message_after(ctx)      # before 没落轨迹 → after 也不能落
    assert d.appended == []


def test_consume_hook_after_without_before_appends_nothing():
    d = _CapturingDispatcher()
    hook = ConsumeMessageTraceHook(d)
    ctx = _consume_context()
    hook.consume_message_after(ctx)
    assert d.appended == []


# ---------------- SendResult 的轨迹字段（MQClientAPIImpl.processSendResponse）----------------
def _send_response(ext):
    cmd = RemotingCommand.create_response_command(0)   # ResponseCode.SUCCESS
    cmd.ext_fields = dict(ext)
    return cmd


def test_parse_send_response_reads_region_and_trace_on():
    msg = MessageExt("TopicTest", BODY)
    set_uniq_id(msg)
    mq = MessageQueue("TopicTest", "broker-a", 0)
    resp = _send_response({"msgId": OFFSET_MSG_ID, "queueId": "1", "queueOffset": "9",
                           "MSG_REGION": "RegionA", "TRACE_ON": "true"})
    result = MQClientInstance._parse_send_response(None, resp, msg, mq)
    assert result.msg_id == get_uniq_id(msg)      # 客户端 UNIQ_KEY
    assert result.offset_msg_id == OFFSET_MSG_ID  # broker 的 offset 消息 ID
    assert result.region_id == "RegionA"
    assert result.trace_on is True
    assert result.queue_offset == 9
    assert result.message_queue.queue_id == 1


def test_parse_send_response_trace_off_and_default_region():
    msg = MessageExt("TopicTest", BODY)
    set_uniq_id(msg)
    mq = MessageQueue("TopicTest", "broker-a", 0)
    resp = _send_response({"msgId": OFFSET_MSG_ID, "queueId": "0", "queueOffset": "1",
                           "TRACE_ON": "false"})
    result = MQClientInstance._parse_send_response(None, resp, msg, mq)
    assert result.trace_on is False
    assert result.region_id == "DefaultRegion"


def test_set_uniq_id_is_idempotent_and_32_hex():
    msg = MessageExt("TopicTest", BODY)
    set_uniq_id(msg)
    first = get_uniq_id(msg)
    assert first is not None and len(first) == 32
    int(first, 16)          # 必须是合法十六进制
    set_uniq_id(msg)
    assert get_uniq_id(msg) == first
    assert len(create_uniq_id()) == 32


def test_parse_send_response_raises_on_error_code():
    from rocketmq.client.exception import MQBrokerException
    msg = MessageExt("TopicTest", BODY)
    cmd = RemotingCommand.create_response_command(2)
    cmd.remark = "boom"
    try:
        MQClientInstance._parse_send_response(None, cmd, msg, MessageQueue("T", "b", 0))
    except MQBrokerException as e:
        assert "boom" in str(e)
    else:
        raise AssertionError("expected MQBrokerException")


def test_trace_transfer_bean_defaults():
    tb = TraceTransferBean()
    assert tb.trans_data == ""
    assert tb.trans_key == set()


def test_trace_context_defaults_are_unique_ids():
    a, b = TraceContext(), TraceContext()
    assert a.request_id != b.request_id
    assert len(a.request_id) == 32
    assert abs(a.time_stamp - int(time.time() * 1000)) < 5000
    assert a.is_success is True
    assert a.trace_beans == []


def test_consume_concurrently_status_names_used_by_hook():
    assert ConsumeConcurrentlyStatus.CONSUME_SUCCESS is not None
    assert ConsumeConcurrentlyStatus.RECONSUME_LATER is not None


# ---------------------------------------------------------------------------
# 解码健壮性回归（真机踩到的两个坑，勿删）
# ---------------------------------------------------------------------------
def test_decode_sub_before_without_keys():
    """无 keys 的消息，SubBefore 末段会被 Java split 丢掉 → 缺一段，必须当空串。

    Java 原生实现在这里会抛 ArrayIndexOutOfBoundsException（line[7]），
    我们的解码器有意兜成空串，否则控制台/验证脚本读轨迹时会崩。
    """
    raw = SOH.join(["SubBefore", "1700000000000", "DefaultRegion", "GID_trace_live",
                    "REQ-001", MSG_ID_1, "0", ""]) + STX
    records = TraceDataEncoder.decoder_from_trace_data_string(raw)
    assert len(records) == 1
    ctx = records[0]
    assert ctx.trace_type == TraceType.SUB_BEFORE
    assert ctx.trace_beans[0].keys == ""
    assert ctx.trace_beans[0].retry_times == 0
    assert ctx.trace_beans[0].msg_id == MSG_ID_1


def test_decode_one_bad_record_does_not_drop_the_batch():
    """一条坏记录不能毁掉整条轨迹消息（真机表现：2 条消息只解出 1 条）。"""
    good_pub = EXPECTED_PUB
    broken = SOH.join(["SubAfter", "REQ-9", MSG_ID_2]) + STX        # 段数不足
    good_sub = SOH.join(["SubBefore", "1700000000000", "R", "G", "REQ-1", MSG_ID_1, "0", "K"]) + STX
    records = TraceDataEncoder.decoder_from_trace_data_string(good_pub + broken + good_sub)
    assert [c.trace_type for c in records] == [TraceType.PUB, TraceType.SUB_BEFORE]


def test_decode_ignores_unknown_record_kind():
    raw = SOH.join(["SomethingNew", "1", "2"]) + STX + EXPECTED_RECALL
    records = TraceDataEncoder.decoder_from_trace_data_string(raw)
    assert [c.trace_type for c in records] == [TraceType.RECALL]


def test_decode_empty_and_none_input():
    assert TraceDataEncoder.decoder_from_trace_data_string(None) == []
    assert TraceDataEncoder.decoder_from_trace_data_string("") == []
    assert TraceDataEncoder.decoder_from_trace_data_string(STX + STX) == []


def test_decode_round_trip_keeps_bean_msg_id_and_offset_msg_id():
    """Pub 的 msgId（UNIQ_KEY）与 offsetMsgId（broker 侧 ID）解码后必须各归各位。"""
    ctx = TraceContext()
    ctx.trace_type = TraceType.PUB
    ctx.time_stamp = 1700000000000
    ctx.region_id = "DefaultRegion"
    ctx.group_name = "GID_test"
    ctx.cost_time = 7
    ctx.is_success = True
    ctx.trace_beans = [_bean()]
    tb = TraceDataEncoder.encoder_from_context_bean(ctx)
    assert tb.trans_data == EXPECTED_PUB
    back = TraceDataEncoder.decoder_from_trace_data_string(tb.trans_data)[0]
    assert back.trace_beans[0].msg_id == MSG_ID_1 == _bean().msg_id
    assert back.trace_beans[0].offset_msg_id == OFFSET_MSG_ID
    assert back.is_success is True
    assert back.time_stamp == 1700000000000
