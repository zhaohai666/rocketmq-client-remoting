# -*- coding: utf-8 -*-
"""消息轨迹真机验证（Python 参考实现）。

前置：NameServer + Broker 已起，且 broker 配置里 **traceTopicEnable=true**
（否则 RMQ_SYS_TRACE_TOPIC 不会被预建，也没法用 admin 建 —— 它是系统 topic，
`validateSystemTopicWhenUpdateTopic` 默认 true 会拒绝创建）。

用法：.venv/bin/python verify_trace_live.py [127.0.0.1:9876]

场景 S1–S17 见下方 main()，全部命中才返回 0。
"""
from __future__ import annotations

import sys
import threading
import time

from rocketmq.client.consumer import DefaultMQPushConsumer
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus, MessageListenerConcurrently
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.trace import TraceConstants, TraceDataEncoder, TraceType
from rocketmq.common.message import Message
from rocketmq.common.message_client_id_setter import get_uniq_id
from rocketmq.common.message_const import MessageConst
from rocketmq.common.mix_all import MixAll
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
# topic 与业务消费组都带 STAMP：`run_trace_live.sh all` 会让三语言**依次**跑在同一个 broker 上，
# 固定名会让第二个语言继承上一轮的位点与残留消息，使断言结果依赖运行顺序。全新 topic + 全新组
# 才能让每次运行的起点一致（真正的鲁棒性另有保障，见 S6 的 body 过滤）。
TOPIC = "TraceTopicLive_%d" % STAMP
NOTRACE_TOPIC = "TraceNoTraceTopic_%d" % STAMP
PRODUCER_GROUP = "GID_trace_producer_live"
CONSUMER_GROUP = "GID_trace_live_%d" % STAMP
TRACE_READER_GROUP = "GID_trace_reader_live_%d" % STAMP

PASS = 0
FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print("  [PASS] %s%s" % (name, ("  " + detail) if detail else ""))
    else:
        FAIL += 1
        print("  [FAIL] %s%s" % (name, ("  " + detail) if detail else ""))


class _TraceCollector(MessageListenerConcurrently):
    """消费 RMQ_SYS_TRACE_TOPIC，把轨迹文本解成 TraceContext 累积起来。"""

    def __init__(self) -> None:
        self.records = []
        self.raw = []           # (轨迹消息 keys, 轨迹消息自身的 topic, body)
        self.lock = threading.Lock()

    def consume_message(self, msgs, context=None):
        with self.lock:
            for msg in msgs:
                body = msg.get_body() or b""
                text = body.decode("utf-8", errors="replace")
                self.raw.append((msg.get_keys() or "", msg.topic, text))
                try:
                    self.records.extend(TraceDataEncoder.decoder_from_trace_data_string(text))
                except Exception as e:  # noqa: BLE001
                    print("  [warn] decode trace failed: %s" % e)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def snapshot(self):
        with self.lock:
            return list(self.records), list(self.raw)


class _MsgCollector(MessageListenerConcurrently):
    def __init__(self) -> None:
        self.msgs = []
        self.lock = threading.Lock()

    def consume_message(self, msgs, context=None):
        with self.lock:
            self.msgs.extend(msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def snapshot(self):
        with self.lock:
            return list(self.msgs)


def _wait_until(pred, timeout_sec: float, interval: float = 0.5):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def main() -> int:
    print("=== RocketMQ message trace live verify (python) ===")
    print("namesrv=%s topic=%s" % (NAMESRV, TOPIC))

    collector = _TraceCollector()
    trace_reader = DefaultMQPushConsumer(TRACE_READER_GROUP)
    trace_reader.set_namesrv_addr(NAMESRV)
    trace_reader.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    trace_reader.subscribe(MixAll.TRACE_TOPIC, "*")
    trace_reader.set_message_listener(collector)
    trace_reader.start()
    print("trace reader started (group=%s, topic=%s)" % (TRACE_READER_GROUP, MixAll.TRACE_TOPIC))

    # ---------- S1 预热建 topic ----------
    warm = DefaultMQProducer(PRODUCER_GROUP + "_warm")
    warm.set_namesrv_addr(NAMESRV)
    warm.start()
    try:
        warm.send(Message(TOPIC, b"warm-up"), 5000)
        check("S1 预热消息发送成功（触发 broker 自动建 topic）", True)
    except Exception as e:  # noqa: BLE001
        check("S1 预热消息发送成功（触发 broker 自动建 topic）", False, str(e))
        return 1

    # 预热后等一下再起消费者：降低"位点解析"与"broker consumequeue 异步分发"的竞态。
    # 这一步只是让时序更宽裕，**不是**正确性保障 —— 预热消息本来就可能被投递，见 S6 的 body 过滤。
    time.sleep(1.5)

    # ---------- S2/S3 打开轨迹的生产者 ----------
    msg_collector = _MsgCollector()
    consumer = DefaultMQPushConsumer(CONSUMER_GROUP)
    consumer.set_namesrv_addr(NAMESRV)
    # ⚠ 必须 CONSUME_FROM_LAST_OFFSET：S1 的预热消息**没有 keys**，用 FIRST_OFFSET 会被
    #   业务消费者一起吃掉 —— 除让计数断言变脏，还会产生一条 keys 为空的 SubBefore
    #   （Java 解码器正是在这一条上 AIOOBE）。从队尾起算，只消费本次要验证的消息。
    consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
    consumer.subscribe(TOPIC, "*")
    consumer.set_message_listener(msg_collector)
    consumer.set_enable_msg_trace(True)          # ← 消费侧轨迹
    consumer.start()
    # 必须先起消费者再发消息（CONSUME_FROM_LAST_OFFSET 语义 + 队列必须先分配）
    assigned = _wait_until(lambda: consumer.assigned_queue_count() > 0, 30)
    check("S2 消费者已分配到队列（先起消费者再发消息）", assigned,
          "assigned=%d" % consumer.assigned_queue_count())

    producer = DefaultMQProducer(PRODUCER_GROUP)
    producer.set_namesrv_addr(NAMESRV)
    producer.set_enable_trace(True)              # ← 发送侧轨迹
    producer.start()

    body = ("trace-live-%d" % STAMP).encode()
    keys = "KeyA KeyB %d" % STAMP
    result = producer.send(Message(TOPIC, body, "TagA", keys), 5000)

    check("S3 SendResult.trace_on == True（broker 默认 traceOn=true）",
          result.trace_on is True, "trace_on=%s" % result.trace_on)
    check("S4 SendResult.msg_id 是客户端 UNIQ_KEY，且与 offset_msg_id 不同",
          bool(result.msg_id) and bool(result.offset_msg_id)
          and result.msg_id != result.offset_msg_id,
          "msgId=%s offsetMsgId=%s" % (result.msg_id, result.offset_msg_id))
    check("S5 SendResult.region_id 已解析（缺省 DefaultRegion）",
          result.region_id == MixAll.DEFAULT_TRACE_REGION_ID, "region=%s" % result.region_id)

    # ---------- S6/S7 消费到消息 ----------
    # ⚠ 只统计**本次发送的**消息：topic 上必然还留着 S1 的预热消息，且"组首次消费"的起始位点
    #   取的是当时的 maxOffset —— broker 的 consumequeue 是**异步分发**的，位点可能落在预热消息
    #   之前，于是预热消息也会被投递过来（实测 C++/.NET 都在这里翻车）。断言的本意是"本次业务
    #   消息被消费到"，所以按 body 过滤，而不是赌 topic 上只有一条消息。
    def _is_primary(m) -> bool:
        return bytes(m.body).startswith(b"trace-live-")

    got = _wait_until(lambda: any(_is_primary(m) for m in msg_collector.snapshot()), 25)
    msgs = [m for m in msg_collector.snapshot() if _is_primary(m)]
    check("S6 业务消费者收到本次发送的那 1 条消息（按 body 过滤预热消息）",
          got and len(msgs) == 1, "count=%d" % len(msgs))
    recv = msgs[0] if msgs else None
    recv_msg_id = recv.msg_id if recv else None
    # Java 语义（MessageDecoder:557-561）：消费侧 MessageExt.getMsgId() **就是** offset 基 ID
    # —— 它先 setMsgId(msgId) 再 setOffsetMsgId(msgId)，两者同值；UNIQ_KEY 只在属性里。
    # 因此能与消费侧 msg_id 对齐的是 SendResult.offset_msg_id，不是 SendResult.msg_id。
    check("S7 消费侧 msg_id == SendResult.offset_msg_id，且 UNIQ_KEY 经属性带到消费侧",
          recv is not None and recv_msg_id == result.offset_msg_id
          and get_uniq_id(recv) == result.msg_id,
          "recv=%s offset=%s uniq=%s" % (recv_msg_id, result.offset_msg_id,
                                         get_uniq_id(recv) if recv else None))

    # ---------- 无 keys 的消息（S17 用，真机数据锁解码器健壮性） ----------
    keyless = producer.send(Message(TOPIC, b"no-keys-here"), 5000)
    keyless_ok = _wait_until(
        lambda: any(bytes(m.body) == b"no-keys-here" for m in msg_collector.snapshot()), 25)
    keyless_id = keyless.offset_msg_id

    # 关掉消费者 → 触发轨迹分发器 flush，保证 SubBefore/SubAfter 落盘
    consumer.shutdown()
    time.sleep(1.0)

    # ---------- S8 等轨迹落地 ----------
    def _found_pub():
        recs, _ = collector.snapshot()
        return any(r.trace_type == TraceType.PUB and r.trace_beans
                   and r.trace_beans[0].msg_id == result.msg_id for r in recs)

    pub_ok = _wait_until(_found_pub, 35)
    producer.shutdown()      # flush 发送侧轨迹
    time.sleep(0.5)
    _wait_until(_found_pub, 10)

    records, raw = collector.snapshot()
    print("  [info] 收到 %d 条轨迹消息 / 解出 %d 条轨迹记录" % (len(raw), len(records)))
    for r in records:
        print("         - %s topic=%s msgId=%s group=%s success=%s code=%s" % (
            r.trace_type.value, r.trace_beans[0].topic if r.trace_beans else "-",
            r.trace_beans[0].msg_id if r.trace_beans else "-", r.group_name,
            r.is_success, r.context_code))

    pubs = [r for r in records if r.trace_type == TraceType.PUB and r.trace_beans
            and r.trace_beans[0].msg_id == result.msg_id]
    check("S8 轨迹里出现 Pub 记录且 msgId 与 SendResult.msg_id 一致", pub_ok and len(pubs) >= 1,
          "count=%d" % len(pubs))

    pub = pubs[0] if pubs else None
    if pub is not None:
        check("S9 Pub 轨迹的 topic / groupName 正确",
              pub.trace_beans[0].topic == TOPIC and pub.group_name == PRODUCER_GROUP,
              "topic=%s group=%s" % (pub.trace_beans[0].topic, pub.group_name))
    else:
        check("S9 Pub 轨迹的 topic / groupName 正确", False, "no pub record")

    check("S10 承载 Pub 轨迹的轨迹消息 keys 里含该 msgId（控制台按 keys 反查）",
          any(result.msg_id in (k or "") for k, _t, _b in raw),
          "raw_keys=%s" % [k for k, _t, _b in raw][:3])

    subs_before = [r for r in records if r.trace_type == TraceType.SUB_BEFORE and r.trace_beans
                   and r.trace_beans[0].msg_id == recv_msg_id]
    check("S11 轨迹里出现 SubBefore 且 msgId 与消费侧一致", len(subs_before) >= 1,
          "count=%d" % len(subs_before))

    subs_after = [r for r in records if r.trace_type == TraceType.SUB_AFTER and r.trace_beans
                  and r.trace_beans[0].msg_id == recv_msg_id]
    if subs_before and subs_after:
        before, after = subs_before[0], subs_after[0]
        check("S12 SubBefore/SubAfter 配对且 requestId 一致、success=true、contextCode=0",
              after.request_id == before.request_id and after.is_success is True
              and after.context_code == 0,
              "req=%s success=%s code=%s" % (after.request_id, after.is_success,
                                             after.context_code))
        check("S13 SubBefore 的 retryTimes 与被消费消息一致",
              before.trace_beans[0].retry_times == msgs[0].reconsume_times,
              "trace=%s msg=%s" % (before.trace_beans[0].retry_times, msgs[0].reconsume_times))
    else:
        check("S12 SubBefore/SubAfter 配对且 requestId 一致、success=true、contextCode=0",
              False, "before=%d after=%d" % (len(subs_before), len(subs_after)))
        check("S13 SubBefore 的 retryTimes 与被消费消息一致", False, "no sub traces")

    # ---------- S14 防递归：轨迹自身不再被追踪 ----------
    check("S14 没有任何轨迹记录的 topic 是轨迹 topic 本身（防递归）",
          all(r.trace_beans[0].topic != MixAll.TRACE_TOPIC for r in records if r.trace_beans),
          "topics=%s" % sorted({r.trace_beans[0].topic for r in records if r.trace_beans}))

    # ---------- S15 关闭轨迹就不产轨迹 ----------
    quiet = DefaultMQProducer(PRODUCER_GROUP + "_quiet")
    quiet.set_namesrv_addr(NAMESRV)
    quiet.set_enable_trace(False)                # ← 显式关闭
    quiet.start()
    quiet.send(Message(NOTRACE_TOPIC, b"no-trace"), 5000)
    quiet.shutdown()
    time.sleep(6.5)
    records2, _ = collector.snapshot()
    leaked = [r for r in records2 if r.trace_type == TraceType.PUB and r.trace_beans
              and r.trace_beans[0].topic == NOTRACE_TOPIC]
    check("S15 enable_trace=false 的生产者不产生 Pub 轨迹", len(leaked) == 0,
          "leaked=%d" % len(leaked))

    # ---------- S16 编码结构自检 ----------
    # 每条轨迹记录都以 FIELD_SPLITOR 结尾（Java encoderFromContextBean 的 `+ STX`），
    # 所以「STX 出现次数」就是记录数；解码侧用 Java split 语义丢掉末尾空串后应得到同样条数。
    # 这条断言的价值在于：只要解码器丢过任何一条记录（真机踩过），这里立刻红。
    all_segments = sum(text.count(TraceConstants.FIELD_SPLITOR)
                       for _k, t, text in raw if t == MixAll.TRACE_TOPIC)
    check("S16 轨迹文本记录数（STX 计数）== 解码出的记录数（Java split 语义）",
          all_segments == len(records) and len(records) > 0,
          "segments=%d decoded=%d" % (all_segments, len(records)))

    # ---------- S17 无 keys 消息的轨迹（SubBefore 末段为空）也要解得出来 ----------
    kl_before = [r for r in records if r.trace_type == TraceType.SUB_BEFORE and r.trace_beans
                 and r.trace_beans[0].msg_id == keyless_id]
    check("S17 无 keys 的消息也能解出 SubBefore 轨迹（空 keys 段不崩）",
          keyless_ok and len(kl_before) >= 1 and kl_before[0].trace_beans[0].keys == "",
          "consumed=%s before=%d keys=%r" % (
              keyless_ok, len(kl_before),
              kl_before[0].trace_beans[0].keys if kl_before else None))

    try:
        trace_reader.shutdown()
    except Exception:  # noqa: BLE001
        pass
    warm.shutdown()

    print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
