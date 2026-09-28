#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""correctTagsOffset 真机验证（Java DefaultMQPushConsumerImpl:713-717，调用点 :394-401）。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_correct_tags_offset_live.py 127.0.0.1:9876

离线单测锁得住「哪些状态要修正 + 闸门何时放行」，锁不住「这条修正真的走到了 broker」：
位点最终由 UPDATE_CONSUMER_OFFSET 落盘，只有真集群能证明 broker 上的已提交位点前移了、
而且是在**一条消息都没投递**的前提下前移的。

两条腿正好覆盖两个空应答状态（订阅表达式永不匹配 ⇒ broker 按组订阅过滤）：
  S1 对照组：同 topic 用 TagA 订阅正常消费 5 条 —— 证明消息确实在队列里，且
     "已提交位点 == 各队列 maxOffset" 这个数值口径本身就是常规消费的落点。
  S2 NO_MATCHED_MSG：换成 TagB 订阅（永不匹配）。broker 侧过滤后应答是
     PULL_RETRY_IMMEDIATELY（MQClientAPIImpl:1095-1097 → NO_MATCHED_MSG）。
     断言：listener 一条都没收到，而每条队列的已提交位点仍等于该队列的 maxOffset。
     没有这条修正时，位点永远停在未提交状态（broker 上查无此记录）。
  S3 NO_NEW_MSG：同一个消费者启动时会自动补上 %RETRY%<group> 订阅，该队列是空的
     → PULL_NOT_FOUND → NO_NEW_MSG。断言：broker 上出现值 == maxOffset(0) 的记录
     （没有修正时这个 key 根本不会进 _consume_offsets，也就没有报文）。
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      MessageListenerConcurrently)
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "CorrectTagsLive_%d" % STAMP
GROUP_CTRL = "GID_CorrectTagsCtrl_%d" % STAMP
GROUP_TEST = "GID_CorrectTagsTest_%d" % STAMP
QUEUES = 4
MSGS = 5

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


def new_client(tag: str) -> MQClientInstance:
    c = MQClientInstance("%s-%d" % (tag, STAMP), [NAMESRV])
    c.start()
    return c


def wait_until(pred, timeout: float, interval: float = 1.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


def queue_snapshot(client: MQClientInstance, topic: str):
    """[(MessageQueue, maxOffset, committedOffset or None)]；路由没就绪时返回 None。"""
    publish = client.get_topic_publish_info(topic)
    if publish is None or not publish.msg_queue_list:
        return None
    out = []
    for mq in publish.msg_queue_list:
        max_off = client.get_max_offset(mq)
        out.append((mq, max_off, None))
    return out


def committed(client: MQClientInstance, group: str, topic: str):
    """[(mq, maxOffset, committed or None)]，committed=None 表示 broker 上查无记录。"""
    publish = client.get_topic_publish_info(topic)
    if publish is None or not publish.msg_queue_list:
        return None
    out = []
    for mq in publish.msg_queue_list:
        try:
            off = client.query_consumer_offset(group, mq, set_zero_if_not_found=False)
        except Exception:  # noqa: BLE001
            off = None
        try:
            max_off = client.get_max_offset(mq)
        except Exception:  # noqa: BLE001
            max_off = -1
        out.append((mq, max_off, off))
    return out


class RecordingListener(MessageListenerConcurrently):
    def __init__(self):
        self.batches = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            self.batches.append([(bytes(m.body), m.get_tags()) for m in msgs])
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def arrivals(self):
        with self._lock:
            return [r for batch in self.batches for r in batch]


def start(group: str, topic: str, listener: RecordingListener,
          expression: str = "*", from_first: bool = False) -> DefaultMQPushConsumer:
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    c.set_consume_thread_nums(1)
    c.consume_message_batch_max_size = 3
    if from_first:
        c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(topic, expression)
    c.start()
    return c


def main() -> int:
    setup = new_client("setup")
    setup.create_topic_in_route(TOPIC, QUEUES, QUEUES)
    print("topic=%s queues=%d" % (TOPIC, QUEUES))

    producer = DefaultMQProducer("CorrectTagsLive_pg_%d" % STAMP)
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    # ---------- S1 对照组：消息确实在，且常规消费的落点就是 maxOffset ----------
    ctrl_listener = RecordingListener()
    ctrl = start(GROUP_CTRL, TOPIC, ctrl_listener, expression="TagA", from_first=True)
    time.sleep(2)
    for i in range(MSGS):
        msg = Message(TOPIC, b"cto-%d" % i)
        msg.set_tags("TagA")
        producer.send(msg)
    print("S1: 已发送 %d 条 TagA，等待对照组 TagA 消费..." % MSGS)
    ok_ctrl = wait_until(lambda: len(ctrl_listener.arrivals()) >= MSGS, 30)
    check("S1-对照组（TagA）收齐 %d 条 —— 消息确实在队列里" % MSGS,
          ok_ctrl and len(ctrl_listener.arrivals()) == MSGS,
          "arrivals=%d" % len(ctrl_listener.arrivals()))

    def ctrl_committed_matches():
        rows = committed(setup, GROUP_CTRL, TOPIC)
        return bool(rows) and all(o is not None and o == m for _, m, o in rows)

    ok = wait_until(ctrl_committed_matches, 25)
    rows = committed(setup, GROUP_CTRL, TOPIC) or []
    check("S1-对照组的已提交位点 == 各队列 maxOffset（数值口径）", ok,
          " ".join("q%d:%s/%d" % (mq.queue_id, o, m) for mq, m, o in rows))
    check("S1-对照组确实把消息推进了队列（maxOffset 总和 > 0）",
          sum(m for _, m, _ in rows) > 0,
          "maxSum=%d" % sum(m for _, m, _ in rows))
    ctrl.shutdown()

    # ---------- S2 NO_MATCHED_MSG：永不匹配的订阅，零投递但位点要走 ----------
    test_listener = RecordingListener()
    test = start(GROUP_TEST, TOPIC, test_listener, expression="TagB")
    print("S2: 消费者（TagB，永不匹配）已启动，等待空应答修正落盘（首跳 10s + 周期 5s）...")

    def test_committed_matches():
        rows = committed(setup, GROUP_TEST, TOPIC)
        return bool(rows) and all(o is not None and o == m for _, m, o in rows)

    ok = wait_until(test_committed_matches, 40)
    rows = committed(setup, GROUP_TEST, TOPIC) or []
    check("S2-零投递（listener 一条都没收到）", len(test_listener.arrivals()) == 0,
          "arrivals=%d" % len(test_listener.arrivals()))
    check("S2-每条队列的已提交位点都 == 该队列 maxOffset（空应答修正生效）", ok,
          " ".join("q%d:%s/%d" % (mq.queue_id, o, m) for mq, m, o in rows))
    check("S2-修正后的位点总和 == 对照组（同一条队列的最大位点）",
          sum(o for _, _, o in rows) == sum(m for _, m, _ in rows),
          "test=%d ctrl=%d" % (sum(o for _, _, o in rows), sum(m for _, m, _ in rows)))

    # ---------- S3 NO_NEW_MSG：%RETRY%<group> 空队列也要留下位点记录 ----------
    retry_topic = "%RETRY%" + GROUP_TEST
    print("S3: 等 %s 的位点记录（空队列 NO_NEW_MSG）..." % retry_topic)

    def retry_committed_present():
        rows_ = committed(setup, GROUP_TEST, retry_topic)
        return bool(rows_) and all(o is not None and o == m for _, m, o in rows_)

    ok = wait_until(retry_committed_present, 40)
    rows_retry = committed(setup, GROUP_TEST, retry_topic) or []
    check("S3-%s 上出现位点记录且等于 maxOffset" % retry_topic, ok,
          " ".join("q%d:%s/%d" % (mq.queue_id, o, m) for mq, m, o in rows_retry))
    check("S3-该位点确实是 0（空队列的 nextBeginOffset）",
          bool(rows_retry) and all(o == 0 for _, _, o in rows_retry),
          "offsets=%s" % [o for _, _, o in rows_retry])

    # 再等一个静默窗口：修正只抬位点、不该投递任何东西
    time.sleep(6)
    check("S3-整轮下来 listener 依旧是 0 条（修正不会凭空投递）",
          len(test_listener.arrivals()) == 0,
          "arrivals=%d" % len(test_listener.arrivals()))

    test.shutdown()
    producer.shutdown()
    setup.shutdown()

    print("\ncorrectTagsOffset live: %d passed, %d failed" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
