#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from client.consumer import DefaultLitePullConsumer
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "LiteCursorLive_%d" % STAMP
GROUP = "GID_LiteCursorLive_%d" % STAMP
QUEUES = 4
NEVER_MATCH = "TagLiteCursorNeverMatch"
BIG_AHEAD = 1000

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


def wait_until(pred, timeout: float, interval: float = 0.2) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


def queues_of(client: MQClientInstance, topic: str) -> list:
    publish = client.get_topic_publish_info(topic)
    if publish is None or not publish.msg_queue_list:
        return []
    return list(publish.msg_queue_list)


def cursor_row(c: DefaultLitePullConsumer, mqs: list) -> str:
    return " ".join("q%d:%s" % (mq.queue_id, c.pull_cursor_of(mq)) for mq in mqs)


def drain(c: DefaultLitePullConsumer, expect: int, timeout: float = 30.0) -> list:
    """反复 poll 直到收齐 expect 条（或超时）。"""
    out: list = []
    deadline = time.time() + timeout
    while len(out) < expect and time.time() < deadline:
        out.extend(c.poll(500))
    return out


def poll_quiet(c: DefaultLitePullConsumer, seconds: float = 1.0) -> list:
    """在给定窗口里盯住缓冲：返回这期间 poll 出来的全部消息（断言「不该有」时用）。"""
    out: list = []
    deadline = time.time() + seconds
    while time.time() < deadline:
        out.extend(c.poll(200))
    return out


def main() -> int:
    setup = MQClientInstance("lite-cursor-setup-%d" % STAMP, [NAMESRV])
    setup.start()
    setup.create_topic_in_route(TOPIC, QUEUES, QUEUES)
    print("topic=%s queues=%d group=%s" % (TOPIC, QUEUES, GROUP))

    producer = DefaultMQProducer("LiteCursorLive_pg_%d" % STAMP)
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    mqs = queues_of(setup, TOPIC)
    check("路由可见：%d 条队列" % QUEUES, len(mqs) == QUEUES, "got=%d" % len(mqs))
    if len(mqs) != QUEUES:
        return 1

    # ---------- S1 对照组：* 订阅 + seek(0)，每条队列一条消息全收 ----------
    for i, mq in enumerate(mqs):
        msg = Message(TOPIC, b"lc-s1-%d" % i)
        msg.set_tags("TagA")
        producer.send(msg, mq=mq)

    c = DefaultLitePullConsumer(GROUP)
    c.set_namesrv_addr(NAMESRV)
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    # 位点越界那一腿绝不能把越界值提交上去：关掉自动提交，只看拉取/交付两条游标。
    c.set_auto_commit(False)
    c.set_pull_interval_millis(200)
    c.assign(mqs)
    c.start()
    time.sleep(1)

    for mq in mqs:
        c.seek(mq, 0)

    got1 = drain(c, QUEUES)
    check("S1-对照组：* 订阅下 4 条钉到队列的消息全收", len(got1) == QUEUES,
          "got=%d bodies=%s" % (len(got1), sorted(bytes(m.body) for m in got1)[:6]))

    def _s1_max_is_one():
        return all(setup.get_max_offset(mq) == 1 for mq in mqs)

    check("S1-每条队列 maxOffset == 1（后面两条腿的标尺）",
          wait_until(_s1_max_is_one, 10),
          " ".join("q%d:%d" % (mq.queue_id, setup.get_max_offset(mq)) for mq in mqs))

    # ---------- S2 NO_MATCHED_MSG：整段被表达式滤掉，游标要跟过整段 ----------
    c.set_sub_expression_for_assign(TOPIC, NEVER_MATCH)
    for mq in mqs:
        c.seek(mq, 0)

    ok2 = wait_until(lambda: all(c.pull_cursor_of(mq) == 1 for mq in mqs), 20)
    check("S2-NO_MATCHED_MSG 后拉取游标越过整段不匹配区间（== maxOffset=1）", ok2,
          cursor_row(c, mqs))
    check("S2-空应答期间零投递", not poll_quiet(c, 1.5), "不该有消息交付")

    # ---------- S3 OFFSET_ILLEGAL：越界位点被 broker 纠正 + 消息真的回来 ----------
    for mq in mqs:
        c.seek(mq, setup.get_max_offset(mq) + BIG_AHEAD)

    ok3 = wait_until(lambda: all(c.pull_cursor_of(mq) == 1 for mq in mqs), 20)
    check("S3-越界位点被 broker 纠正后游标回到 maxOffset（越界自愈）", ok3,
          cursor_row(c, mqs))

    # S2 换上的永不匹配表达式要换回来，否则下面 4 条 TagA 会被 broker 原样滤掉。
    c.set_sub_expression_for_assign(TOPIC, "*")

    # 起点还要能通过心跳/提交之外的路径被 broker 接受：这里只发消息、不碰位点。
    for i, mq in enumerate(mqs):
        msg = Message(TOPIC, b"lc-s3-%d" % i)
        msg.set_tags("TagA")
        producer.send(msg, mq=mq)

    got3 = drain(c, QUEUES, timeout=40.0)
    check("S3-自愈后新消息全部送达（旧实现：游标卡在 +1000，一条都看不到）",
          len(got3) == QUEUES,
          "got=%d bodies=%s" % (len(got3), sorted(bytes(m.body) for m in got3)[:6]))
    check("S3-收到的正是越界之后钉进去的那 4 条",
          sorted(bytes(m.body) for m in got3)[:QUEUES]
          == sorted(b"lc-s3-%d" % i for i in range(QUEUES)),
          "bodies=%s" % sorted(bytes(m.body) for m in got3)[:6])

    c.shutdown()
    producer.shutdown()
    setup.shutdown()

    print("\nlite pull cursor live: %d passed, %d failed" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
