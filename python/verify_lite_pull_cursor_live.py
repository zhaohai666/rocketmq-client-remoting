#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""lite-pull **拉取游标**真机验证（#105，Java DefaultLitePullConsumerImpl#PullTaskImpl.run:982-998）。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_lite_pull_cursor_live.py 127.0.0.1:9876

离线单测（python/tests/test_lite_pull_consumer.py 的 TestPullCursorFollowsNextBeginOffset、
cpp/tests/test_lite_pull_cursor.cpp、rust/src/client/pull_consumer.rs、csharp 的
LitePullCursorTests.cs）只能证明「脚本回的 nextBeginOffset 被跟了」；只有真 broker 能让
下面两件事同时成立：nextBeginOffset 是**broker 算的**，而且跟过去以后**真的能收到消息**。

S1 对照组：每条队列钉 1 条消息 → assign + seek(0) + `*` → 4 条全收（链路本身要通，
    maxOffset == 1 这个前提也是后面两条腿的标尺）。
S2 NO_MATCHED_MSG：把 assign 表达式换成永不匹配的 Tag 再 seek(0)。broker 按表达式把整段
    滤掉后回的 nextBeginOffset **已经越过整段**（= maxOffset）。断言每条队列的拉取游标都到
    maxOffset（旧实现只在 FOUND 时推游标，这里会永远停在 0，每轮重扫同一段）。零投递。
S3 OFFSET_ILLEGAL 自愈（决定性一条）：对每条队列 seek 到 maxOffset + 1000（位点越界）。
    broker 回纠正值 → 游标必须回到 maxOffset；随后每条队列再钉 1 条，4 条必须**全部收到**。
    旧实现的游标永远卡在越界值上：每轮都收到同一个「越界纠正」，新消息一条也看不到。
    这正是这条修正的真机价值 —— 越界之后消费者会**静默**地永远收不到消息。
"""
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
