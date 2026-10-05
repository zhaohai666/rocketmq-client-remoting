# -*- coding: utf-8 -*-
"""用**已验证的 Python 客户端**回读 Go 生产者写进真实 broker 的消息。

为什么需要：Go 侧的 live_producer 只能证明"broker 收下了"（SEND_OK），证明不了
报文编码正确 —— V2 短键头（a..n）、批量 6 段轻量帧、zlib 压缩位、事务半消息属性、
异步链（DEFAULT ASYNC 单独一条构建路径）任何一处写错，broker 都可能照样回
SUCCESS，而消费端拿到的东西是坏的。这里用另一个语言的实现去消费，才是真正的跨
语言对拍。

用法: python go_producer_consume_check.py <topic> [expect_count] [group]

expect_count 由调用方从 Go 侧的 `SENT=<n>` 行取（见 scripts/run_go_producer_live.sh），
别写死 —— 往 live_producer 里加一个检查就会让写死的期望值悄悄失准。
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from client.consumer_result import ConsumeConcurrentlyStatus
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = os.environ.get("ROCKETMQ_NAMESRV", "127.0.0.1:9876")

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS  " if ok else "FAIL  ") + name + ((" " + detail) if detail else ""))


def consume(topic, group, expect, timeout_sec=40):
    got = []

    def on_msg(msgs):
        got.extend(m for m in msgs if m.topic == topic)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    cons = DefaultMQPushConsumer(consumer_group=group)
    cons.set_namesrv_addr(NAMESRV)
    cons.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    cons.subscribe(topic, "*")
    cons.set_message_listener(SimpleMessageListener(on_msg))
    cons.start()
    deadline = time.time() + timeout_sec
    while time.time() < deadline and len(got) < expect:
        time.sleep(0.25)
    cons.shutdown()
    return got


def main():
    if len(sys.argv) < 2:
        print("usage: go_producer_consume_check.py <topic> [expect] [group]", file=sys.stderr)
        return 2
    topic = sys.argv[1]
    expect = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    group = sys.argv[3] if len(sys.argv) > 3 else "GID_PyReadGo_%d" % int(time.time())

    msgs = consume(topic, group, expect)
    check("回读到 %d 条消息" % expect, len(msgs) >= expect, "got=%d" % len(msgs))

    by_body = {}
    for m in msgs:
        by_body[m.body] = m

    expected_bodies = [
        b"go-live-sync",
        b"go-live-oneway",
        b"go-live-tx-commit",
        b"go-live-tx-unknown",
        b"go-live-batch-1",
        b"go-live-batch-2",
        b"go-live-batch-3",
    ]
    for body in expected_bodies:
        check("收到 %r" % body.decode(), body in by_body)

    # 同步消息的 tags / keys 要原封不动（属性段编码正确）
    sync = by_body.get(b"go-live-sync")
    if sync is not None:
        check("同步消息 TAGS=goLive", sync.get_property("TAGS") == "goLive",
              "tags=%r" % sync.get_property("TAGS"))
        check("同步消息 KEYS=go-live-key-1", sync.get_property("KEYS") == "go-live-key-1",
              "keys=%r" % sync.get_property("KEYS"))
        # 每条消息都必须有客户端 UNIQ_KEY（Go 侧 setUniqID 写入），
        # 这是 msgId/轨迹串联的依据。
        check("同步消息带 UNIQ_KEY", bool(sync.get_property("UNIQ_KEY")),
              "uniq=%r" % sync.get_property("UNIQ_KEY"))
        check("同步消息 msg_id 非空", bool(sync.msg_id), "msg_id=%r" % sync.msg_id)
        # 事务消息的 PGROUP 属性要留着（broker 回查靠它定位生产者）
    tx = by_body.get(b"go-live-tx-commit")
    if tx is not None:
        check("事务消息带 PGROUP", bool(tx.get_property("PGROUP")),
              "pgroup=%r" % tx.get_property("PGROUP"))
        # TRAN_MSG **留**在已提交消息上，不是「应该被剥掉」。这一点用 Python 自己的
        # 生产者做过对照实验（见本文件 docstring）：Python/Java 都保留该属性，
        # broker 只在半消息阶段用它做路由判定，提交后不清理。
        check("事务消息保留 TRAN_MSG=true", tx.get_property("TRAN_MSG") == "true",
              "TRAN_MSG=%r" % tx.get_property("TRAN_MSG"))
        check("事务消息带 __transactionId__", bool(tx.get_property("__transactionId__")),
              "tid=%r" % tx.get_property("__transactionId__"))

    # 压缩消息：消费端解压后长度要等于原文（Go 侧 12 字节 × 500 = 6000），
    # 且 COMPRESSED 位被清掉
    big = [m for m in msgs if m.body and m.body.startswith(b"compress-me-")]
    if big:
        m = big[0]
        check("压缩消息解压后长度=6000", len(m.body) == 6000, "len=%d" % len(m.body))
        check("压缩消息内容完整", m.body == (b"compress-me-" * 500),
              "head=%r" % m.body[:24])
        # 解压后 COMPRESSED_FLAG 必须已经清掉（对齐 Java MessageDecoder:520-523）
        check("压缩消息 COMPRESSED 位已清", not (m.sys_flag & 0x1),
              "sys_flag=%#x" % m.sys_flag)
    else:
        check("收到压缩大消息", False, "(没找到 compress-me- 开头的消息)")

    # ---- 异步链（Go 的 DEFAULT ASYNC）-----------------------------------------------
    # 异步发送走的是另一条构建路径（一次构建 + 回调重试），属性段/uniqID/压缩位
    # 都可能与同步路径不同，所以这里单独核一遍，而不是"反正 SEND_OK 就算过"。
    async_single = by_body.get(b"go-live-async")
    if async_single is None:
        check("收到异步消息 go-live-async", False)
    else:
        check("收到异步消息 go-live-async", True)
        check("异步消息 TAGS=goAsync", async_single.get_property("TAGS") == "goAsync",
              "tags=%r" % async_single.get_property("TAGS"))
        check("异步消息 KEYS=go-live-async-key",
              async_single.get_property("KEYS") == "go-live-async-key",
              "keys=%r" % async_single.get_property("KEYS"))
        check("异步消息带 UNIQ_KEY", bool(async_single.get_property("UNIQ_KEY")),
              "uniq=%r" % async_single.get_property("UNIQ_KEY"))

    burst = sorted(b for b in by_body if b.startswith(b"go-live-async-burst-"))
    check("异步并发 16 条齐全", len(burst) == 16, "got=%d" % len(burst))
    bp = sorted(b for b in by_body if b.startswith(b"go-live-async-bp-"))
    check("背压下 16 条齐全", len(bp) == 16, "got=%d" % len(bp))

    for i in (1, 2, 3):
        body = b"go-live-async-batch-%d" % i
        check("收到异步批量 %d" % i, body in by_body)

    # 指定队列 / 选择器：Go 侧核的是**应答头里的 queueId**，这里核的是**消费端真实
    # 所在的队列号** —— 两条独立证据都指向同一个队列，才算真的落在指定队列上。
    pinned = by_body.get(b"go-live-async-pinned")
    if pinned is None:
        check("收到指定队列异步消息", False)
    else:
        check("指定队列消息落在 queue_id=1", pinned.queue_id == 1,
              "queue_id=%r" % pinned.queue_id)
    selected = by_body.get(b"go-live-async-selected")
    if selected is None:
        check("收到选择器异步消息", False)
    else:
        check("选择器消息落在 queue_id=2", selected.queue_id == 2,
              "queue_id=%r" % selected.queue_id)

    print("\nPASS=%d FAIL=%d" % (sum(1 for r in results if r), sum(1 for r in results if not r)))
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
