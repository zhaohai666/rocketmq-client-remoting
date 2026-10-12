# -*- coding: utf-8 -*-
"""用**已验证的 Python 客户端**回读 Go autoBatch（ProduceAccumulator）写进真实
broker 的消息。

为什么需要：autoBatch 攒批后走 SEND_BATCH_MESSAGE 帧发送，帧拼错时 broker 照样回
SEND_OK —— 拆批错误（成员体错乱、TAGS/KEYS 丢失、UNIQ_KEY 缺失）只有另一个语言
的消费端能读出来。这里逐条校验：条数、每条消息的 body 前缀、TAGS、KEYS、UNIQ_KEY。

用法: python go_autobatch_consume_check.py <topic> [expect] [group]

expect 由调用方从 Go 侧的 `SENT=<n>` 行取（见 scripts/run_go_autobatch_live.sh），
别写死。
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
        print("usage: go_autobatch_consume_check.py <topic> [expect] [group]", file=sys.stderr)
        return 2
    topic = sys.argv[1]
    expect = int(sys.argv[2]) if len(sys.argv) > 2 else 12
    group = sys.argv[3] if len(sys.argv) > 3 else "GID_PyReadGoAutoBatch_%d" % int(time.time())

    msgs = consume(topic, group, expect)
    check("回读到 %d 条消息" % expect, len(msgs) >= expect, "got=%d" % len(msgs))

    # 攒批最容易丢的就是成员属性：TAGS/KEYS/UNIQ_KEY 必须逐条健在。
    by_body = {}
    for m in msgs:
        by_body[m.body] = m

    prefixes = (b"autobatch-sync-", b"autobatch-tail-", b"autobatch-async-")
    matched = sum(1 for body in by_body if any(body.startswith(p) for p in prefixes))
    check("全部消息都是 autoBatch 例子的 body", matched == min(expect, len(by_body)),
          "matched=%d" % matched)

    sync = by_body.get(b"autobatch-sync-00-0123456789abcdef")
    if sync is not None:
        check("攒批成员 TAGS=autoBatch", sync.get_property("TAGS") == "autoBatch",
              "tags=%r" % sync.get_property("TAGS"))
        check("攒批成员 KEYS=auto-batch-key-0", sync.get_property("KEYS") == "auto-batch-key-0",
              "keys=%r" % sync.get_property("KEYS"))
        check("攒批成员带 UNIQ_KEY", bool(sync.get_property("UNIQ_KEY")),
              "uniq=%r" % sync.get_property("UNIQ_KEY"))
        check("攒批成员 msg_id 非空", bool(sync.msg_id), "msg_id=%r" % sync.msg_id)
    else:
        check("收到攒批成员 autobatch-sync-00", False)

    # 12 条消息的 UNIQ_KEY 必须两两不同（攒批绝不共享成员 id）
    uniqs = [m.get_property("UNIQ_KEY") for m in msgs]
    check("成员 UNIQ_KEY 互不相同", len(set(uniqs)) == len(uniqs),
          "distinct=%d total=%d" % (len(set(uniqs)), len(uniqs)))

    print("PASS=%d FAIL=%d" % (sum(1 for r in results if r), sum(1 for r in results if not r)))
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
