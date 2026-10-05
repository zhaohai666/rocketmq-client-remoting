# -*- coding: utf-8 -*-
"""用**已验证的 Python 客户端**回读 Go 主动拉取消费者（DefaultMQPullConsumer）在真实
broker 上留下的痕迹。

为什么需要：go/examples/live_pull 里有一半断言是"自问自答" —— 它自己提交位点、自己读
回来，报文里 key 拼错了照样可能自洽（尤其 UPDATE_CONSUMER_OFFSET 和
CONSUMER_SEND_MSG_BACK 的头字段，写错了 broker 也可能回 SUCCESS）。真正能判定的是
**另一个语言**的实现：
  * 它用 Java 的属性名去问 broker「GID_xxx 在 <topic> 的 0 号队列上提交到哪儿了」，
    拿到的必须是 Go 侧写进去的那个**故意只提交一部分**的位点（不是"全消费完"的巧合值）；
  * 它用自己的拉取去读同一段 commitlog，读到的消息序列必须和 Go 侧列出的完全一致；
  * Go 说"sendMessageBack 被 broker 接受"不算数，得看 %RETRY%<group> 里是否真的出现了
    那条消息，并且 %DLQ%<group> 里**没有** —— maxReconsumeTimes 照抄 push 消费者的 -1
    就会直奔 DLQ，这条只在真机上看得到。

用法: python go_pull_consume_check.py <topic> <go_group> <committed> <total>

committed / total 都从 Go 侧最后一行 `PASS=.. FAIL=.. COMMITTED=.. TOTAL=..` 取
（见 scripts/run_go_pull_live.sh），别写死 —— 往 live_pull 里加一个检查就会让写死的
期望值悄悄失准（少要几条 → 假绿）。
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.admin import DefaultMQAdminExt
from client.consumer import DefaultMQPullConsumer
from client.consumer_result import PullStatus
from remoting.exception import RemotingException

NAMESRV = os.environ.get("ROCKETMQ_NAMESRV", "127.0.0.1:9876")

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS  " if ok else "FAIL  ") + name + (("  " + detail) if detail else ""))


def wait_queues(consumer, topic, expect, budget=20):
    """等路由可查（Go 侧刚建的 topic，broker -> nameserver 注册是异步的）。"""
    deadline = time.time() + budget
    queues = []
    while time.time() < deadline:
        try:
            queues = consumer.fetch_subscribe_message_queues(topic)
        except BaseException as e:  # noqa: BLE001
            queues = []
            detail = "%s: %s" % (type(e).__name__, e)
        else:
            detail = ""
        if len(queues) >= expect:
            return queues
        time.sleep(0.5)
    print("      (wait_queues gave up: %s)" % (detail or "%d queues" % len(queues)))
    return queues


def safe_pull(consumer, mq, offset, max_nums=32):
    try:
        return consumer.pull(mq, "*", offset, max_nums), ""
    except RemotingException as e:
        return None, "%s: %s" % (type(e).__name__, e)


def main():
    if len(sys.argv) < 5:
        print("usage: go_pull_consume_check.py <topic> <go_group> <committed> <total>",
              file=sys.stderr)
        return 2
    topic = sys.argv[1]
    go_group = sys.argv[2]
    committed = int(sys.argv[3])
    total = int(sys.argv[4])
    stamp = int(time.time())

    consumer = DefaultMQPullConsumer("GID_PyReadGoPull_%d" % stamp)
    consumer.set_namesrv_addr(NAMESRV)
    consumer.start()
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.start()
    try:
        # ---- S1 Go 建的 topic 只有一个队列 -----------------------------------------
        queues = wait_queues(consumer, topic, 1)
        check("S1 Go 建的 topic 有 1 个读队列", len(queues) == 1, "got=%d" % len(queues))
        if len(queues) != 1:
            return 1
        q0 = queues[0]
        check("S1 队列号是 0 且 broker 名非空",
              q0.queue_id == 0 and bool(q0.broker_name),
              "broker=%s queue=%d" % (q0.broker_name, q0.queue_id))

        # ---- S2 独立数一遍条数 -----------------------------------------------------
        hi = consumer.max_offset(q0)
        lo = consumer.min_offset(q0)
        check("S2 max_offset == Go 报的总条数", hi == total, "max=%d want=%d" % (hi, total))
        check("S2 min_offset == 0", lo == 0, "min=%d" % lo)

        # ---- S3 位点见证：broker 上 Go 那个组提交到哪儿了 ---------------------------
        # 这是本脚本最重要的一条：Go 侧最后一个动作是 UpdateConsumeOffsetToBroker，
        # 这里用**另一个进程、另一个语言**按 Java 的字段名去问 broker。
        got = admin.examine_consumer_offset(go_group, q0)
        check("S3 broker 上 Go 组的已提交位点 == COMMITTED", got == committed,
              "broker=%r committed=%d" % (got, committed))
        check("S3 COMMITTED 是部分位点（不是「全消费完」的巧合值）", committed < total,
              "committed=%d total=%d" % (committed, total))

        # ---- S4 用 Python 的拉取读同一段 log，内容与顺序必须与 Go 侧一致 -----------
        result, err = safe_pull(consumer, q0, committed)
        if result is None:
            check("S4 从 COMMITTED 拉起（Python 侧）", False, err)
        else:
            bodies = [bytes(m.body) for m in result.msg_found_list]
            expect_bodies = [b"go-pull-seed-%d" % i for i in range(committed + 1, 6)] + \
                            [b"go-pull-wake"]
            check("S4 从 COMMITTED 拉起（Python 侧）= FOUND",
                  result.status == PullStatus.FOUND,
                  "status=%s n=%d" % (result.status, len(result.msg_found_list)))
            check("S4 条数 == TOTAL - COMMITTED", len(bodies) == total - committed,
                  "n=%d want=%d" % (len(bodies), total - committed))
            check("S4 正文与顺序完全一致（位点语义对齐）", bodies == expect_bodies,
                  "got=%s" % [b.decode() for b in bodies])
            # 位点是从 1 开始数的"下一条"，第 committed 条正是 seed-(committed+1)
            check("S4 COMMITTED 指向第 %d 条消息" % (committed + 1),
                  bool(bodies) and bodies[0] == b"go-pull-seed-%d" % (committed + 1),
                  "head=%r" % (bodies[0] if bodies else b""))
            if result.msg_found_list:
                m = result.msg_found_list[0]
                check("S4 属性段可解析（KEYS）",
                      m.get_property("KEYS") == "go-pull-key-%d" % (committed + 1),
                      "keys=%r" % m.get_property("KEYS"))
                check("S4 带 UNIQ_KEY", bool(m.get_property("UNIQ_KEY")),
                      "uniq=%r" % m.get_property("UNIQ_KEY"))

        # ---- S5 sendMessageBack 必须落在 %RETRY%<go_group> -------------------------
        # Go 侧回投的是队头那条（go-pull-seed-1），delayLevel=1 → 约 1s 后由投递服务
        # 放回重试队列。
        retry_topic = "%RETRY%" + go_group
        found = False
        detail = "retry topic never became routable"
        deadline = time.time() + 40
        while time.time() < deadline and not found:
            try:
                retry_queues = consumer.fetch_subscribe_message_queues(retry_topic)
            except BaseException as e:  # noqa: BLE001
                detail = "retry topic not routable yet: %s" % e
                time.sleep(1)
                continue
            for rq in retry_queues:
                try:
                    rlo = consumer.min_offset(rq)
                    rhi = consumer.max_offset(rq)
                except BaseException as e:  # noqa: BLE001
                    detail = "%s: %s" % (type(e).__name__, e)
                    continue
                if rhi <= rlo:
                    continue
                r, rerr = safe_pull(consumer, rq, rlo)
                if r is None:
                    detail = rerr
                    continue
                for m in r.msg_found_list:
                    if bytes(m.body) > b"":
                        found = True
                        detail = ("queue=%d body=%s reconsumeTimes=%d"
                                  % (rq.queue_id, bytes(m.body), m.get_reconsume_times()))
                        break
                if found:
                    break
            if not found:
                time.sleep(1)
        check("S5 %RETRY%<group> 拉到了被回投的消息", found, detail)
        if found:
            check("S5 回投的是队头那条（go-pull-seed-1）", "go-pull-seed-1" in detail, detail)
            check("S5 reconsumeTimes 已自增到 1（broker 走了重投路径）",
                  "reconsumeTimes=1" in detail, detail)

        # ---- S6 %DLQ% 必须是空的：maxReconsumeTimes=16 而不是 push 的 -1 -----------
        dlq_topic = "%DLQ%" + go_group
        dlq_msgs = []
        try:
            for dq in consumer.fetch_subscribe_message_queues(dlq_topic):
                try:
                    dlo = consumer.min_offset(dq)
                    dhi = consumer.max_offset(dq)
                except BaseException as e:  # noqa: BLE001
                    dlq_msgs.append("offset query failed: %s" % e)
                    continue
                if dhi <= dlo:
                    continue
                r, _err = safe_pull(consumer, dq, dlo)
                if r is not None:
                    dlq_msgs.extend(bytes(m.body) for m in r.msg_found_list)
        except BaseException:  # noqa: BLE001
            # 路由不存在 = broker 从没建过这个 DLQ topic，正是期望结果
            dlq_msgs = []
        check("S6 %DLQ%<group> 是空的（没有直奔死信队列）", not dlq_msgs,
              "dlq=%s" % dlq_msgs)
    finally:
        try:
            consumer.shutdown()
        finally:
            admin.shutdown()

    print("\nPASS=%d FAIL=%d" % (sum(1 for r in results if r), sum(1 for r in results if not r)))
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
