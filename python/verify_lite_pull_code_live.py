#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""lite-pull **请求码 / broker 开关**真机验证（#107）。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_lite_pull_code_live.py 127.0.0.1:9876

为什么必须真机：`FLAG_LITE_PULL_MESSAGE(0x10)` + `LITE_PULL_MESSAGE(361)` 这条链在
离线 mock 上永远是绿的 —— 少了位、码还是 11 时，报文依然是一个完全合法的 pull，
broker 照常回消息。能把它区分出来的只有 broker 的 `litePullMessageEnable` 开关
（`PullMessageProcessor:325-331` **只拦 361**）：把开关在运行时翻成 false
（UPDATE_BROKER_CONFIG，无需重启）：

  S1 开关默认 true：lite pull 全链路正常（基线）。
  S2 开关 false：
     S2a 裸 361 请求 → NO_PERMISSION(16) + "…for lite pull consumer is forbidden"；
     S2b 同队列同一位点的裸 11 请求 → 照常 SUCCESS 且拿到消息（**对照**：开关只管
         lite，普通 pull 不受影响 —— 没有这条腿，S2a 的失败可能只是 broker 坏了）；
     S2c lite 消费者安静饿死：消息明明在，poll 一条不来、拉取游标纹丝不动
         （旧实现位不置/码为 11，这条腿会收到消息 → 判别器变红）；
     S2d push 消费者照常消费（**对照**：整条消费链路没坏）。
  S3 开关还原 true：lite pull 立即恢复。

退出前**无条件**把 `litePullMessageEnable` 改回原值（与 verify_recall_live.py 同款）。
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.consumer import (ConsumeConcurrentlyStatus, DefaultLitePullConsumer,
                                      DefaultMQPushConsumer, SimpleMessageListener)
from rocketmq.client.exception import MQBrokerException
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message, MessageQueue
from rocketmq.common.sysflag import PullSysFlag
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "LiteCodeLive_%d" % STAMP
BROKER = "broker-a"
CONFIG_KEY = "litePullMessageEnable"
DENY_REMARK = "for lite pull consumer is forbidden"

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


def lite_flag() -> int:
    # 与 Java DefaultLitePullConsumerImpl#pullSyncImpl:1058 的
    # buildSysFlag(false /*commitOffset*/, block, true /*subscription*/,
    #              false /*classFilter*/, true /*litePull*/) 对齐
    return PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                      subscription=True, class_filter=False,
                                      lite_pull=True)


def classic_flag() -> int:
    # DefaultMQPullConsumerImpl.pullSyncImpl:248 的 4 参版本，lite 位必须为 0
    return PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                      subscription=True, class_filter=False)


def set_flag(admin: DefaultMQAdminExt, addr: str, value: str) -> bool:
    try:
        admin.update_broker_config(addr, {CONFIG_KEY: value}, 5000)
        return True
    except Exception as e:  # noqa: BLE001
        print("  [diag] update_broker_config(%s=%s) failed: %s" % (CONFIG_KEY, value, e))
        return False


def read_flag(admin: DefaultMQAdminExt, addr: str):
    try:
        return admin.get_broker_config(addr, 5000).get(CONFIG_KEY)
    except Exception as e:  # noqa: BLE001
        print("  [diag] get_broker_config failed: %s" % e)
        return None


def main() -> int:
    setup = MQClientInstance("lite-code-setup-%d" % STAMP, [NAMESRV])
    setup.start()
    setup.create_topic_in_route(TOPIC, 1, 1)
    print("topic=%s broker=%s" % (TOPIC, BROKER))

    producer = DefaultMQProducer("LiteCodeLive_pg_%d" % STAMP)
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.start()

    addr = setup.broker_addr_of(BROKER) or "127.0.0.1:10911"
    original = None
    consumers: list = []

    def new_lite(group: str) -> DefaultLitePullConsumer:
        c = DefaultLitePullConsumer(group)
        c.set_namesrv_addr(NAMESRV)
        c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        c.set_auto_commit(False)
        q0 = MessageQueue(TOPIC, BROKER, 0)
        c.assign([q0])
        c.start()
        return c

    try:
        original = read_flag(admin, addr)
        check("R0 能读到 broker 的 %s" % CONFIG_KEY, original is not None,
              "value=%s" % original)
        if original != "true":
            check("R0 已临时打开 %s" % CONFIG_KEY, set_flag(admin, addr, "true"))
        if read_flag(admin, addr) != "true":
            check("R0 开关不在 true，后续检查无意义", False)
            return 1

        # ---------- S1 基线：开关 true，lite 正常 ----------
        print("\nS1 开关 true：lite pull 基线")
        m_s1 = Message(TOPIC, b"lite-code-s1")
        producer.send(m_s1, mq=MessageQueue(TOPIC, BROKER, 0))
        c1 = new_lite("LiteCodeLive_g1_%d" % STAMP)
        consumers.append(c1)
        got = []
        deadline = time.time() + 20
        while not got and time.time() < deadline:
            got.extend(c1.poll(timeout=500))
        bodies = {bytes(m.body) for m in got}
        check("S1 lite 消费者收到消息", b"lite-code-s1" in bodies,
              "got=%s" % sorted(bodies))
        q0 = MessageQueue(TOPIC, BROKER, 0)
        check("S1 拉取游标已推进", c1.pull_cursor_of(q0) >= 1,
              "cursor=%s" % c1.pull_cursor_of(q0))

        # ---------- S2 开关 false ----------
        print("\nS2 运行时关闭 %s（UPDATE_BROKER_CONFIG，不重启 broker）" % CONFIG_KEY)
        check("S2 开关已改为 false", set_flag(admin, addr, "false"))
        check("S2 开关读回确认", read_flag(admin, addr) == "false",
              "value=%s" % read_flag(admin, addr))

        # S2a：裸 361 → NO_PERMISSION + 固定 remark
        m_s2 = Message(TOPIC, b"lite-code-s2")
        producer.send(m_s2, mq=q0)
        denied_code = None
        denied_remark = ""
        try:
            setup.pull_message("LiteCodeLive_g1_%d" % STAMP, q0, 0, 32, lite_flag(), 0,
                               "*", 0, "TAG", addr=addr)
            check("S2a 裸 361 被开关拒绝", False, "竟然 SUCCESS —— lite 位/码没生效")
        except MQBrokerException as e:
            denied_code, denied_remark = e.response_code, str(e)
            check("S2a 裸 361 被开关拒绝（NO_PERMISSION=16）", e.response_code == 16,
                  "code=%s remark=%s" % (e.response_code, e))
            check("S2a 拒绝理由正是 lite 开关", DENY_REMARK in str(e), str(e))
        except Exception as e:  # noqa: BLE001
            check("S2a 裸 361 被开关拒绝", False, repr(e))

        # S2b：同一队列同一位点的裸 11 → 照常拿消息（对照组）
        try:
            r = setup.pull_message("LiteCodeLive_g1_%d" % STAMP, q0, 0, 32,
                                   classic_flag(), 0, "*", 0, "TAG", addr=addr)
            got11 = {bytes(m.body) for m in r.msg_found_list}
            check("S2b 对照组：裸 11 不受开关影响，照常拿消息",
                  b"lite-code-s1" in got11 or b"lite-code-s2" in got11,
                  "status=%s got=%s" % (r.status, sorted(got11)))
        except Exception as e:  # noqa: BLE001
            check("S2b 对照组：裸 11 不受开关影响", False, repr(e))

        # S2c：lite 消费者安静饿死（消息在，但一条不来；游标不动）
        c2 = new_lite("LiteCodeLive_g2_%d" % STAMP)
        consumers.append(c2)
        starved = []
        deadline = time.time() + 8
        while time.time() < deadline:
            starved.extend(c2.poll(timeout=500))
        check("S2c 开关关闭期间 lite 消费者一条都收不到", not starved,
              "got=%d" % len(starved))
        check("S2c 拉取游标纹丝不动", c2.pull_cursor_of(q0) == 0,
              "cursor=%s" % c2.pull_cursor_of(q0))

        # S2d：push 消费者照常消费（对照组）
        collected: list = []

        class PushCollector(SimpleMessageListener):
            def __init__(self):
                super().__init__(self._on_msg)

            def _on_msg(self, msgs):
                for m in msgs:
                    collected.append(bytes(m.body))
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

        push = DefaultMQPushConsumer("LiteCodeLive_push_%d" % STAMP)
        push.set_namesrv_addr(NAMESRV)
        push.subscribe(TOPIC, "*")
        push.set_message_listener(PushCollector())
        push.start()
        consumers.append(push)
        m_s2d = Message(TOPIC, b"lite-code-s2d")
        producer.send(m_s2d, mq=q0)
        deadline = time.time() + 20
        while b"lite-code-s2d" not in collected and time.time() < deadline:
            time.sleep(0.5)
        check("S2d 对照组：push 消费者照常收到消息（开关只管 lite）",
              b"lite-code-s2d" in collected, "got=%s" % sorted(set(collected)))

        # ---------- S3 还原 ----------
        print("\nS3 开关还原 true：lite 恢复")
        check("S3 开关已还原", set_flag(admin, addr, "true"))
        m_s3 = Message(TOPIC, b"lite-code-s3")
        producer.send(m_s3, mq=q0)
        c3 = new_lite("LiteCodeLive_g3_%d" % STAMP)
        consumers.append(c3)
        got3 = []
        deadline = time.time() + 20
        while not got3 and time.time() < deadline:
            got3.extend(c3.poll(timeout=500))
        bodies3 = {bytes(m.body) for m in got3}
        check("S3 还原后 lite 消费者立即恢复", bool(bodies3),
              "got=%s" % sorted(bodies3))
    finally:
        for c in consumers:
            try:
                c.shutdown()
            except Exception:  # noqa: BLE001
                pass
        if original is not None:
            ok = set_flag(admin, addr, original)
            print("\n[restore] %s=%s → %s" % (CONFIG_KEY, original, "OK" if ok else "FAILED"))
        try:
            producer.shutdown()
            admin.shutdown()
            setup.shutdown()
        except Exception:  # noqa: BLE001
            pass

    print("\nLitePullCode: PASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
