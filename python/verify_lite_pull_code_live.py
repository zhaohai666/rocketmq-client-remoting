#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Live verification of the lite-pull request code / broker switch (#107).

Usage (requires a local RocketMQ 5.5.1 cluster):
    .venv/bin/python verify_lite_pull_code_live.py 127.0.0.1:9876

Why it must be live: the `FLAG_LITE_PULL_MESSAGE(0x10)` + `LITE_PULL_MESSAGE(361)` chain is
always green on an offline mock -- even with the bit missing and the code still 11, the message is still a perfectly legal pull,
and the broker replies with messages as usual. The only thing that can distinguish it is the broker's `litePullMessageEnable` switch
(`PullMessageProcessor:325-331` blocks only 361): flip the switch to false at runtime
(UPDATE_BROKER_CONFIG, no restart needed):

  S1 the switch defaults to true: the whole lite pull path works (baseline).
  S2 the switch false:
     S2a a bare 361 request -> NO_PERMISSION(16) + "…for lite pull consumer is forbidden";
     S2b a bare 11 request on the same queue at the same offset -> SUCCESS as usual and gets messages (control: the switch only governs
         lite, ordinary pull is unaffected -- without this leg, S2a's failure could just be a broken broker);
     S2c the lite consumer starves quietly: the messages are there, but poll returns nothing and the pull cursor does not budge
         (the old impl leaves the bit unset/code 11, so this leg would receive messages -> the discriminator turns red);
     S2d the push consumer consumes as usual (control: the whole consume path is not broken).
  S3 restore the switch to true: lite pull recovers immediately.

Before exiting, unconditionally restore `litePullMessageEnable` to its original value (same as verify_recall_live.py).
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from client.admin import DefaultMQAdminExt
from client.consumer import (ConsumeConcurrentlyStatus, DefaultLitePullConsumer,
                                      DefaultMQPushConsumer, SimpleMessageListener)
from client.exception import MQBrokerException
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message, MessageQueue
from common.sysflag import PullSysFlag
from remoting.protocol.heartbeat import ConsumeFromWhere

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
    return PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                      subscription=True, class_filter=False,
                                      lite_pull=True)


def classic_flag() -> int:
    # the 4-arg version of DefaultMQPullConsumerImpl.pullSyncImpl:248, the lite bit must be 0
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
        check("R0 can read the broker's %s" % CONFIG_KEY, original is not None,
              "value=%s" % original)
        if original != "true":
            check("R0 temporarily enabled %s" % CONFIG_KEY, set_flag(admin, addr, "true"))
        if read_flag(admin, addr) != "true":
            check("R0 the switch is not true, the following checks are meaningless", False)
            return 1

        # ---------- S1 baseline: switch true, lite works ----------
        print("\nS1 switch true: lite pull baseline")
        m_s1 = Message(TOPIC, b"lite-code-s1")
        producer.send(m_s1, mq=MessageQueue(TOPIC, BROKER, 0))
        c1 = new_lite("LiteCodeLive_g1_%d" % STAMP)
        consumers.append(c1)
        got = []
        deadline = time.time() + 20
        while not got and time.time() < deadline:
            got.extend(c1.poll(timeout=500))
        bodies = {bytes(m.body) for m in got}
        check("S1 the lite consumer received the message", b"lite-code-s1" in bodies,
              "got=%s" % sorted(bodies))
        q0 = MessageQueue(TOPIC, BROKER, 0)
        check("S1 the pull cursor has advanced", c1.pull_cursor_of(q0) >= 1,
              "cursor=%s" % c1.pull_cursor_of(q0))

        # ---------- S2 switch false ----------
        print("\nS2 turn %s off at runtime (UPDATE_BROKER_CONFIG, no broker restart)" % CONFIG_KEY)
        check("S2 the switch has been changed to false", set_flag(admin, addr, "false"))
        check("S2 the switch reads back confirmed", read_flag(admin, addr) == "false",
              "value=%s" % read_flag(admin, addr))

        # S2a: bare 361 -> NO_PERMISSION + fixed remark
        m_s2 = Message(TOPIC, b"lite-code-s2")
        producer.send(m_s2, mq=q0)
        denied_code = None
        denied_remark = ""
        try:
            setup.pull_message("LiteCodeLive_g1_%d" % STAMP, q0, 0, 32, lite_flag(), 0,
                               "*", 0, "TAG", addr=addr)
            check("S2a the bare 361 is rejected by the switch", False, "unexpectedly SUCCESS -- the lite bit/code did not take effect")
        except MQBrokerException as e:
            denied_code, denied_remark = e.response_code, str(e)
            check("S2a the bare 361 is rejected by the switch (NO_PERMISSION=16)", e.response_code == 16,
                  "code=%s remark=%s" % (e.response_code, e))
            check("S2a the rejection reason is exactly the lite switch", DENY_REMARK in str(e), str(e))
        except Exception as e:  # noqa: BLE001
            check("S2a the bare 361 is rejected by the switch", False, repr(e))

        # S2b: a bare 11 on the same queue at the same offset -> gets messages as usual (control)
        try:
            r = setup.pull_message("LiteCodeLive_g1_%d" % STAMP, q0, 0, 32,
                                   classic_flag(), 0, "*", 0, "TAG", addr=addr)
            got11 = {bytes(m.body) for m in r.msg_found_list}
            check("S2b control: the bare 11 is unaffected by the switch and gets messages as usual",
                  b"lite-code-s1" in got11 or b"lite-code-s2" in got11,
                  "status=%s got=%s" % (r.status, sorted(got11)))
        except Exception as e:  # noqa: BLE001
            check("S2b control: the bare 11 is unaffected by the switch", False, repr(e))

        # S2c: the lite consumer starves quietly (the messages are there, but nothing comes; the cursor does not move)
        c2 = new_lite("LiteCodeLive_g2_%d" % STAMP)
        consumers.append(c2)
        starved = []
        deadline = time.time() + 8
        while time.time() < deadline:
            starved.extend(c2.poll(timeout=500))
        check("S2c the lite consumer receives nothing while the switch is off", not starved,
              "got=%d" % len(starved))
        check("S2c the pull cursor does not budge", c2.pull_cursor_of(q0) == 0,
              "cursor=%s" % c2.pull_cursor_of(q0))

        # S2d: the push consumer consumes as usual (control)
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
        check("S2d control: the push consumer receives messages as usual (the switch only governs lite)",
              b"lite-code-s2d" in collected, "got=%s" % sorted(set(collected)))

        # ---------- S3 restore ----------
        print("\nS3 restore the switch to true: lite recovers")
        check("S3 the switch has been restored", set_flag(admin, addr, "true"))
        m_s3 = Message(TOPIC, b"lite-code-s3")
        producer.send(m_s3, mq=q0)
        c3 = new_lite("LiteCodeLive_g3_%d" % STAMP)
        consumers.append(c3)
        got3 = []
        deadline = time.time() + 20
        while not got3 and time.time() < deadline:
            got3.extend(c3.poll(timeout=500))
        bodies3 = {bytes(m.body) for m in got3}
        check("S3 the lite consumer recovers immediately after restoration", bool(bodies3),
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
