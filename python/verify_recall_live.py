#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""定时消息撤回（recallMessage 370）真机验证。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_recall_live.py 127.0.0.1:9876

为什么必须真机：句柄不是客户端能自己拼出来的。只有 broker 在
``SendMessageProcessor#attachRecallHandle`` 里（且仅当消息带 TIMER_* 延迟属性）才会
把句柄挂回 SEND 响应头，撤回是否真的生效也只能靠「到点没投递」来证明。离线单测
（tests/test_recall_message.py）锁的是编解码、报文键名和本地校验顺序，证不了语义。

场景（每条都对应 Java 的一个行为）：
  R1 定时消息的 SendResult 带 recallHandle，普通消息不带。
  R2 句柄能解出 topic / brokerName / 被撤回消息的 uniqKey，且与发送结果一致。
  R3 recall_message 成功返回被撤回消息的 uniqKey（Java 取响应头 msgId）。
  R4 %RETRY% topic 在本地就被拒（Java "topic is not supported"），不打网络。
  R5 非法句柄在本地就被拒（Java "recall handle is invalid"），且是秒回。
  R6 **语义**：同时发两条同样延迟的消息，撤回其中一条 → 到点后对照消息被投递、
     被撤回的那条永远不到；这才是撤回真正的定义。
  R7 全程把 broker 的 recallMessageEnable 改回原值（跑之前是 false 就跑完还是 false）。

broker 的 recallMessageEnable 默认 false（Java BrokerConfig:546），本地 conf 也是 false，
所以脚本先 UPDATE_BROKER_CONFIG 打开，退出前无条件还原。
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.consumer import (ConsumeConcurrentlyStatus, DefaultMQPushConsumer,
                                      MessageListenerConcurrently)
from rocketmq.client.exception import MQClientException
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common import recall_message_handle
from rocketmq.common.message import Message
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "RecallPy_%d" % int(time.time() * 1000)
TOPIC = PREFIX + "_Topic"
GROUP = PREFIX + "_Group"
BROKER = "broker-a"
DELAY_SEC = 12
CONFIG_KEY = "recallMessageEnable"

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


def _broker_addr(client, fallback: str) -> str:
    addr = client.broker_addr_of(BROKER)
    return addr or fallback


def _set_flag(admin: DefaultMQAdminExt, addr: str, value: str) -> bool:
    try:
        admin.update_broker_config(addr, {CONFIG_KEY: value}, 5000)
        return True
    except Exception as e:  # noqa: BLE001
        print("  [diag] update_broker_config(%s=%s) failed: %s" % (CONFIG_KEY, value, e))
        return False


def _read_flag(admin: DefaultMQAdminExt, addr: str):
    try:
        return admin.get_broker_config(addr, 5000).get(CONFIG_KEY)
    except Exception as e:  # noqa: BLE001
        print("  [diag] get_broker_config failed: %s" % e)
        return None


class Collector(MessageListenerConcurrently):
    """把消费到的 body 收进列表；撤回的语义要靠「到点没收到」判断，所以不能丢消息。"""

    def __init__(self):
        self.bodies = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            self.bodies.extend(m.get_body() for m in msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS


def main() -> int:
    producer = DefaultMQProducer(producer_group=GROUP + "_prod")
    producer.set_namesrv_addr(NAMESRV)
    producer.set_send_msg_timeout(5000)
    producer.start()

    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.start()

    original = None
    try:
        addr = _broker_addr(producer._require_client(), "127.0.0.1:10911")
        original = _read_flag(admin, addr)
        check("R0 能读到 broker 的 %s" % CONFIG_KEY, original is not None, "value=%s" % original)
        if original != "true":
            check("R0 已临时打开 recallMessageEnable", _set_flag(admin, addr, "true"))
        if _read_flag(admin, addr) != "true":
            check("R0 recall 未在 broker 上开启，后续检查无意义", False)
            return 1

        # ---- R1/R2：定时消息才带句柄，且句柄内容与发送结果一致 ----
        to_recall = Message(TOPIC, b"to-recall")
        to_recall.put_property("TIMER_DELAY_SEC", str(DELAY_SEC))
        control = Message(TOPIC, b"control")
        control.put_property("TIMER_DELAY_SEC", str(DELAY_SEC))
        plain = Message(TOPIC, b"plain")

        r_recall = producer.send(to_recall)
        r_control = producer.send(control)
        r_plain = producer.send(plain)
        handle = r_recall.recall_handle
        check("R1 定时消息带回 recallHandle", bool(handle), "handle=%s" % handle)
        check("R1 普通消息不带 recallHandle", r_plain.recall_handle is None)
        check("R1 两条定时消息的句柄不同", handle is not None and r_control.recall_handle != handle)

        parsed = None
        try:
            parsed = recall_message_handle.decode_handle(handle or "")
        except MQClientException as e:
            check("R2 broker 的句柄能被我们的编解码器解开", False, str(e))
        if parsed is not None:
            check("R2 broker 的句柄能被我们的编解码器解开", True)
            check("R2 句柄里的 topic/brokerName 与发送目标一致",
                  parsed.topic == TOPIC and parsed.broker_name == BROKER,
                  "topic=%s broker=%s" % (parsed.topic, parsed.broker_name))
            check("R2 句柄里的 uniqKey 就是这条消息的 UNIQ_KEY",
                  parsed.message_id == r_recall.msg_id,
                  "handle=%s send=%s" % (parsed.message_id, r_recall.msg_id))

        # ---- R4/R5：本地校验必须在打网络之前跑完 ----
        try:
            producer.recall_message("%%RETRY%%%s" % GROUP, handle or "")
            check("R4 %RETRY% topic 被拒", False, "没有抛异常")
        except MQClientException as e:
            check("R4 %RETRY% topic 被拒", str(e) == "topic is not supported", str(e))
        began = time.monotonic()
        try:
            producer.recall_message(TOPIC, "not-a-handle")
            check("R5 非法句柄本地即拒", False, "没有抛异常")
        except MQClientException as e:
            cost = time.monotonic() - began
            check("R5 非法句柄本地即拒",
                  str(e) == "recall handle is invalid" and cost < 0.2,
                  "%s (%.3fs)" % (e, cost))

        # ---- R3/R6：真的撤掉了，且没牵连对照消息 ----
        try:
            recalled_id = producer.recall_message(TOPIC, handle)
            check("R3 recall_message 返回被撤回消息的 uniqKey",
                  recalled_id == r_recall.msg_id, "resp=%s send=%s" % (recalled_id, r_recall.msg_id))
        except Exception as e:  # noqa: BLE001
            check("R3 recall_message 返回被撤回消息的 uniqKey", False, repr(e))

        got = _consume_for(DELAY_SEC + 20)
        bodies = set(got)
        check("R6 对照定时消息按时投递", b"control" in bodies, "got=%s" % sorted(bodies))
        check("R6 普通消息已投递", b"plain" in bodies)
        check("R6 被撤回的定时消息永远没投递", b"to-recall" not in bodies,
              "got=%s" % sorted(bodies))
    finally:
        restored = False
        if original is not None:
            try:
                addr = _broker_addr(producer._require_client(), "127.0.0.1:10911")
                _set_flag(admin, addr, original)
                restored = _read_flag(admin, addr) == original
            except Exception as e:  # noqa: BLE001
                print("  [diag] restore failed: %s" % e)
        check("R7 broker 的 recallMessageEnable 已还原为 %s" % original, restored)
        admin.shutdown()
        producer.shutdown()

    print("\nPASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


def _consume_for(window_sec: int):
    """从 0 位点收满一个时间窗，返回收到的 body 列表。

    窗口必须 > 延迟：对照消息要等到点才出现，被撤回的那条则要「整个窗口都不出现」
    才算撤回成功。
    """
    received = Collector()
    consumer = DefaultMQPushConsumer(GROUP + "_c")
    consumer.set_namesrv_addr(NAMESRV)
    consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    consumer.set_message_listener(received)
    consumer.subscribe(TOPIC, "*")
    consumer.pull_timeout_millis = 3000
    consumer.pull_suspend_timeout_millis = 1000
    consumer.start()
    time.sleep(window_sec)
    consumer.shutdown()
    return received.bodies


if __name__ == "__main__":
    sys.exit(main())
