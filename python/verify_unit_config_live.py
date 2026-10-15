# -*- coding: utf-8 -*-
"""Verification of unitName / unitMode / enableStreamRequestType against a real machine.

This answers the user request to "test against a real cluster whether it works": of these three,
unitName and unitMode are not fields that only live on the client, they have observable
consequences on the broker side, so everything is asserted on a real machine:

  U1  unitName goes into the clientId (`ip@instanceName@unitName`), and the producer send path is
      unaffected.
  U2  When a consumer carries unitName plus the @STREAM suffix, **the clientId the broker sees is
      exactly this value** (read back via GET_CONSUMER_CONNECTION_LIST), proving the suffix was not
      reshaped on the client side.
  U3  Sending with unitMode=true to a new topic: the broker goes through
      `buildSysFlag(true, false)` at `AbstractSendMessageProcessor:485-497`, so the auto-created
      topic carries the UNIT bit (0x1) in its sysFlag; the control group with unitMode=false does
      not.
      -- this is the only evidence that unitMode really went live.
  U4  The heartbeat of a consumer with unitMode=true: `ClientManageProcessor:111-116` uses
      `buildSysFlag(false, true)` to create the %RETRY% topic, whose sysFlag carries the UNIT_SUB
      bit (0x2).
  U5  enableStreamRequestType=true: every request carries the `ReqT=0` extra field and the broker
      handles it as usual (both sending and pulling succeed). This field is the type marker used
      by proxy/stream; an ordinary broker ignores it.

Prerequisites: NameServer + Broker are running with `autoCreateTopicEnable=true` and
`autoCreateSubscriptionGroup=true` (the /tmp/rmq_rust_live/broker.conf of this repository keeps the
defaults).

Usage: .venv/bin/python verify_unit_config_live.py [127.0.0.1:9876]
"""
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.admin import DefaultMQAdminExt
from client.consumer import (DefaultLitePullConsumer, DefaultMQPushConsumer)
from client.producer import DefaultMQProducer
from common.message import Message
from common.mix_all import MixAll
from client.consumer_result import ConsumeConcurrentlyStatus, MessageListenerConcurrently
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time())

FLAG_UNIT = 0x1 << 0
FLAG_UNIT_SUB = 0x1 << 1

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def _wait_until(pred, timeout_sec=20.0, interval=0.3):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


class _Collector(MessageListenerConcurrently):
    def __init__(self):
        self.msgs = []
        self.lock = threading.Lock()

    def consume_message(self, msgs, context=None):
        with self.lock:
            self.msgs.extend(msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def bodies(self):
        with self.lock:
            return [m.get_body() for m in self.msgs]


def _producer(group, unit_name=None, unit_mode=False, instance_name=None, stream=False):
    p = DefaultMQProducer(group)
    p.set_namesrv_addr(NAMESRV)
    if instance_name:
        p.set_instance_name(instance_name)
    if unit_name:
        p.set_unit_name(unit_name)
    p.set_unit_mode(unit_mode)
    p.set_enable_stream_request_type(stream)
    return p


def _route_visible(admin, topic):
    """A new topic only gets its own route once the broker registers it with namesrv (every 30s by
    default); only after the route becomes visible does the client heartbeat reach this broker."""
    try:
        route = admin.examine_topic_route(topic)
    except Exception:  # noqa: BLE001
        return False
    return bool(route.queue_datas)


def _sys_flag(admin, broker_addr, topic):
    """Read back the sysFlag of that topic on the broker; returns None when it does not exist."""
    try:
        return admin.examine_topic_config(broker_addr, topic).topic_sys_flag
    except Exception as e:  # noqa: BLE001
        print("    (examine_topic_config(%s) failed: %s)" % (topic, e))
        return None


def main():
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()

    cluster = None
    for _ in range(40):
        try:
            ci = admin.fetch_broker_cluster_info()
            if ci and ci.broker_addr_table:
                cluster = ci
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    if cluster is None:
        check("cluster liveness", False, "no broker registered on the nameServer")
        admin.shutdown()
        return 1
    broker_addr = cluster.get_broker_addrs()[0]
    check("cluster liveness", True, "broker=%s" % broker_addr)

    ip = MixAll.cached_ip_str()
    topics, groups = [], []

    # ---------------- U1 unitName goes into the clientId ----------------
    t1 = "UnitCfgSend_%d" % STAMP
    topics.append(t1)
    p1 = _producer("PID_unit_cfg_%d" % STAMP, unit_name="unitA",
                   instance_name="unitcfg-u1-%d" % STAMP)
    p1.start()
    try:
        expect = "%s@unitcfg-u1-%d@unitA" % (ip, STAMP)
        check("U1 the producer clientId carries the unitName", p1.client_id == expect,
              "%s (expected %s)" % (p1.client_id, expect))
        r = p1.send(Message(t1, b"u1-body"))
        check("U1 the client with a unitName sent successfully", r.msg_id != "", "msgId=%s" % r.msg_id)
    finally:
        p1.shutdown()

    # The control: without a unitName the clientId must not grow an extra segment out of thin air
    p0 = _producer("PID_unit_cfg_%d" % STAMP, instance_name="unitcfg-u1b-%d" % STAMP)
    p0.start()
    try:
        check("U1 control: with no unitName no suffix is appended",
              p0.client_id == "%s@unitcfg-u1b-%d" % (ip, STAMP), p0.client_id)
    finally:
        p0.shutdown()

    # ---------------- U2 the broker side sees @unitName@STREAM ----------------
    t2 = "UnitCfgConn_%d" % STAMP
    g2 = "GID_unit_cfg_conn_%d" % STAMP
    topics.append(t2)
    groups.append(g2)
    push = DefaultMQPushConsumer(g2)
    push.set_namesrv_addr(NAMESRV)
    push.set_instance_name("unitcfg-u2-%d" % STAMP)
    push.set_unit_name("unitA")
    push.set_enable_stream_request_type(True)   # a push consumer has it off by default, turn it on explicitly here
    push.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    push.subscribe(t2, "*")
    collector = _Collector()
    push.set_message_listener(collector)
    prod = _producer("PID_unit_cfg_%d" % STAMP, instance_name="unitcfg-u2p-%d" % STAMP)
    prod.start()
    try:
        prod.send(Message(t2, b"u2-warmup"))
        _wait_until(lambda: _route_visible(admin, t2), 30)
        push.start()
        expect2 = "%s@unitcfg-u2-%d@unitA@STREAM" % (ip, STAMP)
        check("U2 the local clientId", push.client_id == expect2,
              "%s (expected %s)" % (push.client_id, expect2))
        prod.send(Message(t2, b"u2-body"))
        _wait_until(lambda: collector.bodies().count(b"u2-body") >= 1, 20)
        check("U2 the consumer with the @STREAM suffix can receive messages",
              collector.bodies().count(b"u2-body") >= 1, "got=%s" % collector.bodies())
        # Read back from the broker: the clientId is the raw value carried by the heartbeat, not reshaped
        conn = None
        for _ in range(20):
            try:
                conn = admin.examine_consumer_connection_info(g2, broker_addr)
                if conn and conn.connection_set:
                    break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.5)
        ids = sorted({c.client_id for c in conn.connection_set}) if conn else []
        check("U2 the clientId the broker sees contains @unitA@STREAM",
              any(i.endswith("@unitA@STREAM") for i in ids), "brokerIds=%s" % ids)
    finally:
        prod.shutdown()
        push.shutdown()

    # ---------------- U3 unitMode => the auto-created topic carries the UNIT bit ----------------
    t_on = "UnitCfgOn_%d" % STAMP
    t_off = "UnitCfgOff_%d" % STAMP
    topics += [t_on, t_off]
    pg = "PID_unit_cfg_%d" % STAMP
    pon = _producer(pg, unit_mode=True, instance_name="unitcfg-u3on-%d" % STAMP)
    pon.start()
    try:
        pon.send(Message(t_on, b"u3-on"))
    finally:
        pon.shutdown()
    poff = _producer(pg, unit_mode=False, instance_name="unitcfg-u3off-%d" % STAMP)
    poff.start()
    try:
        poff.send(Message(t_off, b"u3-off"))
    finally:
        poff.shutdown()

    # Creating the topic happens synchronously in the send path, but the route/configuration needs a moment to land
    _wait_until(lambda: _sys_flag(admin, broker_addr, t_on) is not None, 10)
    flag_on = _sys_flag(admin, broker_addr, t_on)
    flag_off = _sys_flag(admin, broker_addr, t_off)
    check("U3 the topic created with unitMode=true carries the UNIT bit",
          flag_on is not None and (flag_on & FLAG_UNIT) == FLAG_UNIT,
          "sysFlag=%s" % flag_on)
    check("U3 the control topic created with unitMode=false does not carry the UNIT bit",
          flag_off is not None and not (flag_off & FLAG_UNIT),
          "sysFlag=%s" % flag_off)

    g4 = "GID_unit_cfg_hb_%d" % STAMP
    t4 = "UnitCfgRetry_%d" % STAMP
    topics.append(t4)
    groups.append(g4)
    prod4 = _producer("PID_unit_cfg_%d" % STAMP, instance_name="unitcfg-u4p-%d" % STAMP)
    prod4.start()
    try:
        prod4.send(Message(t4, b"u4-warmup"))
    finally:
        prod4.shutdown()
    retry_topic = MixAll.RETRY_GROUP_TOPIC_PREFIX + g4
    _wait_until(lambda: _route_visible(admin, t4), 30)
    push4 = DefaultMQPushConsumer(g4)
    push4.set_namesrv_addr(NAMESRV)
    push4.set_instance_name("unitcfg-u4-%d" % STAMP)
    push4.set_unit_mode(True)
    push4.subscribe(t4, "*")
    push4.set_message_listener(_Collector())
    push4.start()
    try:
        # The heartbeat is sent once synchronously inside start(); a failure is swallowed by debug, so only the broker-side result can tell
        found = _wait_until(
            lambda: (_sys_flag(admin, broker_addr, retry_topic) or 0) & FLAG_UNIT_SUB, 30)
        check("U4 the %RETRY% topic of a consumer with unitMode=true carries the UNIT_SUB bit", found,
              "sysFlag=%s" % _sys_flag(admin, broker_addr, retry_topic))
    finally:
        push4.shutdown()

    t5 = "UnitCfgStream_%d" % STAMP
    g5 = "GID_unit_cfg_stream_%d" % STAMP
    topics.append(t5)
    groups.append(g5)
    prod5 = _producer("PID_unit_cfg_%d" % STAMP,
                      instance_name="unitcfg-u5p-%d" % STAMP, stream=True)
    prod5.start()
    bodies = []
    try:
        check("U5 the producer clientId with stream enabled",
              prod5.client_id.endswith("@STREAM"), prod5.client_id)
        for i in range(3):
            prod5.send(Message(t5, ("u5-%d" % i).encode()))
    finally:
        prod5.shutdown()
    _wait_until(lambda: _route_visible(admin, t5), 30)
    lite = DefaultLitePullConsumer(g5)
    lite.set_namesrv_addr(NAMESRV)
    lite.set_instance_name("unitcfg-u5-%d" % STAMP)
    lite.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    lite.subscribe(t5, "*")
    lite.start()
    try:
        check("U5 the clientId of a lite consumer carries @STREAM by default",
              lite.client_id.endswith("@STREAM"), lite.client_id)

        def _drain():
            bodies.extend(m.get_body() for m in lite.poll(timeout=1000))
            return len(bodies) >= 3

        got = _wait_until(_drain, 30)
        check("U5 sending + pulling still work normally when every request carries ReqT=0", got, "bodies=%s" % bodies)
    finally:
        lite.shutdown()

    # ---------------- cleanup ----------------
    for t in topics:
        try:
            admin.delete_topic(t)
        except Exception as e:  # noqa: BLE001
            print("    (delete_topic(%s) failed: %s)" % (t, e))
    for g in groups:
        try:
            admin.delete_subscription_group(broker_addr, g, remove_offset=True)
        except Exception as e:  # noqa: BLE001
            print("    (delete_subscription_group(%s) failed: %s)" % (g, e))
    admin.shutdown()

    passed = sum(1 for _, ok, _ in results if ok)
    print("\n== Result: %d/%d passed ==" % (passed, len(results)))
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
