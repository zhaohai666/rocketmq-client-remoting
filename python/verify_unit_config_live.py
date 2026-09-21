# -*- coding: utf-8 -*-
"""unitName / unitMode / enableStreamRequestType 真机验证（Python 参考实现）。

对应用户要求"基于真实集群测试是否正常"：这三项里 unitName 与 unitMode 都不是
只在客户端自 high 的字段，broker 侧有可观测后果，所以全部用真机断言：

  U1  unitName 进 clientId（`ip@instanceName@unitName`），生产者发送链路不受影响。
  U2  消费者带 unitName + @STREAM 后缀时，**broker 看到的 clientId 就是这个值**
      （GET_CONSUMER_CONNECTION_LIST 回读），证明后缀没被客户端单方面加工。
  U3  unitMode=true 发到新 topic：broker 走
      `AbstractSendMessageProcessor:485-497` 的 `buildSysFlag(true, false)`，
      自动建出来的 topic sysFlag 带 UNIT 位（0x1）；unitMode=false 的对照组不带。
      —— 这是 unitMode 真正上线的唯一证据。
  U4  unitMode=true 的消费者心跳：`ClientManageProcessor:111-116` 用
      `buildSysFlag(false, true)` 建 %RETRY% topic，sysFlag 带 UNIT_SUB 位（0x2）。
  U5  enableStreamRequestType=true：每个请求都带 `ReqT=0` 扩展字段，broker 照常处理
      （发送 + 拉取都成功）。该字段是给 proxy/stream 用的类型标记，普通 broker 忽略它。

前置：NameServer + Broker 已起，且 `autoCreateTopicEnable=true`、
`autoCreateSubscriptionGroup=true`（本仓库的 /tmp/rmq_rust_live/broker.conf 都是默认值）。

用法：.venv/bin/python verify_unit_config_live.py [127.0.0.1:9876]
"""
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.consumer import (DefaultLitePullConsumer, DefaultMQPushConsumer)
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message
from rocketmq.common.mix_all import MixAll
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus, MessageListenerConcurrently
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time())

# Java org.apache.rocketmq.common.sysflag.TopicSysFlag
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
    """新 topic 要等 broker 把它注册到 namesrv（默认 30s 一轮）才有独立路由；
    路由可见之后客户端的心跳才会覆盖到这个 broker。"""
    try:
        route = admin.examine_topic_route(topic)
    except Exception:  # noqa: BLE001
        return False
    return bool(route.queue_datas)


def _sys_flag(admin, broker_addr, topic):
    """读回 broker 上该 topic 的 sysFlag；不存在返回 None。"""
    try:
        return admin.examine_topic_config(broker_addr, topic).topic_sys_flag
    except Exception as e:  # noqa: BLE001
        print("    (examine_topic_config(%s) 失败: %s)" % (topic, e))
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
        check("集群探活", False, "nameServer 无 broker 注册")
        admin.shutdown()
        return 1
    broker_addr = cluster.get_broker_addrs()[0]
    check("集群探活", True, "broker=%s" % broker_addr)

    ip = MixAll.cached_ip_str()
    topics, groups = [], []

    # ---------------- U1 unitName 进 clientId ----------------
    t1 = "UnitCfgSend_%d" % STAMP
    topics.append(t1)
    p1 = _producer("PID_unit_cfg_%d" % STAMP, unit_name="unitA",
                   instance_name="unitcfg-u1-%d" % STAMP)
    p1.start()
    try:
        expect = "%s@unitcfg-u1-%d@unitA" % (ip, STAMP)
        check("U1 producer clientId 带 unitName", p1.client_id == expect,
              "%s (期望 %s)" % (p1.client_id, expect))
        r = p1.send(Message(t1, b"u1-body"))
        check("U1 unitName 客户端发送成功", r.msg_id != "", "msgId=%s" % r.msg_id)
    finally:
        p1.shutdown()

    # 对照：不设 unitName 时 clientId 不能凭空多出一段
    p0 = _producer("PID_unit_cfg_%d" % STAMP, instance_name="unitcfg-u1b-%d" % STAMP)
    p0.start()
    try:
        check("U1 对照：无 unitName 不拼后缀",
              p0.client_id == "%s@unitcfg-u1b-%d" % (ip, STAMP), p0.client_id)
    finally:
        p0.shutdown()

    # ---------------- U2 broker 侧看到 @unitName@STREAM ----------------
    t2 = "UnitCfgConn_%d" % STAMP
    g2 = "GID_unit_cfg_conn_%d" % STAMP
    topics.append(t2)
    groups.append(g2)
    # ⚠ 与 U4 同一类顺序问题，而且是**两个**：
    # 1) 订阅不存在的 topic 时客户端路由表为空 → 不发心跳、rebalance 也拿不到队列，
    #    所以先由生产者发一条预热消息把 topic 建出来，并等它注册到 namesrv；
    # 2) 新消费组默认 CONSUME_FROM_LAST_OFFSET（Java 同），首拉会把 offset 直接设成
    #    max，即**消费者启动之前**的消息会被跳过。这里显式改成 FROM_FIRST_OFFSET，
    #    再把待验消息发出去，两条都能对上。
    push = DefaultMQPushConsumer(g2)
    push.set_namesrv_addr(NAMESRV)
    push.set_instance_name("unitcfg-u2-%d" % STAMP)
    push.set_unit_name("unitA")
    push.set_enable_stream_request_type(True)   # 推送消费者默认关，这里显式开
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
        check("U2 本地 clientId", push.client_id == expect2,
              "%s (期望 %s)" % (push.client_id, expect2))
        prod.send(Message(t2, b"u2-body"))
        _wait_until(lambda: collector.bodies().count(b"u2-body") >= 1, 20)
        check("U2 带 @STREAM 后缀的消费者能收到消息",
              collector.bodies().count(b"u2-body") >= 1, "got=%s" % collector.bodies())
        # broker 回读：clientId 是心跳里带上来的原值，没有加工
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
        check("U2 broker 看到的 clientId 含 @unitA@STREAM",
              any(i.endswith("@unitA@STREAM") for i in ids), "brokerIds=%s" % ids)
    finally:
        prod.shutdown()
        push.shutdown()

    # ---------------- U3 unitMode → 自动建 topic 带 UNIT 位 ----------------
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

    # 建 topic 是发送链路里同步做的，但路由/配置落地要一点点时间
    _wait_until(lambda: _sys_flag(admin, broker_addr, t_on) is not None, 10)
    flag_on = _sys_flag(admin, broker_addr, t_on)
    flag_off = _sys_flag(admin, broker_addr, t_off)
    check("U3 unitMode=true 建的 topic 带 UNIT 位",
          flag_on is not None and (flag_on & FLAG_UNIT) == FLAG_UNIT,
          "sysFlag=%s" % flag_on)
    check("U3 unitMode=false 的对照 topic 不带 UNIT 位",
          flag_off is not None and not (flag_off & FLAG_UNIT),
          "sysFlag=%s" % flag_off)

    # ---------------- U4 unitMode 消费者心跳 → %RETRY% 带 UNIT_SUB 位 ----------------
    # ⚠ 顺序必须是「先发消息，再起消费者」：Java 与本实现的心跳都只发给
    # **路由表里出现过**的 broker（sendHeartbeatToAllBroker 遍历 topicRouteTable）。
    # 订阅一个还不存在的 topic 时路由为空 → 一条心跳都发不出去 → retry topic
    # 自然不会创建。先生产一条既建好 topic 又让路由可见（30s 内的 broker 注册），
    # 心跳才有落点。
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
        # 心跳在 start() 里同步发一次；失败会被 debug 吞掉，所以只能靠 broker 侧结果判断
        found = _wait_until(
            lambda: (_sys_flag(admin, broker_addr, retry_topic) or 0) & FLAG_UNIT_SUB, 30)
        check("U4 unitMode=true 的 %RETRY% topic 带 UNIT_SUB 位", found,
              "sysFlag=%s" % _sys_flag(admin, broker_addr, retry_topic))
    finally:
        push4.shutdown()

    # ---------------- U5 ReqT=0 不影响普通 broker ----------------
    # 轻量消费者默认 CONSUME_FROM_LAST_OFFSET（Java 同），所以先生产、再起消费者
    # 会把预热消息跳过去；这里显式改成 FROM_FIRST_OFFSET 才能数到 3 条。
    t5 = "UnitCfgStream_%d" % STAMP
    g5 = "GID_unit_cfg_stream_%d" % STAMP
    topics.append(t5)
    groups.append(g5)
    prod5 = _producer("PID_unit_cfg_%d" % STAMP,
                      instance_name="unitcfg-u5p-%d" % STAMP, stream=True)
    prod5.start()
    bodies = []
    try:
        check("U5 开启 stream 的 producer clientId",
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
        check("U5 轻量消费者 clientId 默认带 @STREAM",
              lite.client_id.endswith("@STREAM"), lite.client_id)

        def _drain():
            bodies.extend(m.get_body() for m in lite.poll(timeout=1000))
            return len(bodies) >= 3

        got = _wait_until(_drain, 30)
        check("U5 每个请求都带 ReqT=0 时发送+拉取仍正常", got, "bodies=%s" % bodies)
    finally:
        lite.shutdown()

    # ---------------- 清理 ----------------
    for t in topics:
        try:
            admin.delete_topic(t)
        except Exception as e:  # noqa: BLE001
            print("    (delete_topic(%s) 失败: %s)" % (t, e))
    for g in groups:
        try:
            admin.delete_subscription_group(broker_addr, g, remove_offset=True)
        except Exception as e:  # noqa: BLE001
            print("    (delete_subscription_group(%s) 失败: %s)" % (g, e))
    admin.shutdown()

    passed = sum(1 for _, ok, _ in results if ok)
    print("\n== 结果: %d/%d 通过 ==" % (passed, len(results)))
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
