# -*- coding: utf-8 -*-
"""路由刷新周期 / 位点落盘周期 真机验证（Python 参考实现）。

对应用户要求"基于真实集群测试是否正常"：``pollNameServerInterval``（Java 默认 30s）与
``persistConsumerOffsetInterval``（Java 默认 5s）都是**以时间为唯一可观测量**的配置项，
不接真集群就只能断言"字段被读到了"，证明不了"周期真的按配置走"。这里两段都用真机行为断言：

  I1  路由刷新周期：两个生产者同时 start，周期分别设 1000ms 与 Java 默认 30000ms；两者都
      用 ``MQClientInstance.register_topic_in_use`` 把一个**尚未创建**的 topic 登记进周期
      刷新集合。之后才用 admin 建 topic（NameServer 侧路由立即可见，已实测 1ms 内），于是
      "缓存里何时出现这个 topic"只由各自的刷新周期决定：
        - 1s 组 ≤6s 看到；
        - 30s 组在那一刻**还看不到**（下一次刷新在启动后 30s）；
        - 30s 组最终也在 ≤40s 内看到（默认值不是"卡死"，只是慢 30 倍）。

  I2  位点落盘周期：两个消费者（周期 1000ms / 60000ms）消费同一 topic 的 3 条消息后
      **不 commit、不 shutdown**，broker 侧位点只能由后台周期任务推上去。断言：
        - 首次落盘发生在 start 后 ~10s（Java ``scheduleAtFixedRate`` 的 initialDelay
          1000*10，不是立刻）；
        - 再发 3 条：1s 组在 5s 内把位点推到 6；此刻 60s 组仍是 3（它的下一次落盘在 ~70s）；
        - 60s 组 ``shutdown()`` 时把 6 落盘（Java persistConsumerOffset 的收尾语义），
          证明它只是"周期没到"，不是坏了。

  I3  透传：生产者/消费者 start 之后，真机实例上的 ``poll_name_server_interval`` 就是调用方
      设的值（不是只在 facade 上存着）。

前置：NameServer + Broker 已起（本仓库 /tmp/rmq_rust_live/broker.conf）。

用法：.venv/bin/python verify_interval_live.py [127.0.0.1:9876]
"""
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.consumer import DefaultMQPushConsumer
from rocketmq.client.consumer_result import (ConsumeConcurrentlyStatus,
                                             MessageListenerConcurrently)
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message
from rocketmq.common.mix_all import MixAll
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time())
FAST_POLL_MS = 1000
SLOW_POLL_MS = 30000        # Java ClientConfig.pollNameServerInterval 默认值
FAST_PERSIST_MS = 1000
SLOW_PERSIST_MS = 60000
PERSIST_INITIAL_DELAY = 10.0  # Java startScheduledTask:423 的 initialDelay = 1000 * 10

results = []
skips = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def _wait_until(pred, timeout_sec, interval=0.2):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


class _Collector(MessageListenerConcurrently):
    def __init__(self):
        self.bodies = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context=None):
        with self._lock:
            self.bodies.extend(bytes(m.body) for m in msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def count(self):
        with self._lock:
            return len(self.bodies)


def _route_cached(producer, topic):
    return topic in producer._mq_client.topic_route_table


def _broker_offset(admin, group, mq):
    """broker 上该 group 这条队列的已提交位点（``set_zero_if_not_found=True`` ⇒ 没提交过回 0）。"""
    off = admin._require_client().query_consumer_offset(
        group, mq, timeout_millis=5000, set_zero_if_not_found=True)
    return int(off or 0)


def _first_queue(admin, topic):
    publish = admin._require_client().get_topic_publish_info(topic)
    return publish.msg_queue_list[0]


def main():
    t_poll = "IntervalPollTopic_%d" % STAMP
    t_persist = "IntervalPersistTopic_%d" % STAMP
    g_poll_fast, g_poll_slow = "G_poll_fast_%d" % STAMP, "G_poll_slow_%d" % STAMP
    g_fast, g_slow = "G_persist_fast_%d" % STAMP, "G_persist_slow_%d" % STAMP
    groups = [g_poll_fast, g_poll_slow, g_fast, g_slow]
    topics = [t_poll, t_persist]

    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.start()
    cluster = admin.fetch_broker_cluster_info()
    if not cluster or not cluster.get_broker_addrs():
        check("集群探活", False, "nameServer 无 broker 注册")
        admin.shutdown()
        return 1
    broker_addr = cluster.get_broker_addrs()[0]
    check("集群探活", True, "broker=%s" % broker_addr)

    producer = None
    consumers = []
    try:
        # ---------------- I1 路由刷新周期 ----------------
        fast = DefaultMQProducer(g_poll_fast)
        fast.set_namesrv_addr(NAMESRV)
        fast.set_instance_name("interval-poll-fast-%d" % STAMP)
        fast.poll_name_server_interval = FAST_POLL_MS
        fast.start()
        slow = DefaultMQProducer(g_poll_slow)
        slow.set_namesrv_addr(NAMESRV)
        slow.set_instance_name("interval-poll-slow-%d" % STAMP)
        slow.start()

        check("I3 生产者实例拿到配置的刷新周期（1s 组）",
              fast._mq_client.poll_name_server_interval == FAST_POLL_MS,
              "client.poll_name_server_interval=%s" % fast._mq_client.poll_name_server_interval)
        check("I3 生产者实例默认 30s（对照组）",
              slow._mq_client.poll_name_server_interval == SLOW_POLL_MS,
              "client.poll_name_server_interval=%s" % slow._mq_client.poll_name_server_interval)

        # 把一个还没创建的 topic 登记进周期刷新集合：两个生产者都不会给它发消息，
        # 所以缓存里何时出现它，只由各自的刷新周期决定。
        for p in (fast, slow):
            p._mq_client.register_topic_in_use(t_poll)
            check("I1 %s 已把 %s 登记进在用 topic 集合" % (p.instance_name, t_poll),
                  t_poll in p._mq_client._topics_in_use)
        check("I1 topic 创建前两边缓存都为空",
              not _route_cached(fast, t_poll) and not _route_cached(slow, t_poll))

        t0 = time.time()
        admin.create_topic(MixAll.DEFAULT_TOPIC, t_poll, 1)
        check("I1 admin 建 topic 成功", True, "%s 1 队列" % t_poll)

        fast_ok = _wait_until(lambda: _route_cached(fast, t_poll), 6.0, 0.1)
        dt_fast = time.time() - t0
        check("I1 1s 周期组 ≤6s 从 NameServer 拉到新 topic 路由", fast_ok,
              "dt=%.2fs 周期=%dms" % (dt_fast, FAST_POLL_MS))
        check("I1 此刻 30s 周期组**还**没拉到（对照：周期决定时机）",
              not _route_cached(slow, t_poll),
              "dt=%.2fs 周期=%dms" % (dt_fast, SLOW_POLL_MS))

        slow_ok = _wait_until(lambda: _route_cached(slow, t_poll), 40.0, 0.5)
        dt_slow = time.time() - t0
        check("I1 30s 周期组最终也拉到（默认值只是慢，不是坏）",
              slow_ok, "dt=%.2fs 周期=%dms" % (dt_slow, SLOW_POLL_MS))
        check("I1 两组间隔与配置同量级（30s 组至少晚 20s）", dt_slow - dt_fast >= 20.0,
              "fast=%.2fs slow=%.2fs" % (dt_fast, dt_slow))
        fast.shutdown()
        slow.shutdown()

        # ---------------- I2 位点落盘周期 ----------------
        admin.create_topic(MixAll.DEFAULT_TOPIC, t_persist, 1)
        mq = _first_queue(admin, t_persist)
        producer = DefaultMQProducer("G_persist_producer_%d" % STAMP)
        producer.set_namesrv_addr(NAMESRV)
        producer.set_instance_name("interval-persist-prod-%d" % STAMP)
        producer.start()
        for i in range(3):
            producer.send(Message(t_persist, ("batch1-%d" % i).encode()))

        fast_c = DefaultMQPushConsumer(g_fast)
        fast_c.set_namesrv_addr(NAMESRV)
        fast_c.set_instance_name("interval-persist-fast-%d" % STAMP)
        fast_c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        fast_c.poll_name_server_interval = 2000
        fast_c.persist_consumer_offset_interval = FAST_PERSIST_MS
        listener_fast = _Collector()
        fast_c.subscribe(t_persist, "*")
        fast_c.set_message_listener(listener_fast)

        slow_c = DefaultMQPushConsumer(g_slow)
        slow_c.set_namesrv_addr(NAMESRV)
        slow_c.set_instance_name("interval-persist-slow-%d" % STAMP)
        slow_c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        slow_c.persist_consumer_offset_interval = SLOW_PERSIST_MS
        listener_slow = _Collector()
        slow_c.subscribe(t_persist, "*")
        slow_c.set_message_listener(listener_slow)

        t_start = time.time()
        fast_c.start()
        consumers.append(fast_c)
        check("I3 消费者实例拿到配置的刷新周期",
              fast_c._mq_client.poll_name_server_interval == 2000,
              "client.poll_name_server_interval=%s" % fast_c._mq_client.poll_name_server_interval)
        slow_c.start()
        consumers.append(slow_c)

        check("I2 两个消费者都消费到 3 条（尚未 commit / shutdown）",
              _wait_until(lambda: listener_fast.count() >= 3 and listener_slow.count() >= 3, 30.0),
              "fast=%d slow=%d" % (listener_fast.count(), listener_slow.count()))
        check("I2 消费后 broker 位点还没有立刻被推上去",
              _broker_offset(admin, g_fast, mq) < 3 and _broker_offset(admin, g_slow, mq) < 3,
              "fast=%d slow=%d" % (_broker_offset(admin, g_fast, mq),
                                   _broker_offset(admin, g_slow, mq)))

        fast_first = _wait_until(lambda: _broker_offset(admin, g_fast, mq) == 3, 20.0, 0.3)
        dt_fast_first = time.time() - t_start
        slow_first = _wait_until(lambda: _broker_offset(admin, g_slow, mq) == 3, 20.0, 0.3)
        dt_slow_first = time.time() - t_start
        check("I2 1s 组首次落盘发生在 initialDelay(~10s) 之后", fast_first,
              "dt=%.2fs 周期=%dms" % (dt_fast_first, FAST_PERSIST_MS))
        check("I2 首笔落盘不早于 Java 的 initialDelay 10s",
              fast_first and dt_fast_first >= PERSIST_INITIAL_DELAY - 0.5,
              "dt=%.2fs" % dt_fast_first)
        check("I2 60s 组同样在 ~10s 完成首笔落盘（周期未到，先走 initialDelay）", slow_first,
              "dt=%.2fs 周期=%dms" % (dt_slow_first, SLOW_PERSIST_MS))

        # 第二批：两组都会消费到，但 broker 侧位点只由各自的周期任务推上去。
        t2 = time.time()
        for i in range(3):
            producer.send(Message(t_persist, ("batch2-%d" % i).encode()))
        check("I2 两个消费者都消费到第二批（6 条）",
              _wait_until(lambda: listener_fast.count() >= 6 and listener_slow.count() >= 6, 20.0),
              "fast=%d slow=%d" % (listener_fast.count(), listener_slow.count()))
        fast_second = _wait_until(lambda: _broker_offset(admin, g_fast, mq) == 6, 5.0, 0.2)
        dt_second = time.time() - t2
        check("I2 1s 组一个周期内把第二批位点推上去", fast_second,
              "dt=%.2fs 周期=%dms" % (dt_second, FAST_PERSIST_MS))
        check("I2 此刻 60s 组仍是 3（周期 60s 远未到，且它确实消费到了 6）",
              _broker_offset(admin, g_slow, mq) == 3 and listener_slow.count() >= 6,
              "broker=%d 已消费=%d 周期=%dms" % (_broker_offset(admin, g_slow, mq),
                                                listener_slow.count(), SLOW_PERSIST_MS))
        dt_slow_first_abs = t2 + dt_second - t_start
        check("I2 60s 组的下一次周期还没到（距首笔落盘 < 周期 60s）",
              dt_slow_first_abs - dt_slow_first < SLOW_PERSIST_MS / 1000.0,
              "elapsed=%.2fs" % (dt_slow_first_abs - dt_slow_first))

        slow_c.shutdown()
        consumers.remove(slow_c)
        slow_final = _wait_until(lambda: _broker_offset(admin, g_slow, mq) == 6, 10.0, 0.3)
        check("I2 60s 组 shutdown() 时把 6 落盘（Java persistConsumerOffset 收尾）",
              slow_final, "broker=%d" % _broker_offset(admin, g_slow, mq))
    except Exception as exc:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        check("验证过程抛出异常", False, "%s: %s" % (type(exc).__name__, exc))
    finally:
        # ---------------- 清理 ----------------
        for c in consumers:
            try:
                c.shutdown()
            except Exception as e:  # noqa: BLE001
                print("    (consumer shutdown 失败: %s)" % e)
        if producer is not None:
            try:
                producer.shutdown()
            except Exception as e:  # noqa: BLE001
                print("    (producer shutdown 失败: %s)" % e)
        for t in topics:
            try:
                admin.delete_topic(t)
                print("    (delete_topic(%s) OK)" % t)
            except Exception as e:  # noqa: BLE001
                print("    (delete_topic(%s) 失败: %s)" % (t, e))
        for g in groups:
            try:
                admin.delete_subscription_group(broker_addr, g, remove_offset=True)
            except Exception as e:  # noqa: BLE001
                print("    (delete_subscription_group(%s) 失败: %s)" % (g, e))
        admin.shutdown()

    failed = [r for r in results if not r[1]]
    print("\n================ 汇总 ================")
    for name, ok, detail in results:
        print("  [%s] %s  %s" % ("PASS" if ok else "FAIL", name, detail))
    for name, detail in skips:
        print("  [SKIP] %s  %s" % (name, detail))
    print("======================================")
    print("总计 %d 项，失败 %d 项，跳过 %d 项" % (len(results), len(failed), len(skips)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
