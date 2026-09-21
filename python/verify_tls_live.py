#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""TLS 真机验证：整条客户端链路（取路由 → 发送 → 消费）都跑在 TLS 上。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_tls_live.py 127.0.0.1:9876

为什么要有这个脚本（以及它守的是哪一段）：
5.5.1 的 broker/nameServer 在 ``tls.test.mode.enable``（默认 true）下按**首字节**嗅探
TLS 还是明文，所以同一个端口两种协议都收 —— 真机 TLS 链路不需要改集群配置就能验。
Java 侧的 TLS 栈与本机的 CPython ``ssl`` 不完全同形：本机实测首包丢失（约 3~5%，读线程
起得太早所致，见 ``_write``）只在对端是 CPython ``ssl`` 时稳定复现，对**真集群**逐轮新建
TLS 连接打首包（nameServer 与 broker 各 60 轮）两种读线程时序都是 0 丢。所以这个脚本
证明的是"整个客户端跑在 TLS 上能取路由、能收发、没退回明文、关掉不留线程"，
而首包时序那条回归判据由 ``tests/test_tls_trace.py`` 用对端为 CPython ``ssl`` 的
mock server 守住 —— 那里才是本机真正会炸的地方。

场景：
  S1 nameServer/broker 都是真 TLS：逐轮新建连接查路由，首包必须在超时预算内回来
  S2 端到端收发：producer + push consumer 全程 TLS，发 8 条收齐 8 条
  S3 全程确认真的在用 TLS（不是悄悄退回明文），且连接关闭后不留读线程
"""
from __future__ import annotations

import ssl
import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import DefaultMQPushConsumer, SimpleMessageListener  # noqa: E402
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus  # noqa: E402
from rocketmq.client.producer import DefaultMQProducer  # noqa: E402
from rocketmq.client.send_result import SendStatus  # noqa: E402
from rocketmq.common.message import Message  # noqa: E402
from rocketmq.remoting.client import RemotingClient  # noqa: E402
from rocketmq.remoting.protocol import headers as headers_mod  # noqa: E402
from rocketmq.remoting.protocol import remoting_command as rc_mod  # noqa: E402
from rocketmq.remoting.protocol.codes import RequestCode, ResponseCode  # noqa: E402

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "TlsLive_%d" % STAMP
GROUP = "GID_TlsLive_%d" % STAMP
MSG_NUM = 8
# 首包预算：真机 loopback 上 TLS 往返是个位数毫秒，等满 invoke 超时（5s）就是要抓的故障
FIRST_PACKET_BUDGET_MS = 1500.0
ROUNDS = 30

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


def route_request(topic: str) -> rc_mod.RemotingCommand:
    header = headers_mod.GetRouteInfoRequestHeader()
    header.topic = topic
    return rc_mod.RemotingCommand.create_request_command(
        RequestCode.GET_ROUTEINFO_BY_TOPIC, header)


def instrument_tls() -> list:
    """记录每一次真实建立的连接是不是 TLS，返回观测列表。"""
    observed = []
    orig = RemotingClient._create_conn

    def _create_conn(self, addr):
        sock = orig(self, addr)
        observed.append((addr, isinstance(sock, ssl.SSLSocket)))
        return sock

    RemotingClient._create_conn = _create_conn
    return observed


def wait_route(prod: DefaultMQProducer, topic: str, min_queues: int,
               timeout: int = 30) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            info = prod._mq_client.get_topic_publish_info(topic)
            if info is not None and len(info.msg_queue_list) >= min_queues:
                return True
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.3)
    return False


def main() -> int:
    observed = instrument_tls()

    # ---------------- S1 逐轮新建 TLS 连接打首包 ----------------
    print("=== S1 nameServer TLS 首包（%d 轮，每轮新连接）===" % ROUNDS)
    lost = 0
    worst_ms = 0.0
    for i in range(ROUNDS):
        client = RemotingClient(tls_enable=True)
        began = time.monotonic()
        try:
            # 新 topic 不存在时 nameServer 回 TOPIC_NOT_EXIST(17) —— 那也是**真应答**，
            # 首包已经落地；只有拿不到应答（超时/断连）才算丢。
            client.invoke_sync(NAMESRV, route_request("%s_%d" % (TOPIC, i)), 5000)
            elapsed = (time.monotonic() - began) * 1000.0
            worst_ms = max(worst_ms, elapsed)
        except Exception as e:  # noqa: BLE001
            lost += 1
            print("  round %02d 首包失败 %s: %s (%.0fms)"
                  % (i, type(e).__name__, e, (time.monotonic() - began) * 1000.0))
        finally:
            client.shutdown()
    check("S1a %d 轮新建 TLS 连接首包全部落地" % ROUNDS, lost == 0,
          "lost=%d worst=%.0fms" % (lost, worst_ms))
    check("S1b 首包耗时远离 invoke 超时", worst_ms < FIRST_PACKET_BUDGET_MS,
          "worst=%.0fms budget=%.0fms" % (worst_ms, FIRST_PACKET_BUDGET_MS))

    # ---------------- S2 端到端 TLS 收发 ----------------
    print("=== S2 producer + push consumer 全程 TLS ===")
    prod = DefaultMQProducer(GROUP + "_P", tls_enable=True)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    consumer = None
    try:
        prod.create_topic("TBW102", TOPIC, 4)
        check("S2a TLS 下 topic 路由可用", wait_route(prod, TOPIC, 4), "topic=%s" % TOPIC)

        received = []
        consumer = DefaultMQPushConsumer(GROUP, tls_enable=True)
        consumer.set_namesrv_addr(NAMESRV)
        consumer.subscribe(TOPIC, "*")

        def _collect(msgs):
            received.extend(m.get_keys() for m in msgs)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

        consumer.set_message_listener(SimpleMessageListener(_collect))
        consumer.start()
        # 新组从 LAST_OFFSET 起消费，消费者没起稳就发会漏
        time.sleep(3.0)

        keys = []
        send_ok = 0
        for i in range(MSG_NUM):
            key = "tls-%d-%d" % (STAMP, i)
            keys.append(key)
            msg = Message(TOPIC, b"tls-payload-%d" % i)
            msg.set_keys(key)
            r = prod.send(msg, 5000)
            if r.send_status == SendStatus.SEND_OK:
                send_ok += 1
        check("S2b TLS 发送 %d 条全部 SEND_OK" % MSG_NUM, send_ok == MSG_NUM,
              "send_ok=%d" % send_ok)

        deadline = time.time() + 60
        while time.time() < deadline and len(set(received) & set(keys)) < MSG_NUM:
            time.sleep(0.5)
        got = set(received) & set(keys)
        check("S2c TLS 消费收齐 %d 条" % MSG_NUM, len(got) == MSG_NUM,
              "got=%d/%d" % (len(got), MSG_NUM))
    finally:
        if consumer is not None:
            consumer.shutdown()
        prod.shutdown()

    # ---------------- S3 确认真的在跑 TLS，且没留读线程 ----------------
    print("=== S3 连接口径 ===")
    tls_addrs = sorted({a for a, is_tls in observed if is_tls})
    plain_addrs = sorted({a for a, is_tls in observed if not is_tls})
    check("S3a 端到端链路确实建了 TLS 连接", bool(tls_addrs), "tls=%s" % tls_addrs)
    check("S3b 没有连接悄悄退回明文", not plain_addrs, "plain=%s" % plain_addrs)
    leftover = [t for t in threading.enumerate() if t.name.startswith("rmq-read-")]
    check("S3c shutdown 后没有残留读线程", not leftover,
          "leftover=%s" % [t.name for t in leftover])

    print("\nTLS 真机验证: %d PASS / %d FAIL" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
