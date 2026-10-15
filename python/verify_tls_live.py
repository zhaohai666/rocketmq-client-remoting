#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import ssl
import sys
import threading
import time

sys.path.insert(0, ".")

from client.consumer import DefaultMQPushConsumer, SimpleMessageListener  # noqa: E402
from client.consumer_result import ConsumeConcurrentlyStatus  # noqa: E402
from client.producer import DefaultMQProducer  # noqa: E402
from client.send_result import SendStatus  # noqa: E402
from common.message import Message  # noqa: E402
from remoting.client import RemotingClient  # noqa: E402
from remoting.protocol import headers as headers_mod  # noqa: E402
from remoting.protocol import remoting_command as rc_mod  # noqa: E402
from remoting.protocol.codes import RequestCode, ResponseCode  # noqa: E402


def _cli_args(argv: list) -> dict:
    opts = {"leg": "plain", "caCert": "", "serverName": "", "clientCert": "", "clientKey": ""}
    it = iter(argv[2:])
    for flag in it:
        if flag.startswith("--"):
            opts[flag[2:]] = next(it, "")
    return opts


CLI = _cli_args(sys.argv)
LEG = CLI["leg"]
if LEG not in ("plain", "ca_verify", "mtls"):
    print("unknown leg: %s (plain|ca_verify|mtls)" % LEG, file=sys.stderr)
    sys.exit(2)
TLS_OPTIONS = None
if LEG in ("ca_verify", "mtls"):
    TLS_OPTIONS = {"caCert": CLI["caCert"], "serverName": CLI["serverName"]}
    if LEG == "mtls":
        TLS_OPTIONS["clientCert"] = CLI["clientCert"]
        TLS_OPTIONS["clientKey"] = CLI["clientKey"]

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "TlsLive_%s_%d" % (LEG, STAMP)
GROUP = "GID_TlsLive_%s_%d" % (LEG, STAMP)
MSG_NUM = 8
# First-packet budget: a TLS round trip over loopback on real hardware is single-digit ms; waiting out the full invoke timeout (5s) is the fault we are hunting
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
    """Record whether each really-established connection is TLS, return the observation list."""
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

    # ---------------- S1 a new TLS connection per round, hit the first packet ----------------
    print("=== S1 nameServer TLS first packet (%d rounds, a new connection per round, leg=%s) ===" % (ROUNDS, LEG))
    lost = 0
    worst_ms = 0.0
    for i in range(ROUNDS):
        client = RemotingClient(tls_enable=True, tls_options=TLS_OPTIONS)
        began = time.monotonic()
        try:
            # When a new topic does not exist the nameServer returns TOPIC_NOT_EXIST(17) -- that is still a real reply,
            # the first packet has landed; only failing to get any reply (timeout/disconnect) counts as lost.
            client.invoke_sync(NAMESRV, route_request("%s_%d" % (TOPIC, i)), 5000)
            elapsed = (time.monotonic() - began) * 1000.0
            worst_ms = max(worst_ms, elapsed)
        except Exception as e:  # noqa: BLE001
            lost += 1
            print("  round %02d first-packet failure %s: %s (%.0fms)"
                  % (i, type(e).__name__, e, (time.monotonic() - began) * 1000.0))
        finally:
            client.shutdown()
    check("S1a %d rounds of new TLS connections all landed the first packet" % ROUNDS, lost == 0,
          "lost=%d worst=%.0fms" % (lost, worst_ms))
    check("S1b first-packet time well away from the invoke timeout", worst_ms < FIRST_PACKET_BUDGET_MS,
          "worst=%.0fms budget=%.0fms" % (worst_ms, FIRST_PACKET_BUDGET_MS))

    # ---------------- S2 end-to-end TLS send/receive ----------------
    print("=== S2 producer + push consumer over TLS throughout ===")
    prod = DefaultMQProducer(GROUP + "_P", tls_enable=True, tls_options=TLS_OPTIONS)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    consumer = None
    try:
        prod.create_topic("TBW102", TOPIC, 4)
        check("S2a topic route works under TLS", wait_route(prod, TOPIC, 4), "topic=%s" % TOPIC)

        received = []
        consumer = DefaultMQPushConsumer(GROUP, tls_enable=True, tls_options=TLS_OPTIONS)
        consumer.set_namesrv_addr(NAMESRV)
        consumer.subscribe(TOPIC, "*")

        def _collect(msgs):
            received.extend(m.get_keys() for m in msgs)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

        consumer.set_message_listener(SimpleMessageListener(_collect))
        consumer.start()
        # A new group consumes from LAST_OFFSET; sending before the consumer is stable will miss messages.
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
        check("S2b TLS sent %d messages all SEND_OK" % MSG_NUM, send_ok == MSG_NUM,
              "send_ok=%d" % send_ok)

        deadline = time.time() + 60
        while time.time() < deadline and len(set(received) & set(keys)) < MSG_NUM:
            time.sleep(0.5)
        got = set(received) & set(keys)
        check("S2c TLS consumption received all %d messages" % MSG_NUM, len(got) == MSG_NUM,
              "got=%d/%d" % (len(got), MSG_NUM))
    finally:
        if consumer is not None:
            consumer.shutdown()
        prod.shutdown()

    # ---------------- S3 confirm TLS is really in use, and no read threads are left ----------------
    print("=== S3 connection accounting ===")
    tls_addrs = sorted({a for a, is_tls in observed if is_tls})
    plain_addrs = sorted({a for a, is_tls in observed if not is_tls})
    check("S3a the end-to-end path really established TLS connections", bool(tls_addrs), "tls=%s" % tls_addrs)
    check("S3b no connection quietly fell back to plaintext", not plain_addrs, "plain=%s" % plain_addrs)
    leftover = [t for t in threading.enumerate() if t.name.startswith("rmq-read-")]
    check("S3c no leftover read threads after shutdown", not leftover,
          "leftover=%s" % [t.name for t in leftover])

    print("\nTLS live verification: %d PASS / %d FAIL" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
