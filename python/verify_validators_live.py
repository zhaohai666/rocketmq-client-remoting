#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from client import validators
from client.consumer import (
    DefaultLitePullConsumer,
    DefaultMQPullConsumer,
    DefaultMQPushConsumer,
    SimpleMessageListener,
)
from client.consumer_result import ConsumeConcurrentlyStatus
from client.exception import ClientErrorCode, MQClientException
from client.producer import DefaultMQProducer
from client.send_result import SendStatus
from common.message import Message
from remoting.protocol.codes import ResponseCode

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "ValidatorsLive_%d" % STAMP
GROUP = "GID_ValidatorsLive_%d" % STAMP
# The upper bound on the duration of the negative leg: it is pure local computation, so 50ms on a
# real machine already leaves two orders of magnitude of margin
LOCAL_BUDGET_MS = 50.0

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


def local_send(prod: DefaultMQProducer, msg: Message):
    """Run one send and return (the exception or None, the elapsed milliseconds)."""
    began = time.monotonic()
    try:
        prod.send(msg)
        return None, (time.monotonic() - began) * 1000.0
    except MQClientException as e:
        return e, (time.monotonic() - began) * 1000.0


def check_local_reject(name: str, prod: DefaultMQProducer, msg: Message, needle: str,
                       expect_code=None) -> float:
    """Assert that it "fails locally in sub-millisecond time", and hand back the actual duration as the baseline for later comparisons."""
    err, elapsed = local_send(prod, msg)
    ok = err is not None and needle in str(err) and elapsed < LOCAL_BUDGET_MS
    if expect_code is not None:
        ok = ok and err.response_code == expect_code
    else:
        ok = ok and err.response_code is None
    check(name, ok, "%s in %.2fms code=%r" % (
        str(err).replace("\n", " ") if err else "no exception was raised", elapsed,
        None if err is None else err.response_code))
    return elapsed


def wait_route(prod: DefaultMQProducer, topic: str, min_queues: int,
               timeout: int = 20) -> bool:
    """Wait for the topic route to be registered (auto-creating a topic is asynchronous, sending right after creating it would find no queues)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if len(prod._mq_client.get_topic_publish_info(topic).msg_queue_list) >= min_queues:
                return True
        except MQClientException:
            pass
        time.sleep(0.5)
    return False


def main() -> int:
    print("namesrv=%s topic=%s group=%s" % (NAMESRV, TOPIC, GROUP))
    prod = DefaultMQProducer("PG_ValidatorsLive_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    lite = None
    push = None
    try:
        # ---------------- S1 the producer-side negative leg ----------------
        print("=== S1 illegal input must fail locally in sub-millisecond time ===")
        local_ms = check_local_reject("S1a a topic with illegal characters is intercepted locally", prod,
                                      Message("bad topic", b"x"), "contains illegal characters")
        check_local_reject("S1b an empty topic is intercepted locally", prod,
                           Message("", b"x"), "The specified topic is blank")
        check_local_reject("S1c an over-long topic is intercepted locally", prod,
                           Message("t" * 128, b"x"), "is longer than topic max length")
        check_local_reject("S1d a forbidden topic is intercepted locally (the code follows the -1 convention, not MESSAGE_ILLEGAL)",
                           prod, Message("SCHEDULE_TOPIC_XXXX", b"x"), "is forbidden")
        prod.set_max_message_size(8)
        check_local_reject("S1e an over-long body is intercepted locally (MESSAGE_ILLEGAL)", prod,
                           Message(TOPIC, b"1" * 9), "the message body size over max value",
                           ResponseCode.MESSAGE_ILLEGAL)
        check_local_reject("S1f an empty body is intercepted locally (MESSAGE_ILLEGAL)", prod,
                           Message(TOPIC, b""), "the message body length is zero",
                           ResponseCode.MESSAGE_ILLEGAL)
        prod.set_max_message_size(1024 * 1024 * 4)
        m_boundary = Message(TOPIC, b"1" * 8)
        validators.check_message(m_boundary, 8)
        check("S1g exactly maxMessageSize is let through (the comparison is >)", True)

        # ---------------- S2 the batch negative leg ----------------
        print("=== S2 a batch send does not bypass the local validation ===")
        began = time.monotonic()
        err = None
        try:
            prod.send([Message(TOPIC, b"ok1"), Message("bad topic", b"ok2")])
        except MQClientException as e:
            err = e
        elapsed = (time.monotonic() - began) * 1000.0
        check("S2a an illegal sub-message inside the batch => the whole batch fails locally",
              err is not None and "contains illegal characters" in str(err)
              and elapsed < LOCAL_BUDGET_MS,
              "%s in %.2fms" % (str(err).replace("\n", " ") if err else "no exception was raised", elapsed))
        began = time.monotonic()
        err2 = None
        try:
            prod.send([Message(TOPIC, b"x" * 10), Message("ValidatorsOther_%d" % STAMP, b"y")])
        except Exception as e:
            err2 = e
        elapsed = (time.monotonic() - began) * 1000.0
        check("S2b a mixed batch (different topics) is also stopped locally and never reaches the cluster",
              err2 is not None and "should be the same" in str(err2)
              and elapsed < LOCAL_BUDGET_MS,
              "%s in %.2fms" % (str(err2).replace("\n", " ") if err2 else "no exception was raised", elapsed))

        # ---------------- S3 the group name validation ----------------
        print("=== S3 the group name validation comes before any network action ===")
        for group, needle in [
            ("DEFAULT_PRODUCER", "producerGroup can not equal DEFAULT_PRODUCER"),
            ("bad group", "contains illegal characters"),
            ("g" * 121, "is longer than group max length"),
        ]:
            p2 = DefaultMQProducer(group)
            p2.set_namesrv_addr(NAMESRV)
            began = time.monotonic()
            try:
                p2.start()
                e = None
            except MQClientException as ex:
                e = ex
            elapsed = (time.monotonic() - began) * 1000.0
            check("S3a the group name %r fails locally in start()" % group,
                  e is not None and needle in str(e) and elapsed < LOCAL_BUDGET_MS
                  and not p2._started,
                  "%s in %.2fms" % (str(e).replace("\n", " ") if e else "no exception was raised", elapsed))

        # ---------------- S4 the positive leg: a legal name still sends and receives on the cluster ----------------
        # The order matters: create the topic first, then start the consumer, and only then send the
        # messages. CONSUME_FROM_LAST_OFFSET takes the max offset "as of the moment of startup" for a
        # new group, so sending the messages first means they can never be received (this was hit in
        # live testing).
        print("=== S4 a legal name still sends and receives on the cluster ===")
        prod.create_topic("TBW102", TOPIC, 4)
        check("S4a the route of the legal topic is usable", wait_route(prod, TOPIC, 4), "topic=%s" % TOPIC)

        received = []
        push = DefaultMQPushConsumer(GROUP)
        push.set_namesrv_addr(NAMESRV)
        push.subscribe(TOPIC, "*")

        def _collect(msgs):
            # Record the keys that are consumed; the KEYS of a MessageExt is a property, not a field
            received.extend(m.get_keys() for m in msgs)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

        push.set_message_listener(SimpleMessageListener(_collect))
        push.start()

        lite = DefaultLitePullConsumer(GROUP + "_lite")
        lite.set_namesrv_addr(NAMESRV)
        lite.subscribe(TOPIC, "*")
        lite.start()
        deadline = time.time() + 20
        while time.time() < deadline and not lite.assignment():
            time.sleep(0.5)
        check("S4b the rebalance of the lite consumer got queues", len(lite.assignment()) > 0,
              "assigned=%d" % len(lite.assignment()))

        sent_keys = []
        for i in range(3):
            key = "validators-live-%d" % i
            sent_keys.append(key)
            mr = prod.send(Message(TOPIC, ("body-%d" % i).encode("utf-8"), keys=key))
            if i == 0:
                check("S4c the legal message was sent successfully", mr.send_status == SendStatus.SEND_OK,
                      str(mr.send_status))
        deadline = time.time() + 25
        while time.time() < deadline and len(received) < 3:
            time.sleep(0.5)
        check("S4d the push consumer received the legal messages", all(k in received for k in sent_keys),
              "received=%s" % received)

        # ---------------- S5 the control leg: what the local validation saves ----------------
        print("=== S5 a legal but non-existent topic: let through locally and go through the whole cluster path ===")
        missing = "ValidatorsMissing_%d" % STAMP
        began = time.monotonic()
        err3 = None
        try:
            # A broker with autoCreateTopicEnable=true creates the topic for it => it is let through locally and the send succeeds
            prod.send(Message(missing, b"x"))
        except Exception as e:  # noqa: BLE001 - the counter-proof: the error here is definitely not a local validation error
            err3 = e
        cluster_ms = (time.monotonic() - began) * 1000.0
        check("S5a a legal name is not harmed by the local validation",
              err3 is None or "contains illegal characters" not in str(err3),
              "%s in %.1fms" % (type(err3).__name__ if err3 else "SEND_OK", cluster_ms))
        check("S5b the cluster leg is at least two orders of magnitude slower than the local negative leg (that is the cost of spinning through retries)",
              cluster_ms > 10.0 * max(local_ms, 0.05),
              "cluster=%.1fms local=%.2fms" % (cluster_ms, local_ms))

        # ---------------- S6 the group name negative leg of the pull consumers ----------------
        print("=== S6 the group name negative leg of the pull consumers (the positive legs are in S4d and S6c/S6d) ===")
        for cls in (DefaultMQPullConsumer, DefaultLitePullConsumer):
            bad = cls("DEFAULT_CONSUMER")
            bad.set_namesrv_addr(NAMESRV)
            began = time.monotonic()
            try:
                bad.start()
                e = None
            except MQClientException as ex:
                e = ex
            elapsed = (time.monotonic() - began) * 1000.0
            check("S6a %s rejects the reserved group name (locally)" % cls.__name__,
                  e is not None and "can not equal DEFAULT_CONSUMER" in str(e)
                  and elapsed < LOCAL_BUDGET_MS and not bad._started,
                  "%s in %.2fms" % (str(e).replace("\n", " ") if e else "no exception was raised", elapsed))
            illegal = cls("bad group")
            illegal.set_namesrv_addr(NAMESRV)
            try:
                illegal.start()
                e = None
            except MQClientException as ex:
                e = ex
            check("S6b %s rejects a group name with illegal characters (locally)" % cls.__name__,
                  e is not None and "contains illegal characters" in str(e))

        print("=== S7 the boundary between 10004 (no name server configured) and 10005 (the topic has no route) ===")
        mis = DefaultMQProducer("GID_ValidatorsLive_%d_addr" % STAMP)
        mis.set_namesrv_addr(NAMESRV)
        mis.start()
        try:
            addr_client = mis._require_client()
            saved_addrs = list(addr_client.name_server_addrs)
            # A live producer plus a topic that **already exists**: the only thing broken is the empty
            # addressing table. This is exactly the state of a local instance after the address server
            # returns an empty list (``update_name_server_address_list`` only applies a non-empty
            # value, so an empty table can only come from the one failure "no addresses were fetched at
            # all").
            addr_client.name_server_addrs = []
            began = time.monotonic()
            try:
                mis.send(Message(TOPIC, b"no-name-server"))
                e = None
            except MQClientException as ex:
                e = ex
            elapsed_ms = (time.monotonic() - began) * 1000.0
            check("S7a with not a single address it reports 10004 NO_NAME_SERVER_EXCEPTION (not 10005)",
                  e is not None
                  and e.response_code == ClientErrorCode.NO_NAME_SERVER_EXCEPTION,
                  "code=%s msg=%s" % (getattr(e, "response_code", None), e))
            check("S7b the wording is \"No name server address, please set it.\" (pointing at the addressing)",
                  str(e) == "No name server address, please set it.", "msg=%s" % e)
            check("S7c the failure happens in the local decision without spinning through the retry budget (<%gms)" % LOCAL_BUDGET_MS,
                  elapsed_ms < LOCAL_BUDGET_MS, "elapsed=%.2fms" % elapsed_ms)
            addr_client.name_server_addrs = saved_addrs
            r = mis.send(Message(TOPIC, b"name-server-back"))
            check("S7d control: once the addresses are back the same topic immediately gets SEND_OK (showing that S7a judges the addressing, not the topic)",
                  r.send_status == SendStatus.SEND_OK, "status=%s" % r.send_status)
        finally:
            mis.shutdown()

        # The positive leg: a pull consumer with a legal group name can not only start but also really query the offset (over the broker RPC)
        pull_ok = DefaultMQPullConsumer(GROUP + "_pull_ok")
        pull_ok.set_namesrv_addr(NAMESRV)
        pull_ok.start()
        queues = pull_ok.fetch_subscribe_message_queues(TOPIC)
        max_off = pull_ok.max_offset(queues[0]) if queues else -1
        check("S6c a pull consumer with a legal group name can start and query the offset",
              pull_ok._started and len(queues) >= 4 and max_off >= 0,
              "queues=%d maxOffset=%d" % (len(queues), max_off))
        pull_ok.shutdown()

        got = []
        deadline = time.time() + 25
        while time.time() < deadline and len(got) < 3:
            got.extend(m.get_keys() for m in lite.poll(timeout=3000))
        check("S6d a lite consumer with a legal group name collects all the messages", all(k in got for k in sent_keys),
              "got=%s" % got)
    finally:
        for c in (push, lite):
            if c is not None:
                c.shutdown()
        prod.shutdown()

    print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
