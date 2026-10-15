#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.consumer import (ConsumeConcurrentlyStatus, DefaultMQPushConsumer,
                                      SimpleMessageListener)
from client.exception import (ClientErrorCode, MQClientException,
                                        RequestTimeoutException)
from client.producer import DefaultMQProducer
from client.request_reply import create_reply_message
from common.message import Message
from common.message_const import MessageConst
from common.mix_all import MixAll
from remoting.protocol.codes import ResponseCode
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "RequestReply_%d" % STAMP
REQUEST_GROUP = "PG_RRReq_%d" % STAMP
CONSUMER_GROUP = "CG_RRReply_%d" % STAMP
WATCH_GROUP = "CG_RRWatch_%d" % STAMP
QUEUE_NUM = 4
# broker.conf states brokerClusterName=DefaultCluster (see /tmp/run_*_live.sh)
CLUSTER = "DefaultCluster"
REPLY_TOPIC = MixAll.get_reply_topic(CLUSTER)
REQUEST_TIMEOUT = 8000

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


def broker_error(exc: BaseException):
    """Extract the original broker error (``MQBrokerException``) out of a wrapping exception.

    ``create_topic_in_route`` wraps "every broker failed" into
    ``MQClientException("create new topic failed", cause=...)``, and ``str()`` **cannot see** the
    broker's code/remark -- they have to be fetched along the ``cause`` chain, otherwise debugging
    on a real machine is nothing but guesswork.
    """
    seen = set()
    cur = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if hasattr(cur, "error_message"):
            return cur
        cur = getattr(cur, "cause", None) or getattr(cur, "__cause__", None)
    return None


def new_producer(group: str, instance: str) -> DefaultMQProducer:
    pass
    p = DefaultMQProducer(group)
    p.set_namesrv_addr(NAMESRV)
    p.set_instance_name(instance)
    p.start()
    return p


def new_consumer(group: str, topic: str, instance: str, on_msgs) -> DefaultMQPushConsumer:
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_instance_name(instance)
    # Consume from the earliest: every topic of this script is brand new and its messages are produced
    # within a short time around the consumer startup, so using LAST would let "who resolves the
    # offset first" decide the outcome (point 8 of the project MEMORY).
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(topic, "*")
    c.set_message_listener(SimpleMessageListener(on_msgs))
    c.start()
    return c


def wait_for(pred, timeout_s: float) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.2)
    return False


class Replier:
    pass

    def __init__(self) -> None:
        self.received: list = []          # (body, correlation_id, reply_to, ttl)
        self.delivered: list = []         # the raw messages delivered by the broker (S8 uses it to verify CLUSTER)
        self.replied: list = []
        self.reply_errors: list = []
        self.lock = threading.Lock()
        self.producer = new_producer("PG_RRReplier_%d" % STAMP, "RRReply")
        self.consumer = new_consumer(CONSUMER_GROUP, TOPIC, "RRConsumer", self._on_messages)

    def shutdown(self) -> None:
        try:
            self.consumer.shutdown()
        finally:
            self.producer.shutdown()

    def _on_messages(self, msgs) -> ConsumeConcurrentlyStatus:
        for m in msgs:
            body = bytes(m.body)
            with self.lock:
                self.delivered.append(m)
                self.received.append((
                    body,
                    m.get_property(MessageConst.PROPERTY_CORRELATION_ID),
                    m.get_property(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT),
                    m.get_property(MessageConst.PROPERTY_MESSAGE_TTL),
                ))
            try:
                reply = create_reply_message(m, b"reply:" + body)
                self.producer.send(reply)
                with self.lock:
                    self.replied.append(body)
            except BaseException as e:  # noqa: BLE001
                with self.lock:
                    self.reply_errors.append("%s: %s" % (type(e).__name__, e))
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS


def main() -> int:
    print("== Request-Reply verification against a real machine  namesrv=%s  topic=%s ==" % (NAMESRV, TOPIC))
    print("   reply topic = %s" % REPLY_TOPIC)

    print("\nS1 creating the request topic; the reply topic is pre-registered by the broker (the client cannot create it)")
    prep = new_producer("PG_RRPrep_%d" % STAMP, "RRPrep")
    try:
        prep.create_topic("TBW102", TOPIC, QUEUE_NUM)
        # <cluster>_REPLY_TOPIC **should not and cannot be created by the client**: when the broker
        # starts, TopicConfigManager.init() has already registered it as a system topic
        # (clusterName + "_" + MixAll.REPLY_TOPIC_POSTFIX, plus addSystemTopic), and
        # validateSystemTopicWhenUpdateTopic defaults to true => a client-side createTopic gets
        # INVALID_PARAMETER(29) "conflict with system topic". That branch has **no log** on the broker
        # side and can only be seen from the client exception, so this also verifies that "the error
        # propagation is accurate" along the way.
        try:
            prep.create_topic("TBW102", REPLY_TOPIC, QUEUE_NUM)
            check("S1 the client-side create_topic(reply topic) was rejected by the broker", False, "it actually succeeded")
        except BaseException as e:  # noqa: BLE001
            be = broker_error(e)
            check("S1 the client-side create_topic(reply topic) is rejected (a broker system topic)",
                  be is not None and be.response_code == ResponseCode.INVALID_PARAMETER
                  and "system topic" in (be.error_message or ""),
                  "code=%s remark=%s" % (getattr(be, "response_code", None),
                                         getattr(be, "error_message", None)))
    finally:
        prep.shutdown()

    requester = new_producer(REQUEST_GROUP, "RRReq")
    # The requester must **already be registered with the broker** (heartbeat) before it can be looked
    # up by channel via REPLY_TO_CLIENT, otherwise the broker's ReplyMessageProcessor finds no channel
    # and replies SYSTEM_ERROR to the replier.
    # Note that _send_heartbeat_to_all_broker iterates over the **local route table**; when it is empty
    # it necessarily returns 0, so the route has to be fetched first (request() does the same thing
    # internally).
    requester._require_client().update_topic_route_info_from_name_server(TOPIC)
    check("S1 the requester registered with the broker (the heartbeat got through)",
          wait_for(lambda: requester._send_heartbeat_to_all_broker() > 0, 20), "")

    replier = Replier()
    # watcher: an independent subscription to <cluster>_REPLY_TOPIC, the hard evidence for S4
    watched: list = []
    watcher = new_consumer(WATCH_GROUP, REPLY_TOPIC, "RRWatch",
                           lambda msgs: _collect(watched, msgs))
    check("S1 the reply consumer started", replier.consumer is not None)
    try:
        # ---------------- S2/S3 one complete round trip ----------------
        print("\nS2/S3 one complete request -> reply round trip")
        msg = Message(TOPIC, b"ping-1")
        msg.set_keys("rr")
        reply = requester.request(msg, REQUEST_TIMEOUT)

        want = b"reply:ping-1"
        check("S3 request() got a reply with the correct body", bytes(reply.body) == want,
              "got=%r want=%r" % (bytes(reply.body), want))
        check("S3 the topic of the reply is <cluster>_REPLY_TOPIC", reply.topic == REPLY_TOPIC,
              "topic=%s" % reply.topic)
        arrive = reply.get_property(MessageConst.PROPERTY_REPLY_MESSAGE_ARRIVE_TIME)
        check("S3 the reply carries REPLY_MESSAGE_ARRIVE_TIME (stamped when the client received it)",
              arrive is not None and arrive.isdigit(), "value=%s" % arrive)

        ok = wait_for(lambda: len(replier.received) >= 1, 15)
        check("S2 the replier received the request message", ok, "received=%d" % len(replier.received))
        if ok:
            _, corr, reply_to, ttl = replier.received[0]
            check("S2 the request message carries CORRELATION_ID (a uuid string)",
                  bool(corr) and len(corr) == 36, "corr=%s" % corr)
            check("S2 the request message carries REPLY_TO_CLIENT = the clientId of the requester",
                  reply_to == requester._require_client().client_id,
                  "reply_to=%s want=%s" % (reply_to, requester._require_client().client_id))
            check("S2 the request message carries TTL = timeout", ttl == str(REQUEST_TIMEOUT), "ttl=%s" % ttl)

        # ---------------- S4 the reply really goes through 325 and lands in REPLY_TOPIC ----------------
        print("\nS4 the reply goes through SEND_REPLY_MESSAGE_V2(325) and lands in <cluster>_REPLY_TOPIC")
        check("S4 the replier successfully sent the reply message",
              wait_for(lambda: len(replier.replied) >= 1, 15),
              "replied=%s errors=%s" % (replier.replied, replier.reply_errors))
        seen = wait_for(lambda: b"reply:ping-1" in watched, 15)
        check("S4 the independent consumer of %s can see that reply" % REPLY_TOPIC, seen,
              "watched=%s" % watched)

        # ---------------- S5 no replier -> RequestTimeoutException ----------------
        print("\nS5 with no replier request() throws RequestTimeoutException")
        silent_topic = TOPIC + "_NoReplier"
        prep2 = new_producer("PG_RRPrep2_%d" % STAMP, "RRPrep2")
        try:
            prep2.create_topic("TBW102", silent_topic, QUEUE_NUM)
        finally:
            prep2.shutdown()
        t0 = time.time()
        raised = None
        try:
            silent = Message(silent_topic, b"nobody-home")
            silent.set_keys("rr-silent")
            requester.request(silent, 3000)
        except BaseException as e:  # noqa: BLE001
            raised = e
        elapsed = time.time() - t0
        check("S5 what is thrown is a RequestTimeoutException (not a silent return / another exception)",
              isinstance(raised, RequestTimeoutException),
              "raised=%s: %s" % (type(raised).__name__, raised))
        check("S5 the exception carries 10006 REQUEST_TIMEOUT_EXCEPTION",
              getattr(raised, "response_code", None) == ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
              "code=%s" % getattr(raised, "response_code", None))
        check("S5 the timeout duration is close to the configured value (2s ~ 12s, showing that it really waited instead of failing immediately)",
              2.0 <= elapsed <= 12.0, "elapsed=%.2fs" % elapsed)

        # ---------------- S6 concurrent requests do not cross-talk ----------------
        print("\nS6 3 concurrent requests, each getting its own reply")
        results: dict = {}
        errors: list = []

        def one(i: int) -> None:
            try:
                m = Message(TOPIC, ("ping-conc-%d" % i).encode())
                m.set_keys("rr-conc")
                r = requester.request(m, REQUEST_TIMEOUT)
                results[i] = bytes(r.body)
            except BaseException as e:  # noqa: BLE001
                errors.append("i=%d %s: %s" % (i, type(e).__name__, e))

        threads = [threading.Thread(target=one, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(REQUEST_TIMEOUT + 5000)
        expect = {i: ("reply:ping-conc-%d" % i).encode() for i in range(3)}
        check("S6 all 3 concurrent requests got replies and they match one to one",
              results == expect and not errors,
              "results=%s errors=%s" % (results, errors))

        # ---------------- S7 the consumer is not damaged by the Reply traffic ----------------
        print("\nS7 the consumer of the replier is still intact (ordinary push consumption is unaffected by Reply)")
        plain = Message(TOPIC, b"plain-after-replies")
        plain.set_keys("rr-plain")
        requester.send(plain)
        ok = wait_for(lambda: any(b == b"plain-after-replies" for b, _, _, _ in replier.received),
                      15)
        check("S7 the ordinary messages afterwards are still consumed", ok,
              "received=%s" % [b for b, _, _, _ in replier.received])

        print("\nS8 CLUSTER is written by the broker; when no reply can be built it reports 10007 rather than another error")
        delivered = None
        with replier.lock:
            for m in replier.delivered:
                if bytes(m.body) == b"ping-1":
                    delivered = m
                    break
        check("S8 the delivered request message carries the broker-written CLUSTER=%s" % CLUSTER,
              delivered is not None
              and delivered.get_property(MessageConst.PROPERTY_CLUSTER) == CLUSTER,
              "cluster=%s" % (delivered.get_property(MessageConst.PROPERTY_CLUSTER)
                              if delivered else None))
        check("S8 the copy of the request message on the client has **no** CLUSTER (the property really is filled in by the broker)",
              msg.get_property(MessageConst.PROPERTY_CLUSTER) is None,
              "value=%s" % msg.get_property(MessageConst.PROPERTY_CLUSTER))
        # Using the wrong object (sending the request with the local copy) -> it must hit 10007, and the wording points at CLUSTER
        raised8 = None
        try:
            create_reply_message(msg, b"pong")
        except BaseException as e:  # noqa: BLE001
            raised8 = e
        check("S8 building a reply from the local copy of the request message -> an MQClientException carrying 10007",
              isinstance(raised8, MQClientException)
              and raised8.response_code == ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION,
              "raised=%s code=%s" % (type(raised8).__name__ if raised8 else None,
                                     getattr(raised8, "response_code", None)))
        check("S8 the wording of the 10007 points at the missing CLUSTER property (the protocol text)",
              raised8 is not None
              and "property[%s] is null." % MessageConst.PROPERTY_CLUSTER in str(raised8),
              "msg=%s" % raised8)
        raised8b = None
        try:
            create_reply_message(None, b"pong")
        except BaseException as e:  # noqa: BLE001
            raised8b = e
        check("S8 a request message of None -> also 10007 (not a bare ValueError)",
              isinstance(raised8b, MQClientException)
              and raised8b.response_code == ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION
              and "requestMessage cannot be null." in str(raised8b),
              "raised=%s code=%s msg=%s" % (type(raised8b).__name__ if raised8b else None,
                                            getattr(raised8b, "response_code", None), raised8b))
    finally:
        requester.shutdown()
        replier.shutdown()
        watcher.shutdown()

    print("\n== Result: PASS=%d FAIL=%d ==" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


def _collect(got: list, msgs) -> ConsumeConcurrentlyStatus:
    for m in msgs:
        got.append(bytes(m.body))
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS


if __name__ == "__main__":
    sys.exit(main())
