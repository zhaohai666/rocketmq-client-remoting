#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sys
import time
from typing import List, Optional, Tuple

sys.path.insert(0, ".")

from client.consumer import (ConsumeFromWhere, DefaultMQPushConsumer,
                                      MessageSelector, SimpleMessageListener)
from client.consumer_result import ConsumeConcurrentlyStatus
from client.exception import MQClientException
from client.producer import DefaultMQProducer
from client.send_result import SendStatus
from common.message import Message
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.rpchook import RPCHook

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "Sql92Live_%d" % STAMP
GROUP = "GID_Sql92Live_%d" % STAMP

PASS = 0
FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print("  PASS  %s" % name)
    else:
        FAIL += 1
        print("  FAIL  %s%s" % (name, ("  <- " + detail) if detail else ""))


def wait_for(cond, timeout: float = 20.0, interval: float = 0.3) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return bool(cond())


class CheckConfigProbe(RPCHook):
    """A hook on the transport that captures the code-46 requests really sent during startup (with the body)."""

    def __init__(self):
        self.requests: List[tuple] = []        # (addr, code)
        self.bodies: List[dict] = []

    def do_before_request(self, remote_addr: str, request: RemotingCommand) -> None:
        self.requests.append((remote_addr, request.code))
        if request.code == RequestCode.CHECK_CLIENT_CONFIG and request.body:
            self.bodies.append(json.loads(request.body.decode("utf-8")))

    @property
    def codes(self) -> List[int]:
        return [code for _, code in self.requests]

    @property
    def check_count(self) -> int:
        return sum(1 for code in self.codes if code == RequestCode.CHECK_CLIENT_CONFIG)


class Collector:
    """Collects the consumed messages. SimpleMessageListener only takes a plain function, so this wraps one to expose a listener."""

    def __init__(self):
        self.msgs = []

    def listener(self) -> SimpleMessageListener:
        return SimpleMessageListener(self._consume)

    def _consume(self, msgs):
        self.msgs.extend(msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def bodies(self) -> List[str]:
        return sorted(m.body.decode() for m in self.msgs)


def new_consumer(group: str, selector: Optional[MessageSelector] = None,
                 expression: Optional[str] = None,
                 probe: Optional[CheckConfigProbe] = None,
                 topic: Optional[str] = None
                 ) -> Tuple[DefaultMQPushConsumer, Collector]:
    """Build a push consumer subscribing to this topic (when probe is non-None the RPC hook is installed to gather evidence)."""
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    if probe is not None:
        c.rpc_hook = probe
    if selector is not None:
        c.subscribe_with_selector(topic or TOPIC, selector)
    else:
        c.subscribe(topic or TOPIC, expression or "*")
    col = Collector()
    c.set_message_listener(col.listener())
    return c, col


def main() -> int:
    print("SQL92 / CHECK_CLIENT_CONFIG live check on %s" % NAMESRV)
    prod = DefaultMQProducer(GROUP + "_P")
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        prod.create_topic("TBW102", TOPIC, 4)
        check("T0 the topic route is ready (4 queues)",
              wait_for(lambda: len(
                  prod._mq_client.get_topic_publish_info(TOPIC).msg_queue_list) >= 4, 20.0))

        # ---------- preflight: does this broker accept SQL92 subscriptions at all? ----------
        # The broker answers CHECK_CLIENT_CONFIG(46) with SYSTEM_ERROR ("The broker does not
        # support consumer to filter message by SQL92") unless it runs with
        # enablePropertyFilter=true, and start() fails the same way here. Detect
        # that up front and say what to do, instead of dying with a raw traceback inside S1.
        preflight, _ = new_consumer(GROUP + "_PREFLIGHT", MessageSelector.by_sql("color = 'red'"))
        try:
            preflight.start()
        except MQClientException as e:
            msg = str(e)
            if "does not support consumer to filter message by" in msg:
                print("!! the broker refuses SQL92 subscriptions: %s" % msg)
                print("!! fix: set enablePropertyFilter=true in broker.conf and restart the broker,")
                print("!!      then re-run this probe (the requirement is the same for any client).")
                print("!! skipping end-to-end SQL92 verification: broker not configured for property filter")
                preflight.shutdown()
                prod.shutdown()
                return 0
            raise
        else:
            preflight.shutdown()
        print("preflight OK: the broker accepts SQL92 subscriptions (enablePropertyFilter=true)")

        # ---------- S1 live evidence: the shape of the code-46 request ----------
        print("\n---------- S1 the code-46 request during startup ----------")
        probe = CheckConfigProbe()
        c1, _ = new_consumer(GROUP + "_SQL", MessageSelector.by_sql("color = 'red'"),
                             probe=probe)
        c1.start()
        check("the SQL92 subscription triggers exactly one CHECK_CLIENT_CONFIG(46)", probe.check_count == 1,
              "codes=%s" % probe.codes)
        body = probe.bodies[0] if probe.bodies else {}
        sd = body.get("subscriptionData") or {}
        check("the body is a CheckClientRequestBody (clientId / group / subscriptionData)",
              body.get("group") == GROUP + "_SQL" and body.get("clientId") == c1.client_id
              and sd.get("expressionType") == "SQL92"
              and sd.get("subString") == "color = 'red'" and sd.get("topic") == TOPIC,
              json.dumps(body))
        c1.shutdown()

        tag_probe = CheckConfigProbe()
        c2, _ = new_consumer(GROUP + "_TAG", probe=tag_probe, expression="tagA || tagB")
        c2.start()
        check("a pure TAG subscription does not send a single code-46 (isTagType short-circuits)",
              tag_probe.check_count == 0, str(tag_probe.codes))
        c2.shutdown()

        # ---------- S2 / S3: bring the consumers up first, then send the messages ----------
        print("\n---------- S2 the broker filters by property / S3 an expression that never matches ----------")
        red_c, red_col = new_consumer(GROUP + "_FILTER",
                                      MessageSelector.by_sql("color = 'red'"))
        green_c, green_col = new_consumer(GROUP + "_NONE",
                                          MessageSelector.by_sql("color = 'green'"))
        all_c, all_col = new_consumer(GROUP + "_ALL")
        for c in (red_c, green_c, all_c):
            c.start()
        sent = {"red": [], "blue": []}
        for color in ("red", "blue"):
            for i in range(3):
                text = "body-%s-%d" % (color, i)
                m = Message(TOPIC, text.encode("utf-8"), keys="%s-%s-%d" % (STAMP, color, i))
                m.put_property("color", color)
                r = prod.send(m)
                check("sending %s succeeded" % text, r.send_status == SendStatus.SEND_OK,
                      str(r.send_status))
                sent[color].append(text)

        wait_for(lambda: len(all_col.msgs) >= 6, 25.0)
        time.sleep(4.0)                        # wait a bit longer to confirm green did not simply "arrive late"
        red_bodies, all_bodies = red_col.bodies(), all_col.bodies()
        check("SQL92(color=red) received 3 messages, exactly the 3 that were sent",
              red_bodies == sorted(sent["red"]), str(red_bodies))
        check("the TAG '*' control group received all 6 messages (red + blue)",
              all_bodies == sorted(sent["red"] + sent["blue"]), str(all_bodies))
        check("none of the 3 blue messages leaked into the SQL92 consumer (proving the broker really filters)",
              all(b not in red_bodies for b in sent["blue"]), str(red_bodies))
        props = {m.properties.get("color") for m in red_col.msgs}
        check("the color property of the received messages is readable and all red", props == {"red"}, str(props))
        check("subscribing to the never-matching color='green' => not a single message received",
              len(green_col.msgs) == 0, green_col.bodies())
        for c in (red_c, green_c, all_c):
            c.shutdown()

        # ---------- S4 counter-proof: an illegal expression makes startup fail ----------
        print("\n---------- S4 an illegal expression fails during startup ----------")
        c4, _ = new_consumer(GROUP + "_BAD", MessageSelector.by_sql("color =="))
        err: Optional[MQClientException] = None
        t0 = time.time()
        try:
            c4.start()
        except MQClientException as e:
            err = e
        cost = time.time() - t0
        check("start() raised an MQClientException", err is not None, repr(err))
        check("the error code is the broker's SUBSCRIPTION_PARSE_FAILED(23)",
              err is not None
              and err.response_code == ResponseCode.SUBSCRIPTION_PARSE_FAILED,
              "code=%s remark=%s" % (getattr(err, "response_code", None), err))
        check("after the failure the consumer did not stay in the started state", c4._started is False)
        check("the illegal expression fails immediately (<5s, not waiting for a timeout fallback)", cost < 5.0, "%.2fs" % cost)
        # Was the rollback clean? The same object with a legal expression should be startable again
        retry_err: Optional[Exception] = None
        try:
            c4.subscribe_with_selector(TOPIC, MessageSelector.by_sql("color = 'blue'"))
            c4.start()
        except Exception as e:                  # noqa: BLE001
            retry_err = e
        check("after fixing the expression it can be started again (the startup rolled back in place)", retry_err is None,
              repr(retry_err))
        if retry_err is None:
            c4.shutdown()

        # ---------- S5 send-first + CONSUME_FROM_FIRST_OFFSET (the regression this leg pins) ----------
        # An earlier draft resolved the initial offset of a SQL92 subscription to the max
        # offset, silently skipping every message produced *before* the consumer started
        # (consumer-first passed only because the queues were empty and max was 0 anyway).
        # A FIRST_OFFSET subscription starts at 0 regardless of the subscription
        # type and lets the broker filter.
        print("\n---------- S5 send-first + CONSUME_FROM_FIRST_OFFSET ----------")
        first_topic = "Sql92First_%d" % STAMP
        prod.create_topic("TBW102", first_topic, 4)
        check("S5 the send-first topic route is ready (4 queues)",
              wait_for(lambda: len(
                  prod._mq_client.get_topic_publish_info(first_topic).msg_queue_list) >= 4, 20.0))
        first_red: List[str] = []
        first_blue: List[str] = []
        for color, sink in (("red", first_red), ("blue", first_blue)):
            for i in range(2):
                text = "first-%s-%d" % (color, i)
                m = Message(first_topic, text.encode("utf-8"),
                            keys="%s-first-%s-%d" % (STAMP, color, i))
                m.put_property("color", color)
                r = prod.send(m)
                check("S5 sending %s succeeded" % text, r.send_status == SendStatus.SEND_OK,
                      str(r.send_status))
                sink.append(text)
        fc, fcol = new_consumer(GROUP + "_FIRST",
                                MessageSelector.by_sql("color = 'red'"),
                                topic=first_topic)
        fc.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        fc.start()
        wait_for(lambda: len(fcol.msgs) >= 2, 25.0)
        time.sleep(2.0)                        # rule out "arriving late" before judging the blue side
        f_bodies = sorted(m.body.decode() for m in fcol.msgs)
        check("S5 SQL92 + FIRST_OFFSET receives the 2 red messages sent BEFORE start "
              "(was: skipped by the old max-offset short-circuit)",
              f_bodies == sorted(first_red), str(f_bodies))
        check("S5 no blue leaks into the send-first SQL92 consumer",
              all(b not in f_bodies for b in first_blue), str(f_bodies))
        fc.shutdown()
    finally:
        prod.shutdown()

    print("\n%d PASS / %d FAIL" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
