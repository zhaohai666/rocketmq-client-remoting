# -*- coding: utf-8 -*-
"""Live verification of CheckForbiddenHook / FilterMessageHook.

Prerequisite: NameServer + Broker already started (a plain config is fine, traceTopicEnable is not needed).

Usage: .venv/bin/python verify_hook_live.py [127.0.0.1:9876]

Scenarios S1–S12 are in main(); the script returns 0 only if all of them hit.

⚠ Two self-discipline rules for this script (learned the hard way in the last trace integration round):
  ① topics / consume groups always carry a STAMP -- `run_hook_live.sh all` runs multiple verification rounds sequentially
     on the same broker, and fixed names would inherit the previous round's committed offsets; ② assertions always filter by body prefix,
     because warm-up messages and the broker's asynchronous consumequeue dispatch can both make a consumer receive a few extra messages, and counting totals would
     give false reds/false greens.
"""
from __future__ import annotations

import sys
import threading
import time

from client.consumer import DefaultMQPushConsumer
from client.consumer_result import ConsumeConcurrentlyStatus, MessageListenerConcurrently
from client.exception import MQClientException
from client.hook import CheckForbiddenHook, CommunicationMode, FilterMessageHook
from client.producer import DefaultMQProducer
from common.message import Message
from common.subscription_data import FilterAPI
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC_CK = "HookCheckForbidden_%d" % STAMP
TOPIC_FILTER = "HookFilterPull_%d" % STAMP
TOPIC_TAG = "HookFilterTag_%d" % STAMP
TOPIC_BOOM = "HookFilterBoom_%d" % STAMP
TOPIC_POP = "HookFilterPop_%d" % STAMP
GROUP = "GID_hook_live_%d" % STAMP
PRODUCER_GROUP = "GID_hook_producer_%d" % STAMP

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


class _Collector(MessageListenerConcurrently):
    def __init__(self) -> None:
        self.msgs = []
        self.lock = threading.Lock()

    def consume_message(self, msgs, context=None):
        with self.lock:
            self.msgs.extend(msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def bodies(self, prefix: str = ""):
        with self.lock:
            return [m.get_body() for m in self.msgs
                    if (m.get_body() or b"").startswith(prefix.encode())]

    def by_prefix(self, *prefixes):
        with self.lock:
            out = []
            for m in self.msgs:
                body = m.get_body() or b""
                if any(body.startswith(p.encode()) for p in prefixes):
                    out.append(m)
            return out


class _ForbidHook(CheckForbiddenHook):
    def __init__(self, forbid: bool, only_topic: str = "") -> None:
        self.forbid = forbid
        self.only_topic = only_topic
        self.calls = 0
        self.last_context = None
        self.modes = []

    def hook_name(self) -> str:
        return "live-forbid"

    def check_forbidden(self, context):
        self.calls += 1
        self.last_context = context
        self.modes.append(context.communication_mode)
        if self.forbid and (not self.only_topic or context.mq.topic == self.only_topic
                            or context.mq.topic.startswith(self.only_topic)):
            raise MQClientException("forbidden by live hook")


class _DropHook(FilterMessageHook):
    pass

    def __init__(self, prefix: bytes = b"drop-") -> None:
        self.prefix = prefix
        self.calls = 0
        self.seen = []

    def hook_name(self) -> str:
        return "live-drop"

    def filter_message(self, context) -> None:
        self.calls += 1
        self.seen.append(len(context.msg_list))
        context.msg_list = [m for m in context.msg_list
                            if not (m.get_body() or b"").startswith(self.prefix)]


class _BoomHook(FilterMessageHook):
    def __init__(self) -> None:
        self.calls = 0

    def hook_name(self) -> str:
        return "live-boom"

    def filter_message(self, context) -> None:
        self.calls += 1
        raise RuntimeError("boom from live hook")


def _wait_until(pred, timeout_sec: float, interval: float = 0.3) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def _start_consumer(topic: str, collector: _Collector, group_suffix: str,
                    pop: bool = False, invisible_ms: int = 0,
                    hooks=()) -> DefaultMQPushConsumer:
    c = DefaultMQPushConsumer("%s_%s" % (GROUP, group_suffix))
    c.set_namesrv_addr(NAMESRV)
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
    c.subscribe(topic, "*")
    c.set_message_listener(collector)
    for h in hooks:
        c.register_filter_message_hook(h)
    if pop:
        c.pop_mode = True
        if invisible_ms:
            c.pop_invisible_time = invisible_ms
        c.pop_batch_nums = 8
    c.start()
    return c


def _warm(topic: str) -> bool:
    """Warm-up: trigger the broker to auto-create the topic (the topic must be created before starting the consumer, otherwise no route)."""
    p = DefaultMQProducer(PRODUCER_GROUP + "_warm")
    p.set_namesrv_addr(NAMESRV)
    p.start()
    try:
        p.send(Message(topic, b"warm-up"), 5000)
        return True
    except Exception as e:  # noqa: BLE001
        print("  [warn] warm-up failed for %s: %s" % (topic, e))
        return False
    finally:
        p.shutdown()


def main() -> int:
    print("=== RocketMQ hook live verify (python) ===")
    print("namesrv=%s stamp=%d" % (NAMESRV, STAMP))

    # ---------- S0 subscription semantics self-check (pure local logic, regression guard) ----------
    sub_all = FilterAPI.build_subscription_data("T", "*")
    sub_tag = FilterAPI.build_subscription_data("T", "TagA||TagB")
    check("S0 SUB_ALL has empty tagsSet/codeSet, explicit tags go into codeSet",
          sub_all.tags_set == set() and sub_all.code_set == set()
          and sub_tag.tags_set == {"TagA", "TagB"}
          and sub_tag.code_set == {2598919, 2598920},
          "sub_all=%s/%s tag=%s/%s" % (sub_all.tags_set, sub_all.code_set,
                                       sub_tag.tags_set, sub_tag.code_set))

    # =============== Part 1: CheckForbiddenHook ===============
    if not _warm(TOPIC_CK):
        check("S1 warm-up creates the topic (for CheckForbidden)", False)
        return 1
    ck_collector = _Collector()
    ck_consumer = _start_consumer(TOPIC_CK, ck_collector, "ck")
    check("S1 warm-up creates the topic + the consumer has queues assigned", _wait_until(
        lambda: ck_consumer.assigned_queue_count() > 0, 30),
        "assigned=%d" % ck_consumer.assigned_queue_count())

    # S2 allow
    allow = _ForbidHook(forbid=False)
    p_allow = DefaultMQProducer(PRODUCER_GROUP + "_allow")
    p_allow.set_namesrv_addr(NAMESRV)
    p_allow.set_retry_times_when_send_failed(2)
    p_allow.register_check_forbidden_hook(allow)
    p_allow.start()
    ok_allow = False
    try:
        p_allow.send(Message(TOPIC_CK, b"allowed-1"), 5000)
        ok_allow = True
    except Exception as e:  # noqa: BLE001
        print("  [warn] allowed send failed: %s" % e)
    check("S2 allow hook: send succeeded and the hook was called once",
          ok_allow and allow.calls == 1,
          "ok=%s calls=%d mode=%s arg_ctx=%s" % (
              ok_allow, allow.calls, allow.modes,
              allow.last_context.communication_mode if allow.last_context else None))
    check("S2b the interception context carries group / mq / unitMode=False / sendResult=None",
          allow.last_context is not None
          and allow.last_context.group == (PRODUCER_GROUP + "_allow")
          and allow.last_context.mq.topic == TOPIC_CK
          and allow.last_context.unit_mode is False
          and allow.last_context.send_result is None,
          "group=%s mq=%s" % (allow.last_context.group if allow.last_context else None,
                              allow.last_context.mq if allow.last_context else None))

    # S3 block (the hook runs on every attempt)
    forbid = _ForbidHook(forbid=True, only_topic=TOPIC_CK)
    p_forbid = DefaultMQProducer(PRODUCER_GROUP + "_forbid")
    p_forbid.set_namesrv_addr(NAMESRV)
    p_forbid.set_retry_times_when_send_failed(2)
    p_forbid.register_check_forbidden_hook(forbid)
    p_forbid.start()
    raised = None
    try:
        p_forbid.send(Message(TOPIC_CK, b"blocked-1"), 5000)
    except MQClientException as e:
        raised = e
    check("S3 block hook: send raises MQClientException, and the hook is called retryTimes+1=3 times",
          raised is not None and forbid.calls == 3,
          "raised=%s calls=%d" % (type(raised).__name__ if raised else None, forbid.calls))

    # S5 a one-way send is also blocked
    forbid_ow = _ForbidHook(forbid=True, only_topic=TOPIC_CK)
    p_ow = DefaultMQProducer(PRODUCER_GROUP + "_oneway")
    p_ow.set_namesrv_addr(NAMESRV)
    p_ow.register_check_forbidden_hook(forbid_ow)
    p_ow.start()
    ow_raised = None
    try:
        p_ow.send_oneway(Message(TOPIC_CK, b"blocked-oneway"))
    except Exception as e:  # noqa: BLE001
        ow_raised = e
    check("S5 a one-way send is also blocked, and the context mode=ONEWAY",
          ow_raised is not None and forbid_ow.modes == [CommunicationMode.ONEWAY],
          "raised=%s modes=%s" % (type(ow_raised).__name__ if ow_raised else None,
                                  forbid_ow.modes))

    # S4 the blocked message did not land on the broker: only allowed-1 did
    got = _wait_until(lambda: len(ck_collector.by_prefix("allowed-")) >= 1, 20)
    time.sleep(2.0)   # leave an arrival window in case the blocked message somehow really was sent
    landed = ck_collector.by_prefix("allowed-", "blocked-")
    check("S4 the blocked message did not land on the broker (only the 1 allowed message did)",
          got and len(landed) == 1 and landed[0].get_body() == b"allowed-1",
          "count=%d bodies=%s" % (len(landed), [m.get_body() for m in landed]))

    p_allow.shutdown()
    p_forbid.shutdown()
    p_ow.shutdown()
    ck_consumer.shutdown()

    # =============== Part 2: FilterMessageHook (pull path) ===============
    if not _warm(TOPIC_FILTER):
        check("S6 warm-up creates the topic (for the filter hook)", False)
        return 1
    drop = _DropHook()
    filter_collector = _Collector()
    filter_consumer = _start_consumer(TOPIC_FILTER, filter_collector, "filter", hooks=[drop])
    if not _wait_until(lambda: filter_consumer.assigned_queue_count() > 0, 30):
        check("S6 the consumer has queues assigned", False)
        return 1

    p = DefaultMQProducer(PRODUCER_GROUP + "_f")
    p.set_namesrv_addr(NAMESRV)
    p.start()
    for i in range(3):
        p.send(Message(TOPIC_FILTER, ("keep-%d" % i).encode()), 5000)
        p.send(Message(TOPIC_FILTER, ("drop-%d" % i).encode()), 5000)

    got = _wait_until(lambda: len(filter_collector.by_prefix("keep-")) >= 3, 25)
    kept = filter_collector.by_prefix("keep-")
    dropped = filter_collector.by_prefix("drop-")
    check("S6 the filter hook works on the pull path: 3 kept, 2 dropped",
          got and len(kept) == 3 and len(dropped) == 0,
          "keep=%d drop=%d hook_calls=%d seen=%s" % (
              len(kept), len(dropped), drop.calls, drop.seen))

    # S7 dropped messages are not redelivered (the pull path skips silently and the offset advances normally)
    time.sleep(8.0)
    check("S7 dropped messages are not redelivered (the offset has advanced, counts unchanged after waiting 8s)",
          len(filter_collector.by_prefix("drop-")) == 0
          and len(filter_collector.by_prefix("keep-")) == 3,
          "keep=%d drop=%d" % (len(filter_collector.by_prefix("keep-")),
                               len(filter_collector.by_prefix("drop-"))))
    p.shutdown()
    filter_consumer.shutdown()

    # =============== Part 3: client-side secondary tag filtering ===============
    if not _warm(TOPIC_TAG):
        check("S8 warm-up creates the topic (for tag filtering)", False)
        return 1
    tag_collector = _Collector()
    tag_consumer = DefaultMQPushConsumer(GROUP + "_tag")
    tag_consumer.set_namesrv_addr(NAMESRV)
    tag_consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
    tag_consumer.subscribe(TOPIC_TAG, "TagA")      # tags_set={TagA} → the client will filter a second time
    tag_consumer.set_message_listener(tag_collector)
    tag_consumer.start()
    if not _wait_until(lambda: tag_consumer.assigned_queue_count() > 0, 30):
        check("S8 the consumer has queues assigned (tag)", False)
        return 1

    p2 = DefaultMQProducer(PRODUCER_GROUP + "_tag")
    p2.set_namesrv_addr(NAMESRV)
    p2.start()
    for i in range(2):
        p2.send(Message(TOPIC_TAG, ("tagA-%d" % i).encode(), tags="TagA"), 5000)
        p2.send(Message(TOPIC_TAG, ("tagB-%d" % i).encode(), tags="TagB"), 5000)

    got = _wait_until(lambda: len(tag_collector.by_prefix("tagA-")) >= 2, 25)
    time.sleep(2.0)
    tag_a = tag_collector.by_prefix("tagA-")
    tag_b = tag_collector.by_prefix("tagB-")
    check("S8 subscribing TagA: only the 2 TagA messages are received (broker hash filtering + client-side secondary filtering)",
          got and len(tag_a) == 2 and len(tag_b) == 0,
          "tagA=%d tagB=%d sub_tags=%s sub_codes=%s" % (
              len(tag_a), len(tag_b), tag_consumer.subscription_data[TOPIC_TAG].tags_set,
              tag_consumer.subscription_data[TOPIC_TAG].code_set))
    p2.shutdown()
    tag_consumer.shutdown()

    # =============== Part 4: a hook exception does not affect consumption ===============
    if not _warm(TOPIC_BOOM):
        check("S9 warm-up creates the topic (for the exception hook)", False)
        return 1
    boom, drop2 = _BoomHook(), _DropHook()
    boom_collector = _Collector()
    boom_consumer = _start_consumer(TOPIC_BOOM, boom_collector, "boom", hooks=[boom, drop2])
    if not _wait_until(lambda: boom_consumer.assigned_queue_count() > 0, 30):
        check("S9 the consumer has queues assigned (boom)", False)
        return 1

    p3 = DefaultMQProducer(PRODUCER_GROUP + "_boom")
    p3.set_namesrv_addr(NAMESRV)
    p3.start()
    p3.send(Message(TOPIC_BOOM, b"keep-boom"), 5000)
    p3.send(Message(TOPIC_BOOM, b"drop-boom"), 5000)
    got = _wait_until(lambda: len(boom_collector.by_prefix("keep-")) >= 1, 25)
    time.sleep(2.0)
    check("S9 a prior hook throwing is swallowed, later hooks still work (an exception does not affect consumption)",
          got and boom.calls >= 1 and drop2.calls >= 1
          and len(boom_collector.by_prefix("keep-")) == 1
          and len(boom_collector.by_prefix("drop-")) == 0,
          "boom_calls=%d drop_calls=%d keep=%d drop=%d" % (
              boom.calls, drop2.calls, len(boom_collector.by_prefix("keep-")),
              len(boom_collector.by_prefix("drop-"))))
    p3.shutdown()
    boom_consumer.shutdown()

    # =============== Part 5: FilterMessageHook (POP path, dropping means ack) ===============
    if not _warm(TOPIC_POP):
        check("S10 warm-up creates the topic (for POP)", False)
        return 1
    pop_drop = _DropHook()
    pop_collector = _Collector()
    invisible_ms = 10000
    pop_consumer = _start_consumer(TOPIC_POP, pop_collector, "pop", pop=True,
                                   invisible_ms=invisible_ms, hooks=[pop_drop])
    if not _wait_until(lambda: pop_consumer.assigned_queue_count() > 0, 30):
        check("S10 the POP consumer has queues assigned", False)
        return 1

    p4 = DefaultMQProducer(PRODUCER_GROUP + "_pop")
    p4.set_namesrv_addr(NAMESRV)
    p4.start()
    for i in range(2):
        p4.send(Message(TOPIC_POP, ("keep-pop-%d" % i).encode()), 5000)
    p4.send(Message(TOPIC_POP, b"drop-pop-0"), 5000)

    got = _wait_until(lambda: len(pop_collector.by_prefix("keep-")) >= 2, 30)
    check("S10 the filter hook works on the POP path: 2 kept, 1 dropped",
          got and len(pop_collector.by_prefix("keep-")) == 2
          and len(pop_collector.by_prefix("drop-")) == 0,
          "keep=%d drop=%d hook_calls=%d" % (
              len(pop_collector.by_prefix("keep-")), len(pop_collector.by_prefix("drop-")),
              pop_drop.calls))

    # S11 the dropped message is already acked: the observation window must be > invisibleTime, otherwise a false green
    print("  ... waiting %.0fs (> invisibleTime=%.0fs) to confirm the dropped message does not come back"
          % (invisible_ms / 1000.0 + 6, invisible_ms / 1000.0))
    time.sleep(invisible_ms / 1000.0 + 6)
    check("S11 the message dropped on the POP path is already acked (observation window > invisibleTime, not resurrected and redelivered)",
          len(pop_collector.by_prefix("keep-")) == 2
          and len(pop_collector.by_prefix("drop-")) == 0,
          "keep=%d drop=%d" % (len(pop_collector.by_prefix("keep-")),
                               len(pop_collector.by_prefix("drop-"))))
    p4.shutdown()
    pop_consumer.shutdown()

    print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
