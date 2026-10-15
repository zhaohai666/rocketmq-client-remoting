# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.admin import DefaultMQAdminExt
from client.exception import MQBrokerException, MQClientException
from client.producer import DefaultMQProducer
from client.send_result import SendStatus
from common.message import Message
from common.mix_all import MixAll

NAMESRV = os.environ.get("ROCKETMQ_NAMESRV", "127.0.0.1:9876")
STAMP = int(time.time())

MAIN_QUEUES = 4
ORDERLY_QUEUES = 1

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS  " if ok else "FAIL  ") + name + (("  [" + detail + "]") if detail else ""))


def _admin():
    # Note: the first positional parameter of DefaultMQAdminExt is rpc_hook, not clientId / group!
    # Passing a string there makes every request blow up with an AttributeError on
    # `hook.do_before_request`, while the route query layer swallows the exception and only
    # shows up as "topic not exist", which is very hard to diagnose.
    a = DefaultMQAdminExt()
    a.set_namesrv_addr(NAMESRV)
    a.start()
    return a


def ensure_topic(admin, topic, queues):
    """Create the topic and wait until the route becomes visible; if it already exists, overwrite the queue count with the one we want."""
    admin.create_topic(MixAll.DEFAULT_TOPIC, topic, queues)
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            route = admin.examine_topic_route(topic)
        except MQClientException:
            route = None
        if route is not None:
            if len(route.get_all_subscribe_message_queue(topic)) == queues:
                return True
            # The route is there but the queue count is wrong: overwrite it once and look again.
            admin.create_topic(MixAll.DEFAULT_TOPIC, topic, queues)
        time.sleep(0.5)
    return False


def producer():
    p = DefaultMQProducer("PID_go_consumer_feed_%d" % STAMP)
    p.set_namesrv_addr(NAMESRV)
    p.set_instance_name("go-consumer-feed-%d" % STAMP)
    return p


def produce(topic, count, orderly_topic=None, orderly_count=0):
    """The main topic: TagA for even indices / TagB for odd ones, with the tag written into
    the body to make the comparison easy.

    The orderly topic: a single queue whose body increases with the index, for the Go orderly
    consumption test case to check ordering.
    """
    admin = _admin()
    try:
        wanted = [(topic, MAIN_QUEUES)]
        if orderly_topic:
            wanted.append((orderly_topic, ORDERLY_QUEUES))
        for name, queues in wanted:
            ok = ensure_topic(admin, name, queues)
            check("created topic %s (%d queues)" % (name, queues), ok)
    finally:
        admin.shutdown()

    p = producer()
    p.start()
    sent = 0
    try:
        for i in range(count):
            tag = "TagA" if i % 2 == 0 else "TagB"
            body = ("%s-%03d" % (tag, i)).encode("utf-8")
            r = p.send(Message(topic=topic, body=body, tags=tag))
            if r.send_status == SendStatus.SEND_OK:
                sent += 1
            else:
                print("send %d -> %s" % (i, r.send_status))
        check("produced %d messages to %s" % (count, topic), sent == count, "sent=%d" % sent)
        print("PRODUCED=%d" % sent)

        if orderly_topic and orderly_count:
            osent = 0
            for i in range(orderly_count):
                body = ("Ord-%03d" % i).encode("utf-8")
                r = p.send(Message(topic=orderly_topic, body=body, tags="TagA"))
                if r.send_status == SendStatus.SEND_OK:
                    osent += 1
            check("produced %d messages to the orderly topic %s" % (orderly_count, orderly_topic),
                  osent == orderly_count, "sent=%d" % osent)
            print("PRODUCED_ORDERLY=%d" % osent)
    finally:
        p.shutdown()
    return sent


def verify(topic, group, count, unreg_group=None):
    admin = _admin()
    try:
        # The consumer side wants the "read queues" (get_all_subscribe_message_queue), not the
        # publish queues: for a read-only topic, and for a route where the master is down and only
        # the slave is left, the two yield different queue sets.
        queues = admin.examine_topic_route(topic).get_all_subscribe_message_queue(topic)
        total = 0
        per_queue = {}
        for mq in queues:
            off = admin.examine_consumer_offset(group, mq)
            if off is None:
                off = -1
            per_queue[mq.queue_id] = off
            if off > 0:
                total += off
        check(
            "the committed offsets of %s on %s on the broker add up to %d" % (group, topic, count),
            total == count,
            "total=%d per_queue=%s" % (total, sorted(per_queue.items())),
        )

        if unreg_group:
            # Graceful unregistration: the Go side sent UNREGISTER_CLIENT on shutdown, so it
            # must no longer appear in the broker's consumer connection list. When the group is
            # completely offline the broker replies SUBSCRIPTION_NOT_EXIST ("not online"), which
            # the client turns into an MQBrokerException -- exactly the expected outcome.
            # Note that MQBrokerException and MQClientException are **siblings**; catching only
            # the latter makes this check end in an exception (that is exactly how the first
            # version broke).
            try:
                conn = admin.examine_consumer_connection_info(unreg_group)
                ids = list(getattr(conn, "connection_set", []) or [])
                check("no leftover connections of %s on the broker after shutdown" % unreg_group,
                      len(ids) == 0, "connections=%d" % len(ids))
            except MQBrokerException as e:
                text = str(e)
                check("%s has been unregistered from the broker after shutdown (group not online)" % unreg_group,
                      "not online" in text, "code=%s %s" % (e.response_code, text))
            except MQClientException as e:
                check("%s has been unregistered from the broker after shutdown (group not online)" % unreg_group, True, str(e))
    finally:
        admin.shutdown()

    print("PASS_TOTAL=%d FAIL_TOTAL=%d"
          % (sum(1 for r in results if r), sum(1 for r in results if not r)))
    return 0 if all(results) else 1


def main():
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2
    mode = sys.argv[1]
    if mode == "produce":
        topic = sys.argv[2]
        count = int(sys.argv[3]) if len(sys.argv) > 3 else 12
        orderly_topic = sys.argv[4] if len(sys.argv) > 4 else None
        orderly_count = int(sys.argv[5]) if len(sys.argv) > 5 else 0
        produce(topic, count, orderly_topic, orderly_count)
        return 0 if all(results) else 1
    if mode == "verify":
        if len(sys.argv) < 5:
            print("usage: verify <topic> <group> <count> [unreg_group]", file=sys.stderr)
            return 2
        unreg = sys.argv[5] if len(sys.argv) > 5 else None
        return verify(sys.argv[2], sys.argv[3], int(sys.argv[4]), unreg)
    print("unknown mode %s" % mode, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
