# -*- coding: utf-8 -*-
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.admin import DefaultMQAdminExt
from client.consumer import DefaultMQPullConsumer
from client.consumer_result import PullStatus
from remoting.exception import RemotingException

NAMESRV = os.environ.get("ROCKETMQ_NAMESRV", "127.0.0.1:9876")

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS  " if ok else "FAIL  ") + name + (("  " + detail) if detail else ""))


def wait_queues(consumer, topic, expect, budget=20):
    """Wait until the route can be queried (the topic was just created on the Go side; the broker -> nameserver registration is asynchronous)."""
    deadline = time.time() + budget
    queues = []
    while time.time() < deadline:
        try:
            queues = consumer.fetch_subscribe_message_queues(topic)
        except BaseException as e:  # noqa: BLE001
            queues = []
            detail = "%s: %s" % (type(e).__name__, e)
        else:
            detail = ""
        if len(queues) >= expect:
            return queues
        time.sleep(0.5)
    print("      (wait_queues gave up: %s)" % (detail or "%d queues" % len(queues)))
    return queues


def safe_pull(consumer, mq, offset, max_nums=32):
    try:
        return consumer.pull(mq, "*", offset, max_nums), ""
    except RemotingException as e:
        return None, "%s: %s" % (type(e).__name__, e)


def main():
    if len(sys.argv) < 5:
        print("usage: go_pull_consume_check.py <topic> <go_group> <committed> <total>",
              file=sys.stderr)
        return 2
    topic = sys.argv[1]
    go_group = sys.argv[2]
    committed = int(sys.argv[3])
    total = int(sys.argv[4])
    stamp = int(time.time())

    consumer = DefaultMQPullConsumer("GID_PyReadGoPull_%d" % stamp)
    consumer.set_namesrv_addr(NAMESRV)
    consumer.start()
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.start()
    try:
        # ---- S1 the topic created by Go has exactly one queue ---------------------------
        queues = wait_queues(consumer, topic, 1)
        check("S1 the topic created by Go has 1 read queue", len(queues) == 1, "got=%d" % len(queues))
        if len(queues) != 1:
            return 1
        q0 = queues[0]
        check("S1 the queue id is 0 and the broker name is not empty",
              q0.queue_id == 0 and bool(q0.broker_name),
              "broker=%s queue=%d" % (q0.broker_name, q0.queue_id))

        # ---- S2 count the messages independently ---------------------------------------
        hi = consumer.max_offset(q0)
        lo = consumer.min_offset(q0)
        check("S2 max_offset == the total count reported by Go", hi == total, "max=%d want=%d" % (hi, total))
        check("S2 min_offset == 0", lo == 0, "min=%d" % lo)

        got = admin.examine_consumer_offset(go_group, q0)
        check("S3 the committed offset of the Go group on the broker == COMMITTED", got == committed,
              "broker=%r committed=%d" % (got, committed))
        check("S3 COMMITTED is a partial offset (not a coincidental \"everything consumed\" value)", committed < total,
              "committed=%d total=%d" % (committed, total))

        # ---- S4 read the same stretch of log with the Python pull; content and order must match Go ---
        result, err = safe_pull(consumer, q0, committed)
        if result is None:
            check("S4 pulled from COMMITTED (Python side)", False, err)
        else:
            bodies = [bytes(m.body) for m in result.msg_found_list]
            expect_bodies = [b"go-pull-seed-%d" % i for i in range(committed + 1, 6)] + \
                            [b"go-pull-wake"]
            check("S4 pulled from COMMITTED (Python side) = FOUND",
                  result.status == PullStatus.FOUND,
                  "status=%s n=%d" % (result.status, len(result.msg_found_list)))
            check("S4 count == TOTAL - COMMITTED", len(bodies) == total - committed,
                  "n=%d want=%d" % (len(bodies), total - committed))
            check("S4 bodies and order match exactly (offset semantics aligned)", bodies == expect_bodies,
                  "got=%s" % [b.decode() for b in bodies])
            # The offset counts the "next" entry starting at 1, so the committed-th message is exactly seed-(committed+1)
            check("S4 COMMITTED points at message number %d" % (committed + 1),
                  bool(bodies) and bodies[0] == b"go-pull-seed-%d" % (committed + 1),
                  "head=%r" % (bodies[0] if bodies else b""))
            if result.msg_found_list:
                m = result.msg_found_list[0]
                check("S4 the property segment is parseable (KEYS)",
                      m.get_property("KEYS") == "go-pull-key-%d" % (committed + 1),
                      "keys=%r" % m.get_property("KEYS"))
                check("S4 carries UNIQ_KEY", bool(m.get_property("UNIQ_KEY")),
                      "uniq=%r" % m.get_property("UNIQ_KEY"))

        # ---- S5 sendMessageBack must land in %RETRY%<go_group> ---------------------------
        # The Go side sends back the head entry (go-pull-seed-1) with delayLevel=1, so the
        # delivery service puts it back into the retry queue after roughly 1s.
        retry_topic = "%RETRY%" + go_group
        found = False
        detail = "retry topic never became routable"
        deadline = time.time() + 40
        while time.time() < deadline and not found:
            try:
                retry_queues = consumer.fetch_subscribe_message_queues(retry_topic)
            except BaseException as e:  # noqa: BLE001
                detail = "retry topic not routable yet: %s" % e
                time.sleep(1)
                continue
            for rq in retry_queues:
                try:
                    rlo = consumer.min_offset(rq)
                    rhi = consumer.max_offset(rq)
                except BaseException as e:  # noqa: BLE001
                    detail = "%s: %s" % (type(e).__name__, e)
                    continue
                if rhi <= rlo:
                    continue
                r, rerr = safe_pull(consumer, rq, rlo)
                if r is None:
                    detail = rerr
                    continue
                for m in r.msg_found_list:
                    if bytes(m.body) > b"":
                        found = True
                        detail = ("queue=%d body=%s reconsumeTimes=%d"
                                  % (rq.queue_id, bytes(m.body), m.get_reconsume_times()))
                        break
                if found:
                    break
            if not found:
                time.sleep(1)
        check("S5 %RETRY%<group> pulled the message that was sent back", found, detail)
        if found:
            check("S5 the message sent back is the head entry (go-pull-seed-1)", "go-pull-seed-1" in detail, detail)
            check("S5 reconsumeTimes has been incremented to 1 (the broker took the redelivery path)",
                  "reconsumeTimes=1" in detail, detail)

        # ---- S6 %DLQ% must be empty: maxReconsumeTimes=16 instead of the push default -1 ---------
        dlq_topic = "%DLQ%" + go_group
        dlq_msgs = []
        try:
            for dq in consumer.fetch_subscribe_message_queues(dlq_topic):
                try:
                    dlo = consumer.min_offset(dq)
                    dhi = consumer.max_offset(dq)
                except BaseException as e:  # noqa: BLE001
                    dlq_msgs.append("offset query failed: %s" % e)
                    continue
                if dhi <= dlo:
                    continue
                r, _err = safe_pull(consumer, dq, dlo)
                if r is not None:
                    dlq_msgs.extend(bytes(m.body) for m in r.msg_found_list)
        except BaseException:  # noqa: BLE001
            # No route means the broker never created this DLQ topic, which is exactly the expected outcome
            dlq_msgs = []
        check("S6 %DLQ%<group> is empty (nothing went straight to the dead-letter queue)", not dlq_msgs,
              "dlq=%s" % dlq_msgs)
    finally:
        try:
            consumer.shutdown()
        finally:
            admin.shutdown()

    print("\nPASS=%d FAIL=%d" % (sum(1 for r in results if r), sum(1 for r in results if not r)))
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
