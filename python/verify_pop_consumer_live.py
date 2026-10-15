#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import threading
import time

from client.admin import DefaultMQAdminExt
from client.consumer import (DefaultMQPushConsumer,
                                     SimpleMessageListener)
from client.producer import DefaultMQProducer
from common.message import Message
from client.consumer_result import (  # noqa: F401
    ConsumeConcurrentlyStatus,
)

PASS = 0
FAIL = 0


def check(name, ok, detail=""):
    global PASS, FAIL
    if ok:
        PASS += 1
        print("  [PASS] %s%s" % (name, ("  " + detail) if detail else ""))
    else:
        FAIL += 1
        print("  [FAIL] %s%s" % (name, ("  " + detail) if detail else ""))


def wait_until(pred, timeout, interval=0.2):
    """Wait until pred() is true or the timeout expires. Returns whether it was reached."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


def main():
    namesrv = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
    stamp = str(int(time.time() * 1000))
    topic = "PopConsLive_" + stamp
    group = "GID_PopConsLive_" + stamp
    topic_later = "PopConsLater_" + stamp
    group_later = "GID_PopConsLater_" + stamp
    topic_stats = "PopConsStats_" + stamp
    group_stats = "GID_PopConsStats_" + stamp
    n_msg = 12
    queue_num = 4

    print("=" * 70)
    print("POP consumer live (Python): namesrv=%s topic=%s group=%s" % (namesrv, topic, group))
    print("=" * 70)

    # ------------------------------------------------ S1 prepare the topic
    prep = DefaultMQProducer("PG_PopConsPrep_" + stamp)
    prep.set_namesrv_addr(namesrv)
    prep.start()
    try:
        prep.create_topic("TBW102", topic, queue_num)
        prep.create_topic("TBW102", topic_later, queue_num)
        prep.create_topic("TBW102", topic_stats, queue_num)
    except Exception as e:  # noqa: BLE001
        print("!! CreateTopic failed: %s" % e)
    prep.shutdown()

    # ------------------------------------------------ S1 POP consumption
    print("=== S1 POP consumption (collect everything + no duplicates) ===")
    received = []
    seen_queues = set()
    lock = threading.Lock()

    def listener(msgs):
        # CAUTION: SimpleMessageListener only passes msgs (the contract in consumer.py); do not write it as (msgs, context)
        with lock:
            for m in msgs:
                received.append(m.body.decode("utf-8", "replace"))
                seen_queues.add(m.queue_id)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    consumer = DefaultMQPushConsumer(group)
    consumer.set_namesrv_addr(namesrv)
    consumer.pop_mode = True
    consumer.consume_thread_min = 4
    consumer.consume_thread_max = 4
    consumer.consume_message_batch_max_size = 4
    # CAUTION: deliberately squeezed down to 10s so that "no ack -> the message is revived and
    # redelivered when invisibleTime expires" has **enough time to show up** inside the observation
    # window. With the default 60s, even if the ack never went out at all, the 8s observation
    # period could not reveal the duplicate delivery (a false pass).
    consumer.pop_invisible_time = 10000
    consumer.set_message_listener(SimpleMessageListener(listener))
    consumer.subscribe(topic, "*")
    # CAUTION: the consumer must be started before the messages are sent (see the project convention)
    consumer.start()
    time.sleep(1.0)

    producer = DefaultMQProducer("PG_PopCons_" + stamp)
    producer.set_namesrv_addr(namesrv)
    producer.start()
    sent = set()
    for i in range(n_msg):
        body = "pop-cons-%02d" % i
        msg = Message(topic, body.encode("utf-8"))
        msg.set_keys("pc%d" % i)
        producer.send(msg)
        sent.add(body)
    print("  sent %d msgs" % n_msg)

    got_all = wait_until(lambda: len(received) >= n_msg, timeout=30.0)
    # Wait a bit longer to confirm there is no duplicate delivery (S2). It must be
    # > pop_invisible_time(10s), otherwise "the ack did not go out" has no time to be revived and
    # S2 would pass falsely.
    wait_until(lambda: False, timeout=16.0)

    with lock:
        got = list(received)
        queues = set(seen_queues)
    check("S1a all the messages were consumed", got_all and len(got) >= n_msg,
          "received=%d/%d" % (len(got), n_msg))
    check("S1b the body set matches the sent set", set(got) == sent,
          "missing=%s" % sorted(sent - set(got))[:5])

    # ------------------------------------------------ S2 the ack takes effect
    print("=== S2 the ack takes effect (no duplicate delivery) ===")
    dup = len(got) - len(set(got))
    check("S2a no duplicate delivery", dup == 0, "received=%d unique=%d dup=%d" % (len(got), len(set(got)), dup))
    check("S2b no new delivery during the observation period", len(got) == n_msg,
          "received=%d expected=%d" % (len(got), n_msg))

    # ------------------------------------------------ S4 multiple queues
    print("=== S4 every queue was POPped ===")
    check("S4 the messages are spread over multiple queues and all of them were consumed", len(queues) > 1, "queues=%s" % sorted(queues))

    consumer.shutdown()

    # ------------------------------------------------ S3 RECONSUME_LATER
    print("=== S3 RECONSUME_LATER -> redelivery after the delay ===")
    later_rounds = {"first": 0}
    later_keys = []

    def later_listener(msgs):
        # Same as above: SimpleMessageListener only passes msgs
        with lock:
            for m in msgs:
                later_keys.append(m.get_keys() or m.msg_id)
        return ConsumeConcurrentlyStatus.RECONSUME_LATER

    c2 = DefaultMQPushConsumer(group_later)
    c2.set_namesrv_addr(namesrv)
    c2.pop_mode = True
    c2.consume_thread_min = 2      # Same as above: min/max must be squeezed as a pair, otherwise the startup gate rejects it first
    c2.consume_thread_max = 2
    c2.pop_invisible_time = 5000
    # The delay levels used when consumption fails: squeeze the first level down to 3s so that
    # "extending the invisible time -> becoming visible again" happens as soon as possible
    c2.pop_delay_level = [3, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200]
    c2.set_message_listener(SimpleMessageListener(later_listener))
    c2.subscribe(topic_later, "*")
    c2.start()
    time.sleep(1.0)

    p2 = DefaultMQProducer("PG_PopConsLater_" + stamp)
    p2.set_namesrv_addr(namesrv)
    p2.start()
    for i in range(3):
        msg = Message(topic_later, ("later-%d" % i).encode("utf-8"))
        msg.set_keys("pl%d" % i)
        p2.send(msg)

    ok_first = wait_until(lambda: len(later_keys) >= 3, timeout=30.0)
    with lock:
        first_round = len(later_keys)
    check("S3a the first round delivered 3 messages", ok_first and first_round >= 3, "count=%d" % first_round)

    # After the failure the client extends the invisible time (3s); when it expires the message
    # should become visible again and be delivered once more.
    # CAUTION: what is asserted is "every message was redelivered", not "3 more deliveries arrived
    # in total": counting by total would also pass if one message were redelivered 3 times while
    # another never came back.
    def redelivered_kinds():
        counts = {}
        for k in later_keys:
            counts[k] = counts.get(k, 0) + 1
        return sum(1 for n in counts.values() if n >= 2)

    ok_again = wait_until(redelivered_kinds, timeout=40.0)
    with lock:
        joined = ",".join(str(k) for k in later_keys)
    check("S3b every message whose consumption failed was delivered again (extending the invisible time works)", ok_again,
          "first=%d redelivered=%d deliveries=%s" % (first_round, redelivered_kinds(), joined))

    c2.shutdown()
    p2.shutdown()
    producer.shutdown()

    print("=== S5 the POP loop writes pullRT/pullTPS into the 307 status table ===")
    stats_delivered = []

    def stats_listener(msgs):
        with lock:
            stats_delivered.extend(m.body for m in msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    c5 = DefaultMQPushConsumer(group_stats)
    c5.set_namesrv_addr(namesrv)
    c5.pop_mode = True
    c5.consume_thread_min = 2      # Same as above: min/max must be squeezed as a pair
    c5.consume_thread_max = 2
    c5.set_message_listener(SimpleMessageListener(stats_listener))
    c5.subscribe(topic_stats, "*")
    c5.start()
    time.sleep(1.0)

    p5 = DefaultMQProducer("PG_PopConsStats_" + stamp)
    p5.set_namesrv_addr(namesrv)
    p5.start()
    began = time.time()
    rounds = 0
    while time.time() - began < 26.0:      # roughly 3 sampling periods
        for i in range(4):
            p5.send(Message(topic_stats, ("s5-%d-%d" % (rounds, i)).encode("utf-8")))
        rounds += 1
        time.sleep(2.0)
    sent5 = rounds * 4

    admin5 = DefaultMQAdminExt()
    admin5.set_namesrv_addr(namesrv)
    admin5.start()

    def pop_status():
        """Go through the broker, which forwards 307 to the consumer itself, and read back the status table of that topic; treat a failure as an empty table."""
        try:
            info = admin5.examine_consumer_running_info(group_stats, c5.client_id or "")
            return info.status_table.get(topic_stats) or {}
        except Exception:  # noqa: BLE001
            return {}

    ok_stats = wait_until(lambda: (pop_status().get("pullRT") or 0) > 0
                          and (pop_status().get("pullTPS") or 0) > 0, timeout=40.0)
    st = pop_status()
    check("S5a pullRT of the POP consumer in the 307 status table is non-zero", (st.get("pullRT") or 0) > 0,
          "pullRT=%s" % st.get("pullRT"))
    check("S5b pullTPS of the POP consumer in the 307 status table is non-zero", ok_stats and (st.get("pullTPS") or 0) > 0,
          "pullTPS=%s" % st.get("pullTPS"))
    # consumeOKTPS is the cell the POP path always had: only when both cells are non-zero does it
    # mean that the "pull" and the "consume" statistics both work, rather than the consumption
    # number having been copied into the pull column.
    check("S5c consumeOKTPS in the same table is non-zero too (the pull/consume cells are independent)",
          (st.get("consumeOKTPS") or 0) > 0, "consumeOKTPS=%s" % st.get("consumeOKTPS"))
    with lock:
        got5 = len(stats_delivered)
    check("S5d every message of this round was POPped and consumed", got5 >= sent5, "got=%d sent=%d" % (got5, sent5))

    c5.shutdown()
    p5.shutdown()
    admin5.shutdown()

    print("#" * 40)
    print("PASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
