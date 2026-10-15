# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.admin import DefaultMQAdminExt
from client.producer import DefaultMQProducer, SendCallback
from client.send_result import SendStatus
from common.message import Message
from common.mix_all import MixAll
from common.sysflag import PermName

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time())

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def _wait_until(pred, timeout_sec=20.0, interval=0.3):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def _producer(instance_name, create_topic_key=None, default_topic_queue_nums=None):
    p = DefaultMQProducer("PID_send_header_%d" % STAMP)
    p.set_namesrv_addr(NAMESRV)
    p.set_instance_name(instance_name)
    if create_topic_key is not None:
        p.set_create_topic_key(create_topic_key)
    if default_topic_queue_nums is not None:
        p.set_default_topic_queue_nums(default_topic_queue_nums)
    return p


def _queue_nums(admin, broker_addr, topic):
    """Read back the (read, write) queue counts of that topic on the broker; returns (None, None) when it does not exist yet."""
    try:
        cfg = admin.examine_topic_config(broker_addr, topic)
    except Exception:  # noqa: BLE001
        return None, None
    return cfg.read_queue_nums, cfg.write_queue_nums


def _total_messages(admin, topic):
    """The number of persisted messages of that topic across all brokers (the minOffset of a new topic is always 0)."""
    try:
        stats = admin.examine_topic_stats(topic)
    except Exception:  # noqa: BLE001
        return -1
    return sum(o.max_offset - o.min_offset for o in stats.offset_table.values())


class _Latch(SendCallback):
    def __init__(self):
        self.done = threading.Event()
        self.result = None
        self.error = None

    def on_success(self, send_result):
        self.result = send_result
        self.done.set()

    def on_exception(self, e):
        self.error = e
        self.done.set()


def main():
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()

    cluster = None
    for _ in range(40):
        try:
            ci = admin.fetch_broker_cluster_info()
            if ci and ci.broker_addr_table:
                cluster = ci
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    if cluster is None:
        check("cluster liveness", False, "no broker registered on the nameServer")
        admin.shutdown()
        return 1
    broker_addr = cluster.get_broker_addrs()[0]
    broker_name = next((name for name, ids in cluster.broker_addr_table.items()
                        if broker_addr in ids.values()), "")
    check("cluster liveness", True, "broker=%s" % broker_addr)

    topics = []
    # H0: TBW102 is the real "template topic" on this broker, and the criteria of H2/H3 both depend on its queue count
    tbw_read, tbw_write = _queue_nums(admin, broker_addr, MixAll.DEFAULT_TOPIC)
    if tbw_write is None:
        check("H0 the TBW102 template is readable", False, "examine_topic_config(TBW102) cannot be read")
        admin.shutdown()
        return 1
    check("H0 the TBW102 template is readable", tbw_write > 0, "read=%s write=%s" % (tbw_read, tbw_write))

    # ---------------- H1 the default: d=4, the created topic takes min(4, TBW102) ----------------
    t1 = "HdrDef_%d" % STAMP
    topics.append(t1)
    p1 = _producer("hdr-h1-%d" % STAMP)
    p1.start()
    try:
        r1 = p1.send(Message(t1, b"h1"))
        check("H1 the send with the default configuration succeeded", r1.send_status == SendStatus.SEND_OK,
              "msgId=%s mq=%s" % (r1.msg_id, r1.message_queue))
    finally:
        p1.shutdown()
    _wait_until(lambda: _queue_nums(admin, broker_addr, t1)[0] is not None, 10)
    h1_read, h1_write = _queue_nums(admin, broker_addr, t1)
    check("H1 the queue count of the topic created with the default d=4 = min(4, TBW102)",
          h1_write == min(4, tbw_write) and h1_read == h1_write,
          "read=%s write=%s (TBW102=%s)" % (h1_read, h1_write, tbw_write))

    # ---------------- H2 set_default_topic_queue_nums(2) really takes effect ----------------
    t2 = "HdrNums_%d" % STAMP
    topics.append(t2)
    p2 = _producer("hdr-h2-%d" % STAMP, default_topic_queue_nums=2)
    p2.start()
    try:
        p2.send(Message(t2, b"h2"))
    finally:
        p2.shutdown()
    _wait_until(lambda: _queue_nums(admin, broker_addr, t2)[0] is not None, 10)
    h2_read, h2_write = _queue_nums(admin, broker_addr, t2)
    check("H2 the topic created with defaultTopicQueueNums=2 has only 2 queues",
          h2_write == min(2, tbw_write) and h2_read == h2_write,
          "read=%s write=%s (the old hardcoded-4 behaviour would be %s)" % (h2_read, h2_write, h1_write))

    # ---------------- H3 set_create_topic_key(src) really takes effect ----------------
    # The template topic must carry PERM_INHERIT, otherwise isInherited at
    # TopicConfigManager:286 does not pass and the broker simply refuses to auto-create the topic.
    src = "HdrSrc_%d" % STAMP
    topics.append(src)
    admin.create_topic_in_broker(broker_addr, src, 3, 3,
                                 PermName.PERM_READ | PermName.PERM_WRITE | PermName.PERM_INHERIT)
    src_read, src_write = _queue_nums(admin, broker_addr, src)
    check("H3 the template topic was created (3 queues, with INHERIT)",
          src_write == 3 and (src_read, src_write) != (tbw_read, tbw_write),
          "src=(%s,%s) tbw102=(%s,%s)" % (src_read, src_write, tbw_read, tbw_write))

    t3 = "HdrSrc_%d" % STAMP
    topics.append(t3)
    p3 = _producer("hdr-h3-%d" % STAMP, create_topic_key=src, default_topic_queue_nums=8)
    p3.start()
    try:
        p3.send(Message(t3, b"h3"))
    finally:
        p3.shutdown()
    _wait_until(lambda: _queue_nums(admin, broker_addr, t3)[0] is not None, 10)
    h3_read, h3_write = _queue_nums(admin, broker_addr, t3)
    check("H3 createTopicKey=template topic is inherited (min(8,3)=3 instead of TBW102's %s)" % tbw_write,
          h3_write == 3 and h3_read == 3,
          "read=%s write=%s" % (h3_read, h3_write))

    # ---------------- H4 all five entry points land as usual ----------------
    t4 = "HdrEntries_%d" % STAMP
    topics.append(t4)
    p4 = _producer("hdr-h4-%d" % STAMP)
    p4.start()
    latch = _Latch()
    landed = {}
    try:
        rs = p4.send(Message(t4, b"h4-sync"))
        landed["sync"] = rs.send_status == SendStatus.SEND_OK

        # Pinned send: the mq is given explicitly, which takes a different path than H1 (the broker name is supplied by the caller)
        mq = rs.message_queue
        rb = p4.send(Message(t4, b"h4-pinned"), mq=mq)
        landed["pinned"] = (rb.send_status == SendStatus.SEND_OK
                            and rb.message_queue.broker_name == mq.broker_name)

        p4.send_oneway(Message(t4, b"h4-oneway"))
        landed["oneway"] = True      # a oneway has no reply, so only the total of H4 can back it up

        batch = [Message(t4, b"h4-batch-%d" % i) for i in range(3)]
        rr = p4.send(batch)
        landed["batch"] = rr.send_status == SendStatus.SEND_OK

        p4.send_async(Message(t4, b"h4-async"), latch, 5000)
        landed["async"] = (latch.done.wait(10) and latch.error is None
                           and latch.result is not None
                           and latch.result.send_status == SendStatus.SEND_OK)
    finally:
        p4.shutdown()

    for entry in ("sync", "pinned", "oneway", "batch", "async"):
        check("H4 the %s entry point sent successfully" % entry, landed.get(entry, False),
              "" if landed.get(entry) else "that entry point did not get SEND_OK")
    # 1 sync + 1 pinned + 1 oneway + 3 batch + 1 async = 7 messages
    got = _wait_until(lambda: _total_messages(admin, t4) >= 7, 20)
    total = _total_messages(admin, t4)
    check("H4 all seven messages were persisted one by one (the batch counted per sub-message)", got and total == 7,
          "total=%s ok=%s" % (total, landed))

    # ---------------- H5 brokerName lands on the selected machine ----------------
    r5 = None
    p5 = _producer("hdr-h5-%d" % STAMP)
    p5.start()
    try:
        t5 = "HdrBrokerName_%d" % STAMP
        topics.append(t5)
        r5 = p5.send(Message(t5, b"h5"))
    finally:
        p5.shutdown()
    check("H5 the broker name of the landing point matches the route (n is exactly it)",
          r5 is not None and r5.send_status == SendStatus.SEND_OK
          and r5.message_queue.broker_name == broker_name,
          "landing=%s route=%s" % (getattr(r5.message_queue, "broker_name", None) if r5 else None,
                               broker_name))

    # ---------------- cleanup ----------------
    for t in topics:
        try:
            admin.delete_topic(t)
        except Exception as e:  # noqa: BLE001
            print("    (delete_topic(%s) failed: %s)" % (t, e))
    admin.shutdown()

    passed = sum(1 for _, ok, _ in results if ok)
    print("\n== Result: %d/%d passed ==" % (passed, len(results)))
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
