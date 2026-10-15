#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.consumer import DefaultMQPullConsumer
from client.producer import DefaultMQProducer
from client.send_result import SendStatus
from common.message import Message
from common.sysflag import PullSysFlag
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.headers import PullMessageRequestHeader

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BROKER_CTL = os.path.join(ROOT, "scripts", "rmq_test_broker.sh")
TOPIC = "FailFastPy_%d" % int(time.time() * 1000)
GROUP = "fail_fast_py_" + TOPIC

# The client-side timeout is deliberately 30s: if death detection went through the timeout path it would have to wait at least this long.
CLIENT_TIMEOUT_MILLIS = 30000
BROKER_SUSPEND_MILLIS = 20000
# failFast should be millisecond-scale; allow 8s for real-machine scheduling (EOF arrival + callback delivery).
FAIL_FAST_LIMIT_SECONDS = 8.0
BASELINE_MSGS = 5

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


def broker_ctl(action: str) -> tuple:
    """Run scripts/rmq_test_broker.sh and return (returncode, output).

    Output goes to a file, not a pipe: start turns the broker into a resident process which inherits the pipe's write end,
    so ``subprocess`` will not return until every write end is closed -- this shows up as the script having already printed its result
    while the caller is still stuck. File redirection has no such problem.
    """
    out_path = "/tmp/rmq_fail_fast_broker_ctl.%s.log" % action
    with open(out_path, "w") as fh:
        proc = subprocess.run(["sh", BROKER_CTL, action], stdout=fh, stderr=fh,
                              timeout=600, start_new_session=True)
    with open(out_path) as fh:
        return proc.returncode, fh.read().strip()


def send_ok(producer: DefaultMQProducer, tag: str, timeout: int = 5000):
    msg = Message(TOPIC, ("fail-fast-%s" % tag).encode())
    msg.set_keys("ff-" + tag)
    return producer.send(msg, timeout)


def parked_pull_request(group: str, mq, queue_offset: int) -> RemotingCommand:
    """Hand-build a long-poll request with suspend=True: bypassing the pull consumer's clamps,
    both the 30s client timeout and the 20s broker suspend are decided by this script."""
    header = PullMessageRequestHeader()
    header.consumer_group = group
    header.topic = mq.topic
    header.queue_id = mq.queue_id
    header.queue_offset = queue_offset
    header.max_msg_nums = 32
    header.sys_flag = PullSysFlag.build_sys_flag(commit_offset=False, suspend=True,
                                                 subscription=True, class_filter=False)
    header.commit_offset = 0
    header.suspend_timeout_millis = BROKER_SUSPEND_MILLIS
    header.subscription = "*"
    header.sub_version = 0
    header.expression_type = "TAG"
    header.max_msg_bytes = -1
    header.request_source = 0
    return RemotingCommand.create_request_command(RequestCode.PULL_MESSAGE, header)


def main() -> int:
    code, out = broker_ctl("status")
    if code != 0:
        print("broker is not running: start namesrv + broker first per the local cluster runbook (%s)" % out)
        return 2

    producer = DefaultMQProducer(GROUP + "_p")
    producer.set_namesrv_addr(NAMESRV)
    producer.set_instance_name("ff_py_%d" % int(time.time()))
    consumer = DefaultMQPullConsumer(GROUP)
    consumer.set_namesrv_addr(NAMESRV)
    consumer.set_instance_name("ff_py_%d" % int(time.time()))
    threads: list = []

    try:
        # ------------------------------------------------------------------ L1
        producer.start()
        results = [send_ok(producer, "base-%d" % i) for i in range(BASELINE_MSGS)]
        landed = sum(1 for r in results if r.send_status == SendStatus.SEND_OK)
        check("L1 baseline: %d synchronous sends SEND_OK" % BASELINE_MSGS, landed == BASELINE_MSGS,
              "landed=%d" % landed)

        consumer.start()
        queues = consumer.fetch_subscribe_message_queues(TOPIC)
        check("L1 got the queues", bool(queues), "queues=%d" % len(queues))
        if not queues:
            return 1
        mq = queues[0]
        client = consumer._mq_client
        broker_addr = client.find_broker_addr_in_route(client.get_topic_route_data(TOPIC),
                                                      mq.broker_name)
        check("L1 got the broker address", bool(broker_addr), "addr=%s" % broker_addr)
        if not broker_addr:
            return 1
        max_offset = consumer.max_offset(mq)
        check("L1 the queue to park the long-poll on has a usable offset", max_offset is not None,
              "mq=%s max_offset=%s" % (mq, max_offset))
        if max_offset is None:
            return 1
        # Sends rotate across queues, so a single queue's tail only covers the part that landed on it,
        # which is why "all 5 really landed" must be judged by the sum across all queues.
        total_max_offset = max_offset
        for q in queues[1:]:
            other = consumer.max_offset(q)
            if other is not None:
                total_max_offset += other
        check("L1 the summed queue tail offsets cover the %d just sent (really landed)" % BASELINE_MSGS,
              total_max_offset >= BASELINE_MSGS, "max_offset sum=%d" % total_max_offset)

        # ------------------------------------------------------------------ L2
        remoting = client.remoting_client
        baseline_in_flight = len(remoting._response_table)
        parked: list = []
        park_done = threading.Event()
        park_lock = threading.Lock()

        def park_one(tag: str) -> None:
            req = parked_pull_request(GROUP, mq, max_offset)
            started = time.monotonic()
            try:
                resp = remoting.invoke_sync(broker_addr, req, CLIENT_TIMEOUT_MILLIS)
                outcome, err = "response:%s" % resp.code, None
            except BaseException as e:  # noqa: BLE001 - we want the exception type
                outcome, err = type(e).__name__, e
            cost = time.monotonic() - started
            with park_lock:
                parked.append((tag, outcome, cost, err))
                if len(parked) == 3:
                    park_done.set()

        threads = [threading.Thread(target=park_one, args=("park-%d" % i,)) for i in range(3)]
        for t in threads:
            t.start()
        time.sleep(2.0)
        still_parked = sum(1 for t in threads if t.is_alive())
        check("L2 the three long-polls really parked on the broker (still not returned after 2s)", still_parked == 3,
              "parked=%d" % still_parked)
        inflight = len(remoting._response_table)
        check("L2 they are in the in-flight table", inflight >= baseline_in_flight + 3,
              "in_flight=%d (baseline=%d)" % (inflight, baseline_in_flight))
        kill_at = time.monotonic()

        # ------------------------------------------------------------------ L3
        code, out = broker_ctl("stop")
        check("L3 stop the broker", code == 0, out.replace("\n", " | "))
        got_all = park_done.wait(60.0)
        for t in threads:
            t.join(1.0)
        check("L3 all parked long-polls returned (nothing got stuck)", got_all and len(parked) == 3,
              "got=%d" % len(parked))
        types = sorted({p[1] for p in parked})
        send_fail = [p for p in parked if p[1] == "RemotingSendRequestException"]
        timeout_like = [p for p in parked if "Timeout" in p[1]]
        check("L3 the error reported is RemotingSendRequestException (the failFast criterion)",
              len(send_fail) == 3, "types=%s" % types)
        check("L3 none was reported as a timeout (wrong type = wrong retry decision)", not timeout_like,
              "timeout=%d" % len(timeout_like))
        worst = max((p[2] for p in parked), default=0.0)
        check("L3 death-detection time far below the %ds client timeout" % (CLIENT_TIMEOUT_MILLIS // 1000),
              worst < FAIL_FAST_LIMIT_SECONDS,
              "worst=%.2fs limit=%.0fs" % (worst, FAIL_FAST_LIMIT_SECONDS))
        if send_fail:
            check("L3 the exception message carries the disconnect reason", "connection closed" in str(send_fail[0][3]),
                  str(send_fail[0][3]))
        left = len(remoting._response_table)
        check("L3 the in-flight table drains after death detection", left < inflight, "in_flight=%d" % left)

        # ------------------------------------------------------------------ L4
        try:
            req = RemotingCommand.create_request_command(
                RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, None)
            resp = remoting.invoke_sync(NAMESRV, req, 3000)
            check("L4 the namesrv connection is not affected (the broker died but it still serves)",
                  resp.code == ResponseCode.SUCCESS, "code=%s" % resp.code)
        except BaseException as e:  # noqa: BLE001
            check("L4 the namesrv connection is not affected (the broker died but it still serves)", False, repr(e))

        # ------------------------------------------------------------------ L5
        code, out = broker_ctl("start")
        check("L5 the broker is brought back up", code == 0, out.replace("\n", " | ")[:160])
        recovered, err = None, None
        deadline = time.monotonic() + 150.0
        while time.monotonic() < deadline:
            try:
                recovered = send_ok(producer, "after-restart")
                break
            except BaseException as e:  # noqa: BLE001 - failures are allowed within the recovery window
                err, recovered = e, None
                time.sleep(2.0)
        check("L5 the same producer instance sends normally after reconnecting",
              recovered is not None and recovered.send_status == SendStatus.SEND_OK,
              "" if recovered else repr(err))

        # Messages that already got SEND_OK must still be there: fail-fast must not claim already-landed messages as lost.
        # After the restart the route must re-register, so retry a few times on failure.
        total_after, tried = 0, 0
        while tried < 10:
            tried += 1
            total_after = 0
            try:
                for q in consumer.fetch_subscribe_message_queues(TOPIC):
                    total_after += consumer.max_offset(q) or 0
                if total_after >= BASELINE_MSGS:
                    break
            except BaseException:  # noqa: BLE001 - the route is not back yet
                pass
            time.sleep(2.0)
        check("L5 the broker still has those %d SEND_OK messages after the restart" % BASELINE_MSGS,
              total_after >= BASELINE_MSGS, "max_offset sum=%d" % total_after)
    finally:
        for t in threads:
            if t.is_alive():
                t.join(1.0)
        try:
            consumer.shutdown()
        except BaseException:  # noqa: BLE001
            pass
        try:
            producer.shutdown()
        except BaseException:  # noqa: BLE001
            pass
        # An exception midway must not leave the cluster stopped: this script borrows everyone's test cluster.
        try:
            if broker_ctl("status")[0] != 0:
                print("  [WARN] the broker is still down during cleanup, bringing it back up")
                code, out = broker_ctl("start")
                print("  [WARN] broker start: %s" % out.replace("\n", " | ")[:160])
        except BaseException as e:  # noqa: BLE001
            print("  [WARN] failed to bring the broker back up during cleanup: %r (a manual restart is needed, see the local cluster runbook)" % e)

    print("\n%d PASS / %d FAIL" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
