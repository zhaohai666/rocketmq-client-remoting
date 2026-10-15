# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.admin import DefaultMQAdminExt
from client.producer import DefaultMQProducer
from client.send_result import SendStatus
from common.message import Message
from remoting.protocol.codes import RemotingSysResponseCode, RequestCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.rpchook import RPCHook

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


class UnregisterProbe(RPCHook):
    """Record every ``UNREGISTER_CLIENT``(35) request together with its response code.

    The hook hangs on the transport layer (``register_rpc_hook``) and is called before encoding, so
    what it captures is exactly the extFields that go on the wire.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.requests = []     # [(remote_addr, ext_fields snapshot)]
        self.responses = []    # [(remote_addr, response_code or None)]
        self.codes = []        # Record all request codes in order, to confirm that 35 comes after the sends

    def do_before_request(self, remote_addr: str, request: RemotingCommand) -> None:
        ext = dict(request.ext_fields or {})
        if not ext and request.custom_header is not None:
            ext = dict(request.custom_header.to_ext_fields() or {})
        with self.lock:
            self.codes.append(request.code)
            if request.code == RequestCode.UNREGISTER_CLIENT:
                self.requests.append((remote_addr, ext))

    def do_after_response(self, remote_addr, request, response) -> None:
        if request.code != RequestCode.UNREGISTER_CLIENT:
            return
        with self.lock:
            self.responses.append((remote_addr, None if response is None else response.code))

    def snapshot(self):
        with self.lock:
            return list(self.requests), list(self.responses), list(self.codes)


def _producer(group, instance_name):
    p = DefaultMQProducer(group)
    p.set_namesrv_addr(NAMESRV)
    p.set_instance_name(instance_name)
    return p


def _connection_client_ids(admin, broker_addr, group):
    """The clientId list that 204 sees; returns None when the broker says the group does not exist."""
    try:
        pc = admin.examine_producer_connection_info(group, broker_addr)
    except Exception as e:  # noqa: BLE001
        if "not exist" in str(e):
            return None
        raise
    return [c.client_id for c in pc.connection_set]


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
    # All known brokers count, master and slave alike: 35 must be sent to each one
    all_addrs = []
    for ids in cluster.broker_addr_table.values():
        all_addrs.extend(ids.values())
    broker_addr = all_addrs[0]
    check("cluster liveness", True, "brokers=%s" % ",".join(all_addrs))

    topic = "Unreg_%d" % STAMP
    group = "PID_unreg_%d" % STAMP
    peer_group = "PID_unreg_peer_%d" % STAMP

    probe = UnregisterProbe()
    p = _producer(group, "unreg-%d" % STAMP)
    p.rpc_hook = probe
    p.start()
    peer = _producer(peer_group, "unreg-peer-%d" % STAMP)
    peer.start()

    try:
        r = p.send(Message(topic, b"unreg-probe"))
        check("U1 the producer sent successfully", r.send_status == SendStatus.SEND_OK,
              "msgId=%s" % r.msg_id)
        peer.send(Message(topic, b"peer"))

        client_id = p.client_id
        deadline = time.time() + 70
        seen = None
        while time.time() < deadline:
            seen = _connection_client_ids(admin, broker_addr, group)
            if seen and client_id in seen:
                break
            time.sleep(1)
        check("U2 after the heartbeat 204 can see this clientId", bool(seen) and client_id in seen,
              "clientId=%s current=%s" % (client_id, seen))
        peer_seen = _connection_client_ids(admin, broker_addr, peer_group)
        check("U2b the control group registration is visible (showing the 204 criterion itself is valid)",
              bool(peer_seen) and peer.client_id in peer_seen, "current=%s" % peer_seen)

        requests, responses, codes = probe.snapshot()
        check("U3 precondition: there should be no 35 before exiting", not requests,
              "count=%d" % len(requests))

        p.shutdown()
        requests, responses, codes = probe.snapshot()

        check("U3 shutdown sent code 35 (one to each known broker)",
              len(requests) == len(all_addrs),
              "count=%d brokers=%d" % (len(requests), len(all_addrs)))
        shape_ok = bool(requests)
        for addr, ext in requests:
            if ext.get("clientID") != client_id or ext.get("producerGroup") != group:
                shape_ok = False
            if ext.get("consumerGroup"):
                shape_ok = False
            if addr not in all_addrs:
                shape_ok = False
        check("U3b the header of 35 is clientID + producerGroup, consumerGroup does not go on the wire (left empty)",
              shape_ok, "ext=%s" % (requests[0][1] if requests else None))
        all_ok = bool(responses) and all(addr in all_addrs for addr, _ in requests) \
            and all(rc == RemotingSysResponseCode.SUCCESS for _, rc in responses)
        check("U4 every 35 is answered with SUCCESS (it goes over a connection that is still open)",
              all_ok and len(responses) == len(requests),
              "responses=%s" % responses)
        # The order: 35 must come after the business sends (otherwise it would unregister a registration that was never established)
        last_send = max((i for i, c in enumerate(codes)
                         if c in (RequestCode.SEND_MESSAGE, RequestCode.SEND_MESSAGE_V2)),
                        default=-1)
        first_unreg = next((i for i, c in enumerate(codes)
                            if c == RequestCode.UNREGISTER_CLIENT), -1)
        check("U4b 35 comes after the business sends", last_send >= 0 and first_unreg > last_send,
              "last_send=%d first_unreg=%d" % (last_send, first_unreg))

        gone = _wait_until(lambda: _connection_client_ids(admin, broker_addr, group) is None, 10)
        check("U5 after exiting, 204 cannot find this group (the broker replies not exist)", gone,
              "current=%s" % _connection_client_ids(admin, broker_addr, group))
        peer_after = _connection_client_ids(admin, broker_addr, peer_group)
        check("U6 the control group is still there (ruling out a false positive from the broker clearing everything)",
              bool(peer_after) and peer.client_id in peer_after, "current=%s" % peer_after)
    finally:
        peer.shutdown()
        try:
            admin.delete_topic(topic)
        except Exception as e:  # noqa: BLE001
            print("    (delete_topic(%s) failed: %s)" % (topic, e))
        admin.shutdown()

    passed = sum(1 for _, ok, _ in results if ok)
    print("\n== Result: %d/%d passed ==" % (passed, len(results)))
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
