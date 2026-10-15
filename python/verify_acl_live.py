#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import threading
import time
from typing import Optional

sys.path.insert(0, ".")

from client.admin import DefaultMQAdminExt
from client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      SimpleMessageListener)
from client.exception import MQBrokerException
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from client.send_result import SendStatus
from common.message import Message
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.rpchook import AclClientRPCHook, SessionCredentials

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
AK = sys.argv[2] if len(sys.argv) > 2 else "AK_TEST"
SK = sys.argv[3] if len(sys.argv) > 3 else "SK_TEST_SECRET_12345678"

STAMP = int(time.time() * 1000)
TOPIC = "AclLive_%d" % STAMP
GROUP = "GID_AclLive_%d" % STAMP

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


def creds(sk: str = SK) -> AclClientRPCHook:
    return AclClientRPCHook(SessionCredentials(AK, sk))


def response_code_of(exc: BaseException) -> Optional[int]:
    """Find the broker response code by walking the cause chain.

    On the broker side all auth failures converge into AbortProcessException(NO_PERMISSION=16),
    so 16 means "rejected".
    You cannot only look at the outermost layer: admin's create_topic_in_route wraps the underlying MQBrokerException
    inside MQClientException(cause=...), and the outermost layer has no response_code at all.
    """
    seen = set()
    cur: Optional[BaseException] = exc
    while isinstance(cur, BaseException) and id(cur) not in seen:
        seen.add(id(cur))
        code = getattr(cur, "response_code", None)
        if code:
            return int(code)
        nxt = getattr(cur, "cause", None) or cur.__cause__
        cur = nxt if isinstance(nxt, BaseException) else None
    return None


def deepest_message(exc: BaseException) -> str:
    """Take the innermost readable message in the exception chain (usually the broker's remark)."""
    msg = str(exc)
    seen = set()
    cur: Optional[BaseException] = exc
    while isinstance(cur, BaseException) and id(cur) not in seen:
        seen.add(id(cur))
        inner = getattr(cur, "error_message", None) or str(cur)
        if inner:
            msg = inner
        nxt = getattr(cur, "cause", None) or cur.__cause__
        cur = nxt if isinstance(nxt, BaseException) else None
    return msg


def is_no_permission(code: Optional[int]) -> bool:
    """The response code for a broker auth failure (NO_PERMISSION=16)."""
    return code == ResponseCode.NO_PERMISSION


def try_call(fn):
    """Run fn and return (ok, detail); on failure the detail always includes the response code found."""
    try:
        fn()
        return True, ""
    except BaseException as e:  # noqa: BLE001 - a live verification needs to see every failure shape
        code = response_code_of(e)
        detail = "%s %s: %s" % (
            type(e).__name__,
            ("code=%s" % code) if code is not None else "code=?",
            deepest_message(e),
        )
        return False, detail


def scenario_admin_create() -> None:
    print("\nS1/S2 admin creates a topic: with credentials vs without")
    # S1 positive
    admin = DefaultMQAdminExt(rpc_hook=creds())
    admin.set_namesrv_addr(NAMESRV)
    admin.start()
    try:
        ok, detail = try_call(lambda: admin.create_topic("TBW102", TOPIC, 4))
        check("S1 admin with credentials created the topic successfully", ok, detail)
    finally:
        admin.shutdown()

    # S2 negative: without credentials
    print("\nS2 an admin without credentials creating a topic must be rejected")
    admin2 = DefaultMQAdminExt()  # no rpc_hook passed
    admin2.set_namesrv_addr(NAMESRV)
    admin2.start()
    try:
        ok, detail = try_call(lambda: admin2.create_topic("TBW102", TOPIC + "_DENIED", 4))
        check("S2 an admin without credentials is rejected by the broker", (not ok) and is_no_permission_detail(detail), detail)
    finally:
        admin2.shutdown()


def is_no_permission_detail(detail: str) -> bool:
    return ("code=%d" % ResponseCode.NO_PERMISSION) in detail


def scenario_bad_producer() -> None:
    """S3 negative: a producer with a wrong secretKey must be rejected by the broker."""
    print("\nS3 a producer with a wrong secretKey must be rejected")
    bad = DefaultMQProducer("PG_AclLiveBad_%d" % STAMP, rpc_hook=creds("WRONG_SECRET_KEY"))
    bad.set_namesrv_addr(NAMESRV)
    bad.start()
    try:
        msg = Message(TOPIC, b"should-not-send")
        msg.set_keys("acl-wrong")
        ok, detail = try_call(lambda: bad.send(msg))
        check("S3 the wrong secretKey is rejected by the broker", (not ok) and is_no_permission_detail(detail), detail)
    finally:
        bad.shutdown()


def scenario_consumer_then_producer() -> None:
    pass
    print("\nS4/S5 producer/consumer with correct credentials (start the consumer before sending)")
    consumer = DefaultMQPushConsumer(GROUP, rpc_hook=creds())
    consumer.set_namesrv_addr(NAMESRV)
    consumer.subscribe(TOPIC, "*")

    got = []
    lock = threading.Lock()

    def on_msg(msgs):
        with lock:
            for m in msgs:
                got.append(bytes(m.body))
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    consumer.set_message_listener(SimpleMessageListener(on_msg))
    consumer.start()
    try:
        # Wait for rebalance to hand out the queues: the initial offset must be resolved while the topic is still empty
        time.sleep(5)

        sent = 0
        evidence = []
        prod = DefaultMQProducer("PG_AclLiveOk_%d" % STAMP, rpc_hook=creds())
        prod.set_namesrv_addr(NAMESRV)
        prod.start()
        try:
            for i in range(3):
                msg = Message(TOPIC, ("acl-ok-%d" % i).encode())
                msg.set_keys("acl-ok")
                try:
                    result = prod.send(msg)
                    # SendStatus is an Enum: str(SendStatus.SEND_OK) == "SendStatus.SEND_OK",
                    # so it must be compared as an enum, not as a string (comparing as a string once misjudged a successful send as sent=0).
                    evidence.append("%s/%s" % (result.send_status, result.msg_id))
                    if result.send_status == SendStatus.SEND_OK:
                        sent += 1
                    else:
                        check("S4 send status of message %d" % i, False, str(result.send_status))
                except BaseException as e:  # noqa: BLE001
                    evidence.append("EXC:%s" % e)
                    check("S4 send of message %d" % i, False, "%s: %s" % (type(e).__name__, e))
        finally:
            prod.shutdown()
        # msgId is generated by the broker: non-empty proves the broker really accepted this signed request
        check("S4 the credentialed producer sent 3 messages", sent == 3, "sent=%d %s" % (sent, evidence))

        # S5: heartbeat / long-polling pull / offset commit are all signed throughout; only receiving everything counts as the consume path passing auth
        deadline = time.time() + 30
        while time.time() < deadline and len(got) < sent:
            time.sleep(0.3)
        check("S5 the credentialed consumer received all %d messages" % sent, len(got) == sent,
              "got=%d bodies=%s" % (len(got), sorted(got)))
    finally:
        consumer.shutdown()


def scenario_negative_rpc() -> None:
    """Fire a raw RPC straight at the broker and read response.code yourself.

    Do not use client.get_consumer_id_list_by_group: it swallows exceptions internally and returns None on failure,
    so "rejected" and "success" look identical to the caller (this once caused a false PASS).
    """
    print("\nS6 a direct broker RPC without credentials must be rejected")
    client = MQClientInstance("AclProbe_%d" % STAMP, [NAMESRV])
    client.start()
    try:
        try:
            addr = client._broker_addr_for_topic(TOPIC)
        except BaseException as e:  # noqa: BLE001
            check("S6 got the broker address", False, "%s: %s" % (type(e).__name__, e))
            return
        request = RemotingCommand.create_request_command(
            RequestCode.GET_CONSUMER_LIST_BY_GROUP, None)
        request.ext_fields["consumerGroup"] = GROUP

        def call() -> None:
            response = client.remoting_client.invoke_sync(addr, request, 5000)
            if response.code != ResponseCode.SUCCESS:
                raise MQBrokerException(response.code, response.remark or "")

        ok, detail = try_call(call)
        check("S6 the uncredentialed broker RPC is rejected",
              (not ok) and is_no_permission_detail(detail),
              "addr=%s %s" % (addr, detail))
    finally:
        client.shutdown()


def scenario_namesrv_without_creds() -> None:
    print("\nS7 a NameServer route query without credentials should still succeed (the hook does not affect the namesrv path)")
    client = MQClientInstance("AclNs_%d" % STAMP, [NAMESRV])
    client.start()
    try:
        ok, detail = try_call(lambda: client.update_topic_route_info_from_name_server(TOPIC))
        check("S7 the uncredentialed namesrv route query succeeds", ok, detail)
    finally:
        client.shutdown()


def main() -> int:
    print("=" * 72)
    print("ACL live: namesrv=%s topic=%s group=%s ak=%s" % (NAMESRV, TOPIC, GROUP, AK))
    print("=" * 72)

    scenario_admin_create()
    scenario_bad_producer()
    scenario_consumer_then_producer()
    scenario_negative_rpc()
    scenario_namesrv_without_creds()

    print("\n===== ACL live summary =====")
    print("  PASS=%d FAIL=%d" % (PASS, FAIL))
    if FAIL == 0:
        print("  result: ACL auth (signing / rejection / namesrv compatibility) passed live")
        return 0
    print("  result: %d item(s) failed" % FAIL)
    return 1


if __name__ == "__main__":
    sys.exit(main())
