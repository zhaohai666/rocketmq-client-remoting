#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Verification of the POP mode (5.x lightweight consumption) against a real machine.

Usage (requires a local RocketMQ 5.5.1 cluster; the broker needs autoCreateTopicEnable/autoCreateSubscriptionGroup):
    .venv/bin/python verify_pop_live.py 127.0.0.1:9876

Why it is tested on its own: POP is the "lightweight consumption" pipeline of 5.x, and this
project previously only had the constants, no implementation. The semantic differences
between POP and pull are the focus of the assertions:
  - it does **not** commit offsets; it is confirmed by ack, and an un-acked message is revived
    and redelivered after invisibleTime;
  - the broker does **not** write a POP_CK for messages of an ordinary topic, so the client has to
    reconstruct the 8-segment CK string itself.

Scenarios:
  S1 create topic(8 queues) + send 10 messages
  S2 multi-queue POP (queue_id=-1, init_mode=0) -> FOUND and messages obtained
  S3 every message is stamped with an **8-segment** POP_CK whose brokerName/queueId match the
     actual message
  S4 ack one -> SUCCESS
  S5 change_invisible_time -> SUCCESS and it returns a **new** 8-segment extraInfo
  S6 the validation really works: an illegal queueId -> MESSAGE_ILLEGAL; an out-of-range offset
     -> NO_MESSAGE
  S7 single-queue POP (a new consumer group + queue_id=0) -> FOUND and all messages have queueId 0
  S8 an un-acked message is revived: POP with a small invisibleTime and no ack, after waiting a
     re-POP obtains the same message again, and its POP_CK has retryFlag=1 (coming from
     %RETRY%<group>_<topic>) -- at-least-once semantics
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from client.consumer_result import PopStatus
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message
from common.message_const import MessageConst
from remoting.protocol import extra_info as ei
from remoting.protocol.codes import ResponseCode

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "PopLive_%d" % STAMP
TOPIC_REVIVE = "PopLiveRevive_%d" % STAMP
GROUP = "GID_PopLive_%d" % STAMP
GROUP_SINGLE = "GID_PopLiveSingle_%d" % STAMP
GROUP_REVIVE = "GID_PopLiveRevive_%d" % STAMP
QUEUE_NUM = 8
N_MSG = 10
REVIVE_QUEUE_NUM = 4
REVIVE_N_MSG = 3
INVISIBLE_SHORT = 5000
REVIVE_WAIT_SEC = 20

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


def main() -> int:
    prod = DefaultMQProducer("PG_PopLive_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    client = prod._mq_client
    assert isinstance(client, MQClientInstance)

    try:
        # ---------------- S1 create the topic + send the messages ----------------
        print("=== S1 preparing the topic and the messages ===")
        prod.create_topic("TBW102", TOPIC, QUEUE_NUM)
        prod.create_topic("TBW102", TOPIC_REVIVE, REVIVE_QUEUE_NUM)
        for i in range(N_MSG):
            prod.send(Message(TOPIC, ("pop-live-%d" % i).encode("utf-8"),
                              keys="pk%d" % i))
        print("  sent %d msgs to %s" % (N_MSG, TOPIC))

        route = client.get_topic_route_data(TOPIC)
        check("S1 the topic route is usable", route is not None and len(route.get_broker_datas()) > 0)
        broker_name = route.get_broker_datas()[0].broker_name
        addr = MQClientInstance.find_broker_addr_in_route(route, broker_name)
        print("  brokerName=%s addr=%s" % (broker_name, addr))

        # ---------------- S2 multi-queue POP ----------------
        print("=== S2 multi-queue POP (the addr goes through the automatic resolution path) ===")
        res = client.pop_message(GROUP, TOPIC, queue_id=-1, max_msg_nums=32,
                                 invisible_time=60000, poll_time=0, init_mode=0)
        check("S2 the POP obtained messages", res.status == PopStatus.FOUND and len(res.msg_found_list) > 0,
              "status=%s count=%d startOffsetInfo=%r" %
              (res.status, len(res.msg_found_list), res.start_offset_info))
        if not res.msg_found_list:
            print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
            return 1

        # ---------------- S3 POP_CK reconstruction ----------------
        print("=== S3 POP_CK reconstruction (8 segments) ===")
        all8 = True
        match = True
        sample = None
        for m in res.msg_found_list:
            ck = m.properties.get(MessageConst.PROPERTY_POP_CK)
            if not ck:
                all8 = False
                break
            seg = ei.split(ck)
            if len(seg) != 8:
                all8 = False
                break
            if ei.get_broker_name(seg) != broker_name or ei.get_queue_id(seg) != m.queue_id:
                match = False
            if sample is None:
                sample = ck
        check("S3a every message has an 8-segment POP_CK", all8, "sample=%r" % sample)
        check("S3b the brokerName/queueId of the CK match the message", match)
        check("S3c 1ST_POP_TIME was filled in",
              all(m.properties.get(MessageConst.PROPERTY_FIRST_POP_TIME) is not None
                  for m in res.msg_found_list))

        # ---------------- S4 ack ----------------
        print("=== S4 ACK ===")
        first = res.msg_found_list[0]
        ck1 = first.properties[MessageConst.PROPERTY_POP_CK]
        seg1 = ei.split(ck1)
        code = client.ack_message(GROUP, TOPIC, ei.get_queue_id(seg1), ck1,
                                  ei.get_queue_offset(seg1), addr=addr)
        check("S4 the ack returned SUCCESS", code == ResponseCode.SUCCESS,
              "code=%d queueId=%d offset=%d" %
              (code, ei.get_queue_id(seg1), ei.get_queue_offset(seg1)))

        # ---------------- S5 change invisible time ----------------
        print("=== S5 CHANGE_MESSAGE_INVISIBLETIME ===")
        second = res.msg_found_list[1]
        ck2 = second.properties[MessageConst.PROPERTY_POP_CK]
        seg2 = ei.split(ck2)
        res5 = client.change_invisible_time(GROUP, TOPIC, ei.get_queue_id(seg2), ck2,
                                            ei.get_queue_offset(seg2), 30000, addr=addr)
        new_seg = ei.split(res5.extra_info) if res5.extra_info else []
        check("S5a extending the invisible time succeeded", res5.success,
              "code=%d popTime=%d invisibleTime=%d" %
              (res5.response_code, res5.pop_time, res5.invisible_time))
        check("S5b it returns a new 8-segment extraInfo and uses the new value",
              len(new_seg) == 8 and ei.get_invisible_time(new_seg) == res5.invisible_time
              and ei.get_pop_time(new_seg) == res5.pop_time,
              "new=%r" % res5.extra_info)

        # ---------------- S6 the validation really works ----------------
        print("=== S6 illegal arguments must be rejected ===")
        bad_queue = client.ack_message(GROUP, TOPIC, QUEUE_NUM + 90, ck1,
                                       ei.get_queue_offset(seg1), addr=addr)
        check("S6a an illegal queueId is rejected (MESSAGE_ILLEGAL)",
              bad_queue == ResponseCode.MESSAGE_ILLEGAL, "code=%d" % bad_queue)
        bad_offset = client.ack_message(GROUP, TOPIC, ei.get_queue_id(seg1), ck1,
                                        1 << 40, addr=addr)
        check("S6b an out-of-range offset is rejected (NO_MESSAGE)",
              bad_offset == ResponseCode.NO_MESSAGE, "code=%d" % bad_offset)

        # ---------------- S7 single-queue POP ----------------
        print("=== S7 single-queue POP ===")
        # Use a new consumer group: the old group already has a pop offset on the same (topic), so a
        # repeated POP would obtain nothing
        res7 = client.pop_message(GROUP_SINGLE, TOPIC, queue_id=0, max_msg_nums=32,
                                  invisible_time=60000, poll_time=0, init_mode=0, addr=addr)
        only_q0 = all(m.queue_id == 0 for m in res7.msg_found_list)
        check("S7 the single-queue POP obtained the messages of queue 0",
              res7.status == PopStatus.FOUND and len(res7.msg_found_list) > 0 and only_q0,
              "status=%s count=%d queueIds=%s" %
              (res7.status, len(res7.msg_found_list),
               sorted({m.queue_id for m in res7.msg_found_list})))

        # ---------------- S8 an un-acked message gets revived ----------------
        print("=== S8 no ack -> revived redelivery (at-least-once semantics) ===")
        for i in range(REVIVE_N_MSG):
            prod.send(Message(TOPIC_REVIVE, ("pop-revive-%d" % i).encode("utf-8"),
                              keys="rv%d" % i))
        route_rv = client.get_topic_route_data(TOPIC_REVIVE)
        addr_rv = MQClientInstance.find_broker_addr_in_route(
            route_rv, route_rv.get_broker_datas()[0].broker_name)
        res8a = client.pop_message(GROUP_REVIVE, TOPIC_REVIVE, queue_id=-1,
                                   invisible_time=INVISIBLE_SHORT, poll_time=0,
                                   init_mode=0, addr=addr_rv)
        first_keys = {m.properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                      or m.msg_id for m in res8a.msg_found_list}
        check("S8a the first POP round obtained messages (no ack)",
              res8a.status == PopStatus.FOUND and len(res8a.msg_found_list) > 0,
              "count=%d" % len(res8a.msg_found_list))

        print("  waiting %ds to let the broker revive it..." % REVIVE_WAIT_SEC)
        time.sleep(REVIVE_WAIT_SEC)

        res8b = client.pop_message(GROUP_REVIVE, TOPIC_REVIVE, queue_id=-1,
                                   invisible_time=60000, poll_time=0, init_mode=0,
                                   addr=addr_rv)
        revived = [m for m in res8b.msg_found_list
                   if (m.properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                       or m.msg_id) in first_keys]
        retry_flags = set()
        for m in revived:
            ck = m.properties.get(MessageConst.PROPERTY_POP_CK)
            if ck:
                retry_flags.add(ei.get_retry(ei.split(ck)))
        check("S8b the un-acked message was revived and redelivered", len(revived) > 0,
              "revived=%d total=%d" % (len(revived), len(res8b.msg_found_list)))
        check("S8c the POP_CK of the revived message has retryFlag=1 (coming from %%RETRY%%<group>_<topic>)",
              "1" in retry_flags,
              "retryFlags=%s" % sorted(retry_flags))

    finally:
        prod.shutdown()

    print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
