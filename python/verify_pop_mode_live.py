#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Live verification of SET_MESSAGE_REQUEST_MODE (401) against a real broker.

Usage (needs a local RocketMQ 5.5.1 cluster whose broker has autoCreateSubscriptionGroup):
    .venv/bin/python verify_pop_mode_live.py 127.0.0.1:9876
    .venv/bin/python verify_pop_mode_live.py 127.0.0.1:9876 l3

Why this needs a live leg of its own: 401 carries **no custom header at all**. The broker
decodes one JSON body object -- {topic, consumerGroup, mode, popShareQueueNum} -- and reads its
four values out of that object alone. A request that puts them in the header instead leaves the
broker with no body to decode, so it never reaches the mode table. The answer code alone is a weak
witness (it can be a generic system error, and a rejected request is easy to mistake for a
successful one); only a broker-side readback proves the mode really switched, and the readback
exists: GET_ALL_MESSAGE_REQUEST_MODE (402) hands back the map the 401 wrote, with the values the
broker actually stored.

  L1 the body form switches the mode: set POP with a share count of 4 on a real topic, then read
     402 and require the entry to name this topic + group with mode POP and the number 4. The
     number is the point: it is a JSON number in the body, and a body that carried it as a string
     or dropped it would not round-trip to 4.
  L2 the stored value is the caller's, not the broker's default: switch the same pair to PULL and
     require 402 to say PULL. If the mode were merely "whatever the first write left", the second
     write would be invisible.
  L3 paired control -- the header form: send the same four values as header fields with no body
     for a second group, print the answer the broker really gives, and require that group to be
     absent from 402. On a live 5.5.1 broker the header form is not silently ignored: with nothing
     to decode the request fails with a system error, and the mode never switches. That is exactly
     the behaviour this leg exists to pin -- the header shape cannot change a request mode.

The subscription group is left to the broker's autoCreateSubscriptionGroup; the topic is created
explicitly because 401 refuses a topic the broker has no config for, and that refusal would be
indistinguishable from a broken request shape.

Closing line `############ PASS=<n> FAIL=<n> ############`; exits non-zero on any FAIL.
"""
from __future__ import annotations

import json
import sys
import time
from typing import Optional, Tuple

sys.path.insert(0, ".")

from client.admin import DefaultMQAdminExt
from common.mix_all import MixAll
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
LEGS = (sys.argv[2] if len(sys.argv) > 2 else "all").split(",")
STAMP = int(time.time())
TOPIC = "PopModeVerifyTopic_%d" % STAMP
GROUP = "PopModeVerifyGroup_%d" % STAMP
CTRL_GROUP = "PopModeVerifyCtrlGroup_%d" % STAMP
SHARE_NUM = 4
ROUTE_WAIT_SEC = 30.0

PASS = 0
FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print("  [PASS] %s %s" % (name, ("  " + detail) if detail else ""))
    else:
        FAIL += 1
        print("  [FAIL] %s %s" % (name, ("  " + detail) if detail else ""))


def selected(leg: str) -> bool:
    return LEGS == ["all"] or leg in LEGS


def broker_addr_of(admin: DefaultMQAdminExt, topic: str) -> Optional[str]:
    """The master address the route advertises for this topic, or None while the route is absent."""
    deadline = time.monotonic() + ROUTE_WAIT_SEC
    while time.monotonic() < deadline:
        try:
            route = admin.examine_topic_route(topic)
        except Exception:  # noqa: BLE001 - a route that has not landed yet is a retry, not a failure
            time.sleep(0.5)
            continue
        if route and route.broker_datas:
            addrs = route.broker_datas[0].broker_addrs or {}
            for broker_id in sorted(addrs):
                if addrs[broker_id]:
                    return str(addrs[broker_id])
        time.sleep(0.5)
    return None


def raw_invoke(admin: DefaultMQAdminExt, addr: str, request: RemotingCommand) -> RemotingCommand:
    """One raw round trip on the admin's own transport: the response code is the observation."""
    client = admin._require_client()
    return client.remoting_client.invoke_sync(addr, request, 10000)


def all_message_request_mode(admin: DefaultMQAdminExt, addr: str) -> Tuple[int, dict]:
    """402 readback: the code the broker answered and the topic -> group -> entry map it stored."""
    request = RemotingCommand.create_request_command(
        RequestCode.GET_ALL_MESSAGE_REQUEST_MODE, None)
    response = raw_invoke(admin, addr, request)
    body = response.body or b""
    if not body:
        return response.code, {}
    wrapper = json.loads(body.decode("utf-8"))
    return response.code, (wrapper.get("messageRequestModeMap") or {})


def entry_for(mode_map: dict, topic: str, group: str) -> Optional[dict]:
    return (mode_map.get(topic) or {}).get(group)


def main() -> int:
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()
    print("=== python POP mode live: ns=%s topic=%s ===" % (NAMESRV, TOPIC))
    try:
        admin.create_topic(MixAll.DEFAULT_TOPIC, TOPIC, 8)
        addr = broker_addr_of(admin, TOPIC)
        if addr is None:
            check("the topic has a broker address to aim at", False, "no route for %s" % TOPIC)
            return 1
        print("  broker addr %s" % addr)

        if selected("l1"):
            try:
                admin.set_message_request_mode(addr, TOPIC, GROUP, "POP", SHARE_NUM)
                code, mode_map = all_message_request_mode(admin, addr)
                check("402 answers SUCCESS once a mode exists",
                      code == ResponseCode.SUCCESS, "code=%s" % code)
                entry = entry_for(mode_map, TOPIC, GROUP)
                check("the body form wrote an entry for this topic and group",
                      entry is not None, "map=%s" % json.dumps(mode_map)[:160])
                if entry is not None:
                    check("the mode the broker stored is the mode the body asked for",
                          entry.get("mode") == "POP", "stored=%s" % entry.get("mode"))
                    check("the share count round-trips as the number the body carried",
                          entry.get("popShareQueueNum") == SHARE_NUM,
                          "stored=%r" % (entry.get("popShareQueueNum"),))
                    check("the entry names the topic and group it was written under",
                          entry.get("topic") == TOPIC and entry.get("consumerGroup") == GROUP,
                          "entry=%s" % json.dumps(entry)[:160])
            except Exception as e:  # noqa: BLE001
                check("the body form is accepted by the broker", False,
                      "%s: %s" % (type(e).__name__, e))

        if selected("l2"):
            try:
                admin.set_message_request_mode(addr, TOPIC, GROUP, "PULL", 0)
                _, mode_map = all_message_request_mode(admin, addr)
                entry = entry_for(mode_map, TOPIC, GROUP)
                check("a second body-form write replaces the stored mode",
                      entry is not None and entry.get("mode") == "PULL",
                      "stored=%s" % (entry or {}).get("mode"))
                check("the share count of the second write is what the stored entry carries",
                      entry is not None and entry.get("popShareQueueNum") == 0,
                      "stored=%r" % ((entry or {}).get("popShareQueueNum"),))
            except Exception as e:  # noqa: BLE001
                check("the second body-form write is accepted", False,
                      "%s: %s" % (type(e).__name__, e))

        if selected("l3"):
            # The header form: the four values as header fields, no body. This is the shape the
            # client used to send, and the one the broker has no way to read.
            request = RemotingCommand.create_request_command(
                RequestCode.SET_MESSAGE_REQUEST_MODE, None)
            request.ext_fields["topic"] = TOPIC
            request.ext_fields["consumerGroup"] = CTRL_GROUP
            request.ext_fields["mode"] = "POP"
            request.ext_fields["popShareQueueNum"] = str(SHARE_NUM)
            response = raw_invoke(admin, addr, request)
            print("  header-form answer: code=%s remark=%s" % (response.code, response.remark))
            _, mode_map = all_message_request_mode(admin, addr)
            check("the header form switches no mode on the broker",
                  entry_for(mode_map, TOPIC, CTRL_GROUP) is None,
                  "map=%s" % json.dumps(mode_map)[:160])
            check("the header form is not the accepted shape the body form is",
                  response.code != ResponseCode.SUCCESS,
                  "code=%s" % response.code)

        return 1 if FAIL else 0
    finally:
        try:
            admin.delete_topic(TOPIC)
        except Exception as e:  # noqa: BLE001 - a topic that will not delete is litter, not a failure
            print("  [note] delete_topic(%s) failed: %s" % (TOPIC, e))
        admin.shutdown()


if __name__ == "__main__":
    rc = main()
    print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
    sys.exit(0 if rc == 0 else 1)
