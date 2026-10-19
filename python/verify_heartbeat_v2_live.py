#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Live verification of heartbeat V2 against a real broker.

Offline tests can only prove that this client implements its own decision table; they cannot
prove what a broker does with the two body fields. This leg pins the wire contract against a
running cluster:

S1 protocol handshake (one raw client instance, hand-built bodies):
   * the broker answers IS_SUPPORT_HEART_BEAT_V2,
   * the broker's per-group ``group -> fingerprint`` table really drives IS_SUB_CHANGE:
     a V1 round (fingerprint 0) followed by the first V2 round is a change, the round after it
     is not, and a widened subscription is a change again,
   * a fingerprint computed from a freshly built but equivalent body matches the remembered
     one (the normalization that drops subVersion / clientID is what makes this true),
   * a ``withoutSub`` round keeps the broker-side subscription set (204 still shows it), while
     the same light body for a group that never delivered one leaves 204 with nothing.

S2 a real push consumer driven through the V2 path: the rounds on the wire are recorded, so the
   leg proves the light body is actually sent, that every round is accepted, that the
   broker-side tag filter still holds after the light rounds (only the matching messages are
   consumed), and that the group stays registered.

S3 is the paired control for S2: the same consumer with the opt-in off never puts a
   non-zero fingerprint or a withoutSub body on the wire, and consumes just the same.
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.admin import DefaultMQAdminExt
from client.consumer import (ConsumeConcurrentlyStatus, DefaultMQPushConsumer,
                             SimpleMessageListener)
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message
from common.subscription_data import FilterAPI
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.heartbeat import (ConsumeFromWhere, ConsumeType, ConsumerData,
                                         HeartbeatData, MessageModel)
from remoting.protocol.subscription import SubscriptionGroupConfig

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "HbV2_%d" % STAMP
PROBE_GROUP = "GID_HbV2Probe_%d" % STAMP
LIGHT_ONLY_GROUP = "GID_HbV2LightOnly_%d" % STAMP
V2_GROUP = "GID_HbV2On_%d" % STAMP
V1_GROUP = "GID_HbV2Off_%d" % STAMP
PRODUCER_GROUP = "PID_HbV2_%d" % STAMP
TAG_A = "TagV2A"
TAG_B = "TagV2B"
EXPR = "%s||%s" % (TAG_A, TAG_B)

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


def probe_heartbeat(group: str, client_id: str, topics=(TOPIC,),
                    expression: str = EXPR) -> HeartbeatData:
    hb = HeartbeatData(client_id)
    cd = ConsumerData(group, ConsumeType.CONSUME_PASSIVELY, MessageModel.CLUSTERING)
    for topic in topics:
        cd.subscription_data_set.add(FilterAPI.build_subscription_data(topic, expression))
    hb.consumer_data_set.add(cd)
    return hb


def subscriptions_of(admin, group, broker_addr) -> dict:
    """The broker's own subscription table for a group (204), keyed by topic."""
    try:
        cc = admin.examine_consumer_connection_info(group, broker_addr)
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}
    return {topic: (sub or {}).get("subString") for topic, sub in cc.subscription_table.items()}


def connected_client_ids(admin, group, broker_addr):
    try:
        cc = admin.examine_consumer_connection_info(group, broker_addr)
    except Exception:  # noqa: BLE001
        return None
    return [c.client_id for c in cc.connection_set]


# ------------------------------------------------------- recorded heartbeat rounds


class HeartbeatRecorder:
    """Wraps the transport so every HEART_BEAT frame on the wire is classified and answered."""

    def __init__(self, inner):
        self._inner = inner
        self._lock = threading.Lock()
        self.full = 0
        self.light = 0
        self.v1 = 0
        self.errors = []
        self.fingerprints = set()

    def __call__(self, addr, request, timeout_millis=None):
        if request.code != RequestCode.HEART_BEAT:
            return self._inner(addr, request, timeout_millis)
        hb = HeartbeatData.decode(request.body)
        response = self._inner(addr, request, timeout_millis)
        with self._lock:
            self.fingerprints.add(hb.heartbeat_fingerprint)
            if hb.heartbeat_fingerprint == 0:
                self.v1 += 1
            elif hb.without_sub:
                self.light += 1
            else:
                self.full += 1
            if response.code != ResponseCode.SUCCESS:
                self.errors.append((addr, response.code, response.remark))
        return response

    def snapshot(self):
        with self._lock:
            return dict(full=self.full, light=self.light, v1=self.v1,
                        errors=list(self.errors), fingerprints=set(self.fingerprints))


def wrap_transport(client: MQClientInstance) -> HeartbeatRecorder:
    recorder = HeartbeatRecorder(client.remoting_client.invoke_sync)
    client.remoting_client.invoke_sync = recorder
    return recorder


# ------------------------------------------------------------------ S1 handshake


def phase_protocol(inst, admin, master, client_id):
    print("\nS1 the broker's own fingerprint table drives the two flags")
    hb = probe_heartbeat(PROBE_GROUP, client_id)

    # A V1 round first: fingerprint 0, so the broker takes the plain registration path.
    v1_version = inst.send_heartbeat(master, hb, 5000)
    subs = subscriptions_of(admin, PROBE_GROUP, master)
    check("S1a V1 heartbeat registers the group and its subscription",
          subs.get(TOPIC) == EXPR, "version=%s broker subscriptionTable=%s" % (v1_version, subs))
    fingerprint = hb.compute_fingerprint()
    check("S1b the fingerprint is non-zero (a zero value keeps the broker on the V1 path)",
          fingerprint != 0, "fingerprint=%d" % fingerprint)

    # First V2 round for the group: the broker still holds 0 for it, so it must call this a
    # subscription change. That is the control that proves the flag comes from the stored
    # fingerprint and not from the request.
    hb.heartbeat_fingerprint = fingerprint
    r1 = inst.send_heartbeat_v2(master, hb, 5000)
    check("S1c the broker declares IS_SUPPORT_HEART_BEAT_V2", r1.support_v2, repr(r1))
    check("S1d the first V2 round after a V1 round reports IS_SUB_CHANGE "
          "(the broker held the V1 fingerprint 0)", r1.sub_change, repr(r1))

    r2 = inst.send_heartbeat_v2(master, hb, 5000)
    check("S1e the same fingerprint again reports no change",
          r2.support_v2 and not r2.sub_change, repr(r2))

    # An equivalent body built from scratch: subVersion is a wall-clock stamp and clientID is
    # per process, so the fingerprint must still match the one the broker just stored.
    rebuilt = probe_heartbeat(PROBE_GROUP, client_id + "-other-process")
    for cd in rebuilt.consumer_data_set:
        for s in cd.subscription_data_set:
            s.sub_version = int(time.time() * 1000)
    rebuilt.heartbeat_fingerprint = rebuilt.compute_fingerprint()
    check("S1f an equivalent body rebuilt in another process yields the same fingerprint",
          rebuilt.heartbeat_fingerprint == fingerprint,
          "rebuilt=%d stored=%d" % (rebuilt.heartbeat_fingerprint, fingerprint))
    r3 = inst.send_heartbeat_v2(master, rebuilt, 5000)
    check("S1g control: the rebuilt body is not seen as a change "
          "(a fingerprint that moved every round would defeat the light body)",
          not r3.sub_change, repr(r3))

    # The light body: no subscription set at all. The broker must keep the one it already holds.
    light = probe_heartbeat(PROBE_GROUP, client_id).without_sub_copy()
    light.heartbeat_fingerprint = fingerprint
    check("S1h the light copy keeps the fingerprint it was copied from",
          light.without_sub and light.heartbeat_fingerprint == fingerprint,
          "fingerprint=%d" % light.heartbeat_fingerprint)
    r4 = inst.send_heartbeat_v2(master, light, 5000)
    check("S1i the broker accepts a withoutSub round", r4.support_v2 and not r4.sub_change,
          repr(r4))
    kept = subscriptions_of(admin, PROBE_GROUP, master)
    check("S1j the broker-side subscription survives the withoutSub round",
          kept.get(TOPIC) == EXPR, "broker subscriptionTable=%s" % kept)

    # Control for S1j: the very same light body for a group that never delivered a subscription
    # leaves nothing on the broker, so the evidence above really comes from the full round.
    orphan = probe_heartbeat(LIGHT_ONLY_GROUP, client_id)
    orphan_light = HeartbeatData(client_id)
    orphan_light.consumer_data_set.add(
        ConsumerData(LIGHT_ONLY_GROUP, ConsumeType.CONSUME_PASSIVELY, MessageModel.CLUSTERING))
    orphan_light.without_sub = True
    orphan_light.heartbeat_fingerprint = orphan.compute_fingerprint()
    inst.send_heartbeat_v2(master, orphan_light, 5000)
    empty = subscriptions_of(admin, LIGHT_ONLY_GROUP, master)
    check("S1k control: a withoutSub round for a fresh group registers no subscription",
          TOPIC not in empty, "broker subscriptionTable=%s" % empty)

    # Widening the subscription changes the fingerprint, and the broker reports the change.
    widened = probe_heartbeat(PROBE_GROUP, client_id, topics=(TOPIC, "HbV2Extra_%d" % STAMP))
    widened.heartbeat_fingerprint = widened.compute_fingerprint()
    check("S1l a widened subscription yields a different fingerprint",
          widened.heartbeat_fingerprint != fingerprint,
          "new=%d old=%d" % (widened.heartbeat_fingerprint, fingerprint))
    r5 = inst.send_heartbeat_v2(master, widened, 5000)
    check("S1m the broker reports IS_SUB_CHANGE for the new fingerprint", r5.sub_change,
          repr(r5))
    check("S1n the broker keeps the clientId registered through all five rounds",
          client_id in (connected_client_ids(admin, PROBE_GROUP, master) or []),
          "seen=%s" % connected_client_ids(admin, PROBE_GROUP, master))


# ------------------------------------------------------- S2/S3 consumer through V2


def run_consumer_leg(group, use_v2, expected, rounds):
    """Start a push consumer, drive several heartbeat rounds, and report what went on the wire."""
    got = []
    lock = threading.Lock()

    def on_msg(msgs):
        with lock:
            got.extend(bytes(m.body) for m in msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    consumer = DefaultMQPushConsumer(group)
    consumer.set_namesrv_addr(NAMESRV)
    consumer.set_message_listener(SimpleMessageListener(on_msg))
    # The messages go out before the consumer starts, so the leg must ask for the first offset:
    # LAST_OFFSET would begin at the tail and the listener would see nothing regardless of V2.
    consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    consumer.subscribe(TOPIC, TAG_A)
    consumer.set_use_heartbeat_v2(use_v2)
    consumer.start()
    try:
        client = consumer._require_client()
        recorder = wrap_transport(client)
        for i in range(rounds):
            consumer._send_heartbeat_to_all_broker()
        deadline = time.time() + 25
        while time.time() < deadline and len(got) < len(expected):
            time.sleep(0.5)
        return consumer, recorder, list(got)
    except Exception:
        consumer.shutdown()
        raise


def phase_consumer(admin, master, expected_a, expected_b):
    print("\nS2 a real push consumer on the V2 path")
    consumer, recorder, got = run_consumer_leg(V2_GROUP, True, expected_a, rounds=3)
    try:
        snap = recorder.snapshot()
        check("S2a the round after the first goes out as the withoutSub body",
              snap["full"] >= 1 and snap["light"] >= 1,
              "full=%d light=%d v1=%d" % (snap["full"], snap["light"], snap["v1"]))
        check("S2b every round is answered by the broker", not snap["errors"],
              "errors=%s" % snap["errors"])
        check("S2c the rounds of one stream share one fingerprint",
              len(snap["fingerprints"]) == 1 and 0 not in snap["fingerprints"],
              "fingerprints=%s" % sorted(snap["fingerprints"]))
        subs = subscriptions_of(admin, V2_GROUP, master)
        check("S2d the broker still holds the tag subscription after the light rounds",
              subs.get(TOPIC) == TAG_A, "broker subscriptionTable=%s" % subs)
        check("S2e the consumer is registered on the broker (204 sees its clientId)",
              consumer.client_id in (connected_client_ids(admin, V2_GROUP, master) or []),
              "clientId=%s" % consumer.client_id)
        bodies = set(got)
        check("S2f the broker-side filter still applies: only the matching tag is consumed",
              bodies == set(expected_a),
              "consumed=%d of %d matched" % (len(bodies), len(expected_a)))
        leaked = [b for b in bodies if b in set(expected_b)]
        check("S2g no message of the other tag reached the listener "
              "(a broker that forgot the subscription would deliver everything)",
              not leaked, "leaked=%s" % leaked)
    finally:
        consumer.shutdown()

    print("\nS3 control: the same consumer with the opt-in off")
    consumer1, recorder1, got1 = run_consumer_leg(V1_GROUP, False, expected_a, rounds=2)
    try:
        snap1 = recorder1.snapshot()
        check("S3a without the opt-in no round carries a fingerprint and none is light",
              snap1["light"] == 0 and snap1["full"] == 0
              and snap1["v1"] >= 1 and snap1["fingerprints"] == {0},
              "v1=%d full=%d light=%d fingerprints=%s"
              % (snap1["v1"], snap1["full"], snap1["light"], sorted(snap1["fingerprints"])))
        check("S3b control: every V1 round is answered too", not snap1["errors"],
              "errors=%s" % snap1["errors"])
        check("S3c the V1 consumer consumes the same messages",
              set(got1) == set(expected_a), "consumed=%d" % len(set(got1)))
        subs1 = subscriptions_of(admin, V1_GROUP, master)
        check("S3d and the broker holds its subscription", subs1.get(TOPIC) == TAG_A,
              "broker subscriptionTable=%s" % subs1)
    finally:
        consumer1.shutdown()


def main() -> int:
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()

    inst = MQClientInstance("hbv2-probe-%d" % STAMP, [NAMESRV])
    inst.start()
    producer = None
    try:
        print("create topic %s (4 queues)" % TOPIC)
        inst.create_topic_in_route(TOPIC, 4, 4)
        route = None
        deadline = time.time() + 30
        while time.time() < deadline:
            route = inst.get_topic_route_data(TOPIC)
            if route and route.broker_datas and route.broker_datas[0].broker_addrs.get(0):
                break
            time.sleep(1)
        if not (route and route.broker_datas):
            print("\n== precondition not met: no route for %s ==" % TOPIC)
            return 1
        bd = route.broker_datas[0]
        master = bd.broker_addrs[0]
        check("S0 route resolves the master broker", bool(master), "master=%s" % master)

        # A heartbeat only registers a group the broker already knows: the registration loop
        # skips a group with no SubscriptionGroupConfig, so the two hand-driven probe groups are
        # created here instead of relying on a consumer's config check.
        for group in (PROBE_GROUP, LIGHT_ONLY_GROUP):
            sgc = SubscriptionGroupConfig(group)
            sgc.consume_enable = True
            admin.create_and_update_subscription_group_config(master, sgc)
        check("S0 the two hand-driven probe groups exist on the broker",
              all(admin.examine_subscription_group_config(master, g) is not None
                  for g in (PROBE_GROUP, LIGHT_ONLY_GROUP)),
              "groups=%s" % [PROBE_GROUP, LIGHT_ONLY_GROUP])

        phase_protocol(inst, admin, master, inst.client_id)

        # Messages for the consumer legs: the listener subscribes TAG_A, so only the A bodies
        # may come back. Each leg gets its own group, so both receive everything.
        producer = DefaultMQProducer(PRODUCER_GROUP)
        producer.set_namesrv_addr(NAMESRV)
        producer.start()
        a_bodies, b_bodies = [], []
        for i in range(3):
            body = ("v2-match-%d-%d" % (STAMP, i)).encode()
            producer.send(Message(TOPIC, body, tags=TAG_A))
            a_bodies.append(body)
        for i in range(2):
            body = ("v2-other-%d-%d" % (STAMP, i)).encode()
            producer.send(Message(TOPIC, body, tags=TAG_B))
            b_bodies.append(body)
        print("sent %d %s and %d %s messages" % (len(a_bodies), TAG_A, len(b_bodies), TAG_B))

        phase_consumer(admin, master, a_bodies, b_bodies)
    finally:
        if producer is not None:
            try:
                producer.shutdown()
            except Exception as e:  # noqa: BLE001
                print("    (producer shutdown failed: %s)" % e)
        try:
            inst.shutdown()
        except Exception as e:  # noqa: BLE001
            print("    (probe instance shutdown failed: %s)" % e)
        for group in (PROBE_GROUP, LIGHT_ONLY_GROUP, V2_GROUP, V1_GROUP):
            try:
                admin.delete_subscription_group(master, group)
            except Exception:  # noqa: BLE001
                pass
        try:
            admin.delete_topic(TOPIC)
        except Exception as e:  # noqa: BLE001
            print("    (delete_topic(%s) failed: %s)" % (TOPIC, e))
        admin.shutdown()

    print("\n== result: %d/%d passed ==" % (PASS, PASS + FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
