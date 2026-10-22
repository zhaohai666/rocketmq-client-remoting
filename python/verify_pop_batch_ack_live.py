#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Live verification of BATCH_ACK_MESSAGE (200151) against a real broker.

Usage (needs a local RocketMQ 5.5.1 cluster whose broker has autoCreateTopicEnable and
autoCreateSubscriptionGroup):
    .venv/bin/python verify_pop_batch_ack_live.py 127.0.0.1:9876
    .venv/bin/python verify_pop_batch_ack_live.py 127.0.0.1:9876 l2,l3

Why this needs a live leg of its own: the POP push consumer answers message by message with
ACK_MESSAGE (200051) and never builds a batch, so nothing in the normal runtime reaches the
batch wire path. The whole capability -- the merge key, the bit index, the byte-minimal base64
bit set, the header-less request shape -- is only reachable through the exported primitive, and
a primitive nobody has pointed at a broker is a primitive that may not work.

The batch body carries no per-message offsets, only a start offset plus a bit set, so the one
question a return code cannot answer is "which messages did the broker really clear". As with
every POP leg, the criterion is therefore a **timed observation window + broker-side observable
state**, never a return code:

  L1 one request, whole batch: pop a full batch, ack it with a single 200151, then keep popping
     both the business topic and the group's POP retry topic for 2.5x the invisibility window.
     A wrong bit or a wrong start offset leaves checkpoints alive, and the revive service puts
     those bodies back inside the window.
  L2 paired control: the same request with exactly one checkpoint left out must bring that one
     message back and leave the rest gone -- the assertion that fails if a bit set is treated as
     "the whole batch". It also closes the retry direction: the revived checkpoint carries the
     retry marker and lives in %RETRY%<group>_<topic>, while the batch body that clears it must
     still name the BUSINESS topic, because the broker re-derives the physical topic from
     marker + group. The window that waits for the revival is a **probe** that stops at the first
     sighting: a sighted checkpoint left to expire is revived again before the follow-up ack
     lands, and no ack can take a revival back, so a longer fixed window would fail on timing
     instead of on the wire path.
  L3 no over-merging: two pop rounds of one queue acked by one request must produce one entry
     per (retry, queueId, ck offset, popTime) and must not collapse the rounds -- collapsing
     them would acknowledge offsets nobody popped.

The broker needs no special switch: 200151 is registered unconditionally and the revive side
handles the batch ack tag the same way, so the standard cluster config already serves it
(enablePopBatchAck only steers the broker's internal re-put of buffered acks, which is inert
while enablePopBufferMerge=false). Every topic is created with a **single queue** so one POP sees
the whole batch, and each leg waits until the broker reports the expected queue depth before
popping -- a partially dispatched batch would make "one request cleared the batch" unprovable.

Closing line `############ PASS=<n> FAIL=<n> ############`; exits non-zero on any FAIL.
"""
from __future__ import annotations

import sys
import time
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, ".")

from client.consumer_result import PopResult, PopStatus
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message
from common.message_const import MessageConst
from remoting.protocol import extra_info as ei
from remoting.protocol.body import BatchAckMessageRequestBody
from remoting.protocol.codes import ResponseCode

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
LEGS = (sys.argv[2] if len(sys.argv) > 2 else "all").split(",")
STAMP = int(time.time())
QUEUE_NUM = 1

# Ten seconds of invisibility keeps every window short enough for the whole run to stay in a
# few minutes while still being far above the broker's 1 s revive scan interval, so a missed
# ack is always observed.
INVISIBLE_MS = 10000
# A checkpoint no ack covered is revived at popTime + invisibleTime, so 2.5x the window is a
# generous bound on "it should have come back by now".
OBSERVE_SEC = INVISIBLE_MS * 2.5 / 1000.0
# L2's first window is a probe, not a fixed window: every sighting leaves a fresh checkpoint
# that is invisible for INVISIBLE_MS, so a long fixed watch sees the same message revive, get
# popped and revive again, and the ack then carries a checkpoint whose invisibility already ran
# out -- the final silence check would fail on timing instead of on anything the client did.
REVIVE_WAIT_SEC = 40.0
REVIVE_TAIL_SEC = 3.0
DEPTH_WAIT_SEC = 30.0
ROUTE_WAIT_SEC = 30.0

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


def selected(leg: str) -> bool:
    return "all" in LEGS or leg in LEGS


# ------------------------------------------------------------------ checkpoints


class Ck:
    """One message's view of its checkpoint, in the terms the batch body is built from."""

    __slots__ = ("raw", "body", "queue_off", "ck_off", "pop_time", "invisible",
                 "retry", "broker", "queue_id", "revive_qid", "segments")

    def __init__(self) -> None:
        self.raw = ""
        self.body = ""
        self.queue_off = 0
        self.ck_off = 0
        self.pop_time = 0
        self.invisible = 0
        self.retry = ""
        self.broker = ""
        self.queue_id = 0
        self.revive_qid = 0
        self.segments = 0

    def __repr__(self) -> str:
        return "%s{ck=%d off=%d pt=%d r=%r q=%d broker=%r}" % (
            self.body, self.ck_off, self.queue_off, self.pop_time,
            self.retry, self.queue_id, self.broker)


def parse_ck(raw: Optional[str], body: str) -> Tuple[Optional[Ck], Optional[str]]:
    """Read every segment a batch body needs out of one checkpoint.

    It works on the captured string, so the observation loop's sightings can be re-parsed after
    the messages themselves have gone back out of sight. Nothing is inferred: a checkpoint that
    cannot be parsed is a problem, not a skip.
    """
    entry = Ck()
    entry.raw = raw or ""
    entry.body = body
    parts = ei.split(entry.raw) if entry.raw else []
    entry.segments = len(parts)
    if entry.segments != 8:
        return entry, "%s: checkpoint has %d segments, want 8: %r" % (body, entry.segments, raw)
    entry.ck_off = ei.get_ck_queue_offset(parts)
    entry.pop_time = ei.get_pop_time(parts)
    entry.invisible = ei.get_invisible_time(parts)
    entry.revive_qid = ei.get_revive_qid(parts)
    entry.retry = ei.get_retry(parts)
    entry.broker = ei.get_broker_name(parts)
    entry.queue_id = ei.get_queue_id(parts)
    # segment 7, not the message's decoded field: this is the value the body turns into a bit index.
    entry.queue_off = ei.get_queue_offset(parts)
    return entry, None


def parse_messages(msgs) -> Tuple[List[Ck], List[str]]:
    out: List[Ck] = []
    problems: List[str] = []
    for m in msgs:
        body = (m.body or b"").decode("utf-8", "replace")
        entry, problem = parse_ck(m.properties.get(MessageConst.PROPERTY_POP_CK), body)
        if problem:
            problems.append(problem)
        elif entry.queue_off != m.queue_offset:
            # The body addresses a message through segment 7. If it were not this message's own
            # queue offset, the body would be acknowledging somebody else's message.
            problems.append("%s: checkpoint segment 7 = %d but the message sits at %d"
                            % (body, entry.queue_off, m.queue_offset))
        out.append(entry)
    return out, problems


def parse_cks(raws: List[str], body: str) -> Tuple[List[Ck], List[str]]:
    out: List[Ck] = []
    problems: List[str] = []
    for raw in raws:
        entry, problem = parse_ck(raw, body)
        if problem:
            problems.append(problem)
        out.append(entry)
    return out, problems


def merge_key(c: Ck) -> str:
    """The grouping key the batch body is built on, recomputed here independently of the
    builder, so "the builder grouped it" is not the same statement as "the key says it should"."""
    return "%s@%d@%d@%d" % (c.retry, c.queue_id, c.ck_off, c.pop_time)


def merge_key_of(ack) -> str:
    """The same key formula, read from an entry the builder produced."""
    return "%s@%d@%d@%d" % (ack.retry, ack.queue_id, ack.start_offset, ack.pop_time)


def distinct_keys(cks: List[Ck]) -> List[str]:
    return sorted({merge_key(c) for c in cks})


def acked_offsets(body: BatchAckMessageRequestBody) -> Dict[int, int]:
    """Expand the body back into the queue offsets it claims to acknowledge: entry start
    offset + bit index."""
    out: Dict[int, int] = {}
    for ack in body.acks:
        # The wire form only carries whole bytes, so the decodable range is 8 bits per byte --
        # an offset beyond it would silently disappear from the broker's walk.
        for index in ack.bit_set.set_indexes():
            out[ack.start_offset + index] = out.get(ack.start_offset + index, 0) + 1
    return out


def offsets_of(cks: List[Ck]) -> List[int]:
    return sorted(c.queue_off for c in cks)


def same_int_set(counts: Dict[int, int], want: List[int]) -> bool:
    """Every wanted offset appears exactly once in the expansion, and nothing else does."""
    uniq = set(want)
    if len(counts) != len(uniq):
        return False
    return all(offset in uniq and n == 1 for offset, n in counts.items())


def bodies_of(cks: List[Ck]) -> str:
    return " ".join(sorted(c.body for c in cks))


def detail_of(cks: List[Ck]) -> str:
    return " ".join(repr(c) for c in cks)


# ------------------------------------------------------------------ observation


class Sighting:
    __slots__ = ("from_topic", "ck", "marker")

    def __init__(self, from_topic: str, ck: str, marker: str) -> None:
        self.from_topic = from_topic
        self.ck = ck
        self.marker = marker


class Eyes:
    """Collects what a POP answered during an observation window."""

    def __init__(self) -> None:
        self.sighted: Dict[str, List[Sighting]] = {}
        self.problems: List[str] = []

    def record(self, msgs, from_topic: str) -> None:
        for m in msgs:
            body = (m.body or b"").decode("utf-8", "replace")
            raw = m.properties.get(MessageConst.PROPERTY_POP_CK) or ""
            parts = ei.split(raw) if raw else []
            marker = ei.get_retry(parts) if len(parts) == 8 else ""
            self.sighted.setdefault(body, []).append(Sighting(from_topic, raw, marker))

    def note(self, problem: str) -> None:
        self.problems.append(problem)

    def sightings(self, body: str) -> List[Sighting]:
        return list(self.sighted.get(body, []))

    def topics_seen(self, body: str) -> List[str]:
        out: List[str] = []
        for s in self.sightings(body):
            if s.from_topic not in out:
                out.append(s.from_topic)
        return out

    def checkpoints_for(self, body: str) -> List[str]:
        """Every checkpoint the window collected for a body. A message sighted more than once
        has one live checkpoint per sighting, and an ack that clears only one of them leaves the
        others to revive it again."""
        return [s.ck for s in self.sightings(body) if s.ck]

    def markers_for(self, body: str) -> List[str]:
        return [s.marker for s in self.sightings(body)]

    def came_back(self, body: str) -> bool:
        return len(self.sighted.get(body, [])) > 0

    def all_bodies(self) -> List[str]:
        return sorted(self.sighted)

    def summary(self) -> str:
        bodies = self.all_bodies()
        return "nothing revived" if not bodies else "revived: %s" % " ".join(bodies)


# ------------------------------------------------------------------ wire driver


class Verifier:
    """Drives the POP wire commands directly.

    It borrows the producer's client instance on purpose: the producer already knows how to reach
    the broker master, and a POP consumer would auto-ack everything, which is exactly what must
    not happen here.
    """

    def __init__(self, prod: DefaultMQProducer, broker_name: str, topic: str) -> None:
        self.client: MQClientInstance = prod._mq_client
        self.broker_name = broker_name
        route = self.client.get_topic_route_data(topic)
        self.addr = MQClientInstance.find_broker_addr_in_route(route, broker_name)

    def pop(self, group: str, topic: str, queue_id: int, max_nums: int,
            poll_ms: int) -> PopResult:
        return self.client.pop_message(
            group, topic, queue_id=queue_id, max_msg_nums=max_nums,
            invisible_time=INVISIBLE_MS, poll_time=poll_ms, init_mode=0,
            broker_name=self.broker_name, addr=self.addr,
            timeout_millis=poll_ms + 5000)

    def batch_ack(self, group: str, topic: str, raws: List[str]) -> int:
        """Send 200151 with the body the builder produced for these checkpoints."""
        return self.client.batch_ack_message(group, topic, raws, addr=self.addr)

    def round(self, group: str, topic: str, eyes: Eyes, state: Dict[str, str]) -> None:
        """Pop the business topic AND the group's POP retry topic once.

        Both, because a checkpoint nobody acked is revived into the retry topic, while a POP of
        the business topic only consults that topic by probability -- watching one of them would
        make a silence ambiguous.
        """
        for t in (topic, ei.build_pop_retry_topic_v1(topic, group)):
            try:
                res = self.pop(group, t, -1, 32, 500)
            except Exception as e:  # noqa: BLE001
                # A POP retry topic only exists once the revive service has put something in it,
                # so "topic not exist" is the expected answer while nothing has been revived;
                # anything else is worth seeing.
                if not state and "not exist" not in str(e):
                    state["error"] = "%s: %s" % (t, e)
                continue
            if res.status == PopStatus.FOUND:
                eyes.record(res.msg_found_list, t)

    def watch(self, group: str, topic: str, window_sec: float, eyes: Eyes) -> None:
        deadline = time.monotonic() + window_sec
        state: Dict[str, str] = {}
        while time.monotonic() < deadline:
            self.round(group, topic, eyes, state)
        if state:
            eyes.note(state["error"])

    def probe(self, group: str, topic: str, wait_for: str, eyes: Eyes) -> bool:
        """Pop both topics until ``wait_for`` turns up, then keep going for a short tail --
        enough rounds for the same window to show that the acked messages did NOT come back, not
        enough to leave the collected checkpoint to expire.

        Returns whether it came back.
        """
        deadline = time.monotonic() + REVIVE_WAIT_SEC
        stopped_at: Optional[float] = None
        state: Dict[str, str] = {}
        while time.monotonic() < deadline:
            self.round(group, topic, eyes, state)
            if stopped_at is None and eyes.came_back(wait_for):
                stopped_at = time.monotonic() + REVIVE_TAIL_SEC
            if stopped_at is not None and time.monotonic() >= stopped_at:
                break
        if state:
            eyes.note(state["error"])
        return eyes.came_back(wait_for)


# ------------------------------------------------------------------ fixtures


def make_topic(prod: DefaultMQProducer, topic: str, bodies: List[str]) -> Optional[str]:
    """Create a single-queue topic and put ``bodies`` on it; returns the broker name."""
    prod.create_topic("TBW102", topic, QUEUE_NUM)
    for i, body in enumerate(bodies):
        last: Optional[Exception] = None
        for _ in range(30):
            try:
                prod.send(Message(topic, body.encode("utf-8"), keys="bak%d" % i))
                last = None
                break
            except Exception as e:  # noqa: BLE001
                last = e
                time.sleep(2)
        if last is not None:
            print("  send %r never succeeded: %s" % (body, last))
            return None
    route = prod._mq_client.get_topic_route_data(topic)
    if route is None or not route.get_broker_datas():
        print("  no route for %s" % topic)
        return None
    return route.get_broker_datas()[0].broker_name


def wait_queue_depth(client: MQClientInstance, topic: str, want: int) -> Tuple[int, bool]:
    """Block until the broker reports ``want`` messages on the topic.

    The consume queue is dispatched asynchronously, so a POP fired straight after the last send
    can answer with a partial batch -- and a partial batch makes "one request cleared the batch"
    unprovable.
    """
    deadline = time.monotonic() + DEPTH_WAIT_SEC
    total = -1
    while True:
        try:
            queues = client.get_topic_subscribe_info(topic)
            total = sum(client.get_max_offset(mq) - client.get_min_offset(mq) for mq in queues)
        except Exception:  # noqa: BLE001 — the topic config is still propagating
            total = -1
        if total >= want:
            return total, True
        if time.monotonic() >= deadline:
            return total, False
        time.sleep(0.5)


def route_visible(client: MQClientInstance, topic: str) -> bool:
    """Wait for the topic route to reach the name server.

    The POP retry topic is created by the revive service when it moves a checkpoint there, so its
    route appearing is broker-side evidence of the revival itself.
    """
    deadline = time.monotonic() + ROUTE_WAIT_SEC
    while True:
        try:
            client.update_topic_route_info_from_name_server(topic)
            route = client.get_topic_route_data(topic)
            if route is not None and route.get_broker_datas():
                return True
        except Exception:  # noqa: BLE001
            pass
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.5)


# ------------------------------------------------------------------ L1


def run_l1(prod: DefaultMQProducer) -> None:
    print("=== L1 one BATCH_ACK_MESSAGE clears a whole POP batch ===")
    batch = 8
    topic = "PyBaAll_%d" % STAMP
    group = "GID_PyBaAll_%d" % STAMP
    bodies = ["ba-all-%d" % i for i in range(batch)]

    broker_name = make_topic(prod, topic, bodies)
    check("L1 sent 8 messages to a single-queue topic", broker_name is not None, topic)
    if broker_name is None:
        return
    depth, ready = wait_queue_depth(prod._mq_client, topic, batch)
    check("L1 all 8 messages are on the queue before popping", ready,
          "depth=%d want=%d" % (depth, batch))
    if not ready:
        return

    v = Verifier(prod, broker_name, topic)
    try:
        res = v.pop(group, topic, 0, batch, 3000)
    except Exception as e:  # noqa: BLE001
        check("L1 POP_MESSAGE answered the batch", False, str(e))
        return
    check("L1 POP_MESSAGE answered the whole batch",
          res.status == PopStatus.FOUND and len(res.msg_found_list) == batch,
          "status=%s got=%d" % (res.status, len(res.msg_found_list)))

    cks, problems = parse_messages(res.msg_found_list)
    check("L1 every message carries a parsable 8-segment checkpoint", not problems,
          "; ".join(problems))
    if len(cks) != batch:
        check("L1 checkpoint count", False, "%d" % len(cks))
        return

    # The body can only address a message through segment 7 and only be routed through segments
    # 4-6, so pin them per message before the builder is trusted with them.
    shape = [c for c in cks
             if c.retry != ei.NORMAL_TOPIC or c.broker != broker_name or c.queue_id != 0
             or c.invisible != INVISIBLE_MS or c.pop_time <= 0 or c.ck_off > c.queue_off]
    check("L1 checkpoint fields the batch body depends on "
          "(marker 0 / logical broker / queueId / invisible / ck<=msg offset)",
          not shape, " ".join(repr(c) for c in shape))
    if shape:
        print("     %s" % detail_of(cks))
        return

    raws = [c.raw for c in cks]
    body = BatchAckMessageRequestBody.from_extra_infos(group, topic, raws)

    # Three body invariants, each one a way the builder could be wrong while still producing a
    # request the broker answers SUCCESS to.
    check("L1 body acknowledges exactly the popped offsets, once each",
          same_int_set(acked_offsets(body), offsets_of(cks)),
          "acked=%s want=%s" % (sorted(acked_offsets(body)), offsets_of(cks)))
    check("L1 body has exactly one entry per merge key",
          len(body.acks) == len(distinct_keys(cks)),
          "entries=%d keys=%s" % (len(body.acks), distinct_keys(cks)))

    # Every field of an entry has to be the value the checkpoint carried: the broker looks the
    # checkpoint up by (group, real topic, queueId, popTime, invisibleTime, revive queue) and then
    # walks the bits from startOffset, so one invented field makes the whole entry a no-op that
    # still answers SUCCESS.
    by_key: Dict[str, Ck] = {merge_key(c): c for c in cks}
    problems = []
    for ack in body.acks:
        if body.broker_name != broker_name or ack.consumer_group != group or ack.topic != topic:
            problems.append("entry c=%r t=%r bodyBroker=%r want c=%r t=%r broker=%r"
                            % (ack.consumer_group, ack.topic, body.broker_name,
                               group, topic, broker_name))
            break
        source = by_key.get(merge_key_of(ack))
        if source is None:
            problems.append("entry %s matches no popped checkpoint" % merge_key_of(ack))
            break
        if (ack.start_offset != source.ck_off or ack.pop_time != source.pop_time
                or ack.invisible_time != source.invisible
                or ack.revive_queue_id != source.revive_qid
                or ack.queue_id != source.queue_id or ack.retry != source.retry
                or len(ack.bit_set) == 0):
            problems.append("entry %s diverges from checkpoint %s: so=%d/%d pt=%d/%d it=%d/%d "
                            "rq=%d/%d q=%d/%d r=%r/%r bits=%s"
                            % (merge_key_of(ack), bodies_of([source]),
                               ack.start_offset, source.ck_off, ack.pop_time, source.pop_time,
                               ack.invisible_time, source.invisible, ack.revive_queue_id,
                               source.revive_qid, ack.queue_id, source.queue_id, ack.retry,
                               source.retry, ack.bit_set.set_indexes()))
            break
    check("L1 every entry echoes its checkpoint fields, names the group, the business topic "
          "and the checkpoint broker", not problems, "; ".join(problems))

    # The wire form is where a base64 bit set goes wrong silently: the broker reads bit 0 of the
    # LAST byte first, so a non-minimal or byte-reversed encoding still decodes as *some* set of
    # offsets.
    encoded = body.encode()
    decoded = BatchAckMessageRequestBody.decode(encoded)
    check("L1 body round-trips through the wire codec",
          len(decoded.acks) == len(body.acks)
          and same_int_set(acked_offsets(decoded), offsets_of(cks)),
          "entries %d -> %d, bytes=%d" % (len(body.acks), len(decoded.acks), len(encoded)))

    code = v.batch_ack(group, topic, raws)
    check("L1 one BATCH_ACK_MESSAGE answered SUCCESS", code == ResponseCode.SUCCESS,
          "code=%d checkpoints=%d entries=%d" % (code, len(cks), len(body.acks)))

    eyes = Eyes()
    v.watch(group, topic, OBSERVE_SEC, eyes)
    check("L1 nothing revived inside 2.5x the invisibility window (the batch really landed)",
          not eyes.all_bodies(), eyes.summary())
    if eyes.problems:
        print("     watch notes: %s" % "; ".join(eyes.problems))


# ------------------------------------------------------------------ L2


def run_l2(prod: DefaultMQProducer) -> None:
    print("=== L2 paired control: an unacked bit comes back, the acked ones do not ===")
    batch = 6
    topic = "PyBaPart_%d" % STAMP
    group = "GID_PyBaPart_%d" % STAMP
    bodies = ["ba-part-%d" % i for i in range(batch)]

    broker_name = make_topic(prod, topic, bodies)
    check("L2 sent 6 messages to a single-queue topic", broker_name is not None, topic)
    if broker_name is None:
        return
    depth, ready = wait_queue_depth(prod._mq_client, topic, batch)
    check("L2 all 6 messages are on the queue before popping", ready, "depth=%d" % depth)
    if not ready:
        return

    v = Verifier(prod, broker_name, topic)
    try:
        res = v.pop(group, topic, 0, batch, 3000)
    except Exception as e:  # noqa: BLE001
        check("L2 POP_MESSAGE answered the batch", False, str(e))
        return
    check("L2 POP_MESSAGE answered the batch",
          res.status == PopStatus.FOUND and len(res.msg_found_list) == batch,
          "status=%s got=%d" % (res.status, len(res.msg_found_list)))
    if res.status != PopStatus.FOUND or len(res.msg_found_list) != batch:
        return
    cks, problems = parse_messages(res.msg_found_list)
    if problems:
        check("L2 checkpoints parsable", False, "; ".join(problems))
        return

    # Drop exactly one: the message with the HIGHEST queue offset, whose bit is the last one the
    # bitmap can carry. Every other message keeps its ack.
    ordered = sorted(cks, key=lambda c: c.queue_off)
    dropped = ordered[-1]
    acked = ordered[:-1]

    body = BatchAckMessageRequestBody.from_extra_infos(group, topic, [c.raw for c in acked])
    counts = acked_offsets(body)
    check("L2 body acknowledges 5 of 6 offsets and not the dropped one",
          same_int_set(counts, offsets_of(acked)) and dropped.queue_off not in counts,
          "acked=%s dropped=%d" % (sorted(counts), dropped.queue_off))

    code = v.batch_ack(group, topic, [c.raw for c in acked])
    check("L2 partial BATCH_ACK_MESSAGE answered SUCCESS", code == ResponseCode.SUCCESS,
          "code=%d, %d of %d checkpoints" % (code, len(acked), len(cks)))

    eyes = Eyes()
    # Stop at the first sighting plus a short tail: the tail is the fair comparison for the acked
    # messages (they would have revived at the same instant if the bitmap had been decorative),
    # and stopping there keeps the checkpoint about to be acked inside its own invisibility.
    brought_back = v.probe(group, topic, dropped.body, eyes)

    # THE control. If the bitmap were decorative -- on the client or on the broker -- this leg
    # looks exactly like L1.
    check("L2 the unacked message really came back", brought_back,
          "bodies=%r topics=%s" % (dropped.body, eyes.topics_seen(dropped.body)))
    came_back_too = [c.body for c in acked if eyes.came_back(c.body)]
    check("L2 the five acked messages stayed gone while the dropped one came back",
          not came_back_too, eyes.summary())
    if not brought_back:
        print("     watch notes: %s" % "; ".join(eyes.problems))
        return

    # The revived checkpoint is addressed with marker "1", and it is that marker -- not the topic
    # name in the reply -- that tells the broker where the checkpoint lives.
    retry_topic = ei.build_pop_retry_topic_v1(topic, group)
    seen_from = eyes.topics_seen(dropped.body)

    # Clear the checkpoint right away: its invisibility started the moment the reply handed it
    # over, and everything in between has to be a local check.
    raws = eyes.checkpoints_for(dropped.body)
    cks2, problems2 = parse_cks(raws, dropped.body)
    if problems2:
        check("L2 the revived checkpoints are parsable", False, "; ".join(problems2))
        return
    check("L2 the revived checkpoints carry the retry marker (they live in the retry queue)",
          bool(cks2) and all(c.retry == ei.RETRY_TOPIC for c in cks2),
          "sightings=%d markers=%s" % (len(raws), eyes.markers_for(dropped.body)))

    second = BatchAckMessageRequestBody.from_extra_infos(group, topic, raws)
    # The body names the BUSINESS topic even though the checkpoints came from the retry topic:
    # the broker re-derives the physical topic from marker + group, so a client that "helpfully"
    # passed the retry topic here would double-wrap the name and ack nothing.
    bad = [a for a in second.acks
           if a.topic != topic or a.retry != ei.RETRY_TOPIC or a.consumer_group != group]
    check("L2 the retry body keeps the business topic and carries the marker on every entry",
          bool(second.acks) and not bad,
          "entries=%d t=%r r=%s" % (len(second.acks), topic, [a.retry for a in second.acks]))

    code2 = v.batch_ack(group, topic, raws)
    check("L2 retry-topic BATCH_ACK_MESSAGE answered SUCCESS", code2 == ResponseCode.SUCCESS,
          "code=%d, %d revived checkpoints -> %d entries" % (code2, len(cks2), len(second.acks)))

    # Which RPC answered the message is deliberately NOT the criterion: a POP of the business
    # topic also consults the retry queue (and the reply topic is rewritten to the requested one),
    # so "seen from" can name either queue's topic for the same physical revival. What is broker
    # state: the retry topic exists, and the checkpoint the reply carried points into it. Probed
    # after the ack, because a route reaching the name server can wait for a broker heartbeat and
    # that wait must not push the ack past the checkpoint's invisibility.
    check("L2 the POP retry topic exists on the broker (the revive service really moved it there)",
          route_visible(prod._mq_client, retry_topic),
          "%s answered from %s" % (retry_topic, ",".join(seen_from)))

    settled = Eyes()
    v.watch(group, topic, OBSERVE_SEC, settled)
    check("L2 the marker-driven retry ack silenced it (no further revival)",
          not settled.came_back(dropped.body), settled.summary())


# ------------------------------------------------------------------ L3


def run_l3(prod: DefaultMQProducer) -> None:
    print("=== L3 two pop rounds, one request: entries group by retry@queueId@ckOffset@popTime ===")
    total, per_round = 4, 2
    topic = "PyBaRounds_%d" % STAMP
    group = "GID_PyBaRounds_%d" % STAMP
    bodies = ["ba-round-%d" % i for i in range(total)]

    broker_name = make_topic(prod, topic, bodies)
    check("L3 sent 4 messages to a single-queue topic", broker_name is not None, topic)
    if broker_name is None:
        return
    depth, ready = wait_queue_depth(prod._mq_client, topic, total)
    check("L3 all 4 messages are on the queue before popping", ready, "depth=%d" % depth)
    if not ready:
        return

    v = Verifier(prod, broker_name, topic)
    try:
        first = v.pop(group, topic, 0, per_round, 3000)
        # The second round asks for 2 more. The first round's messages are invisible for the
        # whole window, so the broker has no choice but to hand out the other two -- which is
        # what makes the two rounds distinguishable by checkpoint.
        second = v.pop(group, topic, 0, per_round, 1000)
    except Exception as e:  # noqa: BLE001
        check("L3 the two POP rounds answered 2 messages each", False, str(e))
        return
    check("L3 first POP answers 2 messages",
          first.status == PopStatus.FOUND and len(first.msg_found_list) == per_round,
          "status=%s got=%d" % (first.status, len(first.msg_found_list)))
    check("L3 second POP answers the other 2 messages",
          second.status == PopStatus.FOUND and len(second.msg_found_list) == per_round,
          "status=%s got=%d" % (second.status, len(second.msg_found_list)))
    if (len(first.msg_found_list) != per_round or len(second.msg_found_list) != per_round):
        return

    cks_a, problems_a = parse_messages(first.msg_found_list)
    cks_b, problems_b = parse_messages(second.msg_found_list)
    if problems_a or problems_b:
        check("L3 both rounds carry parsable checkpoints", False,
              "; ".join(problems_a + problems_b))
        return
    bodies_a = {c.body for c in cks_a}
    overlap = [c.body for c in cks_b if c.body in bodies_a]
    check("L3 the two rounds are disjoint batches of one queue", not overlap,
          "A=%s B=%s" % (bodies_of(cks_a), bodies_of(cks_b)))

    everything = cks_a + cks_b
    body = BatchAckMessageRequestBody.from_extra_infos(
        group, topic, [c.raw for c in everything])
    keys = distinct_keys(everything)
    keys_a = {merge_key(c) for c in cks_a}
    shared = keys_a & {merge_key(c) for c in cks_b}
    check("L3 the two rounds produce different merge keys, so the body cannot collapse them",
          not shared and len(keys) >= 2, "keys=%d shared=%d %s" % (len(keys), len(shared), keys))
    counts = acked_offsets(body)
    check("L3 one body covers all 4 offsets exactly once",
          same_int_set(counts, offsets_of(everything)) and len(body.acks) == len(keys),
          "entries=%d acked=%s want=%s" % (len(body.acks), sorted(counts),
                                           offsets_of(everything)))

    code = v.batch_ack(group, topic, [c.raw for c in everything])
    check("L3 one BATCH_ACK_MESSAGE for two rounds answered SUCCESS", code == ResponseCode.SUCCESS,
          "code=%d, %d checkpoints -> %d entries" % (code, len(everything), len(body.acks)))

    eyes = Eyes()
    v.watch(group, topic, OBSERVE_SEC, eyes)
    check("L3 no message of either round revived inside 2.5x the window",
          not eyes.all_bodies(), eyes.summary())


def main() -> int:
    prod = DefaultMQProducer("PG_PyBa_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    print("=== python POP batch-ack live: ns=%s stamp=%d legs=%s ===" % (NAMESRV, STAMP, LEGS))
    try:
        if selected("l1"):
            run_l1(prod)
        if selected("l2"):
            run_l2(prod)
        if selected("l3"):
            run_l3(prod)
    finally:
        prod.shutdown()

    print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
