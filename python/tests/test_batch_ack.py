# -*- coding: utf-8 -*-
"""Offline tests for the POP batch acknowledge primitive (BATCH_ACK_MESSAGE / 200151).

Everything is asserted against **recorded wire state**: the request code the fake broker
saw, whether the request carried a custom header, and the JSON body decoded back from the
recorded bytes. No timing sleeps -- the seams record synchronously, and the loopback
broker's reply gates the assertions.
"""
from __future__ import annotations

import base64
import json
import socket
import struct
import threading

import pytest

from client.exception import MQClientException
from client.mq_client import MQClientInstance
from remoting.client import RemotingClient
from remoting.protocol import extra_info as ei
from remoting.protocol.bitset import BitSet
from remoting.protocol.body import BatchAck, BatchAckMessageRequestBody
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.headers import AckMessageRequestHeader
from remoting.protocol.remoting_command import RemotingCommand

BROKER = "broker-a"
OTHER_BROKER = "broker-b"
ADDR = "127.0.0.1:10911"
GROUP = "BatchAckUnitTestGroup"
TOPIC = "BatchAckUnitTestTopic"
POP_TIME = 1789613086027
INVISIBLE = 60000
COMPACT_KEYS = ["c", "t", "r", "so", "q", "rq", "pt", "it", "b"]


def _ck(ck_queue_offset: int = 100, pop_time: int = POP_TIME,
        invisible_time: int = INVISIBLE, revive_qid: int = 7,
        topic: str = TOPIC, broker: str = BROKER, queue_id: int = 3,
        queue_offset: int = 100) -> str:
    """A checkpoint string exactly as the POP path stamps it onto a message."""
    return ei.build_extra_info(ck_queue_offset, pop_time, invisible_time, revive_qid,
                               topic, broker, queue_id, queue_offset)


def _resp(code: int, ext: dict = None, body: bytes = None, remark: str = "") -> RemotingCommand:
    r = RemotingCommand(code=code, remark=remark)
    r.ext_fields = dict(ext or {})
    r.body = body
    return r


class _FakeInvoke:
    """Seam for MQClientInstance._invoke_sync: records each request, replies from a preset queue."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []          # [(addr, request, timeout_millis)]

    def __call__(self, addr, request, timeout_millis=None):
        # On the real wire the header map is folded into ext_fields at encode time;
        # trigger it here so "what the broker would actually see" is assertable.
        request.make_custom_header_to_net()
        self.requests.append((addr, request, timeout_millis))
        if len(self.responses) == 1:
            return self.responses[0]
        return self.responses.pop(0)

    @property
    def code(self):
        return self.requests[0][1].code

    @property
    def request(self):
        return self.requests[0][1]

    def body_dict(self, index: int = 0):
        return json.loads(self.requests[index][1].body.decode("utf-8"))


def _client(fake) -> MQClientInstance:
    inst = object.__new__(MQClientInstance)
    inst._invoke_sync = fake
    return inst


# ---------------------------------------------------------------- 1. bit set byte form


class TestBitSetWireForm:
    """The ``b`` field is the base64 of the protocol's bit set bytes: little-endian,
    trailing zero bytes dropped."""

    @pytest.mark.parametrize("bits, expected", [
        ((), ""),
        ((0,), "AQ=="),
        ((3,), "CA=="),
        ((7,), "gA=="),
        ((0, 3), "CQ=="),
        ((8,), "AAE="),
        ((63,), "AAAAAAAAAIA="),
        ((64,), "AAAAAAAAAAAB"),
        ((0, 70), "AQAAAAAAAABA"),
        ((5, 9), "IAI="),
    ])
    def test_exact_base64_for_known_bit_sets(self, bits, expected):
        encoded = BatchAck()
        for index in bits:
            encoded.bit_set.set_bit(index)
        assert encoded.to_dict()["b"] == expected

    @pytest.mark.parametrize("bits, expected", [
        ((), b""),
        ((0,), b"\x01"),
        ((7,), b"\x80"),
        ((3,), b"\x08"),
        ((8,), b"\x00\x01"),
        ((0, 70), b"\x01\x00\x00\x00\x00\x00\x00\x00\x40"),
    ])
    def test_control_little_endian_and_no_word_padding(self, bits, expected):
        """Control for the byte layout: the high bytes of the last word are **not** padded out.

        A word-aligned implementation would emit 8 bytes for bit 7 and 16 for bit 70,
        so this goes red for anything other than "least significant bytes, trimmed".
        """
        bs = BitSet(bits)
        assert bs.to_bytes() == expected
        assert len(bs.to_bytes()) == len(expected)

    def test_control_bit_is_the_bit_of_its_own_byte(self):
        """Bit 9 lives in byte 1 (mask 0x02), not byte 0 and not as a count of bytes."""
        assert BitSet((9,)).to_bytes() == b"\x00\x02"

    def test_roundtrip_through_bytes(self):
        for bits in ((), (0,), (0, 3, 70), (5, 9), (64,)):
            bs = BitSet(bits)
            assert BitSet.from_bytes(bs.to_bytes()).set_indexes() == sorted(bits)

    def test_negative_index_is_rejected(self):
        """A bit index below zero means the checkpoint was subtracted wrongly."""
        with pytest.raises(ValueError):
            BitSet().set_bit(-1)


# ---------------------------------------------------------------- 2. JSON key family


class TestBatchAckJsonKeys:
    def _filled(self) -> BatchAck:
        ack = BatchAck()
        ack.consumer_group = GROUP
        ack.topic = TOPIC
        ack.retry = ei.RETRY_TOPIC
        ack.start_offset = 100
        ack.queue_id = 3
        ack.revive_queue_id = 7
        ack.pop_time = POP_TIME
        ack.invisible_time = INVISIBLE
        ack.bit_set.set_bit(5)
        return ack

    def test_control_only_the_compact_keys_are_written(self):
        """Positive: exactly the compact family, in field order.
        Control: none of the long spellings leaks onto the wire."""
        d = self._filled().to_dict()
        assert list(d.keys()) == COMPACT_KEYS
        for long_name in ("consumerGroup", "topic", "retry", "startOffset", "queueId",
                          "reviveQueueId", "popTime", "invisibleTime", "bitSet"):
            assert long_name not in d

    def test_control_values_keep_protocol_types(self):
        """``r`` is a string flag while the offsets/times are numbers, not strings."""
        d = self._filled().to_dict()
        assert d["c"] == GROUP and d["t"] == TOPIC
        assert d["r"] == "1"
        assert all(isinstance(d[k], int) for k in ("so", "q", "rq", "pt", "it"))

    def test_long_keys_are_accepted_on_read(self):
        long = {"consumerGroup": GROUP, "topic": TOPIC, "retry": "2",
                "startOffset": 10, "queueId": 4, "reviveQueueId": 9,
                "popTime": POP_TIME, "invisibleTime": 30000,
                "bitSet": base64.b64encode(BitSet((0, 1)).to_bytes()).decode("ascii")}
        ack = BatchAck.from_dict(long)
        assert (ack.consumer_group, ack.topic, ack.retry) == (GROUP, TOPIC, "2")
        assert (ack.start_offset, ack.queue_id, ack.revive_queue_id) == (10, 4, 9)
        assert (ack.pop_time, ack.invisible_time) == (POP_TIME, 30000)
        assert ack.bit_set.set_indexes() == [0, 1]

    def test_control_compact_read_matches_long_read(self):
        """Both spellings must land on the same fields, or one of them is being ignored."""
        original = self._filled()
        compact = BatchAck.from_dict(original.to_dict())
        assert compact.to_dict() == original.to_dict()

    def test_control_unknown_keys_are_ignored(self):
        d = dict(self._filled().to_dict())
        d["somethingElse"] = 1
        assert "somethingElse" not in vars(BatchAck.from_dict(d))

    def test_empty_bit_set_reads_back_empty(self):
        assert BatchAck.from_dict({"b": ""}).bit_set.set_indexes() == []


# ---------------------------------------------------------------- 3. merge rule


class TestGrouping:
    def _body(self, *cks: str) -> BatchAckMessageRequestBody:
        return BatchAckMessageRequestBody.from_extra_infos(GROUP, TOPIC, list(cks))

    def test_two_offsets_of_the_same_pop_share_one_entry(self):
        """Core merge: one entry, both bits, indices relative to the checkpoint."""
        body = self._body(_ck(ck_queue_offset=100, queue_offset=100),
                          _ck(ck_queue_offset=100, queue_offset=105))
        assert len(body.acks) == 1
        ack = body.acks[0]
        assert ack.bit_set.set_indexes() == [0, 5]
        # positive: the absolute offsets the broker will expand to
        assert ack.acked_offsets() == [100, 105]
        # control: the raw queue offsets must not be used as indices
        assert 100 not in ack.bit_set.set_indexes()
        assert 105 not in ack.bit_set.set_indexes()

    def test_control_the_merge_key_is_not_the_queue_offset(self):
        """A key built from the per-message offset would split these two into two entries."""
        body = self._body(_ck(ck_queue_offset=100, queue_offset=100),
                          _ck(ck_queue_offset=100, queue_offset=101))
        assert len(body.acks) == 1
        assert body.acks[0].bit_set.set_indexes() == [0, 1]

    @pytest.mark.parametrize("label, mutate", [
        ("popTime", lambda kw: kw.update(pop_time=POP_TIME + 1)),
        ("queueId", lambda kw: kw.update(queue_id=4)),
        ("checkpoint offset", lambda kw: kw.update(ck_queue_offset=101, queue_offset=101)),
        ("retry flag", lambda kw: kw.update(topic=ei.build_pop_retry_topic_v1(TOPIC, GROUP))),
    ])
    def test_control_any_key_field_difference_splits_into_two_entries(self, label, mutate):
        """Positive: two entries. Control: each keeps a single bit, so nothing merged away."""
        first = {"ck_queue_offset": 100, "queue_offset": 100}
        second = {"ck_queue_offset": 100, "queue_offset": 100}
        mutate(second)
        body = self._body(_ck(**first), _ck(**second))
        assert len(body.acks) == 2, "%s must break the merge" % label
        assert [a.bit_set.set_indexes() for a in body.acks] == [[0], [0]]

    def test_control_differences_outside_the_key_stay_merged(self):
        """reviveQid / invisibleTime are not part of the key: they come from the first entry."""
        body = self._body(_ck(queue_offset=100, revive_qid=7, invisible_time=60000),
                          _ck(queue_offset=103, revive_qid=8, invisible_time=99000))
        assert len(body.acks) == 1
        assert body.acks[0].bit_set.set_indexes() == [0, 3]
        assert (body.acks[0].revive_queue_id, body.acks[0].invisible_time) == (7, 60000)

    def test_each_entry_uses_its_own_checkpoint_as_the_base(self):
        """Same message offset in two different pops gives two different bit indices."""
        body = self._body(_ck(ck_queue_offset=100, queue_offset=103),
                          _ck(ck_queue_offset=101, queue_offset=103))
        assert [a.start_offset for a in body.acks] == [100, 101]
        assert [a.bit_set.set_indexes() for a in body.acks] == [[3], [2]]
        assert [a.acked_offsets() for a in body.acks] == [[103], [103]]

    def test_broker_name_comes_from_the_first_checkpoint(self):
        body = self._body(_ck(broker=BROKER, queue_offset=100),
                          _ck(broker=OTHER_BROKER, queue_offset=101))
        assert body.broker_name == BROKER
        # control: the second checkpoint must not win
        assert body.broker_name != OTHER_BROKER

    def test_group_and_topic_are_written_into_every_entry(self):
        body = self._body(_ck(queue_offset=100), _ck(queue_id=4, queue_offset=100))
        assert [(a.consumer_group, a.topic) for a in body.acks] == [(GROUP, TOPIC)] * 2
        # a normal topic is flag "0", a revived one "1"
        assert [a.retry for a in body.acks] == ["0", "0"]

    def test_control_entries_keep_declaration_order(self):
        """Hash-map iteration order would make the body unstable; the first pop comes first."""
        body = self._body(_ck(queue_id=1, queue_offset=100),
                          _ck(queue_id=0, queue_offset=100),
                          _ck(queue_id=1, queue_offset=101))
        assert [a.queue_id for a in body.acks] == [1, 0]

    def test_empty_list_builds_an_empty_body(self):
        body = BatchAckMessageRequestBody.from_extra_infos(GROUP, TOPIC, [])
        assert body.acks == [] and body.broker_name is None

    def test_short_checkpoint_raises_instead_of_silently_dropping(self):
        with pytest.raises(ValueError):
            self._body("0 1 2 3 0 broker-a")


# ---------------------------------------------------------------- 4. the request itself


class TestBatchAckRequest:
    def test_control_request_code_is_the_batch_one(self):
        """Positive: 200151. Control: it must not be the single-ack 200051."""
        fake = _FakeInvoke([_resp(ResponseCode.SUCCESS)])
        code = _client(fake).batch_ack_message(
            GROUP, TOPIC, [_ck(queue_offset=100), _ck(queue_offset=101)], addr=ADDR)
        assert code == ResponseCode.SUCCESS
        assert fake.code == RequestCode.BATCH_ACK_MESSAGE == 200151
        assert fake.code != RequestCode.ACK_MESSAGE

    def test_control_request_carries_no_custom_header(self):
        """The batch form puts everything in the body: no header object, no extFields.

        Encoded at the frame level, because ext_fields is what actually crosses the socket.
        """
        fake = _FakeInvoke([_resp(ResponseCode.SUCCESS)])
        _client(fake).batch_ack_message(GROUP, TOPIC, [_ck(queue_offset=100)], addr=ADDR)
        request = fake.request
        assert request.custom_header is None
        assert request.ext_fields == {}
        frame = request.encode()
        (marked_header_len,) = struct.unpack_from(">i", frame, 4)
        header_json = json.loads(
            frame[8:8 + RemotingCommand.get_header_length(marked_header_len)].decode("utf-8"))
        assert "extFields" not in header_json
        assert header_json["code"] == RequestCode.BATCH_ACK_MESSAGE

    def test_control_single_ack_still_carries_its_header(self):
        """Paired control for the assertion above: a header-bearing request does have extFields.

        Without this, "ext_fields == {}" could pass for any broken request builder.
        """
        fake = _FakeInvoke([_resp(ResponseCode.SUCCESS)])
        header = AckMessageRequestHeader()
        header.consumer_group = GROUP
        header.topic = TOPIC
        header.queue_id = 3
        header.extra_info = _ck(queue_offset=100)
        header.offset = 100
        request = RemotingCommand.create_request_command(RequestCode.ACK_MESSAGE, header)
        fake(ADDR, request, 3000)
        assert fake.request.ext_fields["extraInfo"] == header.extra_info
        assert fake.request.code == RequestCode.ACK_MESSAGE

    def test_recorded_body_decodes_back_into_merged_entries(self):
        fake = _FakeInvoke([_resp(ResponseCode.SUCCESS)])
        cks = [_ck(ck_queue_offset=100, queue_offset=100, queue_id=3),
               _ck(ck_queue_offset=100, queue_offset=104, queue_id=3),
               _ck(ck_queue_offset=200, pop_time=POP_TIME + 5, queue_offset=201, queue_id=2)]
        _client(fake).batch_ack_message(GROUP, TOPIC, cks, addr=ADDR)
        parsed = BatchAckMessageRequestBody.decode(fake.request.body)
        assert parsed.broker_name == BROKER
        assert len(parsed.acks) == 2, "the two pops must be two entries"
        assert parsed.acks[0].bit_set.set_indexes() == [0, 4]
        assert parsed.acks[1].bit_set.set_indexes() == [1]
        assert [a.start_offset for a in parsed.acks] == [100, 200]
        assert [a.queue_id for a in parsed.acks] == [3, 2]
        assert [(a.consumer_group, a.topic, a.retry) for a in parsed.acks] == [
            (GROUP, TOPIC, "0")] * 2
        assert parsed.acks[1].pop_time == POP_TIME + 5

    def test_control_body_json_uses_the_compact_keys_and_the_broker_field(self):
        fake = _FakeInvoke([_resp(ResponseCode.SUCCESS)])
        _client(fake).batch_ack_message(GROUP, TOPIC, [_ck(queue_offset=100)], addr=ADDR)
        raw = fake.body_dict()
        assert list(raw.keys()) == ["brokerName", "acks"]
        assert list(raw["acks"][0].keys()) == COMPACT_KEYS
        assert raw["acks"][0]["b"] == "AQ=="

    def test_control_a_single_checkpoint_is_not_turned_into_a_multi_bit_entry(self):
        fake = _FakeInvoke([_resp(ResponseCode.SUCCESS)])
        _client(fake).batch_ack_message(GROUP, TOPIC, [_ck(queue_offset=100)], addr=ADDR)
        assert len(fake.body_dict()["acks"]) == 1
        assert BitSet.from_bytes(base64.b64decode(fake.body_dict()["acks"][0]["b"])) \
            .set_indexes() == [0]

    def test_error_code_is_returned_not_raised(self):
        fake = _FakeInvoke([_resp(ResponseCode.NO_MESSAGE, remark="no message")])
        code = _client(fake).batch_ack_message(GROUP, TOPIC, [_ck(queue_offset=100)],
                                              addr=ADDR)
        assert code == ResponseCode.NO_MESSAGE
        assert len(fake.requests) == 1

    @pytest.mark.parametrize("passed, expected", [(7777, 7777), (None, 3000)])
    def test_timeout_passthrough_with_default_control(self, passed, expected):
        fake = _FakeInvoke([_resp(ResponseCode.SUCCESS)])
        kwargs = {} if passed is None else {"timeout_millis": passed}
        _client(fake).batch_ack_message(GROUP, TOPIC, [_ck(queue_offset=100)],
                                        addr=ADDR, **kwargs)
        assert fake.requests[0][2] == expected

    def test_control_address_is_resolved_from_the_first_checkpoint(self):
        """brokerName + queueId of entry #0 route the request, not those of a later entry."""
        fake = _FakeInvoke([_resp(ResponseCode.SUCCESS)])
        seen = []

        def fake_addr(broker_name, topic, queue_id):
            seen.append((broker_name, topic, queue_id))
            return "127.0.0.1:19999"

        inst = _client(fake)
        inst.pop_ack_addr = fake_addr        # type: ignore[assignment]
        inst.batch_ack_message(GROUP, TOPIC, [_ck(broker=BROKER, queue_id=3, queue_offset=100),
                                              _ck(broker=OTHER_BROKER, queue_id=9, queue_offset=100)])
        assert seen == [(BROKER, TOPIC, 3)]
        assert fake.requests[0][0] == "127.0.0.1:19999"

    def test_control_empty_list_is_refused_without_touching_the_broker(self):
        """Nothing may go on the wire: a body without a broker name would only be rejected later."""
        fake = _FakeInvoke([_resp(ResponseCode.SUCCESS)])
        with pytest.raises(MQClientException):
            _client(fake).batch_ack_message(GROUP, TOPIC, [], addr=ADDR)
        assert fake.requests == []


# ---------------------------------------------------------------- 5. real loopback socket


class _RecordingBroker:
    """A broker stand-in on a loopback port: records every raw frame and answers it."""

    def __init__(self, response_code: int = ResponseCode.SUCCESS):
        self.response_code = response_code
        self.frames = []
        self._listen = socket.socket()
        self._listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listen.bind(("127.0.0.1", 0))
        self._listen.listen(4)
        self.port = self._listen.getsockname()[1]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    @property
    def addr(self):
        return "127.0.0.1:%d" % self.port

    def stop(self):
        self._stop.set()
        try:
            self._listen.close()
        except OSError:
            pass
        self._thread.join(timeout=3.0)

    def _recv_exact(self, sock, n):
        buf = b""
        while len(buf) < n:
            try:
                chunk = sock.recv(n - len(buf))
            except OSError:
                return None
            if not chunk:
                return None
            buf += chunk
        return buf

    def _loop(self):
        self._listen.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = self._listen.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn):
        try:
            while not self._stop.is_set():
                head = self._recv_exact(conn, 4)
                if head is None:
                    return
                (total,) = struct.unpack(">i", head)
                rest = self._recv_exact(conn, total)
                if rest is None:
                    return
                frame = head + rest
                self.frames.append(frame)
                cmd = RemotingCommand.decode(frame)
                resp = RemotingCommand.create_response_command(self.response_code, None, None)
                resp.opaque = cmd.opaque
                conn.sendall(resp.encode())
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass


def _real_client() -> MQClientInstance:
    """An instance wired to a real socket client; the loopback broker is reached by address."""
    inst = object.__new__(MQClientInstance)
    inst.remoting_client = RemotingClient()
    return inst


class TestBatchAckOverLoopbackSocket:
    """End-to-end at the frame level: what a broker would actually parse off the socket."""

    def test_control_frame_recorded_by_the_broker(self):
        broker = _RecordingBroker()
        client = _real_client()
        try:
            cks = [_ck(queue_offset=100), _ck(queue_offset=102)]
            code = client.batch_ack_message(GROUP, TOPIC, cks, addr=broker.addr)
            assert code == ResponseCode.SUCCESS
            assert len(broker.frames) == 1, "the two checkpoints are one RPC"
            cmd = RemotingCommand.decode(broker.frames[0])
            assert cmd.code == RequestCode.BATCH_ACK_MESSAGE
            assert cmd.code != RequestCode.ACK_MESSAGE
            # the header the broker sees carries no custom header fields at all
            assert cmd.ext_fields in ({}, None)
            body = BatchAckMessageRequestBody.decode(cmd.body)
            assert body.broker_name == BROKER
            assert [a.bit_set.set_indexes() for a in body.acks] == [[0, 2]]
            assert json.loads(cmd.body.decode("utf-8"))["acks"][0]["so"] == 100
        finally:
            client.remoting_client.shutdown()
            broker.stop()

    def test_control_broker_failure_code_surfaces_unchanged(self):
        broker = _RecordingBroker(response_code=ResponseCode.NO_MESSAGE)
        client = _real_client()
        try:
            code = client.batch_ack_message(GROUP, TOPIC, [_ck(queue_offset=100)],
                                            addr=broker.addr)
            assert code == ResponseCode.NO_MESSAGE
            assert len(broker.frames) == 1
            assert RemotingCommand.decode(broker.frames[0]).code == RequestCode.BATCH_ACK_MESSAGE
        finally:
            client.remoting_client.shutdown()
            broker.stop()
