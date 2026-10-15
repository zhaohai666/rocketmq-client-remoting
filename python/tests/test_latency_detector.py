# -*- coding: utf-8 -*-
from __future__ import annotations

import socket
import struct
import threading
import time

from client.latency import LatencyFaultToleranceImpl, MQFaultStrategy
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from remoting.protocol import remoting_command as rc_mod
from remoting.protocol.codes import RequestCode, ResponseCode

BROKER = "broker-a"
TOPIC = "DetectTopic"


def _wait_until(predicate, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def _recording_detector(calls, result=True):
    def detector(addr, timeout_millis):
        calls.append((addr, timeout_millis))
        return result
    return detector


# ---------------------------------------------------------------- Fault tolerance: probe rounds


class TestDetectByOneRound:
    def test_probe_success_restores_reachable(self):
        """Probe success → reachableFlag False→True (released from the isolation window)."""
        t = LatencyFaultToleranceImpl(resolver=lambda name: "10.0.0.1:10911",
                                      service_detector=_recording_detector([], True))
        t.update_fault_item(BROKER, 10.0, 60000, False)
        assert not t.is_reachable(BROKER)
        t.detect_by_one_round()
        assert t.is_reachable(BROKER)

    def test_probe_failure_keeps_unreachable(self):
        pass
        t = LatencyFaultToleranceImpl(resolver=lambda name: "10.0.0.1:10911",
                                      service_detector=_recording_detector([], False))
        t.update_fault_item(BROKER, 10.0, 60000, False)
        t.detect_by_one_round()
        assert not t.is_reachable(BROKER)

    def test_probe_success_on_reachable_item_is_noop(self):
        t = LatencyFaultToleranceImpl(resolver=lambda name: "addr",
                                      service_detector=_recording_detector([], True))
        t.update_fault_item(BROKER, 10.0, 0, True)
        t.detect_by_one_round()
        assert t.is_reachable(BROKER)

    def test_resolver_none_drops_fault_item(self):
        pass
        def _boom(addr, timeout):
            raise AssertionError("no more probing after the resolver returns None")
        t = LatencyFaultToleranceImpl(resolver=lambda name: None, service_detector=_boom)
        t.update_fault_item(BROKER, 10.0, 60000, False)
        t.detect_by_one_round()
        assert t.get_fault_item(BROKER) is None

    def test_check_stamp_gates_round_and_timeout_is_passed(self):
        calls = []
        t = LatencyFaultToleranceImpl(resolver=lambda name: "10.0.0.1:10911",
                                      service_detector=_recording_detector(calls, True))
        t.set_detect_timeout(333)
        t.update_fault_item(BROKER, 10.0, 0, True)
        item = t.get_fault_item(BROKER)
        item.check_stamp = time.time() * 1000.0 + 60_000.0   # the next probe time has not arrived yet
        t.detect_by_one_round()
        assert calls == []
        item.check_stamp = 0.0
        t.detect_by_one_round()
        assert calls == [("10.0.0.1:10911", 333)]
        # After probing, checkStamp is pushed to now + detectInterval
        assert item.check_stamp > time.time() * 1000.0


# ---------------------------------------------------------------- Fault tolerance: candidate set


class TestPickOneAtLeast:
    def test_returns_only_reachable_names(self):
        t = LatencyFaultToleranceImpl()
        t.update_fault_item("b1", 1.0, 0, False)
        t.update_fault_item("b2", 2.0, 0, True)
        for _ in range(20):
            assert t.pick_one_at_least() == "b2"

    def test_none_when_all_unreachable_or_empty(self):
        t = LatencyFaultToleranceImpl()
        assert t.pick_one_at_least() is None
        t.update_fault_item("b1", 1.0, 0, False)
        assert t.pick_one_at_least() is None
        t.remove("b1")
        assert t.pick_one_at_least() is None


# ---------------------------------------------------------------- Detector thread


class TestDetectorThread:
    def _make(self, calls):
        t = LatencyFaultToleranceImpl(resolver=lambda name: "10.0.0.1:10911",
                                      service_detector=_recording_detector(calls, True))
        t.update_fault_item(BROKER, 10.0, 0, True)
        return t

    def test_thread_runs_periodically_then_stops(self):
        calls = []
        t = self._make(calls)
        t.set_start_detector_enable(True)
        t.start_detector(initial_delay=0.0, period=0.05)
        thread = t._detector_thread
        assert thread is not None and thread.daemon
        try:
            assert _wait_until(lambda: len(calls) >= 3, timeout=8.0)
        finally:
            t.shutdown()
            thread.join(timeout=3.0)
        assert not thread.is_alive()

    def test_disabled_switch_spins_without_detecting(self):
        pass
        calls = []
        t = self._make(calls)
        t.set_start_detector_enable(False)
        t.start_detector(initial_delay=0.0, period=0.02)
        try:
            time.sleep(0.2)
            assert calls == []
            t.set_start_detector_enable(True)
            assert _wait_until(lambda: len(calls) >= 1, timeout=5.0)
        finally:
            t.shutdown()

    def test_start_detector_is_idempotent(self):
        calls = []
        t = self._make(calls)
        t.set_start_detector_enable(True)
        t.start_detector(initial_delay=0.0, period=0.05)
        first = t._detector_thread
        t.start_detector(initial_delay=0.0, period=0.05)
        assert t._detector_thread is first
        t.shutdown()

    def test_strategy_enable_propagates_to_tolerance(self):
        s = MQFaultStrategy(False)
        assert not s.is_start_detector_enable()
        assert not s.latency_fault_tolerance.is_start_detector_enable()
        s.set_start_detector_enable(True)
        assert s.is_start_detector_enable()
        assert s.latency_fault_tolerance.is_start_detector_enable()
        s.shutdown()


# ---------------------------------------------------------------- Fake broker (socket)


def _recv_exact(sock, n):
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


def _read_frame(sock):
    head = _recv_exact(sock, 4)
    if head is None:
        return None
    (total,) = struct.unpack(">i", head)
    body = _recv_exact(sock, total)
    if body is None:
        return None
    return head + body


class FakeBrokerServer:
    """Replies SUCCESS + extFields(offset) to every remoting request — the GET_MAX_OFFSET liveness probe can pass."""

    def __init__(self):
        self.codes = []
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
                frame = _read_frame(conn)
                if frame is None:
                    return
                cmd = rc_mod.RemotingCommand.decode(frame)
                self.codes.append(cmd.code)
                resp = rc_mod.RemotingCommand.create_response_command(
                    ResponseCode.SUCCESS, None, None)
                resp.opaque = cmd.opaque
                resp.ext_fields = {"offset": "123"}
                conn.sendall(resp.encode())
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass


class SilentBroker:
    """Receives only, never replies: liveness probes hang until timeout, pinning the "probe timeout → failure" path."""

    def __init__(self):
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

    def _loop(self):
        self._listen.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = self._listen.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            # Accept the connection but reply nothing; the prober can only wait for the timeout
            threading.Thread(target=self._hold, args=(conn,), daemon=True).start()

    def _hold(self, conn):
        self._stop.wait(30.0)
        try:
            conn.close()
        except OSError:
            pass


def _producer_with_client(client_id: str):
    producer = DefaultMQProducer("GID_DetectorUnit")
    client = MQClientInstance(client_id, ["127.0.0.1:9876"])
    producer._mq_client = client
    client.topic_route_table[TOPIC] = None
    return producer, client


class TestServiceDetectorOverSocket:
    """ServiceDetector equivalent: GET_MAX_OFFSET(30) hits the local fake broker over the real remoting protocol."""

    def test_probe_sends_get_max_offset_and_succeeds(self):
        server = FakeBrokerServer()
        producer, client = _producer_with_client("cid_detector_probe_ok")
        try:
            assert producer._detector_probe(server.addr, 3000) is True
            assert server.codes == [RequestCode.GET_MAX_OFFSET]
            assert RequestCode.GET_MAX_OFFSET == 30
        finally:
            client.remoting_client.shutdown()
            server.stop()

    def test_probe_timeout_returns_false(self):
        silent = SilentBroker()
        producer, client = _producer_with_client("cid_detector_probe_timeout")
        try:
            began = time.time()
            assert producer._detector_probe(silent.addr, 300) is False
            assert time.time() - began < 10.0, "probe failure must be bounded by the timeout, never hang forever"
        finally:
            client.remoting_client.shutdown()
            silent.stop()

    def test_full_detect_cycle_recovers_isolated_broker(self):
        """Send-failure isolation → probe success → back in the candidate set (pickOneAtLeast can select it)."""
        server = FakeBrokerServer()
        producer, client = _producer_with_client("cid_detector_cycle")
        try:
            strategy = producer._mq_fault_strategy
            strategy.set_send_latency_fault_enable(True)
            tolerance = strategy.latency_fault_tolerance
            tolerance.detect_timeout_millis = 3000
            tolerance.resolver = lambda name: server.addr
            tolerance.service_detector = producer._detector_probe
            strategy.set_start_detector_enable(True)
            # Aligned with the exception path of sendDefaultImpl: record reachable=False when detection is enabled
            strategy.update_fault_item(BROKER, 10.0, True, False)
            assert not tolerance.is_reachable(BROKER)
            assert tolerance.pick_one_at_least() is None
            tolerance.detect_by_one_round()
            assert tolerance.is_reachable(BROKER)
            assert tolerance.pick_one_at_least() == BROKER
        finally:
            producer._mq_fault_strategy.shutdown()
            client.remoting_client.shutdown()
            server.stop()

    def test_detect_failure_on_dead_port_keeps_unreachable(self):
        """The liveness probe cannot connect (port closed): the broker stays unreachable and must not flip back to reachable due to probing.

        A loopback quirk: a just-closed port only refuses connections for about 2s; the probe timeout here is set to 5000ms to cover it.
        """
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        dead_addr = "127.0.0.1:%d" % port

        producer, client = _producer_with_client("cid_detector_dead")
        try:
            tolerance = producer._mq_fault_strategy.latency_fault_tolerance
            tolerance.detect_timeout_millis = 5000
            tolerance.resolver = lambda name: dead_addr
            tolerance.service_detector = producer._detector_probe
            tolerance.update_fault_item(BROKER, 10.0, 60000, False)
            tolerance.detect_by_one_round()
            assert not tolerance.is_reachable(BROKER)
            assert tolerance.pick_one_at_least() is None
        finally:
            client.remoting_client.shutdown()

    def test_resolver_uses_publish_address_of_master(self):
        """Resolver equivalent: find_broker_address_in_publish — only takes brokerId=0."""
        producer, client = _producer_with_client("cid_detector_resolver")
        try:
            client.broker_addr_table[BROKER] = {0: "10.0.0.1:10911", 1: "10.0.0.2:10911"}
            assert producer._detector_resolver(BROKER) == "10.0.0.1:10911"
            assert producer._detector_resolver("no-such-broker") is None
            producer._mq_client = None
            assert producer._detector_resolver(BROKER) is None
        finally:
            client.remoting_client.shutdown()


# ---------------------------------------------------------------- Configuration API


class TestProducerDetectorConfig:
    def test_defaults_align_classic_client_config(self):
        producer = DefaultMQProducer("GID_DetectorDefaults")
        assert producer.start_detector_enable is False
        assert producer.detect_timeout_millis == 200
        assert producer.detect_interval_millis == 2000
        assert producer.is_start_detector_enable() is False

    def test_setters_and_snapshot_into_tolerance(self):
        producer = DefaultMQProducer("GID_DetectorConfig")
        producer.set_detect_timeout(777)
        producer.set_detect_interval(8888)
        producer.set_start_detector_enable(True)
        assert producer.get_detect_timeout() == 777
        assert producer.get_detect_interval() == 8888
        assert producer.is_start_detector_enable() is True

        # start() takes exactly this snapshot (when _mq_client is None the resolver only fails to resolve, no networking)
        producer._setup_reachability_detector()
        try:
            tolerance = producer._mq_fault_strategy.latency_fault_tolerance
            assert tolerance.detect_timeout_millis == 777
            assert tolerance.detect_interval_millis == 8888
            assert tolerance.is_start_detector_enable() is True
            assert tolerance.resolver == producer._detector_resolver
            assert tolerance.service_detector == producer._detector_probe
            assert tolerance._detector_thread is not None
            assert tolerance._detector_thread.daemon is True
        finally:
            producer._mq_fault_strategy.shutdown()
