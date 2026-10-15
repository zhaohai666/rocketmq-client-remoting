# -*- coding: utf-8 -*-
from __future__ import annotations

import threading

import pytest

from client.mq_client import MQClientInstance
from client.mq_client_manager import MQClientManager
from client.producer import DefaultMQProducer

NS = "127.0.0.1:19876"


def _fresh_manager() -> MQClientManager:
    # Uses the process-level singleton: both MQClientInstance.start-failure self-removal and the facade's register/unregister
    # happen on the singleton, so assertions must look at the same table (the autouse fixture clears the table between tests).
    return MQClientManager.get_instance()


def _make_instance(client_id: str) -> MQClientInstance:
    inst = MQClientInstance(client_id, [NS])
    return inst


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    MQClientManager.get_instance().reset()
    MQClientInstance.INSTANCE_MAP.clear()


# ---------------------------------------------------------------- Cache and creation

def test_same_client_id_reuses_instance():
    """Same clientId: the second get_or_create returns the same instance, created=False."""
    mgr = _fresh_manager()
    inst, created = mgr.get_or_create_mq_client_instance("a@b", lambda: _make_instance("a@b"))
    assert created is True
    again, created2 = mgr.get_or_create_mq_client_instance("a@b", lambda: _make_instance("a@b"))
    assert created2 is False
    assert again is inst
    # The factory should only be called on a cache miss — the second call above did not build a new instance
    assert mgr.get_client_factory("a@b") is inst


def test_different_client_ids_do_not_share():
    pass
    mgr = _fresh_manager()
    a, _ = mgr.get_or_create_mq_client_instance("a@one", lambda: _make_instance("a@one"))
    b, _ = mgr.get_or_create_mq_client_instance("a@two", lambda: _make_instance("a@two"))
    assert a is not b


def test_concurrent_get_or_create_creates_exactly_one():
    """Concurrent get_or_create of the same clientId: only one winner, the rest get the cached instance."""
    mgr = _fresh_manager()
    built = []

    def factory():
        inst = _make_instance("race@id")
        built.append(inst)
        return inst

    results = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        results.append(mgr.get_or_create_mq_client_instance("race@id", factory))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # All get the same instance; only the one that landed in the table (at most 1) sees created=True
    assert all(r[0] is results[0][0] for r in results)
    assert len([r for r in results if r[1] is True]) <= 1
    assert mgr.get_client_factory("race@id") is results[0][0]


# ---------------------------------------------------------------- Reference count lifecycle

def test_refcount_two_facades_share_then_survive_then_close():
    """Two facades sharing one clientId (count 2) → closing one still leaves it alive → all closed before shutdown."""
    mgr = _fresh_manager()
    inst, _ = mgr.get_or_create_mq_client_instance("life@id", lambda: _make_instance("life@id"))
    mgr.register_client("life@id", inst)
    mgr.register_client("life@id", inst)
    assert mgr.refcount_of("life@id") == 2

    # First facade shutdown: the instance is still alive, still cached, connection not closed
    mgr.unregister_client("life@id", inst)
    assert mgr.refcount_of("life@id") == 1
    assert mgr.get_client_factory("life@id") is inst
    assert inst.remoting_client._closed is False

    # A reuser can still get the same instance
    again, created = mgr.get_or_create_mq_client_instance("life@id", lambda: _make_instance("life@id"))
    assert created is False and again is inst

    # Second facade shutdown: count drops to zero → real shutdown + removal from table
    mgr.unregister_client("life@id", inst)
    assert mgr.refcount_of("life@id") == 0
    assert mgr.get_client_factory("life@id") is None
    assert inst.remoting_client._closed is True


def test_unregister_unknown_instance_shuts_it_down_directly():
    """An instance not in the cache (a test double / directly constructed by an old caller): shut down directly per the old semantics,
    without affecting the same-key entry that may exist in the cache."""
    mgr = _fresh_manager()
    cached, _ = mgr.get_or_create_mq_client_instance("mix@id", lambda: _make_instance("mix@id"))
    mgr.register_client("mix@id", cached)

    class _Fake:
        def __init__(self):
            self.shutdown_called = False

        def shutdown(self):
            self.shutdown_called = True

    fake = _Fake()
    mgr.unregister_client("mix@id", fake)
    assert fake.shutdown_called is True
    # The cache entry is untouched
    assert mgr.get_client_factory("mix@id") is cached
    assert mgr.refcount_of("mix@id") == 1


def test_register_client_registers_unmanaged_instance():
    """An instance not created via get_or_create enters the reference-count loop properly after register."""
    mgr = _fresh_manager()
    inst = _make_instance("unmanaged@id")
    mgr.register_client("unmanaged@id", inst)
    assert mgr.refcount_of("unmanaged@id") == 1
    assert mgr.get_client_factory("unmanaged@id") is inst
    mgr.unregister_client("unmanaged@id", inst)
    assert mgr.get_client_factory("unmanaged@id") is None
    assert inst.remoting_client._closed is True


def test_remove_client_factory_drops_entry_without_shutdown():
    pass
    mgr = _fresh_manager()
    inst, _ = mgr.get_or_create_mq_client_instance("drop@id", lambda: _make_instance("drop@id"))
    mgr.remove_client_factory("drop@id", inst)
    assert mgr.get_client_factory("drop@id") is None
    assert inst.remoting_client._closed is False
    # An entry that is not one's own (identity mismatch) must not be deleted by mistake
    other, _ = mgr.get_or_create_mq_client_instance("keep@id", lambda: _make_instance("keep@id"))
    mgr.remove_client_factory("keep@id", inst)  # passing someone else's instance
    assert mgr.get_client_factory("keep@id") is other


# ---------------------------------------------------------------- Instance lifecycle idempotence

def test_instance_start_is_idempotent():
    pass
    inst = _make_instance("idem@id")
    inst.start()
    route_thread = inst._route_refresh_thread
    adjust_thread = inst._adjust_pool_thread
    assert route_thread is not None and adjust_thread is not None
    inst.start()
    inst.start()
    assert inst._route_refresh_thread is route_thread
    assert inst._adjust_pool_thread is adjust_thread
    assert inst._started is True
    inst.shutdown()
    assert inst._started is False


def test_instance_start_failure_removes_itself_from_cache(monkeypatch):
    pass
    mgr = _fresh_manager()
    # Empty name server + dynamic address fetch failure → _start_locked throws 10004 in the dynamic branch
    inst, created = mgr.get_or_create_mq_client_instance(
        "fail@id", lambda: MQClientInstance("fail@id", []))
    assert created is True
    inst.top_addressing.ws_addr = "http://unit-test.invalid"

    def _boom():
        raise RuntimeError("address server unreachable")

    monkeypatch.setattr(inst, "fetch_name_server_addr", _boom)
    with pytest.raises(Exception):
        inst.start()
    assert inst._started is False
    assert mgr.get_client_factory("fail@id") is None


# ---------------------------------------------------------------- 39 transaction check dispatch

def test_transaction_check_handlers_register_dispatch_unregister():
    """The 39 instance-level dispatcher: fans out to registered handlers one by one, dedupes, and removes on unregister."""
    inst = _make_instance("tx@id")
    calls = []
    h1 = lambda cmd, addr: calls.append(("h1", addr))  # noqa: E731
    h2 = lambda cmd, addr: calls.append(("h2", addr))  # noqa: E731

    inst.register_transaction_check_handler(h1)
    inst.register_transaction_check_handler(h1)  # idempotent: repeated registration does not stack
    inst.register_transaction_check_handler(h2)

    inst._dispatch_check_transaction_state(None, "broker-addr")
    assert sorted(calls) == [("h1", "broker-addr"), ("h2", "broker-addr")]

    inst.unregister_transaction_check_handler(h1)
    calls.clear()
    inst._dispatch_check_transaction_state(None, "b")
    assert calls == [("h2", "b")]
    # Unregistering an unregistered handler is a no-op
    inst.unregister_transaction_check_handler(h1)


def test_transaction_check_dispatch_swallows_handler_exception():
    pass
    inst = _make_instance("txerr@id")

    def bad(cmd, addr):
        raise RuntimeError("boom")

    calls = []
    inst.register_transaction_check_handler(bad)
    inst.register_transaction_check_handler(lambda cmd, addr: calls.append(addr))
    inst._dispatch_check_transaction_state(None, "addr-x")
    assert calls == ["addr-x"]


# ---------------------------------------------------------------- Facade-level sharing (real producer)

def test_two_producers_share_instance_and_refcount_survives_partial_shutdown():
    """Facade level: two producers in the same process (same instanceName → same clientId) share the instance;
    closing one leaves the instance alive; only after all are closed is it really shut down."""
    from client.mq_client_manager import MQClientManager as Mgr

    mgr = Mgr.get_instance()
    p1 = DefaultMQProducer("MgrSharePG1")
    p1.set_namesrv_addr(NS)
    p1.set_instance_name("MGR-SHARED-IT")
    p2 = DefaultMQProducer("MgrSharePG2")
    p2.set_namesrv_addr(NS)
    p2.set_instance_name("MGR-SHARED-IT")
    p1.start()
    try:
        p2.start()
        try:
            assert p1._mq_client is p2._mq_client
            client_id = p1.client_id
            assert client_id == p2.client_id
            assert mgr.refcount_of(client_id) == 2
            inst = p1._mq_client
            # Close one first: the instance and its instance-level background threads keep running
            p1.shutdown()
            assert mgr.refcount_of(client_id) == 1
            assert mgr.get_client_factory(client_id) is inst
            assert inst.remoting_client._closed is False
        finally:
            p2.shutdown()
        # All closed: the instance shuts down and is removed from the cache
        assert mgr.refcount_of(client_id) == 0
        assert mgr.get_client_factory(client_id) is None
        assert inst.remoting_client._closed is True
        # After the instance is closed, the p2 side should not have leftover sendable-state confusion — _require_client will reject
        from client.exception import MQClientException
        with pytest.raises(MQClientException):
            p2._require_client()
    finally:
        # Defensive: in case an assertion above fails, shut down the still-running facades
        if p1._started:
            p1.shutdown()
        if p2._started:
            p2.shutdown()


def test_producers_with_different_instance_names_do_not_share():
    """Facade level: different instanceName → different clientId → instances are not shared."""
    mgr = MQClientManager.get_instance()
    p1 = DefaultMQProducer("MgrDiffPG")
    p1.set_namesrv_addr(NS)
    p1.set_instance_name("DIFF-ONE")
    p2 = DefaultMQProducer("MgrDiffPG")
    p2.set_namesrv_addr(NS)
    p2.set_instance_name("DIFF-TWO")
    try:
        p1.start()
        p2.start()
        assert p1._mq_client is not p2._mq_client
        assert mgr.refcount_of(p1.client_id) == 1
        assert mgr.refcount_of(p2.client_id) == 1
    finally:
        p1.shutdown()
        p2.shutdown()
    assert p1._mq_client.remoting_client._closed is True
    assert p2._mq_client.remoting_client._closed is True
