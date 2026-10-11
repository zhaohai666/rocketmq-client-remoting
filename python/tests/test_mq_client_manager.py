# -*- coding: utf-8 -*-
"""MQClientManager（跨门面复用 MQClientInstance）单测。

对应 Java ``client/src/test`` 侧的 MQClientManager 语义（Java 无独立单测类，
行为由集成测试锁定）：

* 同 clientId 的两个门面 start 后共享**同一个** MQClientInstance（计数 2）——
  Java ``MQClientManager.getOrCreateMQClientInstance`` 的 factoryTable 命中；
* 先关一个：实例仍存活（Java ``MQClientInstance.shutdown`` 在 producerTable/
  consumerTable 非空时直接 return 的引用计数等价物）；
* 全关：计数归零，实例才真正 shutdown 并从缓存摘除；
* 不同 clientId：绝不共享（Java 按 clientId 键控）。

另锁三件复用后不能坏的事：

* ``MQClientInstance.start()`` 幂等 —— 复用方再 start 不重复拉起路由刷新/
  弹性巡检这些**实例级**后台线程（Java ServiceState 守卫）；
* ``CHECK_TRANSACTION_STATE(39)`` 实例级分派器按登记扇给各门面处理器、
  重复登记去重、注销摘除（Java producerTable 按组分派的等价物）；
* start 失败的实例从缓存摘除（Java ``cleanupAfterStartFailure`` +
  ``removeClientFactory``），半启动实例不许毒化后续的 get_or_create。
"""
from __future__ import annotations

import threading

import pytest

from client.mq_client import MQClientInstance
from client.mq_client_manager import MQClientManager
from client.producer import DefaultMQProducer

NS = "127.0.0.1:19876"


def _fresh_manager() -> MQClientManager:
    # 用进程级单例：MQClientInstance.start 失败时的自摘除、门面的 register/unregister
    # 都发生在单例上，断言必须看同一张表（autouse fixture 负责用例间清表）。
    return MQClientManager.get_instance()


def _make_instance(client_id: str) -> MQClientInstance:
    inst = MQClientInstance(client_id, [NS])
    return inst


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    MQClientManager.get_instance().reset()
    MQClientInstance.INSTANCE_MAP.clear()


# ---------------------------------------------------------------- 缓存与创建

def test_same_client_id_reuses_instance():
    """同 clientId：第二次 get_or_create 返回同一实例，created=False。"""
    mgr = _fresh_manager()
    inst, created = mgr.get_or_create_mq_client_instance("a@b", lambda: _make_instance("a@b"))
    assert created is True
    again, created2 = mgr.get_or_create_mq_client_instance("a@b", lambda: _make_instance("a@b"))
    assert created2 is False
    assert again is inst
    # factory 未命中缓存时才该被调用 —— 上面第二次没建新实例
    assert mgr.get_client_factory("a@b") is inst


def test_different_client_ids_do_not_share():
    """不同 clientId：各建各的（Java 按 clientId 键控，绝不串台）。"""
    mgr = _fresh_manager()
    a, _ = mgr.get_or_create_mq_client_instance("a@one", lambda: _make_instance("a@one"))
    b, _ = mgr.get_or_create_mq_client_instance("a@two", lambda: _make_instance("a@two"))
    assert a is not b


def test_concurrent_get_or_create_creates_exactly_one():
    """并发 get_or_create 同一 clientId：只有一个赢家，其余拿到缓存实例。"""
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
    # 全部拿到同一实例；只有落表的那一个（≤1 个）看到 created=True
    assert all(r[0] is results[0][0] for r in results)
    assert len([r for r in results if r[1] is True]) <= 1
    assert mgr.get_client_factory("race@id") is results[0][0]


# ---------------------------------------------------------------- 引用计数生命周期

def test_refcount_two_facades_share_then_survive_then_close():
    """任务口径 1-3：同 clientId 两门面共享（计数 2）→ 先关一个仍存活 → 全关才关闭。"""
    mgr = _fresh_manager()
    inst, _ = mgr.get_or_create_mq_client_instance("life@id", lambda: _make_instance("life@id"))
    mgr.register_client("life@id", inst)
    mgr.register_client("life@id", inst)
    assert mgr.refcount_of("life@id") == 2

    # 第一个门面 shutdown：实例仍存活、仍在缓存、连接未关
    mgr.unregister_client("life@id", inst)
    assert mgr.refcount_of("life@id") == 1
    assert mgr.get_client_factory("life@id") is inst
    assert inst.remoting_client._closed is False

    # 复用方仍能拿到同一实例
    again, created = mgr.get_or_create_mq_client_instance("life@id", lambda: _make_instance("life@id"))
    assert created is False and again is inst

    # 第二个门面 shutdown：计数归零 → 真正关闭 + 摘表
    mgr.unregister_client("life@id", inst)
    assert mgr.refcount_of("life@id") == 0
    assert mgr.get_client_factory("life@id") is None
    assert inst.remoting_client._closed is True


def test_unregister_unknown_instance_shuts_it_down_directly():
    """不在缓存里的实例（测试替身 / 旧调用方直接构造的）：按旧语义直接关，
    且不影响缓存中可能存在的同键条目。"""
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
    # 缓存条目原封不动
    assert mgr.get_client_factory("mix@id") is cached
    assert mgr.refcount_of("mix@id") == 1


def test_register_client_registers_unmanaged_instance():
    """未经 get_or_create 的实例 register 后正常进入引用计数闭环。"""
    mgr = _fresh_manager()
    inst = _make_instance("unmanaged@id")
    mgr.register_client("unmanaged@id", inst)
    assert mgr.refcount_of("unmanaged@id") == 1
    assert mgr.get_client_factory("unmanaged@id") is inst
    mgr.unregister_client("unmanaged@id", inst)
    assert mgr.get_client_factory("unmanaged@id") is None
    assert inst.remoting_client._closed is True


def test_remove_client_factory_drops_entry_without_shutdown():
    """start 失败清理路径：只摘表、不 shutdown（对应 Java removeClientFactory）。"""
    mgr = _fresh_manager()
    inst, _ = mgr.get_or_create_mq_client_instance("drop@id", lambda: _make_instance("drop@id"))
    mgr.remove_client_factory("drop@id", inst)
    assert mgr.get_client_factory("drop@id") is None
    assert inst.remoting_client._closed is False
    # 不属于自己（identity 不符）的条目不能误删
    other, _ = mgr.get_or_create_mq_client_instance("keep@id", lambda: _make_instance("keep@id"))
    mgr.remove_client_factory("keep@id", inst)  # 传的是别人的实例
    assert mgr.get_client_factory("keep@id") is other


# ---------------------------------------------------------------- 实例生命周期幂等

def test_instance_start_is_idempotent():
    """复用方再 start：不重复拉起实例级后台线程（Java ServiceState 守卫）。"""
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
    """start 抛错：实例把自己从 MQClientManager 缓存摘掉（Java
    cleanupAfterStartFailure → removeClientFactory），且 _started 回落为 False。"""
    mgr = _fresh_manager()
    # 空 name server + 动态取址失败 → _start_locked 在 dynamic 分支抛 10004
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


# ---------------------------------------------------------------- 39 事务回查分派

def test_transaction_check_handlers_register_dispatch_unregister():
    """39 实例级分派器：逐个扇给登记的处理器、去重、注销摘除。"""
    inst = _make_instance("tx@id")
    calls = []
    h1 = lambda cmd, addr: calls.append(("h1", addr))  # noqa: E731
    h2 = lambda cmd, addr: calls.append(("h2", addr))  # noqa: E731

    inst.register_transaction_check_handler(h1)
    inst.register_transaction_check_handler(h1)  # 幂等：重复登记不叠加
    inst.register_transaction_check_handler(h2)

    inst._dispatch_check_transaction_state(None, "broker-addr")
    assert sorted(calls) == [("h1", "broker-addr"), ("h2", "broker-addr")]

    inst.unregister_transaction_check_handler(h1)
    calls.clear()
    inst._dispatch_check_transaction_state(None, "b")
    assert calls == [("h2", "b")]
    # 注销未登记的处理器是 no-op
    inst.unregister_transaction_check_handler(h1)


def test_transaction_check_dispatch_swallows_handler_exception():
    """一个门面处理器抛错不能影响其他门面（Java catch (Exception ignored)）。"""
    inst = _make_instance("txerr@id")

    def bad(cmd, addr):
        raise RuntimeError("boom")

    calls = []
    inst.register_transaction_check_handler(bad)
    inst.register_transaction_check_handler(lambda cmd, addr: calls.append(addr))
    inst._dispatch_check_transaction_state(None, "addr-x")
    assert calls == ["addr-x"]


# ---------------------------------------------------------------- 门面级共享（真实 producer）

def test_two_producers_share_instance_and_refcount_survives_partial_shutdown():
    """门面级：同进程两个生产者（同 instanceName → 同 clientId）共享实例；
    先关一个实例仍存活，全关后才真正关闭。"""
    from client.mq_client_manager import MQClientManager as Mgr

    mgr = Mgr.get_instance()
    # Java 口径：changeInstanceNameToPID 只改写默认名 "DEFAULT"（且会拼 nanoTime），
    # 显式设置的同名 instanceName 才会让两个门面算出同一个 clientId —— 这正是
    # Java "同进程内 producer/consumer 共享 MQClientInstance" 的前提。
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
            # 先关一个：实例与它的实例级后台线程继续存活
            p1.shutdown()
            assert mgr.refcount_of(client_id) == 1
            assert mgr.get_client_factory(client_id) is inst
            assert inst.remoting_client._closed is False
        finally:
            p2.shutdown()
        # 全关：实例关闭并从缓存摘除
        assert mgr.refcount_of(client_id) == 0
        assert mgr.get_client_factory(client_id) is None
        assert inst.remoting_client._closed is True
        # 关掉实例后 p2 侧不该有残留的可发送状态混乱 —— _require_client 会拒
        from client.exception import MQClientException
        with pytest.raises(MQClientException):
            p2._require_client()
    finally:
        # 防御：万一上面断言失败，把还在跑的门面收掉
        if p1._started:
            p1.shutdown()
        if p2._started:
            p2.shutdown()


def test_producers_with_different_instance_names_do_not_share():
    """门面级：instanceName 不同 → clientId 不同 → 实例不共享。"""
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
