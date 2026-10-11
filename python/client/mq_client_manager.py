# -*- coding: utf-8 -*-
"""MQClientManager：同进程内按 clientId 复用 MQClientInstance。

对应 Java ``org.apache.rocketmq.client.impl.MQClientManager``（``factoryTable``：
clientId → MQClientInstance）+ ``MQClientInstance`` 的引用计数关闭语义。

Java 的经典设计：同进程内同 clientId（``IP@instanceName[@unitName][@STREAM]``）的
producer / consumer / admin 共享**同一个** MQClientInstance——路由缓存、broker 地址表、
心跳、与 broker 的长连接、client 级处理器（326/40/220/221/307/309）都只有一份；
引用计数归零时才真正 ``shutdown()``（停后台线程、断连接），并把条目从表里摘掉。

生命周期契约（与 Java ``getOrCreateMQClientInstance`` / ``MQClientInstance.shutdown`` 一致）：

* 门面 ``start()``：``get_or_create_mq_client_instance`` 拿实例（没有就建）→
  ``register_client`` 计数 +1 → ``instance.start()``（幂等，复用时不会重复拉起
  路由刷新 / name server 刷新 / 弹性巡检这些**实例级**后台线程）。
* 门面 ``shutdown()``：``unregister_client`` 计数 -1；**归零**时把实例摘出缓存并
  在锁外真正 ``shutdown()``；仍有其他门面持有（计数 > 0）时实例继续存活，
  其后台线程与连接不受影响。
* 配置以**第一次创建**时的快照为准（Java ``cloneClientConfig`` 同口径）：复用方后设的
  tls / 代理 / 超时等不会改写已存在的实例。Java 还会因 rpcHook 不同的场景天然只用
  第一份，这里一致：rpcHook 只在 ``created=True``（本门面是创建者）时才注册。

线程安全：所有表操作都在单把锁内完成；真正的 ``shutdown()``（网络 I/O）在锁外执行，
避免长时间持锁阻塞其他门面的 start/unregister。
"""
from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Callable, Dict, Optional, Tuple

from rocketmq_logging import get_logger

if TYPE_CHECKING:
    from .mq_client import MQClientInstance

logger = get_logger()


class _FactoryEntry:
    """缓存条目：实例 + 在用门面数（引用计数）。"""

    __slots__ = ("instance", "refcount")

    def __init__(self, instance: "MQClientInstance"):
        self.instance = instance
        self.refcount = 0


class MQClientManager:
    """进程级单例（对应 Java ``MQClientManager.getInstance()``）。"""

    _INSTANCE: Optional["MQClientManager"] = None
    _SINGLETON_LOCK = threading.Lock()

    @classmethod
    def get_instance(cls) -> "MQClientManager":
        if cls._INSTANCE is None:
            with cls._SINGLETON_LOCK:
                if cls._INSTANCE is None:
                    cls._INSTANCE = MQClientManager()
        return cls._INSTANCE

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # 对应 Java ``factoryTable``：clientId → 条目。
        self._factory_table: Dict[str, _FactoryEntry] = {}

    # ---------------- 查询 / 创建 ----------------
    def get_or_create_mq_client_instance(
            self, client_id: str,
            factory: Callable[[], "MQClientInstance"]) -> Tuple["MQClientInstance", bool]:
        """按 clientId 取缓存实例；没有就用 ``factory()`` 建一个并落表。

        返回 ``(instance, created)``：``created=True`` 表示本调用方是创建者，
        门面据此决定要不要注册自己的 rpcHook（复用方不注册，与 Java 只认
        创建时那份 rpcHook 同口径）。``factory()`` 在锁外执行：构造期间若另一
        线程已抢先落表，则**放弃自己的新实例**、返回已缓存的那份（Java
        ``computeIfAbsent`` 的「只有赢家生效」语义）。
        """
        with self._lock:
            entry = self._factory_table.get(client_id)
            if entry is not None:
                logger.debug("reuse MQClientInstance for clientId: %s", client_id)
                return entry.instance, False
        instance = factory()
        with self._lock:
            existing = self._factory_table.get(client_id)
            if existing is not None:
                return existing.instance, False
            self._factory_table[client_id] = _FactoryEntry(instance)
            logger.info("Created new MQClientInstance for clientId: %s", client_id)
            return instance, True

    # ---------------- 引用计数 ----------------
    def register_client(self, client_id: str, instance: "MQClientInstance") -> None:
        """门面 start() 时调用：在用计数 +1。

        未经 ``get_or_create_mq_client_instance`` 的实例（测试替身 / 旧调用方直接
        构造）也照常登记，保证 unregister 的计数平衡。
        """
        with self._lock:
            entry = self._factory_table.get(client_id)
            if entry is None or entry.instance is not instance:
                entry = _FactoryEntry(instance)
                self._factory_table[client_id] = entry
            entry.refcount += 1

    def unregister_client(self, client_id: str, instance: "MQClientInstance") -> None:
        """门面 shutdown() 时调用：计数 -1；归零才真正关闭。

        归零时先摘表再在锁外 ``shutdown()``——摘表后其他门面的
        ``get_or_create_mq_client_instance`` 自然会建新实例，不会拿到半死的旧实例。
        实例不在表中（测试替身 / 已被摘除的旧实例）时保持门面旧的 shutdown 语义：
        直接关它，调用方无需分辨。
        """
        with self._lock:
            entry = self._factory_table.get(client_id)
            if entry is None or entry.instance is not instance:
                # 不是本 manager 缓存里的那份实例（测试替身 / 已被顶替或摘除的旧实例）：
                # 不动缓存里可能存在的其他条目，只按旧语义直接关调用方手里的实例。
                do_shutdown = True
            else:
                entry.refcount -= 1
                if entry.refcount > 0:
                    logger.debug("MQClientInstance %s still held by %d client(s)",
                                 client_id, entry.refcount)
                    return
                del self._factory_table[client_id]
                do_shutdown = True
        logger.info("the client factory [%s] shutdown OK", client_id)
        instance.shutdown()

    def remove_client_factory(self, client_id: str,
                              instance: Optional["MQClientInstance"] = None) -> None:
        """把实例从缓存摘掉但**不** shutdown（对应 Java ``removeClientFactory``）。

        ``MQClientInstance.start()`` 失败时由实例自己调用：半启动的实例不能留在
        表里毒化后续的 ``get_or_create``；传 ``instance`` 时只摘自己的条目，
        防止误删一个已顶替上来的新实例。
        """
        with self._lock:
            entry = self._factory_table.get(client_id)
            if entry is None:
                return
            if instance is not None and entry.instance is not instance:
                return
            del self._factory_table[client_id]

    # ---------------- 观测 / 测试 ----------------
    def get_client_factory(self, client_id: str) -> Optional["MQClientInstance"]:
        with self._lock:
            entry = self._factory_table.get(client_id)
            return entry.instance if entry is not None else None

    def refcount_of(self, client_id: str) -> int:
        """该 clientId 当前的在用计数（观测 / 单测用）。"""
        with self._lock:
            entry = self._factory_table.get(client_id)
            return entry.refcount if entry is not None else 0

    def reset(self) -> None:
        """清空缓存（测试隔离用）。只摘表、不 shutdown——与旧行为一致，
        泄漏的实例交由 GC / 进程退出回收，不引入新的关闭副作用。"""
        with self._lock:
            self._factory_table.clear()


__all__ = ["MQClientManager"]
