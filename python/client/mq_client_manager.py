# -*- coding: utf-8 -*-
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
    pass

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
        self._factory_table: Dict[str, _FactoryEntry] = {}

    # ---------------- 查询 / 创建 ----------------
    def get_or_create_mq_client_instance(
            self, client_id: str,
            factory: Callable[[], "MQClientInstance"]) -> Tuple["MQClientInstance", bool]:
        pass
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
        pass
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
