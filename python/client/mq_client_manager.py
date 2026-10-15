# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Callable, Dict, Optional, Tuple

from rocketmq_logging import get_logger

if TYPE_CHECKING:
    from .mq_client import MQClientInstance

logger = get_logger()


class _FactoryEntry:
    """Cache entry: instance + number of facades in use (reference count)."""

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

    # ---------------- query / create ----------------
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

    # ---------------- reference counting ----------------
    def register_client(self, client_id: str, instance: "MQClientInstance") -> None:
        """Called when a facade's start() runs: increments the in-use count by 1.

        Instances not created via ``get_or_create_mq_client_instance`` (test doubles / instances
        directly constructed by old callers) are still registered normally, keeping the unregister
        count balanced.
        """
        with self._lock:
            entry = self._factory_table.get(client_id)
            if entry is None or entry.instance is not instance:
                entry = _FactoryEntry(instance)
                self._factory_table[client_id] = entry
            entry.refcount += 1

    def unregister_client(self, client_id: str, instance: "MQClientInstance") -> None:
        """Called when a facade's shutdown() runs: decrements the count by 1; only a zero count truly closes it.

        On zeroing, the entry is removed from the table first, then ``shutdown()`` runs outside the
        lock -- after removal, other facades' ``get_or_create_mq_client_instance`` naturally build a
        fresh instance and won't pick up a half-dead old one. When the instance is not in the table
        (test double / already-removed old instance), keep the facade's old shutdown semantics: just
        close it; the caller need not distinguish.
        """
        with self._lock:
            entry = self._factory_table.get(client_id)
            if entry is None or entry.instance is not instance:
                # not the instance held in this manager's cache (test double / already-replaced or
                # removed old instance): do not touch other entries that may exist in the cache; just
                # close the instance the caller holds, following the old semantics.
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

    # ---------------- observation / testing ----------------
    def get_client_factory(self, client_id: str) -> Optional["MQClientInstance"]:
        with self._lock:
            entry = self._factory_table.get(client_id)
            return entry.instance if entry is not None else None

    def refcount_of(self, client_id: str) -> int:
        """Current in-use count for this clientId (for observation / unit tests)."""
        with self._lock:
            entry = self._factory_table.get(client_id)
            return entry.refcount if entry is not None else 0

    def reset(self) -> None:
        """Clear the cache (for test isolation). Only remove from the table, don't shutdown -- consistent
        with the old behavior; leaked instances are reclaimed by GC / process exit, introducing no new
        shutdown side effects."""
        with self._lock:
            self._factory_table.clear()


__all__ = ["MQClientManager"]
