# -*- coding: utf-8 -*-
from __future__ import annotations

import atexit
import itertools
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import List, Optional

from common.message import Message, MessageQueue
from common.message_const import MessageConst
from common.mix_all import MixAll
from rocketmq_logging import get_logger
from .trace import (AccessChannel, TraceConstants, TraceContext, TraceDataEncoder,
                    TraceTransferBean)

logger = get_logger()


class TraceDispatcherType(Enum):
    pass

    PRODUCE = "PRODUCE"
    CONSUME = "CONSUME"


class _BrokerSetSelector:
    pass

    def __init__(self, counter: "itertools.count"):
        self._counter = counter

    def select(self, mqs: List[MessageQueue], msg: Message, arg) -> MessageQueue:
        broker_set = arg or set()
        filtered = [q for q in mqs if q.broker_name in broker_set]
        if not filtered:
            filtered = list(mqs)
        pos = next(self._counter) % len(filtered)
        return filtered[pos]


class AsyncTraceDispatcher:
    """Async trace dispatcher."""

    _COUNTER = itertools.count(1)
    _INSTANCE_NUM = itertools.count(0)
    WAIT_FOR_SHUTDOWN = 5000
    FLUSH_TRACE_INTERVAL = 5000

    def __init__(self, group: str, type_: TraceDispatcherType, batch_num: int = 10,
                 trace_topic_name: Optional[str] = None, rpc_hook=None) -> None:
        self.batch_num = min(batch_num, 20)
        self.max_msg_size = 128000
        self.trace_instance_id = next(self._INSTANCE_NUM)
        self.group = group
        self.type = type_
        self.trace_context_queue: "queue.Queue[TraceContext]" = queue.Queue(maxsize=2048)
        self.discard_count = 0
        self.stopped = False
        self.is_started = False
        self.worker: Optional[threading.Thread] = None
        self.access_channel = AccessChannel.LOCAL
        self.namespace_v2 = ""
        self.host_producer = None
        self.host_consumer = None
        self._send_which_queue = itertools.count(0)
        self._last_flush_time = int(time.time() * 1000)
        self._lock = threading.RLock()
        self.trace_topic_name = trace_topic_name or MixAll.TRACE_TOPIC
        self._executor = ThreadPoolExecutor(max_workers=4,
                                            thread_name_prefix="MQTraceSendThread_%d_" % self.trace_instance_id)
        self.trace_producer = self._get_and_create_trace_producer(rpc_hook)

    # ---------------- internal producer ----------------
    def _get_and_create_trace_producer(self, rpc_hook):
        from .producer import DefaultMQProducer
        producer = DefaultMQProducer(self._gen_group_name_for_trace(), rpc_hook)
        producer.set_send_msg_timeout(5000)
        producer.set_max_message_size(self.max_msg_size)
        # WARNING: must disable tracing for itself, otherwise trace messages would be traced again -> infinite recursion
        producer.set_enable_trace(False)
        return producer

    def _gen_group_name_for_trace(self) -> str:
        return "%s-%s-%s-%d" % (TraceConstants.GROUP_NAME_PREFIX, self.group,
                                self.type.value, next(self._COUNTER))

    def get_trace_topic_name(self) -> str:
        return self.trace_topic_name

    def set_host_producer(self, host) -> None:
        self.host_producer = host

    def set_host_consumer(self, host) -> None:
        self.host_consumer = host

    def set_namespace_v2(self, namespace_v2: Optional[str]) -> None:
        pass
        self.namespace_v2 = namespace_v2 or ""

    def get_namespace_v2(self) -> Optional[str]:
        return self.namespace_v2

    def _client_id(self) -> str:
        """The host client's clientId (used as clientHost for EndTransaction trace)."""
        host = self.host_producer if self.host_producer is not None else self.host_consumer
        client = getattr(host, "_mq_client", None) if host is not None else None
        return getattr(client, "client_id", "") or "" if client is not None else ""

    # ---------------- lifecycle ----------------
    def start(self, name_srv_addr: str, access_channel: Optional[AccessChannel] = None) -> None:
        with self._lock:
            if not self.is_started:
                self.trace_producer.set_namesrv_addr(name_srv_addr)
                self.trace_producer.set_instance_name(
                    "%s_%s" % (TraceConstants.TRACE_INSTANCE_NAME, name_srv_addr))
                self.trace_producer.set_namespace_v2(self.namespace_v2)
                self.trace_producer.set_enable_trace(False)
                self.trace_producer.start()
                self.is_started = True
        self.access_channel = access_channel or AccessChannel.LOCAL
        if self.worker is None:
            self.stopped = False
            self.worker = threading.Thread(target=self._async_run, daemon=True,
                                           name="MQ-AsyncArrayDispatcher-Thread%d" % self.trace_instance_id)
            self.worker.start()
        atexit.register(self.shutdown)

    def shutdown(self) -> None:
        try:
            self.flush()
        except Exception as e:  # noqa: BLE001
            logger.error("trace dispatcher flush before shutdown failed: %s", e)
        try:
            self._executor.shutdown(wait=False)
        except Exception:  # noqa: BLE001
            pass
        if self.is_started:
            try:
                self.trace_producer.shutdown()
            except Exception as e:  # noqa: BLE001
                logger.debug("trace producer shutdown failed: %s", e)
        try:
            atexit.unregister(self.shutdown)
        except Exception:  # noqa: BLE001
            pass
        self.stopped = True

    # ---------------- enqueue / flush ----------------
    def append(self, ctx) -> bool:
        pass
        try:
            self.trace_context_queue.put_nowait(ctx)
            return True
        except queue.Full:
            self.discard_count += 1
            logger.info("buffer full%d ,context is %s", self.discard_count, ctx)
            return False

    def flush(self) -> None:
        pass
        while not self.trace_context_queue.empty():
            try:
                self._flush_trace_context(True)
            except Exception as e:  # noqa: BLE001
                logger.error("flushTraceContext error: %s", e)

    def _async_run(self) -> None:
        while not self.stopped:
            try:
                self._flush_trace_context(False)
            except Exception as e:  # noqa: BLE001
                logger.error("flushTraceContext error: %s", e)

    def _flush_trace_context(self, force_flush: bool) -> None:
        size = self.trace_context_queue.qsize()
        if size != 0:
            now = int(time.time() * 1000)
            if force_flush or size >= self.batch_num \
                    or (now - self._last_flush_time) > self.FLUSH_TRACE_INTERVAL:
                context_list: List[TraceContext] = []
                for _ in range(self.batch_num):
                    try:
                        context_list.append(self.trace_context_queue.get_nowait())
                    except queue.Empty:
                        break
                self._async_send_trace_message(context_list)
                return
        time.sleep(0.005)

    def _async_send_trace_message(self, context_list: List[TraceContext]) -> None:
        if not context_list:
            return
        self._last_flush_time = int(time.time() * 1000)
        self._executor.submit(self._send_trace_data, context_list)

    # ---------------- send ----------------
    def _send_trace_data(self, context_list: List[TraceContext]) -> None:
        pass
        bean_map = {}
        for context in context_list:
            access_channel = context.access_channel or self.access_channel
            region_id = context.region_id
            if not region_id or not context.trace_beans:
                continue
            if access_channel == AccessChannel.CLOUD:
                trace_topic = TraceConstants.TRACE_TOPIC_PREFIX + region_id
            else:
                trace_topic = self.trace_topic_name
            topic = context.trace_beans[0].topic
            key = topic + TraceConstants.CONTENT_SPLITOR + trace_topic
            bean_map.setdefault(key, []).append(TraceDataEncoder.encoder_from_context_bean(context))
        for key, bean_list in bean_map.items():
            topic, trace_topic = key.split(TraceConstants.CONTENT_SPLITOR)
            self._flush_data(bean_list, topic, trace_topic)

    def _flush_data(self, trans_bean_list: List[TraceTransferBean], topic: str,
                    trace_topic: str) -> None:
        if not trans_bean_list:
            return
        buffer = ""
        key_set = set()
        count = 0
        for bean in trans_bean_list:
            key_set.update(bean.trans_key)
            buffer += bean.trans_data
            count += 1
            if len(buffer) >= self.max_msg_size:
                self._send_trace_data_by_mq(key_set, buffer, trace_topic)
                buffer = ""
                key_set = set()
                count = 0
        if count > 0:
            self._send_trace_data_by_mq(key_set, buffer, trace_topic)
        trans_bean_list.clear()

    def _send_trace_data_by_mq(self, key_set: set, data: str, trace_topic: str) -> None:
        msg = Message(trace_topic, data.encode("utf-8"))
        # keys holds the **original message's msgId** (not offsetMsgId); the console uses it to look up the trace
        msg.set_keys(MessageConst.KEY_SEPARATOR.join(sorted(k for k in key_set if k)))
        try:
            trace_broker_set = self._try_get_message_queue_broker_set(self.trace_producer, trace_topic)
            if not trace_broker_set:
                self.trace_producer.send(msg, 5000)
            else:
                self.trace_producer.send_by_selector(msg, _BrokerSetSelector(self._send_which_queue),
                                                     trace_broker_set, 5000)
        except Exception as e:  # noqa: BLE001
            logger.error("send trace data failed, the traceData is %s: %s", data, e)

    @staticmethod
    def _try_get_message_queue_broker_set(producer, topic: str) -> set:
        pass
        broker_set = set()
        try:
            publish = producer._topic_publish_info(topic)
            for q in publish.msg_queue_list:
                broker_set.add(q.broker_name)
        except Exception as e:  # noqa: BLE001
            logger.debug("tryGetMessageQueueBrokerSet(%s) failed: %s", topic, e)
        return broker_set


__all__ = ["TraceDispatcherType", "AsyncTraceDispatcher"]
