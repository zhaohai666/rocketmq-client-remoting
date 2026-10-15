# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Callable, Dict, Optional

from common.message import Message
from common.message_const import MessageConst
from common.mix_all import MixAll
from .exception import ClientErrorCode, MQClientException, RequestTimeoutException

logger = logging.getLogger(__name__)


DEFAULT_REQUEST_TIMEOUT_MILLIS = 3000


class RequestCallback:
    pass

    def on_success(self, response_message: Optional[Message]) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def on_exception(self, e: BaseException) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class RequestResponseFuture:
    pass

    def __init__(self, correlation_id: str, timeout_millis: int,
                 request_callback: Optional[RequestCallback] = None):
        self.correlation_id = correlation_id
        self.timeout_millis = timeout_millis
        self.request_callback = request_callback
        self.begin_timestamp = int(time.time() * 1000)
        self.response_msg: Optional[Message] = None
        self.send_request_ok: bool = True
        self.cause: Optional[BaseException] = None
        self._event = threading.Event()
        self._callback_once = threading.Lock()
        self._callback_fired = False

    # ---------- Wait / dispatch ----------
    def wait_response_message(self, timeout_millis: int) -> Optional[Message]:
        pass
        self._event.wait(max(0, timeout_millis) / 1000.0)
        return self.response_msg

    def put_response_message(self, response_msg: Optional[Message]) -> None:
        pass
        self.response_msg = response_msg
        self._event.set()

    def is_timeout(self) -> bool:
        return int(time.time() * 1000) - self.begin_timestamp > self.timeout_millis

    def execute_request_callback(self) -> None:
        pass
        if self.request_callback is None:
            return
        with self._callback_once:
            if self._callback_fired:
                return
            self._callback_fired = True
        if self.send_request_ok and self.cause is None:
            self.request_callback.on_success(self.response_msg)
        else:
            self.request_callback.on_exception(self.cause)


class RequestFutureHolder:
    pass

    def __init__(self) -> None:
        self.request_future_table: Dict[str, RequestResponseFuture] = {}
        self._lock = threading.Lock()
        self._producers: set = set()
        self._sweep_thread: Optional[threading.Thread] = None
        self._sweep_stop = threading.Event()

    def put_request(self, correlation_id: str, future: RequestResponseFuture) -> None:
        with self._lock:
            self.request_future_table[correlation_id] = future

    def get_request(self, correlation_id: str) -> Optional[RequestResponseFuture]:
        with self._lock:
            return self.request_future_table.get(correlation_id)

    def remove_request(self, correlation_id: str) -> Optional[RequestResponseFuture]:
        with self._lock:
            return self.request_future_table.pop(correlation_id, None)

    def put_response(self, correlation_id: str, response_msg: Optional[Message]) -> Optional[RequestResponseFuture]:
        pass
        future = self.remove_request(correlation_id)
        if future is None:
            return None
        future.put_response_message(response_msg)
        future.execute_request_callback()
        return future

    def scan_expired_request(self) -> int:
        pass
        expired: list = []
        with self._lock:
            for key in list(self.request_future_table.keys()):
                future = self.request_future_table.get(key)
                if future is not None and future.is_timeout():
                    removed = self.request_future_table.pop(key, None)
                    if removed is not None:
                        expired.append(removed)
                        logger.warning("remove timeout request, CorrelationId=%s", removed.correlation_id)
        for future in expired:
            try:
                future.cause = RequestTimeoutException(
                    ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION, "request timeout, no reply message.")
                future.execute_request_callback()
            except Exception:
                logger.warning("scanResponseTable, operationComplete Exception", exc_info=True)
        return len(expired)

    def start_scheduled_task(self, producer=None) -> None:
        pass
        with self._lock:
            self._producers.add(producer)
            if self._sweep_thread is not None and self._sweep_thread.is_alive():
                return
            self._sweep_stop.clear()

            def _run() -> None:
                if self._sweep_stop.wait(3.0):
                    return
                while not self._sweep_stop.wait(1.0):
                    try:
                        self.scan_expired_request()
                    except Exception:
                        logger.error("scan RequestFutureTable exception", exc_info=True)

            self._sweep_thread = threading.Thread(target=_run, name="RequestHouseKeepingService", daemon=True)
            self._sweep_thread.start()

    def shutdown_scheduled_task(self, producer=None) -> None:
        pass
        with self._lock:
            self._producers.discard(producer)
            if self._producers or self._sweep_thread is None:
                return
            thread = self._sweep_thread
            self._sweep_thread = None
            stop = self._sweep_stop
        stop.set()


REQUEST_FUTURE_HOLDER = RequestFutureHolder()


def create_correlation_id() -> str:
    pass
    return str(uuid.uuid4())


def create_reply_message(request_message: Message, body: bytes) -> Message:
    pass
    if request_message is None:
        raise MQClientException("create reply message fail, requestMessage cannot be null.",
                                ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION)
    cluster = request_message.get_property(MessageConst.PROPERTY_CLUSTER)
    if not cluster:
        raise MQClientException(
            "create reply message fail, requestMessage error, property[%s] is null."
            % MessageConst.PROPERTY_CLUSTER,
            ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION)
    reply = Message()
    reply.topic = MixAll.get_reply_topic(cluster)
    reply.set_body(body)
    reply.put_property(MessageConst.PROPERTY_MESSAGE_TYPE, MixAll.REPLY_MESSAGE_FLAG)
    reply.put_property(MessageConst.PROPERTY_CORRELATION_ID,
                       request_message.get_property(MessageConst.PROPERTY_CORRELATION_ID))
    reply.put_property(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT,
                       request_message.get_property(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT))
    reply.put_property(MessageConst.PROPERTY_MESSAGE_TTL,
                       request_message.get_property(MessageConst.PROPERTY_MESSAGE_TTL))
    return reply


def is_reply_message(msg: Message) -> bool:
    """A message with MSG_TYPE == "reply" must be sent via SEND_REPLY_MESSAGE_V2 (325)."""
    return msg.get_property(MessageConst.PROPERTY_MESSAGE_TYPE) == MixAll.REPLY_MESSAGE_FLAG


def react_callback(callback: Optional[Callable[[Optional[Message], Optional[BaseException]], None]],
                   future: RequestResponseFuture) -> None:
    """(Optional) Adapt a callback into the ``(response, cause)`` form, convenient for scripts."""
    if callback is None:
        return
    callback(future.response_msg, future.cause)
