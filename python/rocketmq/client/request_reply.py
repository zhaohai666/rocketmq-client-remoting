# -*- coding: utf-8 -*-
"""Request-Reply（5.x）客户端侧支撑。

对应 Java 的这几个类：
- ``org.apache.rocketmq.client.producer.RequestResponseFuture``
- ``org.apache.rocketmq.client.producer.RequestFutureHolder``
- ``org.apache.rocketmq.client.utils.MessageUtil#createReplyMessage``
- ``org.apache.rocketmq.client.impl.ClientRemotingProcessor#receiveReplyMessage``

协议回顾（照 Java 逐字段复刻）：:

    请求方 (producer.request)                        应答方 (push consumer)
    ─────────────────────────────                    ──────────────────────
    msg.properties[CORRELATION_ID] = uuid
    msg.properties[REPLY_TO_CLIENT] = clientId  ──►  收到请求消息（broker 已写入 CLUSTER）
    msg.properties[TTL] = timeoutMillis               create_reply_message(request_msg, body):
                                                        topic       = <CLUSTER>_REPLY_TOPIC
                                                        CORRELATION_ID / REPLY_TO_CLIENT / TTL 原样带回
                                                        MSG_TYPE    = "reply"
    ◄── PUSH_REPLY_MESSAGE_TO_CLIENT(326) ────────    producer.send(reply) →
        （broker 按 REPLY_TO_CLIENT 找到请求方连接）       broker 走 SEND_REPLY_MESSAGE_V2(325)

两个关键点（错了真机就不通）：
1. 应答消息必须带 ``MSG_TYPE == "reply"``，客户端发送时据此把请求码从
   ``SEND_MESSAGE_V2(310)`` 换成 ``SEND_REPLY_MESSAGE_V2(325)``；
   broker 的 ``ReplyMessageProcessor`` 只在 324/325 上注册。
2. ``REPLY_TO_CLIENT`` 是**请求方的 clientId**，broker 用它在 producerManager 里
   反查 channel 才能把应答推回来 —— 所以请求方必须发过心跳（已注册为 producer）。
"""
from __future__ import annotations

import threading
import time
import uuid
from typing import Callable, Dict, Optional

from ..common.message import Message
from ..common.message_const import MessageConst
from ..common.mix_all import MixAll

# 应答消息的请求码：MSG_TYPE == "reply" 时用这两个（Java MQClientAPIImpl.sendMessage）
# 我们统一用 V2 头，所以取 SEND_REPLY_MESSAGE_V2。

DEFAULT_REQUEST_TIMEOUT_MILLIS = 3000


class RequestCallback:
    """对应 Java ``RequestCallback``。"""

    def on_success(self, response_message: Optional[Message]) -> None:  # pragma: no cover - 接口
        raise NotImplementedError

    def on_exception(self, e: BaseException) -> None:  # pragma: no cover - 接口
        raise NotImplementedError


class RequestResponseFuture:
    """对应 Java ``RequestResponseFuture``：一次 request 的等待槽。

    与 Java 的差异（有意）：Java 额外起了一个 ``scanExpiredRequest`` 定时线程清理超时项；
    本实现由 ``request()`` 的 ``finally`` 保证移除，故不需要后台扫描线程。
    """

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

    # ---------- 等待 / 投递 ----------
    def wait_response_message(self, timeout_millis: int) -> Optional[Message]:
        """对应 Java ``waitResponseMessage``：等 latch（超时返回 None）。"""
        self._event.wait(max(0, timeout_millis) / 1000.0)
        return self.response_msg

    def put_response_message(self, response_msg: Optional[Message]) -> None:
        """对应 Java ``putResponseMessage``（会 countDown latch，允许多次调用）。"""
        self.response_msg = response_msg
        self._event.set()

    def is_timeout(self) -> bool:
        return int(time.time() * 1000) - self.begin_timestamp > self.timeout_millis

    def execute_request_callback(self) -> None:
        """对应 Java ``executeRequestCallback``：回调只允许触发一次。"""
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
    """对应 Java ``RequestFutureHolder``：correlationId → 等待槽 的全局表。

    Java 里是**跨 producer 共享的单例**（``RequestFutureHolder.getInstance()``），
    因为应答由 clientId 级别的 remoting 通道推回，与具体 producer 实例无关。
    """

    def __init__(self) -> None:
        self.request_future_table: Dict[str, RequestResponseFuture] = {}
        self._lock = threading.Lock()

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
        """接收侧入口（对齐 Java ``processReplyMessage``）：投递应答。

        Java 在这里做的是 **``getRequestFutureTable().remove(correlationId)``** ——
        用「谁摘到谁负责」保证「应答到达」与「超时清理」两条路径只会有一个生效。
        本实现照搬这个语义。

        返回被填充的 future；查不到（已超时/已移除）时返回 None，调用方据此只记日志。
        """
        future = self.remove_request(correlation_id)
        if future is None:
            return None
        future.put_response_message(response_msg)
        # 对齐 Java：成功路径也走 executeRequestCallback，让「只回调一次」的守卫生效；
        # 同步调用方（callback 为空）靠 put_response_message 唤醒。
        future.execute_request_callback()
        return future


#: 进程内单例（对齐 Java 的 ``INSTANCE``）。应答方与请求方在同一进程时也共用它。
REQUEST_FUTURE_HOLDER = RequestFutureHolder()


def create_correlation_id() -> str:
    """对应 Java ``CorrelationIdUtil.createCorrelationId``（随机 UUID 字符串）。"""
    return str(uuid.uuid4())


def create_reply_message(request_message: Message, body: bytes) -> Message:
    """对应 Java ``MessageUtil.createReplyMessage``：由请求消息派生出应答消息。

    ``CLUSTER`` 属性由 **broker** 在投递时写入（``SendMessageProcessor``），
    拿不到就说明这条消息不是经 broker 转发过来的（或 topic 配得不对），
    与 Java 一样直接抛错，而不是造一条投不出去的应答。
    """
    if request_message is None:
        raise ValueError("create reply message fail, requestMessage cannot be null.")
    cluster = request_message.get_property(MessageConst.PROPERTY_CLUSTER)
    if not cluster:
        raise ValueError(
            "create reply message fail, requestMessage error, property[%s] is null."
            % MessageConst.PROPERTY_CLUSTER)
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
    """MSG_TYPE == "reply" 的发送要走 SEND_REPLY_MESSAGE_V2(325)。"""
    return msg.get_property(MessageConst.PROPERTY_MESSAGE_TYPE) == MixAll.REPLY_MESSAGE_FLAG


def react_callback(callback: Optional[Callable[[Optional[Message], Optional[BaseException]], None]],
                   future: RequestResponseFuture) -> None:
    """（可选）把回调适配成 ``(response, cause)`` 形式，便于脚本使用。"""
    if callback is None:
        return
    callback(future.response_msg, future.cause)
