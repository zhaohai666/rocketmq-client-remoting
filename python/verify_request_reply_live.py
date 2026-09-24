#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Request-Reply（5.x）真机验证。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_request_reply_live.py 127.0.0.1:9876

链路（三侧 C++/.NET/Python 同一套场景）：

    请求方 producer.request(msg, timeout)               应答方 push consumer
    ────────────────────────────────────              ─────────────────────
    CORRELATION_ID = uuid
    REPLY_TO_CLIENT = <本客户端 clientId>          ──► 收到请求消息（broker 已写 CLUSTER）
    TTL = timeout                                      create_reply_message(request_msg, body)
                                                         → topic = <CLUSTER>_REPLY_TOPIC
                                                         → MSG_TYPE = "reply"
    ◄── PUSH_REPLY_MESSAGE_TO_CLIENT(326) ─────────       producer.send(reply) → 325
        由 broker 按 REPLY_TO_CLIENT 找到请求方连接推回

场景：
  S1 只建请求 topic；断言客户端 **不能** 建 <cluster>_REPLY_TOPIC（broker 的系统 topic），
     并确认请求方已心跳注册（REPLY_TO_CLIENT 要靠它反查 channel）
  S2 请求消息确实带上了 CORRELATION_ID / REPLY_TO_CLIENT / TTL 三个属性
  S3 request() 拿回应答：body 正确、topic 是 <cluster>_REPLY_TOPIC、带 REPLY_MESSAGE_ARRIVE_TIME
  S4 应答确实走了 SEND_REPLY_MESSAGE_V2(325)：第三个客户端订阅 <cluster>_REPLY_TOPIC 能看到该消息
     （broker 的 storeReplyMessageEnable 默认 true，应答会落盘 → 可被独立订阅证明）
  S5 无应答方时 request() 在 timeout 附近抛 RequestTimeoutException（而不是卡死/静默返回），
     并且带上 Java 的 10006 ``REQUEST_TIMEOUT_EXCEPTION`` —— 只有类型没有码不够：调用方按
     ``response_code`` 分流时，"消息已发出去、只是没等到应答"和别的客户端故障是两类处置
  S6 并发 3 个 request：每个拿到的都是自己的应答（CORRELATION_ID 不串台）
  S7 应答方（消费者）本身是普通 push 消费者：Reply 流量不影响后续普通消费
  S8 CLUSTER 属性由 broker 在存储时写入（``SendMessageProcessor``）：投递到的消息有、
     客户端手里那份没有 —— 这正是 ``MessageUtil.createReplyMessage`` 存在意义；用错对象
     （拿本地那份发请求）必须撞上带 10007 ``CREATE_REPLY_MESSAGE_EXCEPTION`` 的
     MQClientException，文案点到缺失的 CLUSTER，而不是裸 ValueError

⚠ 两个真机必踩点：
   1) <cluster>_REPLY_TOPIC 是 broker 启动时注册的**系统 topic**
      （TopicConfigManager.init() → addSystemTopic），客户端 create_topic 它会被
      INVALID_PARAMETER(29)「conflict with system topic」拒绝，且 broker 侧**不留日志**。
      不要去"预建"它 —— 路由本来就在。
   2) 消费者一律用 CONSUME_FROM_FIRST_OFFSET，避免默认 CONSUME_FROM_LAST_OFFSET 带来的
      时序敏感（见项目 MEMORY 里的第 8 条硬性约定）。
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import (ConsumeConcurrentlyStatus, DefaultMQPushConsumer,
                                      SimpleMessageListener)
from rocketmq.client.exception import (ClientErrorCode, MQClientException,
                                        RequestTimeoutException)
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.request_reply import create_reply_message
from rocketmq.common.message import Message
from rocketmq.common.message_const import MessageConst
from rocketmq.common.mix_all import MixAll
from rocketmq.remoting.protocol.codes import ResponseCode
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "RequestReply_%d" % STAMP
REQUEST_GROUP = "PG_RRReq_%d" % STAMP
CONSUMER_GROUP = "CG_RRReply_%d" % STAMP
WATCH_GROUP = "CG_RRWatch_%d" % STAMP
QUEUE_NUM = 4
# broker.conf 里写的是 brokerClusterName=DefaultCluster（见 /tmp/run_*_live.sh）
CLUSTER = "DefaultCluster"
REPLY_TOPIC = MixAll.get_reply_topic(CLUSTER)
REQUEST_TIMEOUT = 8000

PASS = 0
FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print("  [PASS] %s%s" % (name, ("  " + detail) if detail else ""))
    else:
        FAIL += 1
        print("  [FAIL] %s%s" % (name, ("  " + detail) if detail else ""))


def broker_error(exc: BaseException):
    """从包装异常里取出 broker 的原始错误（``MQBrokerException``）。

    ``create_topic_in_route`` 会把「逐个 broker 都失败」包成
    ``MQClientException("create new topic failed", cause=...)``，而 ``str()`` **看不到**
    broker 的 code/remark —— 必须顺着 ``cause`` 链取，否则真机排障只能靠猜。
    """
    seen = set()
    cur = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if hasattr(cur, "error_message"):
            return cur
        cur = getattr(cur, "cause", None) or getattr(cur, "__cause__", None)
    return None


def new_producer(group: str, instance: str) -> DefaultMQProducer:
    """instance_name 必须各不相同。

    clientId 对齐 Java ``ClientConfig#buildMQClientId`` = ``<本机IP>@<instanceName>``
    （没有秒级时间戳兜底），所以同一台机器上只有 instanceName 不同才能保证 clientId 不同；
    而 REPLY_TO_CLIENT 用的就是 clientId，撞名会让 broker 把应答推到错误的那条连接上。
    """
    p = DefaultMQProducer(group)
    p.set_namesrv_addr(NAMESRV)
    p.set_instance_name(instance)
    p.start()
    return p


def new_consumer(group: str, topic: str, instance: str, on_msgs) -> DefaultMQPushConsumer:
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_instance_name(instance)
    # 从最早开始消费：本脚本的主题都是全新且消息在消费者启动前后不长的时间里产生，
    # 用 LAST 会让"谁先解析位点"决定结果（项目 MEMORY 第 8 条）。
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(topic, "*")
    c.set_message_listener(SimpleMessageListener(on_msgs))
    c.start()
    return c


def wait_for(pred, timeout_s: float) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.2)
    return False


class Replier:
    """应答方：普通 push 消费者 + 用来发应答的生产者。

    这就是 Java 文档里 Request-Reply 的标准写法：消费者收到请求后
    ``MessageUtil.createReplyMessage`` + ``producer.send(reply)``。
    """

    def __init__(self) -> None:
        self.received: list = []          # (body, correlation_id, reply_to, ttl)
        self.delivered: list = []         # broker 投递的原始消息（S8 要用它验 CLUSTER）
        self.replied: list = []
        self.reply_errors: list = []
        self.lock = threading.Lock()
        self.producer = new_producer("PG_RRReplier_%d" % STAMP, "RRReply")
        self.consumer = new_consumer(CONSUMER_GROUP, TOPIC, "RRConsumer", self._on_messages)

    def shutdown(self) -> None:
        try:
            self.consumer.shutdown()
        finally:
            self.producer.shutdown()

    def _on_messages(self, msgs) -> ConsumeConcurrentlyStatus:
        for m in msgs:
            body = bytes(m.body)
            with self.lock:
                self.delivered.append(m)
                self.received.append((
                    body,
                    m.get_property(MessageConst.PROPERTY_CORRELATION_ID),
                    m.get_property(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT),
                    m.get_property(MessageConst.PROPERTY_MESSAGE_TTL),
                ))
            try:
                reply = create_reply_message(m, b"reply:" + body)
                self.producer.send(reply)
                with self.lock:
                    self.replied.append(body)
            except BaseException as e:  # noqa: BLE001
                with self.lock:
                    self.reply_errors.append("%s: %s" % (type(e).__name__, e))
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS


def main() -> int:
    print("== Request-Reply 真机验证  namesrv=%s  topic=%s ==" % (NAMESRV, TOPIC))
    print("   应答 topic = %s" % REPLY_TOPIC)

    print("\nS1 建请求 topic；应答 topic 由 broker 预注册（客户端不能建）")
    prep = new_producer("PG_RRPrep_%d" % STAMP, "RRPrep")
    try:
        prep.create_topic("TBW102", TOPIC, QUEUE_NUM)
        # <cluster>_REPLY_TOPIC **不该也不能由客户端创建**：broker 启动时
        # TopicConfigManager.init() 已把它注册成系统 topic
        # （clusterName + "_" + MixAll.REPLY_TOPIC_POSTFIX，并 addSystemTopic），
        # validateSystemTopicWhenUpdateTopic 默认 true ⇒ 客户端 createTopic 会拿到
        # INVALID_PARAMETER(29)「conflict with system topic」。该分支在 broker 侧
        # **没有日志**，只能从客户端异常看出来，所以顺手把「错误传播是准的」也验了。
        try:
            prep.create_topic("TBW102", REPLY_TOPIC, QUEUE_NUM)
            check("S1 客户端 create_topic(应答 topic) 被 broker 拒绝", False, "竟然建成功了")
        except BaseException as e:  # noqa: BLE001
            be = broker_error(e)
            check("S1 客户端 create_topic(应答 topic) 被拒（broker 系统 topic）",
                  be is not None and be.response_code == ResponseCode.INVALID_PARAMETER
                  and "system topic" in (be.error_message or ""),
                  "code=%s remark=%s" % (getattr(be, "response_code", None),
                                         getattr(be, "error_message", None)))
    finally:
        prep.shutdown()

    requester = new_producer(REQUEST_GROUP, "RRReq")
    # 请求方必须**先被 broker 登记**（心跳）才能被 REPLY_TO_CLIENT 反查到 channel，
    # 否则 broker 的 ReplyMessageProcessor 找不到 channel，回给应答方 SYSTEM_ERROR。
    # 注意 _send_heartbeat_to_all_broker 遍历的是**本地路由表**，路由表空时它必然返回 0，
    # 所以要先取一次路由（request() 内部也是这个顺序）。
    requester._require_client().update_topic_route_info_from_name_server(TOPIC)
    check("S1 请求方已注册到 broker（心跳送达）",
          wait_for(lambda: requester._send_heartbeat_to_all_broker() > 0, 20), "")

    replier = Replier()
    # watcher：独立订阅 <cluster>_REPLY_TOPIC，是 S4 的硬证据
    watched: list = []
    watcher = new_consumer(WATCH_GROUP, REPLY_TOPIC, "RRWatch",
                           lambda msgs: _collect(watched, msgs))
    check("S1 应答消费者已启动", replier.consumer is not None)
    try:
        # ---------------- S2/S3 一次完整往返 ----------------
        print("\nS2/S3 一次完整 request → reply 往返")
        msg = Message(TOPIC, b"ping-1")
        msg.set_keys("rr")
        reply = requester.request(msg, REQUEST_TIMEOUT)

        want = b"reply:ping-1"
        check("S3 request() 拿到应答且 body 正确", bytes(reply.body) == want,
              "got=%r want=%r" % (bytes(reply.body), want))
        check("S3 应答 topic 是 <cluster>_REPLY_TOPIC", reply.topic == REPLY_TOPIC,
              "topic=%s" % reply.topic)
        arrive = reply.get_property(MessageConst.PROPERTY_REPLY_MESSAGE_ARRIVE_TIME)
        check("S3 应答带 REPLY_MESSAGE_ARRIVE_TIME（客户端收到时打的戳）",
              arrive is not None and arrive.isdigit(), "value=%s" % arrive)

        ok = wait_for(lambda: len(replier.received) >= 1, 15)
        check("S2 应答方收到请求消息", ok, "received=%d" % len(replier.received))
        if ok:
            _, corr, reply_to, ttl = replier.received[0]
            check("S2 请求消息带 CORRELATION_ID（uuid 字符串）",
                  bool(corr) and len(corr) == 36, "corr=%s" % corr)
            check("S2 请求消息带 REPLY_TO_CLIENT=请求方 clientId",
                  reply_to == requester._require_client().client_id,
                  "reply_to=%s want=%s" % (reply_to, requester._require_client().client_id))
            check("S2 请求消息带 TTL=timeout", ttl == str(REQUEST_TIMEOUT), "ttl=%s" % ttl)

        # ---------------- S4 应答确实走了 325 且落到了 REPLY_TOPIC ----------------
        print("\nS4 应答走 SEND_REPLY_MESSAGE_V2(325) 且落到 <cluster>_REPLY_TOPIC")
        check("S4 应答方成功发出了应答消息",
              wait_for(lambda: len(replier.replied) >= 1, 15),
              "replied=%s errors=%s" % (replier.replied, replier.reply_errors))
        seen = wait_for(lambda: b"reply:ping-1" in watched, 15)
        check("S4 订阅 %s 的独立消费者能看到该应答" % REPLY_TOPIC, seen,
              "watched=%s" % watched)

        # ---------------- S5 无应答方 → RequestTimeoutException ----------------
        print("\nS5 无应答方时 request() 抛 RequestTimeoutException")
        silent_topic = TOPIC + "_NoReplier"
        prep2 = new_producer("PG_RRPrep2_%d" % STAMP, "RRPrep2")
        try:
            prep2.create_topic("TBW102", silent_topic, QUEUE_NUM)
        finally:
            prep2.shutdown()
        t0 = time.time()
        raised = None
        try:
            silent = Message(silent_topic, b"nobody-home")
            silent.set_keys("rr-silent")
            requester.request(silent, 3000)
        except BaseException as e:  # noqa: BLE001
            raised = e
        elapsed = time.time() - t0
        check("S5 抛的是 RequestTimeoutException（不是静默返回/别的异常）",
              isinstance(raised, RequestTimeoutException),
              "raised=%s: %s" % (type(raised).__name__, raised))
        # 只有类型不够：Java 的 waitResponse 抛的是 RequestTimeoutException(10006, ...)，
        # 调用方按 response_code 分流时才知道"消息已发出去、只是没等到应答"（对方可能只是慢），
        # 这跟"发送本身失败"是两类处置。
        check("S5 异常带上 Java 的 10006 REQUEST_TIMEOUT_EXCEPTION",
              getattr(raised, "response_code", None) == ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
              "code=%s" % getattr(raised, "response_code", None))
        check("S5 超时时长接近设定值（2s ~ 12s，说明真的等了而不是立即失败）",
              2.0 <= elapsed <= 12.0, "elapsed=%.2fs" % elapsed)

        # ---------------- S6 并发请求不串台 ----------------
        print("\nS6 并发 3 个 request，各自拿到自己的应答")
        results: dict = {}
        errors: list = []

        def one(i: int) -> None:
            try:
                m = Message(TOPIC, ("ping-conc-%d" % i).encode())
                m.set_keys("rr-conc")
                r = requester.request(m, REQUEST_TIMEOUT)
                results[i] = bytes(r.body)
            except BaseException as e:  # noqa: BLE001
                errors.append("i=%d %s: %s" % (i, type(e).__name__, e))

        threads = [threading.Thread(target=one, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(REQUEST_TIMEOUT + 5000)
        expect = {i: ("reply:ping-conc-%d" % i).encode() for i in range(3)}
        check("S6 3 个并发 request 全部拿到应答且一一对应",
              results == expect and not errors,
              "results=%s errors=%s" % (results, errors))

        # ---------------- S7 消费者未被 Reply 流量破坏 ----------------
        print("\nS7 应答方的消费者仍然完好（普通 push 消费不受 Reply 影响）")
        plain = Message(TOPIC, b"plain-after-replies")
        plain.set_keys("rr-plain")
        requester.send(plain)
        ok = wait_for(lambda: any(b == b"plain-after-replies" for b, _, _, _ in replier.received),
                      15)
        check("S7 后续普通消息仍被消费", ok,
              "received=%s" % [b for b, _, _, _ in replier.received])

        # ---------------- S8 CLUSTER 属性来自 broker；拿不到时是 10007 ----------------
        # Java ``MessageUtil.createReplyMessage``（:46/49）抛的是
        # ``MQClientException(ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION=10007, ...)``。
        # 这一条既要验"错误码带上了"，也要验它**为什么**存在：CLUSTER 是 broker 在存储时
        # 补的（``SendMessageProcessor``），客户端手里那份消息永远没有 —— 所以应答必须
        # 建立在**投递到的**那条消息上，用错对象就会撞上 10007。
        print("\nS8 CLUSTER 由 broker 写入；造不出应答时报 10007 而不是别的错")
        delivered = None
        with replier.lock:
            for m in replier.delivered:
                if bytes(m.body) == b"ping-1":
                    delivered = m
                    break
        check("S8 投递到的请求消息带 broker 写入的 CLUSTER=%s" % CLUSTER,
              delivered is not None
              and delivered.get_property(MessageConst.PROPERTY_CLUSTER) == CLUSTER,
              "cluster=%s" % (delivered.get_property(MessageConst.PROPERTY_CLUSTER)
                              if delivered else None))
        check("S8 客户端手里那份请求消息**没有** CLUSTER（属性确实是 broker 补的）",
              msg.get_property(MessageConst.PROPERTY_CLUSTER) is None,
              "value=%s" % msg.get_property(MessageConst.PROPERTY_CLUSTER))
        # 用错对象（拿本地那份发请求）→ 必须撞 10007，且文案指到 CLUSTER
        raised8 = None
        try:
            create_reply_message(msg, b"pong")
        except BaseException as e:  # noqa: BLE001
            raised8 = e
        check("S8 拿本地那份请求消息造应答 → MQClientException 带 10007",
              isinstance(raised8, MQClientException)
              and raised8.response_code == ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION,
              "raised=%s code=%s" % (type(raised8).__name__ if raised8 else None,
                                     getattr(raised8, "response_code", None)))
        check("S8 10007 的文案点到缺失的 CLUSTER 属性（Java 原文）",
              raised8 is not None
              and "property[%s] is null." % MessageConst.PROPERTY_CLUSTER in str(raised8),
              "msg=%s" % raised8)
        raised8b = None
        try:
            create_reply_message(None, b"pong")
        except BaseException as e:  # noqa: BLE001
            raised8b = e
        check("S8 请求消息为 None → 同样是 10007（不是裸 ValueError）",
              isinstance(raised8b, MQClientException)
              and raised8b.response_code == ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION
              and "requestMessage cannot be null." in str(raised8b),
              "raised=%s code=%s msg=%s" % (type(raised8b).__name__ if raised8b else None,
                                            getattr(raised8b, "response_code", None), raised8b))
    finally:
        requester.shutdown()
        replier.shutdown()
        watcher.shutdown()

    print("\n== 结果: PASS=%d FAIL=%d ==" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


def _collect(got: list, msgs) -> ConsumeConcurrentlyStatus:
    for m in msgs:
        got.append(bytes(m.body))
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS


if __name__ == "__main__":
    sys.exit(main())
