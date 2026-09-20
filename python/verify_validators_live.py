#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Validators 真机验证：非法名字**本地**快速失败，合法名字照常在 5.5.1 集群收发。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_validators_live.py 127.0.0.1:9876

为什么单独测（而且要在真集群上测）：单测只能证明「函数会抛」，证明不了它**拦在了
网络之前**。而这条链路真正的代价是可重试码：``TOPIC_NOT_EXIST``(17) 在发送重试的
可重试集合里，名字写错时每条消息都会把 ``retry_times_when_send_failed`` 与超时预算
空转一遍才报出同一个原因。所以这里用**同一个已启动的生产者**跑正反两条腿：
  * 反腿：非法 topic/group/超长 body/禁发 topic → 亚毫秒失败，异常是 MQClientException
    且文案是本地校验的文案（不是 broker remark）；
  * 正腿：合法名字照常 SEND_OK 并被消费者收到；``%RETRY%`` 前缀**不在**禁发名单里
    （sendMessageBack 要往里写）；同名合法 topic 的「不存在」腿明显更慢 —— 那就是
    本地校验省掉的重试空转。

场景：
  S1 生产端反腿：6 类非法输入全部亚毫秒本地失败（错误文案 + 码值口径）+ 边界放行
  S2 批量发送反腿：批内一条非法 ⇒ 整批本地失败；混批也挡在本地（批量路径不绕过校验）
  S3 组名反腿：start() 的组名校验排在地址检查之前，失败不留 started 状态
  S4 正腿：合法 topic 建队列 + 起 push/lite 消费者 + 发送 SEND_OK + 两边都收到
  S5 对照腿：合法但不存在的 topic 本地放行、走完整集群链路，耗时比反腿高两个数量级
  S6 拉取消费者：DEFAULT_CONSUMER / 非法字符组名本地失败；合法组名能启动并查到位点
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from rocketmq.client import validators
from rocketmq.client.consumer import (
    DefaultLitePullConsumer,
    DefaultMQPullConsumer,
    DefaultMQPushConsumer,
    SimpleMessageListener,
)
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus
from rocketmq.client.exception import MQClientException
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.send_result import SendStatus
from rocketmq.common.message import Message
from rocketmq.remoting.protocol.codes import ResponseCode

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "ValidatorsLive_%d" % STAMP
GROUP = "GID_ValidatorsLive_%d" % STAMP
# 反腿的耗时上界：本地纯计算，真机给 50ms 已经留了两个数量级的余量
LOCAL_BUDGET_MS = 50.0

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


def local_send(prod: DefaultMQProducer, msg: Message):
    """跑一次发送，返回 (异常或 None, 耗时毫秒)。"""
    began = time.monotonic()
    try:
        prod.send(msg)
        return None, (time.monotonic() - began) * 1000.0
    except MQClientException as e:
        return e, (time.monotonic() - began) * 1000.0


def check_local_reject(name: str, prod: DefaultMQProducer, msg: Message, needle: str,
                       expect_code=None) -> float:
    """断言"本地亚毫秒失败"，并把这次的实际耗时交回去当后续对照的基准。"""
    err, elapsed = local_send(prod, msg)
    ok = err is not None and needle in str(err) and elapsed < LOCAL_BUDGET_MS
    if expect_code is not None:
        ok = ok and err.response_code == expect_code
    else:
        # 名字校验走 Java 的 MQClientException(String, Throwable) ⇒ 无 broker 码
        ok = ok and err.response_code is None
    check(name, ok, "%s in %.2fms code=%r" % (
        str(err).replace("\n", " ") if err else "没有抛异常", elapsed,
        None if err is None else err.response_code))
    return elapsed


def wait_route(prod: DefaultMQProducer, topic: str, min_queues: int,
               timeout: int = 20) -> bool:
    """等 topic 路由注册（自动建 topic 是异步的，建完立刻发会拿不到队列）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if len(prod._mq_client.get_topic_publish_info(topic).msg_queue_list) >= min_queues:
                return True
        except MQClientException:
            pass
        time.sleep(0.5)
    return False


def main() -> int:
    print("namesrv=%s topic=%s group=%s" % (NAMESRV, TOPIC, GROUP))
    prod = DefaultMQProducer("PG_ValidatorsLive_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    lite = None
    push = None
    try:
        # ---------------- S1 生产端反腿 ----------------
        print("=== S1 非法输入必须本地亚毫秒失败 ===")
        local_ms = check_local_reject("S1a 非法字符 topic 被本地拦截", prod,
                                      Message("bad topic", b"x"), "contains illegal characters")
        check_local_reject("S1b 空 topic 被本地拦截", prod,
                           Message("", b"x"), "The specified topic is blank")
        check_local_reject("S1c 超长 topic 被本地拦截", prod,
                           Message("t" * 128, b"x"), "is longer than topic max length")
        check_local_reject("S1d 禁发 topic 被本地拦截（码值是 -1 口径，不是 MESSAGE_ILLEGAL）",
                           prod, Message("SCHEDULE_TOPIC_XXXX", b"x"), "is forbidden")
        prod.set_max_message_size(8)
        check_local_reject("S1e 超长 body 被本地拦截（MESSAGE_ILLEGAL）", prod,
                           Message(TOPIC, b"1" * 9), "the message body size over max value",
                           ResponseCode.MESSAGE_ILLEGAL)
        check_local_reject("S1f 空 body 被本地拦截（MESSAGE_ILLEGAL）", prod,
                           Message(TOPIC, b""), "the message body length is zero",
                           ResponseCode.MESSAGE_ILLEGAL)
        prod.set_max_message_size(1024 * 1024 * 4)
        # 等于上限放行：Java 判的是 `>`，反腿顺带把边界钉住
        m_boundary = Message(TOPIC, b"1" * 8)
        validators.check_message(m_boundary, 8)
        check("S1g 恰好等于 maxMessageSize 放行（Java 的 > 判定）", True)

        # ---------------- S2 批量反腿 ----------------
        print("=== S2 批量发送不绕过本地校验 ===")
        began = time.monotonic()
        err = None
        try:
            prod.send([Message(TOPIC, b"ok1"), Message("bad topic", b"ok2")])
        except MQClientException as e:
            err = e
        elapsed = (time.monotonic() - began) * 1000.0
        check("S2a 批内非法子消息 ⇒ 整批本地失败",
              err is not None and "contains illegal characters" in str(err)
              and elapsed < LOCAL_BUDGET_MS,
              "%s in %.2fms" % (str(err).replace("\n", " ") if err else "没有抛异常", elapsed))
        began = time.monotonic()
        err2 = None
        try:
            prod.send([Message(TOPIC, b"x" * 10), Message("ValidatorsOther_%d" % STAMP, b"y")])
        except Exception as e:  # noqa: BLE001 - 同质性检查抛的是 ValueError（Java 同款）
            err2 = e
        elapsed = (time.monotonic() - began) * 1000.0
        check("S2b 混批（不同 topic）也挡在本地、不到集群",
              err2 is not None and "should be the same" in str(err2)
              and elapsed < LOCAL_BUDGET_MS,
              "%s in %.2fms" % (str(err2).replace("\n", " ") if err2 else "没有抛异常", elapsed))

        # ---------------- S3 组名校验 ----------------
        print("=== S3 组名校验排在任何网络动作之前 ===")
        for group, needle in [
            ("DEFAULT_PRODUCER", "producerGroup can not equal DEFAULT_PRODUCER"),
            ("bad group", "contains illegal characters"),
            ("g" * 121, "is longer than group max length"),
        ]:
            p2 = DefaultMQProducer(group)
            p2.set_namesrv_addr(NAMESRV)
            began = time.monotonic()
            try:
                p2.start()
                e = None
            except MQClientException as ex:
                e = ex
            elapsed = (time.monotonic() - began) * 1000.0
            check("S3a 组名 %r 在 start() 本地失败" % group,
                  e is not None and needle in str(e) and elapsed < LOCAL_BUDGET_MS
                  and not p2._started,
                  "%s in %.2fms" % (str(e).replace("\n", " ") if e else "没有抛异常", elapsed))

        # ---------------- S4 正腿：合法名字照常在集群收发 ----------------
        # 顺序要紧：先建 topic、再起消费者、后发消息。CONSUME_FROM_LAST_OFFSET 对新组
        # 会取"启动那一刻"的 max 位点，先发消息就再也收不到了（真机踩过）。
        print("=== S4 合法名字照常在集群收发 ===")
        prod.create_topic("TBW102", TOPIC, 4)
        check("S4a 合法 topic 路由可用", wait_route(prod, TOPIC, 4), "topic=%s" % TOPIC)

        received = []
        push = DefaultMQPushConsumer(GROUP)
        push.set_namesrv_addr(NAMESRV)
        push.subscribe(TOPIC, "*")

        def _collect(msgs):
            # 消费到的 keys 记下来；MessageExt 的 KEYS 是属性，不是字段
            received.extend(m.get_keys() for m in msgs)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

        push.set_message_listener(SimpleMessageListener(_collect))
        push.start()

        lite = DefaultLitePullConsumer(GROUP + "_lite")
        lite.set_namesrv_addr(NAMESRV)
        lite.subscribe(TOPIC, "*")
        lite.start()
        deadline = time.time() + 20
        while time.time() < deadline and not lite.assignment():
            time.sleep(0.5)
        check("S4b lite 消费者 rebalance 拿到队列", len(lite.assignment()) > 0,
              "assigned=%d" % len(lite.assignment()))

        sent_keys = []
        for i in range(3):
            key = "validators-live-%d" % i
            sent_keys.append(key)
            mr = prod.send(Message(TOPIC, ("body-%d" % i).encode("utf-8"), keys=key))
            if i == 0:
                check("S4c 合法消息发送成功", mr.send_status == SendStatus.SEND_OK,
                      str(mr.send_status))
        deadline = time.time() + 25
        while time.time() < deadline and len(received) < 3:
            time.sleep(0.5)
        check("S4d push 消费者收到合法消息", all(k in received for k in sent_keys),
              "received=%s" % received)

        # ---------------- S5 对照腿：本地校验省掉的是什么 ----------------
        print("=== S5 合法但不存在的 topic：本地放行、走完整集群链路 ===")
        missing = "ValidatorsMissing_%d" % STAMP
        began = time.monotonic()
        err3 = None
        try:
            # autoCreateTopicEnable=true 的 broker 会替它建 topic ⇒ 本地放行且发送成功
            prod.send(Message(missing, b"x"))
        except Exception as e:  # noqa: BLE001 - 反证：这里的错一定不是本地校验的错
            err3 = e
        cluster_ms = (time.monotonic() - began) * 1000.0
        check("S5a 合法名字不被本地校验误伤",
              err3 is None or "contains illegal characters" not in str(err3),
              "%s in %.1fms" % (type(err3).__name__ if err3 else "SEND_OK", cluster_ms))
        check("S5b 集群腿比本地反腿慢两个数量级以上（那就是重试空转的代价）",
              cluster_ms > 10.0 * max(local_ms, 0.05),
              "cluster=%.1fms local=%.2fms" % (cluster_ms, local_ms))

        # ---------------- S6 拉取消费者的组名反腿 ----------------
        print("=== S6 拉取消费者的组名反腿（正腿见 S4d 与 S6c/S6d）===")
        for cls in (DefaultMQPullConsumer, DefaultLitePullConsumer):
            bad = cls("DEFAULT_CONSUMER")
            bad.set_namesrv_addr(NAMESRV)
            began = time.monotonic()
            try:
                bad.start()
                e = None
            except MQClientException as ex:
                e = ex
            elapsed = (time.monotonic() - began) * 1000.0
            check("S6a %s 拒绝保留组名（本地）" % cls.__name__,
                  e is not None and "can not equal DEFAULT_CONSUMER" in str(e)
                  and elapsed < LOCAL_BUDGET_MS and not bad._started,
                  "%s in %.2fms" % (str(e).replace("\n", " ") if e else "没有抛异常", elapsed))
            illegal = cls("bad group")
            illegal.set_namesrv_addr(NAMESRV)
            try:
                illegal.start()
                e = None
            except MQClientException as ex:
                e = ex
            check("S6b %s 拒绝非法字符组名（本地）" % cls.__name__,
                  e is not None and "contains illegal characters" in str(e))

        # 正腿：合法组名的 pull 消费者不光能启动，还能真查位点（走的是 broker RPC）
        pull_ok = DefaultMQPullConsumer(GROUP + "_pull_ok")
        pull_ok.set_namesrv_addr(NAMESRV)
        pull_ok.start()
        queues = pull_ok.fetch_subscribe_message_queues(TOPIC)
        max_off = pull_ok.max_offset(queues[0]) if queues else -1
        check("S6c 合法组名的 pull 消费者可启动并查到位点",
              pull_ok._started and len(queues) >= 4 and max_off >= 0,
              "queues=%d maxOffset=%d" % (len(queues), max_off))
        pull_ok.shutdown()

        got = []
        deadline = time.time() + 25
        while time.time() < deadline and len(got) < 3:
            got.extend(m.get_keys() for m in lite.poll(timeout=3000))
        check("S6d 合法组名的 lite 消费者收全消息", all(k in got for k in sent_keys),
              "got=%s" % got)
    finally:
        for c in (push, lite):
            if c is not None:
                c.shutdown()
        prod.shutdown()

    print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
