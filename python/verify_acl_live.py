#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""ACL 鉴权真机验证：连一个开了认证的 broker，验证签名被接受、缺失/错误凭据被拒绝。

用法（需本地 RocketMQ 5.5.1 集群，broker 开了 authenticationEnabled）：
    .venv/bin/python verify_acl_live.py 127.0.0.1:9876 [accessKey] [secretKey]

broker 侧的账号来自 broker.conf：
    authenticationEnabled=true
    authenticationMetadataProvider=org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider
    initAuthenticationUser={"username":"AK_TEST","password":"SK_TEST_SECRET_12345678"}
    （AuthMigrator/initUser 会把 username 当 accessKey、password 当 secretKey 建 SUPER 用户）

场景：
  S1 正向：带正确凭据的 admin 建 topic → 成功（管理路径的签名被 broker 接受）
  S2 反向：**不带凭据**的 admin 建 topic → broker 返回 NO_PERMISSION(16)
  S3 反向：**secretKey 错误**的生产者发送 → NO_PERMISSION(16)
  S4 正向：带正确凭据的生产者发 3 条 → SEND_OK（msgId 由 broker 赋值）
  S5 正向：带正确凭据的消费者收满 3 条（心跳 / 长轮询拉取 / 位点提交都带签名）
  S6 反向：**不带凭据**的直接 broker RPC（GET_CONSUMER_LIST_BY_GROUP）→ NO_PERMISSION(16)
  S7 正向：不带凭据但访问 **NameServer**（路由查询）仍成功 —— 证明钩子只影响需要鉴权的目标，
           没有把 namesrv 路径打坏

⚠ S4/S5 必须**先起消费者再发送**：Java 默认 CONSUME_FROM_LAST_OFFSET 会把新消费组的初始
  位点解析成该队列当时的 maxOffset（见 scenario_consumer_then_producer 的注释）。
  反过来写会得到一个"正确但测不出东西"的假失败，且因 broker 异步分发而在三语言间结果不一致。
"""
from __future__ import annotations

import sys
import threading
import time
from typing import Optional

sys.path.insert(0, ".")

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      SimpleMessageListener)
from rocketmq.client.exception import MQBrokerException
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.send_result import SendStatus
from rocketmq.common.message import Message
from rocketmq.remoting.protocol.codes import RequestCode, ResponseCode
from rocketmq.remoting.protocol.remoting_command import RemotingCommand
from rocketmq.remoting.rpchook import AclClientRPCHook, SessionCredentials

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
AK = sys.argv[2] if len(sys.argv) > 2 else "AK_TEST"
SK = sys.argv[3] if len(sys.argv) > 3 else "SK_TEST_SECRET_12345678"

STAMP = int(time.time() * 1000)
TOPIC = "AclLive_%d" % STAMP
GROUP = "GID_AclLive_%d" % STAMP

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


def creds(sk: str = SK) -> AclClientRPCHook:
    return AclClientRPCHook(SessionCredentials(AK, sk))


def response_code_of(exc: BaseException) -> Optional[int]:
    """沿 cause 链找 broker 响应码。

    broker 侧**所有**鉴权失败都抛 AbortProcessException(NO_PERMISSION=16)
    （broker/auth/pipeline/AuthenticationPipeline.java:53），所以 16 就是「被拒绝」。
    不能只看最外层：admin 的 create_topic_in_route 会把底层 MQBrokerException
    包进 MQClientException(cause=...)，最外层根本没有 response_code。
    """
    seen = set()
    cur: Optional[BaseException] = exc
    while isinstance(cur, BaseException) and id(cur) not in seen:
        seen.add(id(cur))
        code = getattr(cur, "response_code", None)
        if code:
            return int(code)
        nxt = getattr(cur, "cause", None) or cur.__cause__
        cur = nxt if isinstance(nxt, BaseException) else None
    return None


def deepest_message(exc: BaseException) -> str:
    """取异常链里最内层的可读消息（通常就是 broker 的 remark）。"""
    msg = str(exc)
    seen = set()
    cur: Optional[BaseException] = exc
    while isinstance(cur, BaseException) and id(cur) not in seen:
        seen.add(id(cur))
        inner = getattr(cur, "error_message", None) or str(cur)
        if inner:
            msg = inner
        nxt = getattr(cur, "cause", None) or cur.__cause__
        cur = nxt if isinstance(nxt, BaseException) else None
    return msg


def is_no_permission(code: Optional[int]) -> bool:
    """broker 鉴权失败的响应码（NO_PERMISSION=16）。"""
    return code == ResponseCode.NO_PERMISSION


def try_call(fn):
    """执行 fn，返回 (ok, detail)；失败时 detail 里一定带上找到的响应码。"""
    try:
        fn()
        return True, ""
    except BaseException as e:  # noqa: BLE001 - 真机验证需要看到所有失败形态
        code = response_code_of(e)
        detail = "%s %s: %s" % (
            type(e).__name__,
            ("code=%s" % code) if code is not None else "code=?",
            deepest_message(e),
        )
        return False, detail


def scenario_admin_create() -> None:
    print("\nS1/S2 admin 建 topic：带凭据 vs 不带凭据")
    # S1 正向
    admin = DefaultMQAdminExt(rpc_hook=creds())
    admin.set_namesrv_addr(NAMESRV)
    admin.start()
    try:
        ok, detail = try_call(lambda: admin.create_topic("TBW102", TOPIC, 4))
        check("S1 带凭据 admin 建 topic 成功", ok, detail)
    finally:
        admin.shutdown()

    # S2 反向：不带凭据
    print("\nS2 不带凭据的 admin 建 topic 必须被拒")
    admin2 = DefaultMQAdminExt()  # 不传 rpc_hook
    admin2.set_namesrv_addr(NAMESRV)
    admin2.start()
    try:
        ok, detail = try_call(lambda: admin2.create_topic("TBW102", TOPIC + "_DENIED", 4))
        check("S2 无凭据 admin 被 broker 拒绝", (not ok) and is_no_permission_detail(detail), detail)
    finally:
        admin2.shutdown()


def is_no_permission_detail(detail: str) -> bool:
    return ("code=%d" % ResponseCode.NO_PERMISSION) in detail


def scenario_bad_producer() -> None:
    """S3 反向：secretKey 错误的生产者必须被 broker 拒绝。"""
    print("\nS3 错误 secretKey 的生产者必须被拒")
    bad = DefaultMQProducer("PG_AclLiveBad_%d" % STAMP, rpc_hook=creds("WRONG_SECRET_KEY"))
    bad.set_namesrv_addr(NAMESRV)
    bad.start()
    try:
        msg = Message(TOPIC, b"should-not-send")
        msg.set_keys("acl-wrong")
        ok, detail = try_call(lambda: bad.send(msg))
        check("S3 错误 secretKey 被 broker 拒绝", (not ok) and is_no_permission_detail(detail), detail)
    finally:
        bad.shutdown()


def scenario_consumer_then_producer() -> None:
    """S4 + S5 正向：**先起消费者、再发消息**。

    顺序不能反过来（这是本文件最容易踩的坑）：Java 默认 CONSUME_FROM_LAST_OFFSET，
    首次消费且无已提交位点时初始位点被解析为该队列**当时的** maxOffset
    （RebalancePushImpl.java:174-190）。所以"先发 3 条、再起消费者"会（正确地）
    一条都收不到。更麻烦的是 broker 的 consumequeue 是异步分发/刷盘的，刚发完立刻
    查 maxOffset 可能读到 0 —— 于是同一场景在三种语言间**结果不确定**：
    实测同一时序下 Python 收 0 条（读到 maxOffset=3），C++/.NET 收 3 条（读到 0）。
    先把消费者起好、等 rebalance 分配完队列，再发送，才是确定性的、只测 ACL 的顺序。
    """
    print("\nS4/S5 带正确凭据的生产者/消费者（先起消费者再发送）")
    consumer = DefaultMQPushConsumer(GROUP, rpc_hook=creds())
    consumer.set_namesrv_addr(NAMESRV)
    consumer.subscribe(TOPIC, "*")

    got = []
    lock = threading.Lock()

    def on_msg(msgs):
        with lock:
            for m in msgs:
                got.append(bytes(m.body))
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    consumer.set_message_listener(SimpleMessageListener(on_msg))
    consumer.start()
    try:
        # 等 rebalance 把队列分下来：初始位点必须在"topic 还空着"的时候解析
        time.sleep(5)

        sent = 0
        evidence = []
        prod = DefaultMQProducer("PG_AclLiveOk_%d" % STAMP, rpc_hook=creds())
        prod.set_namesrv_addr(NAMESRV)
        prod.start()
        try:
            for i in range(3):
                msg = Message(TOPIC, ("acl-ok-%d" % i).encode())
                msg.set_keys("acl-ok")
                try:
                    result = prod.send(msg)
                    # SendStatus 是 Enum：str(SendStatus.SEND_OK) == "SendStatus.SEND_OK"，
                    # 必须按枚举比较，不能拿字符串比（曾因此把成功的发送误判成 sent=0）。
                    evidence.append("%s/%s" % (result.send_status, result.msg_id))
                    if result.send_status == SendStatus.SEND_OK:
                        sent += 1
                    else:
                        check("S4 第 %d 条发送状态" % i, False, str(result.send_status))
                except BaseException as e:  # noqa: BLE001
                    evidence.append("EXC:%s" % e)
                    check("S4 第 %d 条发送" % i, False, "%s: %s" % (type(e).__name__, e))
        finally:
            prod.shutdown()
        # msgId 由 broker 生成：非空即证明 broker 真的接受了这条签名请求
        check("S4 带凭据生产者发送 3 条", sent == 3, "sent=%d %s" % (sent, evidence))

        # S5：心跳 / 长轮询拉取 / 位点提交全程带签名，收满才算消费链路鉴权通过
        deadline = time.time() + 30
        while time.time() < deadline and len(got) < sent:
            time.sleep(0.3)
        check("S5 带凭据消费者收满 %d 条" % sent, len(got) == sent,
              "got=%d bodies=%s" % (len(got), sorted(got)))
    finally:
        consumer.shutdown()


def scenario_negative_rpc() -> None:
    """直连 broker 发一个原始 RPC，自己看 response.code。

    不能用 client.get_consumer_id_list_by_group：它内部把异常吞掉、失败返回 None，
    于是「被拒绝」和「成功」在调用方看来一模一样（曾因此误判成 PASS）。
    """
    print("\nS6 不带凭据的直接 broker RPC 必须被拒")
    client = MQClientInstance("AclProbe_%d" % STAMP, [NAMESRV])
    client.start()
    try:
        try:
            addr = client._broker_addr_for_topic(TOPIC)
        except BaseException as e:  # noqa: BLE001
            check("S6 拿到 broker 地址", False, "%s: %s" % (type(e).__name__, e))
            return
        request = RemotingCommand.create_request_command(
            RequestCode.GET_CONSUMER_LIST_BY_GROUP, None)
        request.ext_fields["consumerGroup"] = GROUP

        def call() -> None:
            response = client.remoting_client.invoke_sync(addr, request, 5000)
            if response.code != ResponseCode.SUCCESS:
                raise MQBrokerException(response.code, response.remark or "")

        ok, detail = try_call(call)
        check("S6 无凭据 broker RPC 被拒绝",
              (not ok) and is_no_permission_detail(detail),
              "addr=%s %s" % (addr, detail))
    finally:
        client.shutdown()


def scenario_namesrv_without_creds() -> None:
    print("\nS7 不带凭据走 NameServer 路由查询仍应成功（钩子不影响 namesrv 路径）")
    client = MQClientInstance("AclNs_%d" % STAMP, [NAMESRV])
    client.start()
    try:
        ok, detail = try_call(lambda: client.update_topic_route_info_from_name_server(TOPIC))
        check("S7 无凭据 namesrv 路由查询成功", ok, detail)
    finally:
        client.shutdown()


def main() -> int:
    print("=" * 72)
    print("ACL live: namesrv=%s topic=%s group=%s ak=%s" % (NAMESRV, TOPIC, GROUP, AK))
    print("=" * 72)

    scenario_admin_create()
    scenario_bad_producer()
    scenario_consumer_then_producer()
    scenario_negative_rpc()
    scenario_namesrv_without_creds()

    print("\n===== ACL live summary =====")
    print("  PASS=%d FAIL=%d" % (PASS, FAIL))
    if FAIL == 0:
        print("  result: ACL 鉴权（签名 / 拒绝 / namesrv 兼容）真机通过")
        return 0
    print("  result: %d 项失败" % FAIL)
    return 1


if __name__ == "__main__":
    sys.exit(main())
