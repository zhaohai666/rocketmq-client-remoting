# -*- coding: utf-8 -*-
"""NamespaceRpcHook（namespaceV2 请求头）与钩子组合顺序的单测。

Java 语义锚点（5.5.1 逐条核对）：

  * ``client/rpchook/NamespaceRpcHook#doBeforeRequest``：只在
    ``StringUtils.isNotEmpty(clientConfig.getNamespaceV2())`` 时动作，且只加两个
    扩展字段 ``nsd="true"``、``ns=<namespaceV2>``；``doAfterResponse`` 为空实现。
  * ``common/MixAll.java:122-123``：
    ``RPC_REQUEST_HEADER_NAMESPACED_FIELD = "nsd"``、
    ``RPC_REQUEST_HEADER_NAMESPACE_FIELD = "ns"``。
  * ``MQClientAPIImpl:329-335`` 的注册**顺序**：Namespace → Stream（仅
    enableStreamRequestType 时）→ 用户 rpcHook（即 ACL 签名）→ DynamicalExtField。
    Namespace 必须排在 ACL **之前**，nsd/ns 才会被算进签名内容（Go 端口
    ``go/remoting/rpchooks.go`` 的同段注释解释过：顺序反了签名照样「看起来合法」，
    只是 broker 验不过）。
  * ``NamespaceRpcHookTest#testDoBeforeRequestWithoutNamespace``：未配置时
    extFields 必须**原样不动**（Java 断言 ``getExtFields()`` 为 null；本端口的
    ``RemotingCommand.ext_fields`` 默认是空 dict，等价断言即「仍为空、没被创建出
    任何键」）。
  * ``AsyncTraceDispatcher.start()``:155：``traceProducer.setNamespaceV2(namespaceV2)``
    —— 分发器的 namespaceV2 要传给内部轨迹生产者；宿主 facade 在建分发器时
    ``dispatcher.setNamespaceV2(...)``（Java DefaultMQProducer:384、
    DefaultMQPushConsumer:769）。
"""
from __future__ import annotations

import threading

from client.consumer import (DefaultLitePullConsumer, DefaultMQPullConsumer,
                             DefaultMQPushConsumer)
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from client.trace_dispatcher import AsyncTraceDispatcher, TraceDispatcherType
from common.mix_all import MixAll
from remoting.protocol.codes import RequestCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.rpchook import (AclClientRPCHook, NamespaceRpcHook, RPCHook,
                              SessionCredentials, StreamTypeRPCHook)

ADDR = "127.0.0.1:9876"
NS_V2 = "MQ_TEST_INSTANCE_ID"


def _request() -> RemotingCommand:
    """与 Java 单测同款：一条 PULL_MESSAGE 请求，未做过 makeCustomHeaderToNet。"""
    return RemotingCommand.create_request_command(RequestCode.PULL_MESSAGE)


def _new_instance(prefix: str, **kwargs) -> MQClientInstance:
    """只构造、不 start() 的实例（与 test_unit_config 同款套路）。

    ``MQClientInstance.__init__`` 会写 ``INSTANCE_MAP[client_id]``，用完要摘掉。
    """
    return MQClientInstance("%s_%d" % (prefix, threading.get_ident()),
                            ["127.0.0.1:9876"], **kwargs)


def _drop(inst: MQClientInstance) -> None:
    MQClientInstance.INSTANCE_MAP.pop(inst.client_id, None)


# ---------------------------------------------------------------- 常量与钩子本体
def test_ext_field_names_match_java_mix_all():
    """Java MixAll.java:122-123 的两个键名，一字都不能差。"""
    assert MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD == "nsd"
    assert MixAll.RPC_REQUEST_HEADER_NAMESPACE_FIELD == "ns"


def test_hook_adds_nsd_true_and_ns_value():
    """Java 的 doBeforeRequest 只有两行：nsd="true"、ns=<namespaceV2>。"""
    req = _request()
    NamespaceRpcHook(NS_V2).do_before_request(ADDR, req)
    assert req.ext_fields[MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD] == "true"
    assert req.ext_fields[MixAll.RPC_REQUEST_HEADER_NAMESPACE_FIELD] == NS_V2


def test_hook_adds_nothing_else():
    req = _request()
    NamespaceRpcHook(NS_V2).do_before_request(ADDR, req)
    assert set(req.ext_fields.keys()) == {"nsd", "ns"}


def test_empty_namespace_leaves_ext_fields_untouched():
    """Java 的 isNotEmpty 守卫 + 单测「getExtFields() 仍为 null」的等价断言。

    钩子绝不能因为「配置为空」就创建出扩展字段 —— 未配置 namespaceV2 的请求
    必须与从未跑过该钩子完全一致。
    """
    for empty in (None, ""):
        req = _request()
        NamespaceRpcHook(empty).do_before_request(ADDR, req)
        assert req.ext_fields == {}, empty


def test_do_after_response_is_noop():
    """Java 的 doAfterResponse 是空实现。"""
    hook = NamespaceRpcHook(NS_V2)
    req, resp = _request(), _request()
    hook.do_after_response(ADDR, req, resp)
    assert req.ext_fields == {}
    assert resp.ext_fields == {}


# ---------------------------------------------------------------- 组合顺序
def test_composition_order_namespace_stream_acl():
    """MQClientAPIImpl:329-335：Namespace → Stream → 用户（ACL）。"""
    inst = _new_instance("ns_order", enable_stream_request_type=True,
                         namespace_v2=NS_V2)
    try:
        inst.remoting_client.register_rpc_hook(
            AclClientRPCHook(SessionCredentials("AK", "SK")))
        kinds = [type(h) for h in inst.remoting_client.rpc_hooks]
        assert kinds == [NamespaceRpcHook, StreamTypeRPCHook, AclClientRPCHook], kinds
    finally:
        _drop(inst)


def test_namespace_hook_registered_before_user_hook_without_stream():
    """stream 关闭时也一样：Namespace 仍在用户 rpcHook 之前。"""
    inst = _new_instance("ns_order_no_stream")
    try:
        user = RPCHook()
        inst.remoting_client.register_rpc_hook(user)
        kinds = [type(h) for h in inst.remoting_client.rpc_hooks]
        assert kinds.index(NamespaceRpcHook) < kinds.index(RPCHook), kinds
    finally:
        _drop(inst)


def test_namespace_hook_registered_even_without_namespace():
    """Java 无条件注册该钩子（未配置时由钩子内部的守卫兜底）。"""
    inst = _new_instance("ns_always_on")
    try:
        assert [type(h) for h in inst.remoting_client.rpc_hooks][0] is NamespaceRpcHook
        assert inst.remoting_client.rpc_hooks[0].namespace_v2 == ""
    finally:
        _drop(inst)


def test_lite_pull_consumer_forwards_namespace_v2_to_its_client():
    """facade 的 namespace_v2 要一路传进 MQClientInstance 的钩子链。"""
    consumer = DefaultLitePullConsumer("CID_ns_forward")
    consumer.set_namesrv_addr("127.0.0.1:9876")
    consumer.client_id = "ns-forward-client"
    consumer.set_namespace_v2(NS_V2)
    inst = consumer._create_client()
    try:
        assert inst.namespace_v2 == NS_V2
        assert inst.remoting_client.rpc_hooks[0].namespace_v2 == NS_V2
    finally:
        _drop(inst)


# ---------------------------------------------------------------- ACL 签名覆盖
def _signed_request(namespace_v2=None, ns_hook_first=True, body=b""):
    """按指定顺序跑 Namespace 与 ACL 钩子，返回签好名的请求。"""
    req = _request()
    req.body = body
    req.add_ext_field("topic", "MyTopic")
    acl = AclClientRPCHook(SessionCredentials("AK_TEST", "SK_TEST_SECRET_12345678"))
    ns_hook = NamespaceRpcHook(namespace_v2)
    if ns_hook_first:
        ns_hook.do_before_request(ADDR, req)
        acl.do_before_request(ADDR, req)
    else:
        acl.do_before_request(ADDR, req)
        ns_hook.do_before_request(ADDR, req)
    return req


def test_acl_signature_covers_namespace_fields():
    """(d) 设了 namespaceV2 之后签名必须变 —— 证明 nsd/ns 被算进了签名内容。"""
    with_ns = _signed_request(NS_V2)
    without_ns = _signed_request(None)
    assert with_ns.ext_fields["nsd"] == "true"
    assert with_ns.ext_fields["ns"] == NS_V2
    assert with_ns.ext_fields["Signature"] != without_ns.ext_fields["Signature"]
    # 签名内容（排序后的 value 串）里必须能找到 ns 的值；Signature 键自身在
    # combineRequestContent 里被排除，所以可以直接对已签名的请求重算内容。
    content = AclClientRPCHook.build_request_content(with_ns)
    assert NS_V2.encode("utf-8") in content


def test_wrong_hook_order_would_not_be_covered_by_signature():
    """反例锚点：Namespace 排在 ACL 之后时，ns/nsd 不参与签名。

    此时 extFields 带着没有签名背书的 nsd/ns —— broker 侧按「全部 extFields 的
    value 排序拼接」重算签名就对不上（Java 注释 reserve field signature 所指的
    正是这个坑），所以注册顺序是语义而不是风格。
    """
    wrong = _signed_request(NS_V2, ns_hook_first=False)
    right = _signed_request(NS_V2, ns_hook_first=True)
    # 错误顺序的签名 == 根本没配 namespace 的签名（ns/nsd 没进签名内容）
    assert wrong.ext_fields["Signature"] == _signed_request(None).ext_fields["Signature"]
    assert right.ext_fields["Signature"] != wrong.ext_fields["Signature"]


# ---------------------------------------------------------------- facade 配置面
def test_setters_exist_on_every_facade():
    """Java 五个 facade 都继承 ClientConfig，因此都有 set/getNamespaceV2。"""
    for facade in (DefaultMQProducer("PID_ns_setter"),
                   DefaultMQPushConsumer("CID_ns_setter"),
                   DefaultMQPullConsumer("CID_ns_setter"),
                   DefaultLitePullConsumer("CID_ns_setter")):
        facade.set_namespace_v2(NS_V2)
        assert facade.get_namespace_v2() == NS_V2, type(facade).__name__
        assert facade.namespace_v2 == NS_V2, type(facade).__name__
    from client.admin import DefaultMQAdminExt
    admin = DefaultMQAdminExt()
    admin.set_namespace_v2(NS_V2)
    assert admin.get_namespace_v2() == NS_V2
    assert admin.namespace_v2 == NS_V2


def test_namespace_v2_defaults_empty_everywhere():
    """Java ClientConfig.namespaceV2 默认 null；本端口用 "" 表达同样的「未配置」。"""
    for facade in (DefaultMQProducer("PID_ns_default"),
                   DefaultMQPushConsumer("CID_ns_default"),
                   DefaultMQPullConsumer("CID_ns_default"),
                   DefaultLitePullConsumer("CID_ns_default"),
                   _admin()):
        assert facade.namespace_v2 in (None, ""), type(facade).__name__


def _admin():
    # 注意 DefaultMQAdminExt 的第一个位置参数是 rpc_hook，不是 group。
    from client.admin import DefaultMQAdminExt
    return DefaultMQAdminExt()


# ---------------------------------------------------------------- 轨迹分发器
class _FakeTraceProducer:
    """只记录调用的轨迹内部生产者（不做任何网络操作）。"""

    def __init__(self):
        self.namespace_v2 = None
        self.started = False

    def set_namesrv_addr(self, addr):
        pass

    def set_instance_name(self, name):
        pass

    def set_namespace_v2(self, ns):
        self.namespace_v2 = ns

    def set_enable_trace(self, enable):
        pass

    def start(self):
        self.started = True

    def shutdown(self):
        pass


def test_dispatcher_start_propagates_namespace_v2_to_trace_producer():
    """对应 Java AsyncTraceDispatcher.start():155。"""
    d = AsyncTraceDispatcher("GID_ns", TraceDispatcherType.PRODUCE, 10, None, None)
    fake = _FakeTraceProducer()
    d.trace_producer = fake
    d.set_namespace_v2(NS_V2)
    d.start("127.0.0.1:9876")
    try:
        assert fake.namespace_v2 == NS_V2
        assert fake.started
    finally:
        d.shutdown()


def test_dispatcher_defaults_to_no_namespace():
    d = AsyncTraceDispatcher("GID_ns", TraceDispatcherType.PRODUCE, 10, None, None)
    assert d.get_namespace_v2() in (None, "")
    assert d.namespace_v2 == ""


def test_producer_wires_its_namespace_v2_into_dispatcher(monkeypatch):
    """对应 Java DefaultMQProducer.start():384 的 dispatcher.setNamespaceV2。"""
    started = []
    monkeypatch.setattr(AsyncTraceDispatcher, "start",
                        lambda self, *a, **kw: started.append(self))
    p = DefaultMQProducer("PID_ns_disp")
    p.set_namesrv_addr("127.0.0.1:9876")
    p.set_namespace_v2(NS_V2)
    p.set_enable_trace(True)
    p._start_trace_dispatcher()
    assert started and started[0].namespace_v2 == NS_V2
    started[0].shutdown()


def test_push_consumer_wires_its_namespace_v2_into_dispatcher(monkeypatch):
    """对应 Java DefaultMQPushConsumer.start():769 的 dispatcher.setNamespaceV2。"""
    started = []
    monkeypatch.setattr(AsyncTraceDispatcher, "start",
                        lambda self, *a, **kw: started.append(self))
    c = DefaultMQPushConsumer("CID_ns_disp")
    c.set_namesrv_addr("127.0.0.1:9876")
    c.set_namespace_v2(NS_V2)
    c.set_enable_trace(True)
    c._start_trace_dispatcher()
    assert started and started[0].namespace_v2 == NS_V2
    started[0].shutdown()
