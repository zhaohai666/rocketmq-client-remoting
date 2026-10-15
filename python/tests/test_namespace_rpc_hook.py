# -*- coding: utf-8 -*-
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
    pass
    return RemotingCommand.create_request_command(RequestCode.PULL_MESSAGE)


def _new_instance(prefix: str, **kwargs) -> MQClientInstance:
    """An instance that is only constructed, not start()ed (the same trick as test_unit_config).

    ``MQClientInstance.__init__`` writes ``INSTANCE_MAP[client_id]``, which must be removed after use.
    """
    return MQClientInstance("%s_%d" % (prefix, threading.get_ident()),
                            ["127.0.0.1:9876"], **kwargs)


def _drop(inst: MQClientInstance) -> None:
    MQClientInstance.INSTANCE_MAP.pop(inst.client_id, None)


# ---------------------------------------------------------------- Constants and the hook itself
def test_ext_field_names_match_the_wire_keys():
    pass
    assert MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD == "nsd"
    assert MixAll.RPC_REQUEST_HEADER_NAMESPACE_FIELD == "ns"


def test_hook_adds_nsd_true_and_ns_value():
    pass
    req = _request()
    NamespaceRpcHook(NS_V2).do_before_request(ADDR, req)
    assert req.ext_fields[MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD] == "true"
    assert req.ext_fields[MixAll.RPC_REQUEST_HEADER_NAMESPACE_FIELD] == NS_V2


def test_hook_adds_nothing_else():
    req = _request()
    NamespaceRpcHook(NS_V2).do_before_request(ADDR, req)
    assert set(req.ext_fields.keys()) == {"nsd", "ns"}


def test_empty_namespace_leaves_ext_fields_untouched():
    pass
    for empty in (None, ""):
        req = _request()
        NamespaceRpcHook(empty).do_before_request(ADDR, req)
        assert req.ext_fields == {}, empty


def test_do_after_response_is_noop():
    pass
    hook = NamespaceRpcHook(NS_V2)
    req, resp = _request(), _request()
    hook.do_after_response(ADDR, req, resp)
    assert req.ext_fields == {}
    assert resp.ext_fields == {}


# ---------------------------------------------------------------- Composition order
def test_composition_order_namespace_stream_acl():
    """MQClientAPIImpl:329-335: Namespace → Stream → user (ACL)."""
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
    """The same when stream is off: Namespace still comes before the user rpcHook."""
    inst = _new_instance("ns_order_no_stream")
    try:
        user = RPCHook()
        inst.remoting_client.register_rpc_hook(user)
        kinds = [type(h) for h in inst.remoting_client.rpc_hooks]
        assert kinds.index(NamespaceRpcHook) < kinds.index(RPCHook), kinds
    finally:
        _drop(inst)


def test_namespace_hook_registered_even_without_namespace():
    pass
    inst = _new_instance("ns_always_on")
    try:
        assert [type(h) for h in inst.remoting_client.rpc_hooks][0] is NamespaceRpcHook
        assert inst.remoting_client.rpc_hooks[0].namespace_v2 == ""
    finally:
        _drop(inst)


def test_lite_pull_consumer_forwards_namespace_v2_to_its_client():
    """The facade's namespace_v2 must be passed all the way into MQClientInstance's hook chain."""
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


# ---------------------------------------------------------------- ACL signature coverage
def _signed_request(namespace_v2=None, ns_hook_first=True, body=b""):
    """Run the Namespace and ACL hooks in the specified order and return the signed request."""
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
    """(d) After namespaceV2 is set, the signature must change — proving nsd/ns are included in the signed content."""
    with_ns = _signed_request(NS_V2)
    without_ns = _signed_request(None)
    assert with_ns.ext_fields["nsd"] == "true"
    assert with_ns.ext_fields["ns"] == NS_V2
    assert with_ns.ext_fields["Signature"] != without_ns.ext_fields["Signature"]
    # The signed content (the sorted value string) must contain the ns value; the Signature key itself is
    # excluded in combineRequestContent, so the content can be recomputed directly on the signed request.
    content = AclClientRPCHook.build_request_content(with_ns)
    assert NS_V2.encode("utf-8") in content


def test_wrong_hook_order_would_not_be_covered_by_signature():
    pass
    wrong = _signed_request(NS_V2, ns_hook_first=False)
    right = _signed_request(NS_V2, ns_hook_first=True)
    # The wrong order's signature == the signature with no namespace configured at all (ns/nsd not in the signed content)
    assert wrong.ext_fields["Signature"] == _signed_request(None).ext_fields["Signature"]
    assert right.ext_fields["Signature"] != wrong.ext_fields["Signature"]


# ---------------------------------------------------------------- Facade configuration surface
def test_setters_exist_on_every_facade():
    pass
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
    pass
    for facade in (DefaultMQProducer("PID_ns_default"),
                   DefaultMQPushConsumer("CID_ns_default"),
                   DefaultMQPullConsumer("CID_ns_default"),
                   DefaultLitePullConsumer("CID_ns_default"),
                   _admin()):
        assert facade.namespace_v2 in (None, ""), type(facade).__name__


def _admin():
    # Note: DefaultMQAdminExt's first positional parameter is rpc_hook, not group.
    from client.admin import DefaultMQAdminExt
    return DefaultMQAdminExt()


# ---------------------------------------------------------------- Trace dispatcher
class _FakeTraceProducer:
    """An internal trace producer that only records calls (no network operations whatsoever)."""

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
    pass
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
    pass
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
    pass
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
