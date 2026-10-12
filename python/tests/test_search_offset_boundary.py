# -*- coding: utf-8 -*-
from __future__ import annotations

import threading

from client.admin import DefaultMQAdminExt
from client.mq_client import MQClientInstance
from common.boundary_type import BoundaryType
from common.message import MessageQueue
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.headers import SearchOffsetRequestHeader
from remoting.protocol.remoting_command import RemotingCommand

MQ = MessageQueue("T_Boundary", "broker-a", 2)
ADDR = "127.0.0.1:10911"


def _new_instance(prefix: str) -> MQClientInstance:
    """只构造、不 start() 的实例：不建连、不发心跳，纯离线断言。"""
    return MQClientInstance("%s_%d" % (prefix, threading.get_ident()),
                            ["127.0.0.1:9876"])


def _drop(inst: MQClientInstance) -> None:
    MQClientInstance.INSTANCE_MAP.pop(inst.client_id, None)


# ---------------------------------------------------------------- 报文形状
def test_wire_value_is_the_uppercase_enum_name_not_the_lowercase_name():
    pass
    for boundary, expected in ((BoundaryType.LOWER, "LOWER"), (BoundaryType.UPPER, "UPPER")):
        header = SearchOffsetRequestHeader()
        header.topic, header.queue_id, header.timestamp = MQ.topic, MQ.queue_id, 1700000000000
        header.boundary_type = boundary
        assert header.to_ext_fields()["boundaryType"] == expected


def test_unset_boundary_omits_the_field():
    """@CFNullable：没设就整键不写，不能发 ``boundaryType=""``。"""
    header = SearchOffsetRequestHeader()
    header.topic, header.queue_id, header.timestamp = MQ.topic, MQ.queue_id, 1
    assert "boundaryType" not in header.to_ext_fields()
    header.from_ext_fields({})
    assert header.boundary_type is None


def test_decode_is_lenient_on_unknown_type_values():
    """``BoundaryType.getType``：只有 upper（大小写不敏感）才是 UPPER。"""
    header = SearchOffsetRequestHeader()
    for text, expected in (("UPPER", BoundaryType.UPPER), ("upper", BoundaryType.UPPER),
                           ("Upper", BoundaryType.UPPER), ("LOWER", BoundaryType.LOWER),
                           ("lower", BoundaryType.LOWER), ("", BoundaryType.LOWER),
                           ("junk", BoundaryType.LOWER)):
        header.from_ext_fields({"boundaryType": text})
        assert header.boundary_type is expected, text
    assert BoundaryType.get_type(None) is BoundaryType.LOWER
    assert BoundaryType.get_type(123) is BoundaryType.LOWER
    assert BoundaryType.UPPER.lowercase_name == "upper"


# ---------------------------------------------------------------- 调用链
class _Capture:
    """把 ``_invoke_sync`` 收到的请求记下来（并落 extFields），回一个带 offset 的应答。"""

    def __init__(self):
        self.requests = []

    def __call__(self, addr, request, timeout_millis=None):
        request.make_custom_header_to_net()
        self.requests.append(request)
        resp = RemotingCommand(code=ResponseCode.SUCCESS)
        resp.ext_fields["offset"] = "42"
        return resp

    @property
    def ext_fields(self):
        return self.requests[0].ext_fields

    @property
    def header(self):
        return self.requests[0].custom_header


def _patched_client(prefix: str):
    inst = _new_instance(prefix)
    cap = _Capture()
    inst._invoke_sync = cap  # type: ignore[method-assign]
    return inst, cap


def test_client_always_sends_lower_by_default():
    pass
    inst, cap = _patched_client("soa_lower")
    try:
        assert inst.search_offset_by_timestamp(MQ, 1700000000000, addr=ADDR) == 42
        assert cap.requests[0].code == RequestCode.SEARCH_OFFSET_BY_TIMESTAMP
        assert cap.header.boundary_type is BoundaryType.LOWER
        assert cap.ext_fields == {
            "topic": "T_Boundary", "queueId": "2", "timestamp": "1700000000000",
            "boundaryType": "LOWER"}
    finally:
        _drop(inst)


def test_client_can_ask_for_upper():
    inst, cap = _patched_client("soa_upper")
    try:
        assert inst.search_offset_by_timestamp(
            MQ, 1700000000000, addr=ADDR, boundary_type=BoundaryType.UPPER) == 42
        assert cap.ext_fields["boundaryType"] == "UPPER"
    finally:
        _drop(inst)


def test_none_boundary_keeps_the_field_off_the_wire():
    pass
    inst, cap = _patched_client("soa_none")
    try:
        inst.search_offset_by_timestamp(MQ, 1, addr=ADDR, boundary_type=None)
        assert "boundaryType" not in cap.ext_fields
        assert list(cap.ext_fields) == ["topic", "queueId", "timestamp"]
    finally:
        _drop(inst)


# ---------------------------------------------------------------- admin facade
def test_admin_exposes_the_boundary_method_names():
    pass
    admin = DefaultMQAdminExt()

    class _Stub:
        def __init__(self):
            self.calls = []

        def search_offset_by_timestamp(self, mq, timestamp, timeout_millis=5000, addr=None,
                                       boundary_type=BoundaryType.LOWER):
            self.calls.append((mq, timestamp, boundary_type))
            return 7 if boundary_type is BoundaryType.LOWER else 8

    stub = _Stub()
    admin._started = True
    admin._mq_client = stub  # type: ignore[assignment]
    assert admin.search_offset(MQ, 5) == 7
    assert admin.search_lower_boundary_offset(MQ, 5) == 7
    assert admin.search_upper_boundary_offset(MQ, 5) == 8
    assert [c[2] for c in stub.calls] == [BoundaryType.LOWER, BoundaryType.LOWER,
                                          BoundaryType.UPPER]
