# -*- coding: utf-8 -*-
"""Auth user / ACL management RPCs: recorded request code, exact extFields, exact body JSON,
untouched target address, and the decoded reply."""
from __future__ import annotations

import json

import pytest

from client.admin import DefaultMQAdminExt
from client.exception import MQBrokerException
from remoting.protocol.admin_body import (AclEntry, AclInfo, AclPolicy, Decision,
                                            PolicyType, UserInfo, UserStatus, UserType)
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand

ADDR = "127.0.0.1:10911"
USERNAME = "alice"
SUBJECT = "topic:OrderTopic"
REMARK = "auth is not enabled"

# name -> (request code constant, the published number it must keep)
AUTH_CODES = [
    ("create_user", RequestCode.AUTH_CREATE_USER, 3001),
    ("update_user", RequestCode.AUTH_UPDATE_USER, 3002),
    ("delete_user", RequestCode.AUTH_DELETE_USER, 3003),
    ("get_user", RequestCode.AUTH_GET_USER, 3004),
    ("list_users", RequestCode.AUTH_LIST_USER, 3005),
    ("create_acl", RequestCode.AUTH_CREATE_ACL, 3006),
    ("update_acl", RequestCode.AUTH_UPDATE_ACL, 3007),
    ("delete_acl", RequestCode.AUTH_DELETE_ACL, 3008),
    ("get_acl", RequestCode.AUTH_GET_ACL, 3009),
    ("list_acls", RequestCode.AUTH_LIST_ACL, 3010),
]

# name -> one call of the admin API against the given address
CALLS = {
    "create_user": lambda a, addr: a.create_user(addr, USERNAME, "secret123",
                                                 UserType.NORMAL),
    "update_user": lambda a, addr: a.update_user(addr, USERNAME, "secret123",
                                                 UserType.NORMAL),
    "delete_user": lambda a, addr: a.delete_user(addr, USERNAME),
    "get_user": lambda a, addr: a.get_user(addr, USERNAME),
    "list_users": lambda a, addr: a.list_users(addr, filter="%"),
    "create_acl": lambda a, addr: a.create_acl(addr, SUBJECT, ["TopicA"], ["Pub"],
                                               ["10.0.0.1"], Decision.ALLOW),
    "update_acl": lambda a, addr: a.update_acl(addr, SUBJECT, ["TopicA"], ["Pub"],
                                               ["10.0.0.1"], Decision.ALLOW),
    "delete_acl": lambda a, addr: a.delete_acl(addr, SUBJECT, "TopicA"),
    "get_acl": lambda a, addr: a.get_acl(addr, SUBJECT),
    "list_acls": lambda a, addr: a.list_acls(addr, subject_filter="sub%",
                                             resource_filter="Topic%"),
}


# ------------------------------------------------------------------ Test doubles

def _ok(body: bytes = b"") -> RemotingCommand:
    r = RemotingCommand()
    r.code = ResponseCode.SUCCESS
    r.body = body
    return r


def _err(code: int, remark: str) -> RemotingCommand:
    r = RemotingCommand()
    r.code = code
    r.remark = remark
    return r


class _FakeClient:
    """Records every outgoing request (address, command, timeout) without touching the network."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.requests = []  # [(addr, RemotingCommand, timeout_millis)]

    def _invoke_sync(self, addr, request, timeout_millis=None):
        self.requests.append((addr, request, timeout_millis))
        if not self._responses:
            raise AssertionError("unexpected extra request: code=%s" % request.code)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def _check_response(self, response):
        if response.code == ResponseCode.SUCCESS:
            return response
        raise MQBrokerException(response.code, response.remark or "")

    @property
    def last(self):
        assert len(self.requests) == 1, self.requests
        return self.requests[0]

    @property
    def only_request(self) -> RemotingCommand:
        return self.last[1]

    @property
    def only_ext(self) -> dict:
        return self.only_request.ext_fields

    @property
    def only_body_text(self) -> str:
        body = self.only_request.body
        assert body is not None, "expected a request body"
        return body.decode("utf-8")

    @property
    def only_body(self) -> dict:
        return json.loads(self.only_body_text)


def _admin(client: _FakeClient) -> DefaultMQAdminExt:
    admin = DefaultMQAdminExt()
    # The VIP switch is left ON on purpose: an accidental address rewrite then shows up in
    # the recorded address instead of being masked by the default configuration.
    admin.vip_channel_enabled = True
    admin._mq_client = client
    admin._started = True
    return admin


# ------------------------------------------------------------------ Expected wire shapes

def user_body(username=USERNAME, password="secret123", user_type=UserType.NORMAL,
              user_status=None) -> dict:
    body = {"username": username, "password": password, "userType": user_type}
    return body if user_status is None else dict(body, userStatus=user_status)


def acl_body(subject=SUBJECT, resources=("TopicA", "TopicB"), actions=("Pub", "Sub"),
             source_ips=("10.0.0.1",), decision=Decision.ALLOW,
             policy_type=None) -> dict:
    entries = [{"resource": r, "actions": list(actions), "sourceIps": list(source_ips),
                "decision": decision} for r in resources]
    policy = {"entries": entries}
    if policy_type is not None:
        policy = dict(policyType=policy_type, **policy)
    return {"subject": subject, "policies": [policy]}


def wire(obj) -> str:
    """The exact text this port puts on the wire, byte for byte."""
    return json.dumps(obj, ensure_ascii=False)


# The convenience form of each body-carrying RPC, as positional args and as keywords.
CONVENIENCE_CALLS = {
    "create_user": ((USERNAME, "secret123", UserType.NORMAL),
                    {"user": USERNAME, "password": "secret123",
                     "user_type": UserType.NORMAL}),
    "update_user": ((USERNAME, "secret123", UserType.NORMAL),
                    {"user": USERNAME, "password": "secret123",
                     "user_type": UserType.NORMAL}),
    "create_acl": ((SUBJECT, ["TopicA"], ["Pub"], ["10.0.0.1"], Decision.ALLOW),
                   {"acl": SUBJECT, "resources": ["TopicA"], "actions": ["Pub"],
                    "source_ips": ["10.0.0.1"], "decision": Decision.ALLOW}),
    "update_acl": ((SUBJECT, ["TopicA"], ["Pub"], ["10.0.0.1"], Decision.ALLOW),
                   {"acl": SUBJECT, "resources": ["TopicA"], "actions": ["Pub"],
                    "source_ips": ["10.0.0.1"], "decision": Decision.ALLOW}),
}


# ------------------------------------------------------------------ Enum spellings
def test_enum_wire_spellings_are_byte_exact():
    """The case of these strings is part of the contract and is deliberately inconsistent."""
    assert (UserType.SUPER, UserType.NORMAL) == ("Super", "Normal")
    assert (UserStatus.ENABLE, UserStatus.DISABLE) == ("enable", "disable")
    assert (PolicyType.CUSTOM, PolicyType.DEFAULT) == ("Custom", "Default")
    assert (Decision.ALLOW, Decision.DENY) == ("Allow", "Deny")


@pytest.mark.parametrize("name,code,literal", AUTH_CODES)
def test_each_rpc_records_its_published_request_code(name, code, literal):
    """Drives every auth RPC once and pins the code it puts on the wire."""
    assert code == literal
    client = _FakeClient([_ok()])
    CALLS[name](_admin(client), ADDR)
    addr, request, _ = client.last
    assert request.code == literal
    assert addr == ADDR


# ------------------------------------------------------------------ create / update user
def test_create_user_convenience_body_omits_user_status():
    """Control for "unset keys are omitted": emitting userStatus as null turns this red."""
    client = _FakeClient([_ok()])
    admin = _admin(client)

    result = admin.create_user(ADDR, USERNAME, "secret123", UserType.NORMAL)

    assert result is None
    assert client.only_request.code == 3001
    assert client.only_ext == {"username": USERNAME}
    assert client.only_body_text == wire(user_body())
    assert "userStatus" not in client.only_body


def test_create_user_object_body_keeps_all_four_keys_in_order():
    client = _FakeClient([_ok()])
    user = UserInfo(username=USERNAME, password="secret123",
                    user_type=UserType.SUPER, user_status=UserStatus.ENABLE)

    _admin(client).create_user(ADDR, user)

    assert client.only_request.code == 3001
    assert client.only_ext == {"username": USERNAME}
    assert client.only_body_text == wire(user_body(user_type=UserType.SUPER,
                                                  user_status=UserStatus.ENABLE))
    assert list(client.only_body.keys()) == ["username", "password", "userType",
                                              "userStatus"]


@pytest.mark.parametrize("name,literal,expected_ext", [
    ("create_user", 3001, {"username": USERNAME}),
    ("update_user", 3002, {"username": USERNAME}),
    ("create_acl", 3006, {"subject": SUBJECT}),
    ("update_acl", 3007, {"subject": SUBJECT}),
])
def test_convenience_form_is_equivalent_positionally_and_by_keyword(name, literal,
                                                                   expected_ext):
    """Both bindings of the convenience signature build the same request."""
    args, kwargs = CONVENIENCE_CALLS[name]
    client = _FakeClient([_ok(), _ok()])
    admin = _admin(client)

    getattr(admin, name)(ADDR, *args)
    getattr(admin, name)(ADDR, **kwargs)

    first, second = client.requests
    assert [r.code for _, r, _ in (first, second)] == [literal, literal]
    assert [r.ext_fields for _, r, _ in (first, second)] == [expected_ext, expected_ext]
    assert first[1].body == second[1].body
    assert first[0] == second[0] == ADDR


USER_CALL = {
    "create_user": ((USERNAME, "secret123", UserType.NORMAL),
                    {"user": USERNAME, "password": "secret123",
                     "user_type": UserType.NORMAL}),
    "update_user": ((USERNAME, "secret123", UserType.NORMAL),
                    {"user": USERNAME, "password": "secret123",
                     "user_type": UserType.NORMAL}),
    "create_acl": ((SUBJECT, ["TopicA"], ["Pub"], ["10.0.0.1"], Decision.ALLOW),
                   {"acl": SUBJECT, "resources": ["TopicA"], "actions": ["Pub"],
                    "source_ips": ["10.0.0.1"], "decision": Decision.ALLOW}),
    "update_acl": ((SUBJECT, ["TopicA"], ["Pub"], ["10.0.0.1"], Decision.ALLOW),
                   {"acl": SUBJECT, "resources": ["TopicA"], "actions": ["Pub"],
                    "source_ips": ["10.0.0.1"], "decision": Decision.ALLOW}),
}


def test_update_user_uses_its_own_code_and_the_same_body():
    client = _FakeClient([_ok()])
    _admin(client).update_user(ADDR, USERNAME, "rotated", UserType.NORMAL)
    assert client.only_request.code == 3002
    assert client.only_ext == {"username": USERNAME}
    assert client.only_body == user_body(password="rotated")


def test_delete_user_sends_the_header_only():
    client = _FakeClient([_ok()])
    admin = _admin(client)

    assert admin.delete_user(ADDR, USERNAME) is None

    assert client.only_request.code == 3003
    assert client.only_ext == {"username": USERNAME}
    assert client.only_request.body is None


def test_void_ops_never_read_a_response_body():
    """A void op only checks SUCCESS: an unparsable body must not reach a decoder."""
    client = _FakeClient([_ok(b"not-json-at-all"), _ok(b"{also: broken")])
    admin = _admin(client)

    assert admin.delete_user(ADDR, USERNAME) is None
    assert admin.delete_acl(ADDR, SUBJECT, "TopicA") is None
    assert len(client.requests) == 2


# ------------------------------------------------------------------ get / list user
def test_get_user_decodes_a_single_user_object():
    client = _FakeClient([_ok(wire(user_body(user_type=UserType.SUPER)).encode("utf-8"))])
    admin = _admin(client)

    user = admin.get_user(ADDR, USERNAME)

    assert client.only_request.code == 3004
    assert client.only_ext == {"username": USERNAME}
    assert isinstance(user, UserInfo)
    assert (user.username, user.password, user.user_type) == (
        USERNAME, "secret123", UserType.SUPER)
    assert user.user_status is None


def test_get_user_absent_body_decodes_to_none():
    client = _FakeClient([_ok(b"")])
    assert _admin(client).get_user(ADDR, USERNAME) is None


def test_get_user_partial_body_leaves_unset_fields_none():
    client = _FakeClient([_ok(b'{"username":"bob"}')])
    user = _admin(client).get_user(ADDR, "bob")
    assert (user.username, user.password, user.user_type, user.user_status) == (
        "bob", None, None, None)


def test_list_users_sends_the_filter_and_decodes_an_array():
    users = [user_body(username="a1"),
             user_body(username="a2", user_type=UserType.SUPER)]
    client = _FakeClient([_ok(wire(users).encode("utf-8"))])
    admin = _admin(client)

    result = admin.list_users(ADDR, filter="Topic%")

    assert client.only_request.code == 3005
    assert client.only_ext == {"filter": "Topic%"}
    assert [u.username for u in result] == ["a1", "a2"]
    assert result[1].user_type == UserType.SUPER
    assert all(u.user_status is None for u in result)


def test_list_users_omits_an_unset_filter_instead_of_sending_empty_text():
    """Control for "never send an empty string for an unset header field"."""
    client = _FakeClient([_ok(b"[]")])
    result = _admin(client).list_users(ADDR)
    assert client.only_ext == {}
    assert result == []


def test_list_users_empty_body_decodes_to_empty_list():
    for raw in (b"", b"[]"):
        client = _FakeClient([_ok(raw)])
        assert _admin(client).list_users(ADDR, filter="%") == []


def test_list_users_ragged_array_decodes_defensively():
    client = _FakeClient([_ok(b'[{"username":"ok"},"junk",{"username":"other"}]')])
    users = _admin(client).list_users(ADDR, filter="%")
    assert [u.username for u in users] == ["ok", "other"]


# ------------------------------------------------------------------ create / update acl
def test_create_acl_convenience_builds_one_untyped_policy():
    """Controls for "the convenience policy has no policyType" and "one entry per resource"."""
    client = _FakeClient([_ok()])
    admin = _admin(client)

    result = admin.create_acl(ADDR, SUBJECT, ["TopicA", "TopicB"], ["Pub", "Sub"],
                              ["10.0.0.1"], Decision.ALLOW)

    assert result is None
    assert client.only_request.code == 3006
    assert client.only_ext == {"subject": SUBJECT}
    assert client.only_body_text == wire(acl_body())
    policy = client.only_body["policies"][0]
    assert "policyType" not in policy
    assert list(policy.keys()) == ["entries"]
    assert [e["resource"] for e in policy["entries"]] == ["TopicA", "TopicB"]


def test_create_acl_convenience_repeats_actions_and_ips_per_resource():
    client = _FakeClient([_ok()])
    _admin(client).create_acl(ADDR, SUBJECT, ["TopicA", "TopicB", "TopicC"], ["Pub"],
                              ["10.0.0.1", "10.0.0.2"], Decision.DENY)
    assert client.only_body == acl_body(resources=("TopicA", "TopicB", "TopicC"),
                                        actions=("Pub",),
                                        source_ips=("10.0.0.1", "10.0.0.2"),
                                        decision=Decision.DENY)
    for entry in client.only_body["policies"][0]["entries"]:
        assert entry["actions"] == ["Pub"]
        assert entry["sourceIps"] == ["10.0.0.1", "10.0.0.2"]
        assert entry["decision"] == "Deny"


def test_create_acl_convenience_takes_a_single_resource_string():
    """A bare resource becomes one entry, not one entry per character."""
    client = _FakeClient([_ok()])
    _admin(client).create_acl(ADDR, SUBJECT, "TopicA", ["Pub"], ["10.0.0.1"],
                              Decision.ALLOW)
    assert client.only_body == acl_body(resources=("TopicA",), actions=("Pub",))


def test_create_acl_object_body_carries_the_policy_type():
    client = _FakeClient([_ok()])
    acl = AclInfo(subject=SUBJECT, policies=[
        AclPolicy(policy_type=PolicyType.CUSTOM,
                  entries=[AclEntry(resource="TopicA", actions=["Pub"],
                                    source_ips=["10.0.0.1"], decision=Decision.DENY)]),
        AclPolicy(policy_type=PolicyType.DEFAULT,
                  entries=[AclEntry(resource="TopicB", actions=["Sub"],
                                    source_ips=["10.0.0.2"], decision=Decision.ALLOW)]),
    ])

    _admin(client).create_acl(ADDR, acl)

    assert client.only_ext == {"subject": SUBJECT}
    body = client.only_body
    assert [p["policyType"] for p in body["policies"]] == ["Custom", "Default"]
    assert [e["resource"] for e in body["policies"][1]["entries"]] == ["TopicB"]
    assert client.only_body_text == wire({
        "subject": SUBJECT,
        "policies": [
            {"policyType": "Custom",
             "entries": [{"resource": "TopicA", "actions": ["Pub"],
                          "sourceIps": ["10.0.0.1"], "decision": "Deny"}]},
            {"policyType": "Default",
             "entries": [{"resource": "TopicB", "actions": ["Sub"],
                          "sourceIps": ["10.0.0.2"], "decision": "Allow"}]},
        ]})


def test_acl_object_round_trips_through_encode_and_decode():
    acl = AclInfo.for_resources(SUBJECT, ["TopicA"], ["Pub"], ["10.0.0.1"], Decision.ALLOW)
    back = AclInfo.decode(acl.encode())
    assert back.subject == SUBJECT
    assert len(back.policies) == 1
    assert back.policies[0].policy_type is None
    entry = back.policies[0].entries[0]
    assert (entry.resource, entry.actions, entry.source_ips, entry.decision) == (
        "TopicA", ["Pub"], ["10.0.0.1"], "Allow")


def test_update_acl_uses_its_own_code():
    client = _FakeClient([_ok()])
    _admin(client).update_acl(ADDR, SUBJECT, ["TopicA"], ["Pub"], ["10.0.0.1"],
                              Decision.ALLOW)
    assert client.only_request.code == 3007
    assert client.only_ext == {"subject": SUBJECT}
    assert client.only_body == acl_body(resources=("TopicA",), actions=("Pub",))


def test_delete_acl_carries_only_subject_and_resource():
    """Control for the delete header shape: no policy type, no resource list, no body."""
    client = _FakeClient([_ok()])
    admin = _admin(client)

    assert admin.delete_acl(ADDR, SUBJECT, "TopicA") is None

    request = client.only_request
    assert request.code == 3008
    assert request.ext_fields == {"subject": SUBJECT, "resource": "TopicA"}
    assert set(request.ext_fields) == {"subject", "resource"}
    assert "policyType" not in request.ext_fields
    assert request.body is None


def test_acl_entry_keeps_an_empty_list_distinct_from_an_absent_field():
    """A granted-with-no-actions entry is not the same request as one that omits actions."""
    empty = AclEntry.from_dict({"resource": "TopicA", "actions": [], "sourceIps": []})
    assert empty.to_dict() == {"resource": "TopicA", "actions": [], "sourceIps": []}
    absent = AclEntry.from_dict({"resource": "TopicA"})
    assert absent.to_dict() == {"resource": "TopicA"}


# ------------------------------------------------------------------ get / list acl
def test_get_acl_decodes_a_single_acl_object():
    acl = AclInfo.for_resources(SUBJECT, ["TopicA", "TopicB"], ["Pub", "Sub"],
                                ["10.0.0.1"], Decision.ALLOW)
    client = _FakeClient([_ok(acl.encode())])
    admin = _admin(client)

    got = admin.get_acl(ADDR, SUBJECT)

    assert client.only_request.code == 3009
    assert client.only_ext == {"subject": SUBJECT}
    assert isinstance(got, AclInfo)
    assert got.subject == SUBJECT
    assert len(got.policies) == 1
    assert got.policies[0].policy_type is None
    assert [e.resource for e in got.policies[0].entries] == ["TopicA", "TopicB"]
    assert got.policies[0].entries[1].actions == ["Pub", "Sub"]


def test_get_acl_absent_body_decodes_to_none():
    client = _FakeClient([_ok(b"")])
    assert _admin(client).get_acl(ADDR, SUBJECT) is None


def test_list_acls_sends_both_filters():
    acls = [AclInfo.for_resources("sub-a", ["TopicA"], ["Pub"], ["10.0.0.1"],
                                  Decision.ALLOW),
            AclInfo.for_resources("sub-b", ["TopicB"], ["Sub"], ["10.0.0.2"],
                                  Decision.DENY)]
    client = _FakeClient([_ok(wire([a.to_dict() for a in acls]).encode("utf-8"))])
    admin = _admin(client)

    result = admin.list_acls(ADDR, subject_filter="sub%", resource_filter="Topic%")

    assert client.only_request.code == 3010
    assert client.only_ext == {"subjectFilter": "sub%", "resourceFilter": "Topic%"}
    assert [a.subject for a in result] == ["sub-a", "sub-b"]
    assert result[1].policies[0].entries[0].decision == "Deny"


@pytest.mark.parametrize("kwargs,expected", [
    ({"subject_filter": "sub%"}, {"subjectFilter": "sub%"}),
    ({"resource_filter": "Topic%"}, {"resourceFilter": "Topic%"}),
    ({"subject_filter": "sub%", "resource_filter": "Topic%"},
     {"subjectFilter": "sub%", "resourceFilter": "Topic%"}),
    ({}, {}),
])
def test_list_acls_omits_the_filter_that_was_not_given(kwargs, expected):
    client = _FakeClient([_ok(b"[]")])
    assert _admin(client).list_acls(ADDR, **kwargs) == []
    assert client.only_ext == expected


def test_list_acls_empty_or_malformed_body_decodes_to_empty_list():
    for raw in (b"", b"[]", b'{"subject":"not-an-array"}'):
        client = _FakeClient([_ok(raw)])
        assert _admin(client).list_acls(ADDR, subject_filter="%") == []


# ------------------------------------------------------------------ Addressing
@pytest.mark.parametrize("name", sorted(CALLS))
def test_target_address_is_passed_through_unchanged(name):
    """No VIP rewrite, no master resolution, no NameServer indirection -- even with the
    VIP channel switch enabled."""
    odd = "10.20.30.40:10921"
    client = _FakeClient([_ok()])
    CALLS[name](_admin(client), odd)
    addr, _, _ = client.last
    assert addr == odd
    assert addr != "10.20.30.40:10919", "the VIP channel would move this to port 10919"


# ------------------------------------------------------------------ Result handling
@pytest.mark.parametrize("name,code,literal", AUTH_CODES)
def test_non_success_response_raises_with_code_and_remark(name, code, literal):
    """Every auth RPC goes through the SUCCESS-only gate; code and remark travel together."""
    client = _FakeClient([_err(ResponseCode.NO_PERMISSION, REMARK)])
    admin = _admin(client)

    with pytest.raises(MQBrokerException) as exc:
        CALLS[name](admin, ADDR)

    assert exc.value.response_code == ResponseCode.NO_PERMISSION
    assert exc.value.error_message == REMARK
    assert str(exc.value) == REMARK
    assert client.only_request.code == literal


def test_request_code_not_supported_is_not_swallowed():
    client = _FakeClient([_err(ResponseCode.REQUEST_CODE_NOT_SUPPORTED, REMARK)])
    with pytest.raises(MQBrokerException) as exc:
        _admin(client).get_user(ADDR, USERNAME)
    assert exc.value.response_code == 3


# ------------------------------------------------------------------ Timeout plumbing
def test_explicit_timeout_is_used_and_default_follows_the_admin_setting():
    client = _FakeClient([_ok(b"")])
    admin = _admin(client)
    admin.set_timeout_millis(7777)

    admin.get_user(ADDR, USERNAME)
    assert client.requests[0][2] == 7777

    client2 = _FakeClient([_ok(b"")])
    _admin(client2).get_user(ADDR, USERNAME, timeout_millis=1234)
    assert client2.requests[0][2] == 1234
