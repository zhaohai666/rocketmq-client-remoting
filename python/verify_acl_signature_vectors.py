# -*- coding: utf-8 -*-
import sys

sys.path.insert(0, ".")

from remoting.protocol.remoting_command import RemotingCommand
from remoting.rpchook import AclClientRPCHook, SessionCredentials

FAIL = 0


def check(name, got, want):
    global FAIL
    ok = got == want
    if not ok:
        FAIL += 1
    print("[%s] %s" % ("PASS" if ok else "FAIL", name))
    if not ok:
        print("      got  = %r" % (got,))
        print("      want = %r" % (want,))


def build(ext, body):
    cmd = RemotingCommand.create_request_command(310, None)
    cmd.ext_fields = dict(ext)
    cmd.body = body
    return cmd


# ---- V1 ----
V1_EXT = {"topic": "MyTopic", "producerGroup": "MyGroup", "a": "1", "Zz": "last", "batch": "false"}
V1_HEX = "414b5f544553546c6173743166616c73654d7947726f75704d79546f7069630102030405"
V1_SIG = "qQhdzvXfV+g0r8LdwNCt+chJ4XY="

cmd = build(V1_EXT, bytes([1, 2, 3, 4, 5]))
hook = AclClientRPCHook(SessionCredentials("AK_TEST", "SK_TEST_SECRET_12345678"))
hook.do_before_request("127.0.0.1:9876", cmd)
check("V1 content_hex", AclClientRPCHook.build_request_content(cmd).hex(), V1_HEX)
check("V1 signature", cmd.ext_fields["Signature"], V1_SIG)
check("V1 AccessKey extField", cmd.ext_fields["AccessKey"], "AK_TEST")

# ---- V2 (with SecurityToken) ----
V2_HEX = ("414b5f54455354544f4b454e2d4142436c6173743166616c73654d7947726f7570"
          "4d79546f7069630102030405")
V2_SIG = "5YIp2FNQL8pxQP3w6YKnSv3kAsw="

cmd = build(V1_EXT, bytes([1, 2, 3, 4, 5]))
hook = AclClientRPCHook(SessionCredentials("AK_TEST", "SK_TEST_SECRET_12345678", "TOKEN-ABC"))
hook.do_before_request("127.0.0.1:9876", cmd)
check("V2 content_hex", AclClientRPCHook.build_request_content(cmd).hex(), V2_HEX)
check("V2 signature", cmd.ext_fields["Signature"], V2_SIG)
check("V2 SecurityToken extField", cmd.ext_fields["SecurityToken"], "TOKEN-ABC")

# ---- V3 (no body) ----
cmd = build({"a": "1", "b": "2"}, None)
hook = AclClientRPCHook(SessionCredentials("AK", "SK"))
hook.do_before_request("127.0.0.1:9876", cmd)
check("V3 content_hex", AclClientRPCHook.build_request_content(cmd).hex(), "414b3132")
check("V3 signature", cmd.ext_fields["Signature"], "d3vJKL2iRdr4ZykZfY+lxfQlfdc=")

# ---- V4 (pre-existing Signature must be excluded) ----
cmd = build({"topic": "MyTopic", "Signature": "SHOULD_BE_EXCLUDED"}, None)
check("V4 Signature excluded", AclClientRPCHook.build_request_content(cmd), b"MyTopic")

# ---- V5 order independence: extFields with equal values but different insertion order must yield the same signature ----
a = build({"topic": "MyTopic", "producerGroup": "MyGroup", "a": "1", "Zz": "last", "batch": "false"},
          bytes([1, 2, 3, 4, 5]))
b = build({"batch": "false", "Zz": "last", "a": "1", "producerGroup": "MyGroup", "topic": "MyTopic"},
          bytes([1, 2, 3, 4, 5]))
h = AclClientRPCHook(SessionCredentials("AK_TEST", "SK_TEST_SECRET_12345678"))
h.do_before_request("x", a)
h.do_before_request("x", b)
check("V5 insertion order irrelevant", a.ext_fields["Signature"], b.ext_fields["Signature"])

print("")
print("acl python cross-check: %s" % ("ALL PASS" if FAIL == 0 else "%d FAILED" % FAIL))
sys.exit(1 if FAIL else 0)
