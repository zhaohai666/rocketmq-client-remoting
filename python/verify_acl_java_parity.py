# -*- coding: utf-8 -*-
"""对拍：Python AclClientRPCHook vs Java AclClientRPCHook（AclProbe 输出的固定向量）。

Java 侧向量（/tmp/aclprobe/AclProbe.java 输出）：
  V1 sk=SK_TEST_SECRET_12345678 ext{topic=MyTopic,producerGroup=MyGroup,a=1,Zz=last,batch=false}
     body=0102030405  content_hex=414b5f544553546c6173743166616c73654d7947726f75704d79546f7069630102030405
     signature=qQhdzvXfV+g0r8LdwNCt+chJ4XY=
  V2 + SecurityToken=TOKEN-ABC
     content_hex=414b5f54455354544f4b454e2d4142436c6173743166616c73654d7947726f75704d79546f7069630102030405
     signature=5YIp2FNQL8pxQP3w6YKnSv3kAsw=
  V3 sk=SK ext{a=1,b=2} body=None  content_hex=414b3132  signature=d3vJKL2iRdr4ZykZfY+lxfQlfdc=
"""
import sys

sys.path.insert(0, ".")

from rocketmq.remoting.protocol.remoting_command import RemotingCommand
from rocketmq.remoting.rpchook import AclClientRPCHook, SessionCredentials

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

# ---- V2 (带 SecurityToken) ----
V2_HEX = ("414b5f54455354544f4b454e2d4142436c6173743166616c73654d7947726f7570"
          "4d79546f7069630102030405")
V2_SIG = "5YIp2FNQL8pxQP3w6YKnSv3kAsw="

cmd = build(V1_EXT, bytes([1, 2, 3, 4, 5]))
hook = AclClientRPCHook(SessionCredentials("AK_TEST", "SK_TEST_SECRET_12345678", "TOKEN-ABC"))
hook.do_before_request("127.0.0.1:9876", cmd)
check("V2 content_hex", AclClientRPCHook.build_request_content(cmd).hex(), V2_HEX)
check("V2 signature", cmd.ext_fields["Signature"], V2_SIG)
check("V2 SecurityToken extField", cmd.ext_fields["SecurityToken"], "TOKEN-ABC")

# ---- V3 (无 body) ----
cmd = build({"a": "1", "b": "2"}, None)
hook = AclClientRPCHook(SessionCredentials("AK", "SK"))
hook.do_before_request("127.0.0.1:9876", cmd)
check("V3 content_hex", AclClientRPCHook.build_request_content(cmd).hex(), "414b3132")
check("V3 signature", cmd.ext_fields["Signature"], "d3vJKL2iRdr4ZykZfY+lxfQlfdc=")

# ---- V4 (预先存在的 Signature 必须被排除) ----
cmd = build({"topic": "MyTopic", "Signature": "SHOULD_BE_EXCLUDED"}, None)
check("V4 Signature 被排除", AclClientRPCHook.build_request_content(cmd), b"MyTopic")

# ---- V5 顺序无关性：插入顺序不同的等价 extFields 必须得到同一签名 ----
a = build({"topic": "MyTopic", "producerGroup": "MyGroup", "a": "1", "Zz": "last", "batch": "false"},
          bytes([1, 2, 3, 4, 5]))
b = build({"batch": "false", "Zz": "last", "a": "1", "producerGroup": "MyGroup", "topic": "MyTopic"},
          bytes([1, 2, 3, 4, 5]))
h = AclClientRPCHook(SessionCredentials("AK_TEST", "SK_TEST_SECRET_12345678"))
h.do_before_request("x", a)
h.do_before_request("x", b)
check("V5 插入顺序无关", a.ext_fields["Signature"], b.ext_fields["Signature"])

print("")
print("acl python cross-check: %s" % ("ALL PASS" if FAIL == 0 else "%d FAILED" % FAIL))
sys.exit(1 if FAIL else 0)
