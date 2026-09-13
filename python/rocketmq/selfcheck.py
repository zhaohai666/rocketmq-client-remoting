# -*- coding: utf-8 -*-
"""协议自检（selfcheck）：无集群环境下的协议编解码回环验证。

覆盖：
  1. RemotingCommand JSON 序列化回环（code/remark/extFields/body）
  2. RemotingCommand ROCKETMQ 二进制序列化回环（含 ext_fields map）
  3. custom_header -> ext_fields -> decode_command_custom_header 字段映射
  4. 消息 17 段编码 -> decode_messages 回环（普通 topic 与超长 topic V2 魔数）
  5. AclRPCHook 签名注入与校验字段存在
"""
from __future__ import annotations

import struct
import sys

from .common.message import Message, MessageBatch, MessageExt
from .common.message_decoder import (MESSAGE_MAGIC_CODE_V2, create_message_id, crc32,
                                     decode_batch_messages, decode_messages,
                                     encode_message_ext, string2bytes)
from .remoting.protocol.headers import PutKVConfigRequestHeader, SendMessageRequestHeaderV2
from .remoting.protocol.remoting_command import RemotingCommand, SerializeType


def build_v2_frame(topic: str, body: bytes, properties_bytes: bytes) -> bytes:
    """手工构造 V2 魔数（topic 长度 2 字节）的 17 段帧，模拟 broker 写入超长 topic 的场景。"""
    topic_bytes = topic.encode("utf-8")
    store_size = (4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 8
                  + 4 + len(body) + 2 + len(topic_bytes) + 2 + len(properties_bytes))
    buf = bytearray()
    buf += struct.pack(">i", store_size)                                     # 1 TOTALSIZE
    buf += struct.pack(">i", MESSAGE_MAGIC_CODE_V2)                          # 2 MAGICCODE
    buf += struct.pack(">I", crc32(body))                                    # 3 BODYCRC
    buf += struct.pack(">i", 0)                                              # 4 QUEUEID
    buf += struct.pack(">i", 0)                                              # 5 FLAG
    buf += struct.pack(">q", 0)                                              # 6 QUEUEOFFSET
    buf += struct.pack(">q", 2048)                                           # 7 PHYSICALOFFSET
    buf += struct.pack(">i", 0)                                              # 8 SYSFLAG
    buf += struct.pack(">q", 1700000000000)                                  # 9 BORNTIMESTAMP
    buf += struct.pack(">I", 0x7F000001) + struct.pack(">I", 54321)          # 10 BORNHOST
    buf += struct.pack(">q", 1700000000123)                                  # 11 STORETIMESTAMP
    buf += struct.pack(">I", 0x7F000001) + struct.pack(">I", 10911)          # 12 STOREHOST
    buf += struct.pack(">i", 0)                                              # 13 RECONSUMETIMES
    buf += struct.pack(">q", 0)                                              # 14 PREPARED TX OFFSET
    buf += struct.pack(">i", len(body))
    buf += body                                                              # 15 BODY
    buf += struct.pack(">H", len(topic_bytes))
    buf += topic_bytes                                                       # 16 TOPIC
    buf += struct.pack(">H", len(properties_bytes))
    buf += properties_bytes                                                  # 17 PROPERTIES
    return bytes(buf)


def _check(name: str, cond: bool, detail: str = "") -> bool:
    status = "PASS" if cond else "FAIL"
    print("[%s] %s%s" % (status, name, (" - " + detail) if detail and not cond else ""))
    return cond


def run_selfcheck() -> int:
    results = []

    # 1) JSON 回环
    cmd = RemotingCommand.create_request_command(105)  # GET_ROUTEINFO_BY_TOPIC
    cmd.remark = "hello \u4e2d\u6587"
    cmd.ext_fields["topic"] = "TopicTest"
    cmd.ext_fields["namesrv"] = "127.0.0.1:9876"
    cmd.body = b"\x01\x02\x03payload"
    data = cmd.encode()
    dec = RemotingCommand.decode(data)
    results.append(_check("JSON round-trip",
                          dec.code == cmd.code and dec.remark == cmd.remark
                          and dec.flag == cmd.flag and dec.body == cmd.body
                          and dec.ext_fields == cmd.ext_fields and dec.opaque == cmd.opaque,
                          "code=%s remark=%s body=%s ext=%s" % (dec.code, dec.remark, dec.body, dec.ext_fields)))

    # 2) ROCKETMQ 二进制回环（强制该命令用 ROCKETMQ 序列化）
    cmd2 = RemotingCommand.create_request_command(105)
    cmd2.remark = "binary \u5b57\u6bb5"
    cmd2.ext_fields["topic"] = "TopicTest"
    cmd2.ext_fields["producerGroup"] = "PG"
    cmd2.body = b"payload-binary"
    cmd2.serialize_type_current_rpc = SerializeType.ROCKETMQ
    data2 = cmd2.encode()
    dec2 = RemotingCommand.decode(data2)
    results.append(_check("ROCKETMQ binary round-trip",
                          dec2.code == cmd2.code and dec2.remark == cmd2.remark
                          and dec2.body == cmd2.body and dec2.ext_fields == cmd2.ext_fields
                          and dec2.opaque == cmd2.opaque and dec2.serialize_type_current_rpc == SerializeType.ROCKETMQ,
                          "code=%s remark=%s ext=%s stype=%s" % (dec2.code, dec2.remark, dec2.ext_fields,
                                                                 dec2.serialize_type_current_rpc)))

    # 3) custom_header 序列化 + 反 decode 映射（V2 短字段）
    hdr = SendMessageRequestHeaderV2()
    hdr.producer_group = "PG_1"
    hdr.topic = "TopicTest"
    hdr.default_topic = "TBW102"
    hdr.default_topic_queue_nums = 4
    hdr.queue_id = 1
    hdr.sys_flag = 0
    hdr.born_timestamp = 1700000000000
    hdr.flag = 0
    hdr.reconsume_times = 0
    hdr.unit_mode = False
    hdr.consumer_retry_times = 0
    hdr.batch = False
    cmd3 = RemotingCommand.create_request_command(310, hdr)  # SEND_MESSAGE_V2
    cmd3.serialize_type_current_rpc = SerializeType.ROCKETMQ
    data3 = cmd3.encode()
    dec3 = RemotingCommand.decode(data3)
    h2 = dec3.decode_command_custom_header(SendMessageRequestHeaderV2)
    results.append(_check("custom_header V2 mapping",
                          h2.producer_group == "PG_1" and h2.topic == "TopicTest"
                          and h2.queue_id == 1 and h2.born_timestamp == 1700000000000
                          and h2.default_topic_queue_nums == 4,
                          "producerGroup=%s queueId=%s bornTs=%s" % (h2.producer_group, h2.queue_id,
                                                                      h2.born_timestamp)))

    # 4) 消息 17 段回环（MessageExt -> encode -> decode）
    ext = MessageExt()
    ext.set_topic("TopicTest")
    ext.set_body(b"hello rocketmq \xe4\xb8\xad\xe6\x96\x87")
    ext.set_flag(2)
    ext.set_body_crc(crc32(ext.get_body()))
    ext.set_queue_id(3)
    ext.set_sys_flag(0)
    ext.set_born_timestamp(1700000000000)
    ext.set_store_timestamp(1700000000123)
    ext.set_store_host("127.0.0.1")
    ext.store_host_port = 10911
    ext.set_born_host("127.0.0.1")
    ext.born_host_port = 54321
    ext.set_commit_log_offset(1024)
    ext.set_queue_offset(88)
    ext.set_reconsume_times(1)
    ext.set_properties({"TAGS": "TagA", "KEYS": "key1 key2"})
    raw = encode_message_ext(ext)
    exts = decode_messages(raw, read_body=True)
    ok = len(exts) == 1
    if ok:
        got = exts[0]
        ok = (got.get_topic() == "TopicTest" and got.get_body() == ext.get_body()
              and got.get_flag() == 2 and got.get_queue_id() == 3
              and got.get_queue_offset() == 88 and got.get_commit_log_offset() == 1024
              and got.get_born_timestamp() == 1700000000000
              and got.get_store_timestamp() == 1700000000123
              and got.store_host_port == 10911 and got.born_host_port == 54321
              and got.get_reconsume_times() == 1
              and got.get_body_crc() == crc32(ext.get_body())
              and got.get_properties().get("TAGS") == "TagA"
              and got.get_properties().get("KEYS") == "key1 key2"
              and got.get_msg_id() == create_message_id(
                  struct.pack(">I", 0x7F000001) + struct.pack(">I", 10911), 1024))
    results.append(_check("message 17-seg round-trip", ok,
                          "len=%d got=%s" % (len(exts), exts[0].get_msg_id() if exts else None)))

    # 5) V2 魔数超长 topic（>127 字符）解码：手工构造 broker 侧 V2 帧
    long_topic = "TopicTestLong" + "x" * 130
    raw2 = build_v2_frame(long_topic, b"batch-body", string2bytes("TAGS\x01TagB\x02"))
    exts2 = decode_messages(raw2, read_body=True)
    results.append(_check("long-topic V2 magic decode",
                          len(exts2) == 1 and exts2[0].get_topic() == long_topic
                          and exts2[0].get_body() == b"batch-body"
                          and exts2[0].get_properties().get("TAGS") == "TagB",
                          "topic_len=%d got=%s" % (len(long_topic), exts2[0].get_topic() if exts2 else None)))

    # 5.1) 批量消息（6 段轻量格式）回环
    batch = MessageBatch.generate_from_list([
        Message(topic="TopicTest", body=b"m1", tags="TagA"),
        Message(topic="TopicTest", body=b"m2", tags="TagA"),
    ])
    decoded_batch = decode_batch_messages(batch.get_body())
    results.append(_check("batch message 6-seg round-trip",
                          [m.get_body() for m in decoded_batch] == [b"m1", b"m2"]
                          and all(m.get_properties().get("TAGS") == "TagA" for m in decoded_batch),
                          "count=%d bodies=%s" % (len(decoded_batch), [m.get_body() for m in decoded_batch])))

    # 6) ACL RPCHook 资源签名注入
    from .remoting.rpchook import AclRPCHook, SessionCredentials
    cred = SessionCredentials(access_key="AK", secret_key="SK", security_token="token123")
    hook = AclRPCHook(cred)
    req = RemotingCommand.create_request_command(310, SendMessageRequestHeaderV2())
    hook.do_before_request("127.0.0.1:9876", req)
    results.append(_check("AclRPCHook signature injection",
                          "AccessKey" in req.ext_fields and "Signature" in req.ext_fields
                          and "SecurityToken" in req.ext_fields
                          and req.ext_fields.get("AccessKey") == "AK"
                          and req.ext_fields.get("SecurityToken") == "token123",
                          "keys=%s" % sorted(req.ext_fields.keys())))

    passed = all(results)
    print("")
    print("selfcheck: %d/%d passed" % (sum(1 for r in results if r), len(results)))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(run_selfcheck())