# -*- coding: utf-8 -*-
"""自动压缩（zlib）真实集群 + 跨客户端联调。

为什么需要它：单元测试只证明「zlib 往返正确」，证明不了
 1. producer 真的在超过 compressMsgBodyOverHowmuch(4096) 时压缩并置 COMPRESSED_FLAG；
 2. broker 存的是压缩体（storeSize 远小于原文）；
 3. 消费端真的解压并把 COMPRESSED_FLAG 清掉（对齐 Java MessageDecoder 第 520-523 行）；
 4. **别的客户端（Java）产生的压缩消息，我们能正确解压** —— 这是真机才暴露的静默数据损坏点。

载荷是**确定性**的（重复一行固定文本后截断），与 Java 探针 CompressProbe 完全相同，
因此两端各自本地重建后比较 CRC32 即可，无需交换文件。

⚠ CRC32 显示值会不同，这不是 bug：Java ``UtilAll.crc32`` 返回
``(int)(value & 0x7FFFFFFF)``，砍掉了最高位；本脚本用标准 CRC-32。
所以 Java 打印 1785582993 对应本脚本打印 3933066641（差正好 2^31）。
判定互通要看各自的 ``match=`` 字段，不要直接比两边打印的 CRC 数字。

用法：
    python verify_compression_live.py selftest
    python verify_compression_live.py send   <topic> <group> <size>
    python verify_compression_live.py recv   <topic> <group> <size>
"""
import os
import sys
import time
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Windows 控制台默认 GBK，docstring 里的 ⚠ 会让 print 直接抛 UnicodeEncodeError
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")

from rocketmq.client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message, MessageExt, MessageQueue
from rocketmq.common.message_decoder import encode_message_ext, decode_message
from rocketmq.common.sysflag import MessageSysFlag
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = os.environ.get("ROCKETMQ_NAMESRV", "127.0.0.1:9876")
LINE = b"rocketmq-compress-interop-payload-line-0123456789\n"

results = []


def build_payload(size):
    """与 Java CompressProbe.buildPayload 完全一致（必须逐字节相同）。"""
    out = bytearray()
    while len(out) < size:
        out += LINE
    return bytes(out[:size])


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def crc32(data):
    """与 Java UtilAll.crc32 同值（zlib.crc32 即标准 CRC-32）。"""
    return zlib.crc32(data) & 0xFFFFFFFF


def recv_one(topic, group, timeout_sec=30):
    """消费 1 条消息（CONSUME_FROM_FIRST_OFFSET），超时返回 None。"""
    got = []

    def on_msg(msgs):
        if not got:
            got.extend(msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    cons = DefaultMQPushConsumer(consumer_group=group)
    cons.set_namesrv_addr(NAMESRV)
    cons.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    cons.subscribe(topic, "*")
    cons.set_message_listener(SimpleMessageListener(on_msg))
    cons.start()
    deadline = time.time() + timeout_sec
    while time.time() < deadline and not got:
        time.sleep(0.25)
    cons.shutdown()
    return got[0] if got else None


def do_send(topic, group, size):
    payload = build_payload(size)
    prod = DefaultMQProducer(group)
    prod.set_namesrv_addr(NAMESRV)
    prod.set_send_msg_timeout(10000)
    prod.start()
    sr = prod.send(Message(topic, payload))
    prod.shutdown()
    print("SEND_OK len=%d crc32=%d msgId=%s" % (len(payload), crc32(payload), sr.msg_id))
    return 0


def do_recv(topic, group, size):
    payload = build_payload(size)
    m = recv_one(topic, group)
    if m is None:
        print("RECV_TIMEOUT")
        return 3
    body = m.get_body()
    ok = len(body) == size and crc32(body) == crc32(payload)
    print("RECV_OK len=%d crc32=%d storeSize=%s match=%d"
          % (len(body), crc32(body), getattr(m, "store_size", "?"), 1 if ok else 0))
    return 0 if ok else 1


def selftest():
    stamp = int(time.time())
    topic = "CompressLivePy_%d" % stamp
    group = "CompressLivePyGroup_%d" % stamp
    size = 8192  # 远超 compressMsgBodyOverHowmuch(4096)
    payload = build_payload(size)
    payload_crc = crc32(payload)
    print("payload len=%d crc32=%d（确定性载荷，与 Java 探针同算法）" % (size, payload_crc))

    # 1) 发出
    prod = DefaultMQProducer("CompressLivePyProducer_%d" % stamp)
    prod.set_namesrv_addr(NAMESRV)
    prod.set_send_msg_timeout(10000)
    prod.start()
    sr = prod.send(Message(topic, payload))
    prod.shutdown()
    ok_send = sr.send_status.name == "SEND_OK"
    check("发送 %dB 消息（触发自动压缩）" % size, ok_send, "msgId=%s" % sr.msg_id)

    # 2) 收回来，正文必须与原文逐字节一致（证明"压缩-存储-解压"闭环）
    m = recv_one(topic, group)
    if m is None:
        check("消费回压缩消息", False, "30s 超时")
        return 1
    body = m.get_body()
    store_size = getattr(m, "store_size", 0)
    check("消费回压缩消息", True, "len=%d storeSize=%d" % (len(body), store_size))
    check("解压后正文与原文一致（len + CRC32）",
          len(body) == size and crc32(body) == payload_crc,
          "期望 len=%d crc=%d 实际 len=%d crc=%d" % (size, payload_crc, len(body), crc32(body)))

    # 3) broker 里存的确实是压缩体：storeSize 应远小于原文长度
    check("broker 侧存储为压缩体（storeSize 远小于原文）",
          store_size > 0 and store_size < size // 2,
          "storeSize=%d 原文=%d 压缩比=%d:1"
          % (store_size, size, size // (store_size if store_size else 1)))

    # 4) 解压后 COMPRESSED_FLAG 必须被清掉（对齐 Java MessageDecoder 第 523 行）
    sys_flag = getattr(m, "sys_flag", 0)
    check("解压后 COMPRESSED_FLAG 已清除",
          not MessageSysFlag.is_compressed(sys_flag), "sysFlag=%d" % sys_flag)

    # 5) 小消息不应被压缩（阈值语义）
    small_topic = "CompressLivePySmall_%d" % stamp
    small_group = "CompressLivePySmallGroup_%d" % stamp
    small = b"tiny-payload-under-threshold"
    prod2 = DefaultMQProducer("CompressLivePySmallProducer_%d" % stamp)
    prod2.set_namesrv_addr(NAMESRV)
    prod2.start()
    prod2.send(Message(small_topic, small))
    prod2.shutdown()
    sm = recv_one(small_topic, small_group, 20)
    if sm is not None:
        check("小于阈值(4096)的消息不压缩",
              sm.get_body() == small and not MessageSysFlag.is_compressed(getattr(sm, "sys_flag", 0)),
              "len=%d storeSize=%s" % (len(sm.get_body()), getattr(sm, "store_size", "?")))
    else:
        check("小于阈值(4096)的消息不压缩", False, "20s 未消费到")

    # 6) 复现"线上带压缩标志"的编解码路径，确认标志位语义闭环
    probe = MessageExt(topic, payload)
    probe.sys_flag = MessageSysFlag.set_compression_type(
        MessageSysFlag.COMPRESSED_FLAG, MessageSysFlag.ZLIB_TYPE)
    stored = encode_message_ext(probe, need_compress=True)
    raw = decode_message(stored, decompress_body=False)
    restored = decode_message(stored, decompress_body=True)
    check("带压缩标志编码后存储体变小", len(stored) < size // 2,
          "stored=%d 原文=%d" % (len(stored), size))
    check("不解压解码拿到压缩字节、解压解码还原原文",
          raw is not None and restored is not None
          and len(raw.get_body()) < size
          and restored.get_body() == payload
          and not MessageSysFlag.is_compressed(restored.sys_flag),
          "rawLen=%s restoredLen=%s"
          % (len(raw.get_body()) if raw else "?", len(restored.get_body()) if restored else "?"))

    failed = [r for r in results if not r[1]]
    print("\n==== compression live summary ====")
    print("%s (pass=%d fail=%d)" % ("ALL PASS" if not failed else "FAILED",
                                    len(results) - len(failed), len(failed)))
    return 1 if failed else 0


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "selftest"
    if mode == "send":
        return do_send(sys.argv[2], sys.argv[3], int(sys.argv[4]))
    if mode == "recv":
        return do_recv(sys.argv[2], sys.argv[3], int(sys.argv[4]))
    if mode != "selftest":
        print(__doc__)
        return 2
    return selftest()


if __name__ == "__main__":
    sys.exit(main())
