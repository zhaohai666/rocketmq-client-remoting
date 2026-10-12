# -*- coding: utf-8 -*-
import os
import sys
import time
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Windows 控制台默认 GBK，docstring 里的 ⚠ 会让 print 直接抛 UnicodeEncodeError
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")

from client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from client.consumer_result import ConsumeConcurrentlyStatus
from client.producer import DefaultMQProducer
from common.message import Message, MessageExt, MessageQueue
from common.message_decoder import encode_message_ext, decode_message
from common.sysflag import MessageSysFlag
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = os.environ.get("ROCKETMQ_NAMESRV", "127.0.0.1:9876")
LINE = b"rocketmq-compress-interop-payload-line-0123456789\n"

results = []


def build_payload(size):
    pass
    out = bytearray()
    while len(out) < size:
        out += LINE
    return bytes(out[:size])


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def crc32(data):
    pass
    return zlib.crc32(data) & 0xFFFFFFFF


def recv_n(topic, group, n, timeout_sec=30):
    """消费 n 条消息（CONSUME_FROM_FIRST_OFFSET），超时返回已收到的（可能不足 n）。"""
    got = []

    def on_msg(msgs):
        got.extend(m for m in msgs if m.topic == topic)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    cons = DefaultMQPushConsumer(consumer_group=group)
    cons.set_namesrv_addr(NAMESRV)
    cons.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    cons.subscribe(topic, "*")
    cons.set_message_listener(SimpleMessageListener(on_msg))
    cons.start()
    deadline = time.time() + timeout_sec
    while time.time() < deadline and len(got) < n:
        time.sleep(0.25)
    cons.shutdown()
    return got


def recv_one(topic, group, timeout_sec=30):
    """消费 1 条消息（CONSUME_FROM_FIRST_OFFSET），超时返回 None。"""
    got = recv_n(topic, group, 1, timeout_sec)
    return got[0] if got else None


CODEC_NAMES = {"zlib": MessageSysFlag.ZLIB_TYPE,
               "lz4": MessageSysFlag.LZ4_TYPE,
               "zstd": MessageSysFlag.ZSTD_TYPE}


def do_send(topic, group, size, codec="zlib"):
    ctype = CODEC_NAMES.get(codec)
    if ctype is None:
        print("SEND_FAIL unknown codec=%s" % codec, file=sys.stderr)
        return 2
    payload = build_payload(size)
    prod = DefaultMQProducer(group)
    prod.set_namesrv_addr(NAMESRV)
    prod.set_send_msg_timeout(10000)
    # 阈值以下不会压缩，矩阵要的是「真的压过」，所以调用方给够载荷尺寸（默认阈值 4 KiB）。
    prod.set_compress_type(ctype)
    prod.start()
    sr = prod.send(Message(topic, payload))
    prod.shutdown()
    print("SEND_OK codec=%s len=%d crc32=%d msgId=%s"
          % (codec, len(payload), crc32(payload), sr.msg_id))
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

    # 7) 另外两个后端各过一遍真机闭环：生产端压缩 → broker 存压缩体 → 消费端解压还原。
    #    与 zlib 那条的区别在于「自己的发送路径真的用上了这个后端」；缺包时打 SKIP，
    #    不算失败（message_decoder 在缺 lz4/zstandard 时是**明确抛错**的，不会静默透传）。
    import importlib.util
    for backend_name, ctype, mod in (
            ("lz4", MessageSysFlag.LZ4_TYPE, "lz4.frame"),
            ("zstd", MessageSysFlag.ZSTD_TYPE, "zstandard"),
    ):
        tag = "后端 %s 真机往返" % backend_name
        if importlib.util.find_spec(mod) is None:
            print("[SKIP] %s  本 venv 没装 %s" % (tag, mod.split(".")[0]))
            continue
        bt = "CompressLivePy_%s_%d" % (backend_name, stamp)
        bg = "CompressLivePyGroup_%s_%d" % (backend_name, stamp)
        bp = DefaultMQProducer("CompressLivePyProducer_%s_%d" % (backend_name, stamp))
        bp.set_namesrv_addr(NAMESRV)
        bp.set_send_msg_timeout(10000)
        bp.set_compress_type(ctype)
        bp.start()
        bp.send(Message(bt, payload))
        bp.shutdown()
        bm = recv_one(bt, bg)
        if bm is None:
            check(tag, False, "30s 未消费到")
            continue
        bbody = bm.get_body()
        bstore = getattr(bm, "store_size", 0) or 0
        check(tag,
              len(bbody) == size and crc32(bbody) == payload_crc
              and 0 < bstore < size // 2
              and not MessageSysFlag.is_compressed(getattr(bm, "sys_flag", 0)),
              "len=%d storeSize=%d sysFlag=%d" % (len(bbody), bstore, getattr(bm, "sys_flag", 0)))

    reuse_topic = "CompressLivePyReuse_%d" % stamp
    reuse_group = "CompressLivePyReuseGroup_%d" % stamp
    rp = DefaultMQProducer("CompressLivePyReuseProducer_%d" % stamp)
    rp.set_namesrv_addr(NAMESRV)
    rp.set_send_msg_timeout(10000)
    rp.start()
    reused = Message(reuse_topic, payload)
    rp.send(reused)
    after_first = reused.get_body()
    rp.send(reused)
    rp.shutdown()
    check("复用同一条 Message：发送后调用方 body 未被就地改写（prevBody 还原）",
          len(after_first) == size and crc32(after_first) == payload_crc,
          "第一次发送后 len=%d crc=%d（失败=调用方拿到了压缩流）"
          % (len(after_first), crc32(after_first)))
    rms = recv_n(reuse_topic, reuse_group, 2)
    check("复用同一条 Message：两次真机往返都还原原文",
          len(rms) == 2 and all(
              len(x.get_body()) == size and crc32(x.get_body()) == payload_crc for x in rms),
          "收到 %d 条 storeSize=%s len=%s"
          % (len(rms), [getattr(x, "store_size", "?") for x in rms],
             [len(x.get_body()) for x in rms]))

    failed = [r for r in results if not r[1]]
    print("\n==== compression live summary ====")
    print("%s (pass=%d fail=%d)" % ("ALL PASS" if not failed else "FAILED",
                                    len(results) - len(failed), len(failed)))
    return 1 if failed else 0


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "selftest"
    if mode == "send":
        return do_send(sys.argv[2], sys.argv[3], int(sys.argv[4]),
                       sys.argv[5] if len(sys.argv) > 5 else "zlib")
    if mode == "recv":
        return do_recv(sys.argv[2], sys.argv[3], int(sys.argv[4]))
    if mode != "selftest":
        print(__doc__)
        return 2
    return selftest()


if __name__ == "__main__":
    sys.exit(main())
