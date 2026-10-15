# -*- coding: utf-8 -*-
import os
import sys
import time
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# The Windows console defaults to GBK, and the ⚠ in docstrings would make print raise UnicodeEncodeError directly
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
    """Consume n messages (CONSUME_FROM_FIRST_OFFSET); on timeout return what has been received (possibly fewer than n)."""
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
    """Consume 1 message (CONSUME_FROM_FIRST_OFFSET); on timeout return None."""
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
    # Below the threshold nothing is compressed; the matrix wants "really compressed", so the caller supplies a large enough payload (default threshold 4 KiB).
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
    size = 8192  # far above compressMsgBodyOverHowmuch(4096)
    payload = build_payload(size)
    payload_crc = crc32(payload)
    print("payload len=%d crc32=%d (deterministic payload, crc32 uses the same algorithm as the probe)" % (size, payload_crc))

    # 1) send
    prod = DefaultMQProducer("CompressLivePyProducer_%d" % stamp)
    prod.set_namesrv_addr(NAMESRV)
    prod.set_send_msg_timeout(10000)
    prod.start()
    sr = prod.send(Message(topic, payload))
    prod.shutdown()
    ok_send = sr.send_status.name == "SEND_OK"
    check("send a %dB message (trigger auto-compression)" % size, ok_send, "msgId=%s" % sr.msg_id)

    # 2) receive it back; the body must be byte-for-byte identical to the original (proving the "compress-store-decompress" loop)
    m = recv_one(topic, group)
    if m is None:
        check("consume the compressed message back", False, "30s timeout")
        return 1
    body = m.get_body()
    store_size = getattr(m, "store_size", 0)
    check("consume the compressed message back", True, "len=%d storeSize=%d" % (len(body), store_size))
    check("decompressed body matches the original (len + CRC32)",
          len(body) == size and crc32(body) == payload_crc,
          "expected len=%d crc=%d actual len=%d crc=%d" % (size, payload_crc, len(body), crc32(body)))

    # 3) the broker really stores a compressed body: storeSize should be far smaller than the original length
    check("broker-side storage is a compressed body (storeSize far smaller than the original)",
          store_size > 0 and store_size < size // 2,
          "storeSize=%d original=%d ratio=%d:1"
          % (store_size, size, size // (store_size if store_size else 1)))

    sys_flag = getattr(m, "sys_flag", 0)
    check("COMPRESSED_FLAG is cleared after decompression",
          not MessageSysFlag.is_compressed(sys_flag), "sysFlag=%d" % sys_flag)

    # 5) small messages must not be compressed (threshold semantics)
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
        check("messages below the threshold (4096) are not compressed",
              sm.get_body() == small and not MessageSysFlag.is_compressed(getattr(sm, "sys_flag", 0)),
              "len=%d storeSize=%s" % (len(sm.get_body()), getattr(sm, "store_size", "?")))
    else:
        check("messages below the threshold (4096) are not compressed", False, "not consumed within 20s")

    # 6) reproduce the codec path of a "wire message with the compression flag set", confirming the flag-bit semantics loop
    probe = MessageExt(topic, payload)
    probe.sys_flag = MessageSysFlag.set_compression_type(
        MessageSysFlag.COMPRESSED_FLAG, MessageSysFlag.ZLIB_TYPE)
    stored = encode_message_ext(probe, need_compress=True)
    raw = decode_message(stored, decompress_body=False)
    restored = decode_message(stored, decompress_body=True)
    check("the stored body shrinks after encoding with the compression flag", len(stored) < size // 2,
          "stored=%d original=%d" % (len(stored), size))
    check("decoding without decompression yields compressed bytes, decoding with decompression restores the original",
          raw is not None and restored is not None
          and len(raw.get_body()) < size
          and restored.get_body() == payload
          and not MessageSysFlag.is_compressed(restored.sys_flag),
          "rawLen=%s restoredLen=%s"
          % (len(raw.get_body()) if raw else "?", len(restored.get_body()) if restored else "?"))

    # 7) run the other two backends through the live loop as well: producer compression -> broker stores compressed body -> consumer decompresses back.
    #    the difference from the zlib line is that "our own send path really used this backend"; print SKIP when the package is missing,
    #    which does not count as failure (message_decoder explicitly raises when lz4/zstandard are missing, it never silently passes through).
    import importlib.util
    for backend_name, ctype, mod in (
            ("lz4", MessageSysFlag.LZ4_TYPE, "lz4.frame"),
            ("zstd", MessageSysFlag.ZSTD_TYPE, "zstandard"),
    ):
        tag = "live round trip for backend %s" % backend_name
        if importlib.util.find_spec(mod) is None:
            print("[SKIP] %s  %s is not installed in this venv" % (tag, mod.split(".")[0]))
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
            check(tag, False, "not consumed within 30s")
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
    check("reusing one Message: the caller's body is not rewritten in place after sending (prevBody restored)",
          len(after_first) == size and crc32(after_first) == payload_crc,
          "after the first send len=%d crc=%d (failure=the caller got the compressed stream)"
          % (len(after_first), crc32(after_first)))
    rms = recv_n(reuse_topic, reuse_group, 2)
    check("reusing one Message: both live round trips restore the original",
          len(rms) == 2 and all(
              len(x.get_body()) == size and crc32(x.get_body()) == payload_crc for x in rms),
          "received %d storeSize=%s len=%s"
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
