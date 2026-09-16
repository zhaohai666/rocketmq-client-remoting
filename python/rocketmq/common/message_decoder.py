# -*- coding: utf-8 -*-
"""消息二进制编解码（对应 org.apache.rocketmq.common.message.MessageDecoder）。

本模块严格对齐 Java 侧的两条编码路径，切勿混用：

1) 17 段存储格式 ``MessageDecoder.encode(MessageExt, needCompress)`` / ``decode(ByteBuffer)``
   用于 broker 写入与 pull/get 返回的消息体：

   ======= ================================ ======================================
   序号     字段                             编码
   ======= ================================ ======================================
   1        TOTALSIZE                        int(4)
   2        MAGICCODE                        int(4)  -626843481 (v1) / -626843477 (v2)
   3        BODYCRC                          int(4)
   4        QUEUEID                          int(4)
   5        FLAG                             int(4)
   6        QUEUEOFFSET                      long(8)
   7        PHYSICALOFFSET                   long(8)
   8        SYSFLAG                          int(4)
   9        BORNTIMESTAMP                    long(8)
   10       BORNHOST                         4|16B ip + 4B port
   11       STORETIMESTAMP                   long(8)
   12       STOREHOST                        4|16B ip + 4B port
   13       RECONSUMETIMES                   int(4)
   14       PREPAREDTRANSACTIONOFFSET        long(8)
   15       BODY                             int(4) len + body
   16       TOPIC                            1B(v1)|2B(v2) len + topic
   17       PROPERTIES                       short(2) len + k\\x01v\\x02 串
   ======= ================================ ======================================

2) 6 段轻量格式 ``MessageDecoder.encodeMessage(Message)`` / ``decodeMessage(ByteBuffer)``
   仅用于**批量消息**（MessageBatch 的 body），不含 topic / crc：

   TOTALSIZE(4) | MAGICCODE(4, 固定 0) | BODYCRC(4, 固定 0) | FLAG(4)
   | BODY(4+len) | PROPERTIES(2+len)
"""
from __future__ import annotations

import socket
import struct
import zlib
from typing import Dict, List, Optional, Sequence, Tuple

from .message import Message, MessageExt, MessageBatch
from .message_const import MessageConst
from .sysflag import MessageSysFlag

CHARSET_UTF8 = "utf-8"
NAME_VALUE_SEPARATOR = 1
PROPERTY_SEPARATOR = 2

MESSAGE_MAGIC_CODE = -626843481
MESSAGE_MAGIC_CODE_V2 = -626843477
BLANK_MAGIC_CODE = -875286124

# 字段固定偏移（与 Java MessageDecoder 常量一致）
MESSAGE_MAGIC_CODE_POSITION = 4
MESSAGE_FLAG_POSITION = 16
MESSAGE_PHYSIC_OFFSET_POSITION = 28
QUEUE_OFFSET_POSITION = 4 + 4 + 4 + 4 + 4
PHY_POS_POSITION = 4 + 4 + 4 + 4 + 4 + 8
SYSFLAG_POSITION = 4 + 4 + 4 + 4 + 4 + 8 + 8
MESSAGE_STORE_TIMESTAMP_POSITION = 56

_HEX_TABLE = "0123456789ABCDEF"


# ---------------------------------------------------------------- 基础工具


def string2bytes(s: str) -> bytes:
    """UTF-8 编码（Java MessageDecoder 内的私有工具）。"""
    return s.encode(CHARSET_UTF8)


def bytes2string(bs: bytes) -> str:
    """Java UtilAll.bytes2string：逐字节转**大写**十六进制（msgId 依赖大小写）。"""
    return "".join(_HEX_TABLE[(b >> 4) & 0x0F] + _HEX_TABLE[b & 0x0F] for b in bs)


def string2bytes_hex(hex_string: Optional[str]) -> Optional[bytes]:
    """Java UtilAll.string2bytes：十六进制字符串 -> 字节。"""
    if not hex_string:
        return None
    try:
        return bytes.fromhex(hex_string)
    except ValueError:
        return None


def ip_and_port_to_bytes(ip: str, port: int, v6: bool = False) -> bytes:
    addr = _ip_to_bytes(ip, v6)
    return addr + struct.pack(">I", port)


def _ip_to_bytes(ip: str, v6: bool = False) -> bytes:
    if v6:
        return socket.inet_pton(socket.AF_INET6, ip)
    return socket.inet_pton(socket.AF_INET, ip)


def bytes_to_ip_and_port(raw: bytes) -> Tuple[str, int]:
    if len(raw) == 8:
        ip = socket.inet_ntop(socket.AF_INET, raw[:4])
        port = struct.unpack(">I", raw[4:8])[0]
    else:
        ip = socket.inet_ntop(socket.AF_INET6, raw[:16])
        port = struct.unpack(">I", raw[16:20])[0]
    return ip, port


def crc32(data: bytes) -> int:
    """Java UtilAll.crc32 使用标准 CRC32（poly 0xEDB88320），与 zlib.crc32 一致。"""
    return zlib.crc32(data) & 0xFFFFFFFF


# ------------------------------------------------------- 属性串 <-> Map


def message_properties_2_string(properties: Optional[Dict[str, str]]) -> str:
    """Java MessageDecoder.messageProperties2String：k\\x01v\\x02 逐项拼接。"""
    if properties is None:
        return ""
    parts = []
    for name, value in properties.items():
        if value is None:
            continue
        parts.append("%s%c%s%c" % (name, NAME_VALUE_SEPARATOR, value, PROPERTY_SEPARATOR))
    return "".join(parts)


def string_2_message_properties(properties_str: Optional[str]) -> Dict[str, str]:
    """Java MessageDecoder.string2messageProperties。"""
    result: Dict[str, str] = {}
    if not properties_str:
        return result
    length = len(properties_str)
    index = 0
    while index < length:
        new_index = properties_str.find(chr(PROPERTY_SEPARATOR), index)
        if new_index < 0:
            new_index = length
        if new_index - index >= 3:
            kv_sep = properties_str.find(chr(NAME_VALUE_SEPARATOR), index)
            if kv_sep > index and kv_sep < new_index - 1:
                result[properties_str[index:kv_sep]] = properties_str[kv_sep + 1:new_index]
        index = new_index + 1
    return result


# ---------------------------------------------------------------- msgId


def create_message_id(addr_bytes: bytes, offset: int) -> str:
    """ip+port(8 或 20B) + 8B commitLogOffset -> 十六进制 msgId。"""
    return bytes2string(bytes(addr_bytes) + struct.pack(">q", offset))


def decode_message_id(msg_id: str) -> Tuple[str, int, int]:
    """Java MessageDecoder.decodeMessageId -> (ip, port, offset)。"""
    raw = bytes.fromhex(msg_id)
    ip_len = 4 if len(raw) == 16 else 16
    family = socket.AF_INET if ip_len == 4 else socket.AF_INET6
    ip = socket.inet_ntop(family, raw[:ip_len])
    port = struct.unpack(">I", raw[ip_len:ip_len + 4])[0]
    offset = struct.unpack(">q", raw[ip_len + 4:ip_len + 12])[0]
    return ip, port, offset


# ------------------------------------------------- 压缩 / 解压（可选依赖）


def normalize_compression_type(compression_type: int) -> int:
    """把 sysFlag 里解出的压缩类型归一化到"真实算法"。

    对齐 Java ``CompressionType.findByValue`` 的向后兼容映射::

        case 1: return LZ4;
        case 2: return ZSTD;
        case 0: // To be compatible for older versions without compression type
        case 3: return ZLIB;

    即**类型位为 0 的老版本压缩消息按 ZLIB 处理**。这是必需的：老版本客户端
    （无类型位能力）产出的压缩消息类型位就是 0，若不映射到 ZLIB，解压会失败，
    而外层又会照样清掉 COMPRESSED_FLAG，结果是**静默返回压缩字节流**——数据损坏
    且事后无法识别。
    """
    if compression_type == 0:
        return MessageSysFlag.ZLIB_TYPE
    return compression_type


def _unsupported(compression_type: int) -> RuntimeError:
    """对应 Java ``CompressorFactory.getCompressor`` 在未知类型时抛的异常。

    Java 的 ``CompressionType.findByValue`` 只认 0/3(ZLIB)、1(LZ4)、2(ZSTD)，
    其余返回 null，``CompressorFactory`` 随即抛 ``IllegalArgumentException``。
    **绝不能原样透传**：调用方（``decode_message``）在解压后会清掉
    ``COMPRESSED_FLAG``，透传等于把压缩字节流当正文交出去且事后无法识别，
    属于静默数据损坏。C++ 侧同语义（``CompressorFactory::decompress`` 抛 runtime_error）。
    """
    return RuntimeError("unsupported compression type: %d" % compression_type)


def _compress(data: bytes, compression_type: int, level: int = 5) -> bytes:
    ctype = normalize_compression_type(compression_type)
    if ctype == MessageSysFlag.ZLIB_TYPE:
        return zlib.compress(data, level)
    if ctype == MessageSysFlag.LZ4_TYPE:
        return _lz4_frame().compress(data)
    if ctype == MessageSysFlag.ZSTD_TYPE:
        return _zstd().compress(data)
    raise _unsupported(compression_type)


def _decompress(data: bytes, compression_type: int) -> bytes:
    ctype = normalize_compression_type(compression_type)
    if ctype == MessageSysFlag.ZLIB_TYPE:
        return zlib.decompress(data)
    if ctype == MessageSysFlag.LZ4_TYPE:
        return _lz4_frame().decompress(data)
    if ctype == MessageSysFlag.ZSTD_TYPE:
        return _zstd().decompress(data)
    raise _unsupported(compression_type)


def decompress_body(data: bytes, compression_type: int) -> bytes:
    """按压缩类型解压（``_decompress`` 的公开入口）。

    除 ``decode_message`` 之外还有第二个调用方：Request-Reply 的应答是从
    ``PUSH_REPLY_MESSAGE_TO_CLIENT(326)`` 直接推过来的裸包，不走消息解码路径，
    需要自己按 ``sysFlag`` 判断并解压（对齐 Java
    ``ClientRemotingProcessor#receiveReplyMessage`` 里的 Compressor 分支）。
    """
    return _decompress(data, compression_type)


class _Lz4Codec:
    """LZ4 Frame 编解码器（Java lz4-java 的 Frame 格式）。

    Java 侧用 ``LZ4FrameOutputStream`` / ``LZ4FrameInputStream``，Python 的
    ``lz4.frame`` 实现的是同一个 LZ4 Frame 规范，二者字节互通。

    未安装 ``lz4`` 时**抛错而非静默透传**：静默透传会把压缩字节流当正文返回，
    属不可察觉的数据损坏（对齐 Java 抛异常的行为）。
    """

    @staticmethod
    def _mod():
        try:
            import lz4.frame  # type: ignore
        except ImportError as e:  # pragma: no cover - 取决于环境
            raise RuntimeError(
                "lz4 compression requires the 'lz4' package: pip install lz4"
            ) from e
        return lz4.frame

    @staticmethod
    def compress(data: bytes) -> bytes:
        return _Lz4Codec._mod().compress(data)

    @staticmethod
    def decompress(data: bytes) -> bytes:
        return _Lz4Codec._mod().decompress(data)


class _ZstdCodec:
    """ZSTD 编解码器（Java zstd-jni 的 ZstdOutputStream/ZstdInputStream）。

    未安装 ``zstandard`` 时抛错，理由同 LZ4。
    """

    @staticmethod
    def _mod():
        try:
            import zstandard  # type: ignore
        except ImportError as e:  # pragma: no cover - 取决于环境
            raise RuntimeError(
                "zstd compression requires the 'zstandard' package: pip install zstandard"
            ) from e
        return zstandard

    @staticmethod
    def compress(data: bytes) -> bytes:
        return _ZstdCodec._mod().ZstdCompressor().compress(data)

    @staticmethod
    def decompress(data: bytes) -> bytes:
        # Java ZstdInputStream 读的是带 content-size 的普通 zstd 帧，
        # 这里用流式 API 以兼容未写入 content-size 的帧（Java 默认不写）。
        import io

        dctx = _ZstdCodec._mod().ZstdDecompressor()
        with dctx.stream_reader(io.BytesIO(data)) as reader:
            return reader.read()


def _lz4_frame() -> type:
    return _Lz4Codec


def _zstd() -> type:
    return _ZstdCodec


# ------------------------------------------- 1) 17 段存储格式：MessageExt


def _topic_length_size(magic_code: int) -> int:
    return 2 if magic_code == MESSAGE_MAGIC_CODE_V2 else 1


def encode_message_ext(message_ext: MessageExt, need_compress: bool = False) -> bytes:
    """对应 Java ``MessageDecoder.encode(MessageExt, boolean needCompress)``。

    注意：Java 侧 topic 长度固定写 1 字节、魔数固定写 v1（-626843481），
    storeSize > 0 时直接按其分配缓冲（尾部不足会按需补齐）。
    """
    body = message_ext.get_body() or b""
    if need_compress and (message_ext.get_sys_flag() & MessageSysFlag.COMPRESSED_FLAG):
        compression_type = MessageSysFlag.get_compression_type(message_ext.get_sys_flag())
        body = _compress(body, compression_type)
    body_length = len(body)

    topic_bytes = string2bytes(message_ext.get_topic())
    topic_len = len(topic_bytes)
    properties_bytes = string2bytes(message_properties_2_string(message_ext.get_properties()))
    properties_length = len(properties_bytes)

    sys_flag = message_ext.get_sys_flag()
    bornhost_length = 20 if (sys_flag & MessageSysFlag.BORNHOST_V6_FLAG) else 8
    storehost_length = 20 if (sys_flag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) else 8

    computed_size = (4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8
                     + bornhost_length + storehost_length + 4 + 8
                     + 4 + body_length
                     + 1 + topic_len
                     + 2 + properties_length)
    store_size = message_ext.get_store_size() if message_ext.get_store_size() > 0 else computed_size
    store_size = max(store_size, computed_size)

    born_host = message_ext.get_born_host() or "127.0.0.1"
    born_port = int(message_ext.born_host_port or 0)
    store_host = message_ext.get_store_host() or "127.0.0.1"
    store_port = int(message_ext.store_host_port or 0)

    buf = bytearray()
    buf += struct.pack(">i", store_size)                       # 1 TOTALSIZE
    buf += struct.pack(">i", MESSAGE_MAGIC_CODE)               # 2 MAGICCODE
    buf += struct.pack(">I", message_ext.get_body_crc())       # 3 BODYCRC
    buf += struct.pack(">i", message_ext.get_queue_id())       # 4 QUEUEID
    buf += struct.pack(">i", message_ext.get_flag())           # 5 FLAG
    buf += struct.pack(">q", message_ext.get_queue_offset())   # 6 QUEUEOFFSET
    buf += struct.pack(">q", message_ext.get_commit_log_offset())  # 7 PHYSICALOFFSET
    buf += struct.pack(">i", sys_flag)                         # 8 SYSFLAG
    buf += struct.pack(">q", message_ext.get_born_timestamp())  # 9 BORNTIMESTAMP
    buf += ip_and_port_to_bytes(born_host, born_port, bool(sys_flag & MessageSysFlag.BORNHOST_V6_FLAG))
    buf += struct.pack(">q", message_ext.get_store_timestamp())  # 11 STORETIMESTAMP
    buf += ip_and_port_to_bytes(store_host, store_port, bool(sys_flag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG))
    buf += struct.pack(">i", message_ext.get_reconsume_times())  # 13 RECONSUMETIMES
    buf += struct.pack(">q", message_ext.get_prepared_transaction_offset())  # 14
    buf += struct.pack(">i", body_length)                      # 15 BODY
    buf += body
    buf += struct.pack(">B", topic_len)                        # 16 TOPIC
    buf += topic_bytes
    buf += struct.pack(">H", properties_length)                # 17 PROPERTIES
    buf += properties_bytes
    return bytes(buf)


def decode_message(raw: bytes, read_body: bool = True, decompress_body: bool = True,
                   is_client: bool = True, check_crc: bool = False) -> Optional[MessageExt]:
    """把 17 段消息解码为 MessageExt（对应 ``MessageDecoder.decode``）。"""
    try:
        msg_ext = MessageExt()
        offset = 0
        (store_size,) = struct.unpack_from(">i", raw, offset); offset += 4
        (magic_code,) = struct.unpack_from(">i", raw, offset); offset += 4
        if magic_code not in (MESSAGE_MAGIC_CODE, MESSAGE_MAGIC_CODE_V2):
            # Java MessageVersion.valueOfMagicCode 对未知魔数抛异常 -> decode 返回 null
            return None
        use_v2 = magic_code == MESSAGE_MAGIC_CODE_V2
        (body_crc,) = struct.unpack_from(">I", raw, offset); offset += 4
        (queue_id,) = struct.unpack_from(">i", raw, offset); offset += 4
        (flag,) = struct.unpack_from(">i", raw, offset); offset += 4
        (queue_offset,) = struct.unpack_from(">q", raw, offset); offset += 8
        (physic_offset,) = struct.unpack_from(">q", raw, offset); offset += 8
        (sys_flag,) = struct.unpack_from(">i", raw, offset); offset += 4
        (born_timestamp,) = struct.unpack_from(">q", raw, offset); offset += 8

        bornhost_len = 20 if (sys_flag & MessageSysFlag.BORNHOST_V6_FLAG) else 8
        born_host, born_port = bytes_to_ip_and_port(raw[offset:offset + bornhost_len]); offset += bornhost_len
        (store_timestamp,) = struct.unpack_from(">q", raw, offset); offset += 8
        storehost_len = 20 if (sys_flag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) else 8
        store_host, store_port = bytes_to_ip_and_port(raw[offset:offset + storehost_len]); offset += storehost_len
        (reconsume_times,) = struct.unpack_from(">i", raw, offset); offset += 4
        (prepared_transaction_offset,) = struct.unpack_from(">q", raw, offset); offset += 8

        msg_ext.set_store_size(store_size)
        msg_ext.set_body_crc(body_crc)
        msg_ext.set_queue_id(queue_id)
        msg_ext.set_flag(flag)
        msg_ext.set_queue_offset(queue_offset)
        msg_ext.set_commit_log_offset(physic_offset)
        msg_ext.set_sys_flag(sys_flag)
        msg_ext.set_born_timestamp(born_timestamp)
        msg_ext.set_born_host(born_host)
        msg_ext.born_host_port = born_port
        msg_ext.set_store_timestamp(store_timestamp)
        msg_ext.set_store_host(store_host)
        msg_ext.store_host_port = store_port
        msg_ext.set_reconsume_times(reconsume_times)
        msg_ext.set_prepared_transaction_offset(prepared_transaction_offset)

        # 15 BODY
        (body_len,) = struct.unpack_from(">i", raw, offset); offset += 4
        if body_len > 0:
            if read_body:
                body = raw[offset:offset + body_len]
                if check_crc and crc32(body) != body_crc:
                    raise ValueError("Msg crc is error")
                if decompress_body and (sys_flag & MessageSysFlag.COMPRESSED_FLAG):
                    compression_type = MessageSysFlag.get_compression_type(sys_flag)
                    body = _decompress(body, compression_type)
                    msg_ext.set_sys_flag(MessageSysFlag.clear_compressed_flag(sys_flag))
                msg_ext.set_body(bytes(body))
            else:
                # Java readBody=false 时跳过 body 且不给 body 赋值 -> null
                msg_ext.set_body(None)
            offset += body_len
        else:
            msg_ext.set_body(None)

        # 16 TOPIC
        if use_v2:
            (topic_len,) = struct.unpack_from(">H", raw, offset); offset += 2
        else:
            (topic_len,) = struct.unpack_from(">B", raw, offset); offset += 1
        msg_ext.set_topic(raw[offset:offset + topic_len].decode(CHARSET_UTF8)); offset += topic_len

        # 17 PROPERTIES
        (properties_length,) = struct.unpack_from(">H", raw, offset); offset += 2
        if properties_length > 0:
            properties_bytes = raw[offset:offset + properties_length]; offset += properties_length
            msg_ext.set_properties(string_2_message_properties(properties_bytes.decode(CHARSET_UTF8)))

        # msgId = storeHost(ip+port) + commitLogOffset
        store_addr_raw = _ip_to_bytes(store_host, storehost_len == 20) + struct.pack(">I", store_port)
        msg_ext.set_msg_id(create_message_id(store_addr_raw, physic_offset))
        if is_client:
            msg_ext.set_offset_msg_id(msg_ext.get_msg_id())
        return msg_ext
    except Exception:
        return None


def decode_messages(raw: bytes, read_body: bool = True) -> List[MessageExt]:
    """把 17 段消息流解码为 MessageExt 列表（对应 ``MessageDecoder.decodes``，用于 pull 结果）。"""
    result: List[MessageExt] = []
    pos = 0
    total = len(raw)
    while pos < total:
        if total - pos < 4:
            break
        (store_size,) = struct.unpack_from(">i", raw, pos)
        if store_size <= 0 or store_size > total - pos:
            break
        msg = decode_message(raw[pos:pos + store_size], read_body=read_body)
        if msg is None:
            break
        result.append(msg)
        pos += store_size
    return result


# --------------------------------------- 2) 6 段轻量格式：批量消息 body


def encode_message(message: Message) -> bytes:
    """对应 Java ``MessageDecoder.encodeMessage(Message)``：批量消息的单条编码。

    只写 TOTALSIZE / MAGICCODE(0) / BODYCRC(0) / FLAG / BODY / PROPERTIES。
    """
    body = message.get_body() or b""
    properties_bytes = string2bytes(message_properties_2_string(message.get_properties()))
    properties_length = len(properties_bytes)
    store_size = 4 + 4 + 4 + 4 + 4 + len(body) + 2 + properties_length

    buf = bytearray()
    buf += struct.pack(">i", store_size)   # 1 TOTALSIZE
    buf += struct.pack(">i", 0)            # 2 MAGICCODE（批量场景固定 0）
    buf += struct.pack(">i", 0)            # 3 BODYCRC
    buf += struct.pack(">i", message.get_flag())  # 4 FLAG
    buf += struct.pack(">i", len(body))    # 5 BODY
    buf += body
    buf += struct.pack(">H", properties_length)  # 6 PROPERTIES
    buf += properties_bytes
    return bytes(buf)


def encode_messages(messages: Sequence[Message]) -> bytes:
    """对应 Java ``MessageDecoder.encodeMessages(List<Message>)``：拼接成批量消息 body。"""
    out = bytearray()
    for msg in messages:
        out += encode_message(msg)
    return bytes(out)


def decode_batch_message(raw: bytes) -> Message:
    """对应 Java ``MessageDecoder.decodeMessage(ByteBuffer)``：单条批量单元 -> Message。"""
    offset = 0
    offset += 4   # TOTALSIZE
    offset += 4   # MAGICCODE
    offset += 4   # BODYCRC
    (flag,) = struct.unpack_from(">i", raw, offset); offset += 4
    (body_len,) = struct.unpack_from(">i", raw, offset); offset += 4
    body = raw[offset:offset + body_len]; offset += body_len
    (properties_len,) = struct.unpack_from(">H", raw, offset); offset += 2
    properties = string_2_message_properties(raw[offset:offset + properties_len].decode(CHARSET_UTF8))
    msg = Message()
    msg.set_flag(flag)
    msg.set_body(bytes(body))
    msg.set_properties(properties)
    return msg


def decode_batch_messages(raw: bytes) -> List[Message]:
    """对应 Java ``MessageDecoder.decodeMessages(ByteBuffer)``：批量消息 body -> List[Message]。"""
    result: List[Message] = []
    pos = 0
    total = len(raw)
    while pos < total:
        if total - pos < 4:
            break
        (store_size,) = struct.unpack_from(">i", raw, pos)
        if store_size <= 0 or store_size > total - pos:
            break
        result.append(decode_batch_message(raw[pos:pos + store_size]))
        pos += store_size
    return result


def count_inner_msg_num(raw: bytes) -> int:
    """对应 Java ``MessageDecoder.countInnerMsgNum``。"""
    count = 0
    pos = 0
    total = len(raw)
    while pos < total:
        count += 1
        (size,) = struct.unpack_from(">i", raw, pos)
        if size <= 0 or size > total - pos:
            break
        pos += size
    return count


__all__ = [
    "CHARSET_UTF8", "NAME_VALUE_SEPARATOR", "PROPERTY_SEPARATOR",
    "MESSAGE_MAGIC_CODE", "MESSAGE_MAGIC_CODE_V2", "BLANK_MAGIC_CODE",
    "QUEUE_OFFSET_POSITION", "PHY_POS_POSITION", "SYSFLAG_POSITION",
    "MESSAGE_STORE_TIMESTAMP_POSITION",
    "string2bytes", "bytes2string", "string2bytes_hex",
    "ip_and_port_to_bytes", "bytes_to_ip_and_port", "crc32",
    "message_properties_2_string", "string_2_message_properties",
    "create_message_id", "decode_message_id",
    "encode_message_ext", "decode_message", "decode_messages",
    "encode_message", "encode_messages", "decode_batch_message", "decode_batch_messages",
    "count_inner_msg_num",
]
