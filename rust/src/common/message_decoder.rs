//! 消息二进制编解码（对应 Java `org.apache.rocketmq.common.message.MessageDecoder`，
//! 参考实现 `python/rocketmq/common/message_decoder.py`）。
//!
//! 本模块严格对齐 Java 侧的两条编码路径，切勿混用：
//!
//! 1) 17 段存储格式 `MessageDecoder#encode(MessageExt, needCompress)` /
//!    `decode(ByteBuffer)`，用于 broker 写入与 pull/get 返回的消息体：
//!
//!    | 序号 | 字段 | 编码 |
//!    | --- | --- | --- |
//!    | 1 | TOTALSIZE | int(4) |
//!    | 2 | MAGICCODE | int(4) `-626843481`(v1) / `-626843477`(v2) |
//!    | 3 | BODYCRC | int(4) |
//!    | 4 | QUEUEID | int(4) |
//!    | 5 | FLAG | int(4) |
//!    | 6 | QUEUEOFFSET | long(8) |
//!    | 7 | PHYSICALOFFSET | long(8) |
//!    | 8 | SYSFLAG | int(4) |
//!    | 9 | BORNTIMESTAMP | long(8) |
//!    | 10 | BORNHOST | 4\|16B ip + 4B port |
//!    | 11 | STORETIMESTAMP | long(8) |
//!    | 12 | STOREHOST | 4\|16B ip + 4B port |
//!    | 13 | RECONSUMETIMES | int(4) |
//!    | 14 | PREPAREDTRANSACTIONOFFSET | long(8) |
//!    | 15 | BODY | int(4) len + body |
//!    | 16 | TOPIC | 1B(v1)\|2B(v2) len + topic |
//!    | 17 | PROPERTIES | short(2) len + `k\x01v\x02` 串 |
//!
//! 2) 6 段轻量格式 `MessageDecoder#encodeMessage(Message)` / `decodeMessage(ByteBuffer)`，
//!    仅用于**批量消息**（`MessageBatch` 的 body），不含 topic / crc：
//!    `TOTALSIZE(4) | MAGICCODE(4, 固定 0) | BODYCRC(4, 固定 0) | FLAG(4) | BODY(4+len) |
//!    PROPERTIES(2+len)`

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::common::buffer::{Reader, Writer};
use crate::common::message::{Message, MessageExt};
use crate::common::sysflag::MessageSysFlag;
use crate::common::{compression, util_all};
use crate::error::{Error, Result};
use crate::remoting::protocol::ext_fields::StringMap;

/// `k` 与 `v` 之间的分隔符（Java `MessageDecoder.NAME_VALUE_SEPARATOR`）。
pub const NAME_VALUE_SEPARATOR: char = '\u{1}';
/// 属性项之间的分隔符（Java `MessageDecoder.PROPERTY_SEPARATOR`）。
pub const PROPERTY_SEPARATOR: char = '\u{2}';

/// v1 魔数（Java `MessageDecoder.MESSAGE_MAGIC_CODE`）。
pub const MESSAGE_MAGIC_CODE: i32 = -626_843_481;
/// v2 魔数：topic 长度域从 1 字节变 2 字节。
pub const MESSAGE_MAGIC_CODE_V2: i32 = -626_843_477;
/// commitlog 里「空白记录」的魔数，不是合法消息帧。
pub const BLANK_MAGIC_CODE: i32 = -875_286_124;

/// 对应 Java `MessageDecoder.MESSAGE_MAGIC_CODE_POSITION`。
pub const MESSAGE_MAGIC_CODE_POSITION: usize = 4;
/// 对应 Java `MessageDecoder.MESSAGE_FLAG_POSITION`。
pub const MESSAGE_FLAG_POSITION: usize = 16;
/// 对应 Java `MessageDecoder.PHY_POS_POSITION`（`4*5 + 8`）。
pub const PHY_POS_POSITION: usize = 4 + 4 + 4 + 4 + 4 + 8;
/// 对应 Java `MessageDecoder.QUEUE_OFFSET_POSITION`（`4*5`）。
pub const QUEUE_OFFSET_POSITION: usize = 4 + 4 + 4 + 4 + 4;
/// 对应 Java `MessageDecoder.SYSFLAG_POSITION`（`4*5 + 8 + 8`）。
pub const SYSFLAG_POSITION: usize = 4 + 4 + 4 + 4 + 4 + 8 + 8;
/// 对应 Java `MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSITION`。
pub const MESSAGE_STORE_TIMESTAMP_POSITION: usize = 56;

/// v1/v2 帧里 topic 长度域的字节数差异之外的固定头部（含 BODY 长度域）。
const HEADER_WITHOUT_BODY_LEN: usize = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 4 + 8;

pub use crate::common::util_all::crc32;

/// 对应 Python `string2bytes`：UTF-8 编码。
pub fn string2bytes(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// 对应 Java `UtilAll.bytes2string` / Python `bytes2string`：逐字节**大写**十六进制
/// （msgId 依赖大小写）。
pub fn bytes2string(bytes: &[u8]) -> String {
    util_all::bytes_2_string(bytes)
}

/// 对应 Java `UtilAll.string2bytes` / Python `string2bytes_hex`：十六进制 -> 字节。
pub fn string2bytes_hex(hex_string: &str) -> Option<Vec<u8>> {
    util_all::string_2_bytes(hex_string)
}

/// 对应 Python `ip_and_port_to_bytes`：ip 字节 + 4 字节大端端口。
pub fn ip_and_port_to_bytes(ip: &str, port: u32, v6: bool) -> Result<Vec<u8>> {
    let mut out = ip_to_bytes(ip, v6)?;
    out.extend_from_slice(&port.to_be_bytes());
    Ok(out)
}

/// 对应 Python `_ip_to_bytes`：`socket.inet_pton`。
pub fn ip_to_bytes(ip: &str, v6: bool) -> Result<Vec<u8>> {
    let addr: IpAddr = ip
        .parse()
        .map_err(|_| Error::Decode(format!("illegal ip address: {ip}")))?;
    match (addr, v6) {
        (IpAddr::V4(v4), false) => Ok(v4.octets().to_vec()),
        (IpAddr::V6(v6addr), true) => Ok(v6addr.octets().to_vec()),
        (IpAddr::V4(v4), true) => Ok(to_v6_compatible(&v4).octets().to_vec()),
        (IpAddr::V6(v6addr), false) => match v6addr.to_ipv4() {
            Some(v4) => Ok(v4.octets().to_vec()),
            None => Err(Error::Decode(format!(
                "ipv6 address {ip} needs BORNHOST_V6_FLAG / STOREHOSTADDRESS_V6_FLAG"
            ))),
        },
    }
}

fn to_v6_compatible(v4: &Ipv4Addr) -> Ipv6Addr {
    let octets = v4.octets();
    Ipv6Addr::new(0, 0, 0, 0, 0, 0xFFFF, u16::from_be_bytes([octets[0], octets[1]]),
        u16::from_be_bytes([octets[2], octets[3]]))
}

/// 对应 Python `bytes_to_ip_and_port`：8 字节 = ipv4+port，20 字节 = ipv6+port。
pub fn bytes_to_ip_and_port(raw: &[u8]) -> Result<(String, u32)> {
    let (ip_len, family_v6) = match raw.len() {
        8 => (4, false),
        20 => (16, true),
        other => return Err(Error::Decode(format!("illegal host bytes length: {other}"))),
    };
    let ip = bytes_to_ip(&raw[..ip_len], family_v6)?;
    let port = u32::from_be_bytes([raw[ip_len], raw[ip_len + 1], raw[ip_len + 2], raw[ip_len + 3]]);
    Ok((ip, port))
}

fn bytes_to_ip(bytes: &[u8], v6: bool) -> Result<String> {
    if v6 {
        if bytes.len() != 16 {
            return Err(Error::Decode("illegal ipv6 address length".into()));
        }
        let mut octets = [0u8; 16];
        octets.copy_from_slice(bytes);
        Ok(Ipv6Addr::from(octets).to_string())
    } else {
        if bytes.len() != 4 {
            return Err(Error::Decode("illegal ipv4 address length".into()));
        }
        Ok(Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]).to_string())
    }
}

/// 对应 Python `create_message_id`：`addr(8|20B) + commitLogOffset(8B)` -> 大写十六进制。
pub fn create_message_id(addr_bytes: &[u8], offset: i64) -> String {
    let mut raw = addr_bytes.to_vec();
    raw.extend_from_slice(&offset.to_be_bytes());
    bytes2string(&raw)
}

/// 对应 Java `MessageDecoder.decodeMessageId` / Python `decode_message_id`：
/// `(ip, port, commitLogOffset)`。
pub fn decode_message_id(msg_id: &str) -> Result<(String, u32, i64)> {
    let raw = string2bytes_hex(msg_id)
        .ok_or_else(|| Error::Decode(format!("illegal msgId: {msg_id}")))?;
    let ip_len = match raw.len() {
        16 => 4,
        28 => 16,
        other => return Err(Error::Decode(format!("illegal msgId length: {other}"))),
    };
    let (ip, port) = bytes_to_ip_and_port(&raw[..ip_len + 4])?;
    let offset = i64::from_be_bytes([
        raw[ip_len + 4],
        raw[ip_len + 5],
        raw[ip_len + 6],
        raw[ip_len + 7],
        raw[ip_len + 8],
        raw[ip_len + 9],
        raw[ip_len + 10],
        raw[ip_len + 11],
    ]);
    Ok((ip, port, offset))
}

/// 对应 Java `MessageDecoder.messageProperties2String`：`k\x01v\x02` 逐项拼接。
///
/// 值为 `None` 的项跳过 —— Rust 的 `StringMap` 存的是 `String`，没有 null 值，
/// 语义上等价于 Java 里 map 中不存在该 key。
pub fn message_properties_2_string(properties: &StringMap) -> String {
    let mut sb = String::new();
    for (name, value) in properties.iter() {
        sb.push_str(name);
        sb.push(NAME_VALUE_SEPARATOR);
        sb.push_str(value);
        sb.push(PROPERTY_SEPARATOR);
    }
    sb
}

/// 对应 Java `MessageDecoder.string2messageProperties` / Python
/// `string_2_message_properties`。
///
/// 与 Java 一样按 **字符**（不是字节）做下标运算：`newIndex - index >= 3` 且
/// `index < kvSep < newIndex - 1` 才算合法片段，所以长度 < 3 的片段、没有 kv 分隔符的
/// 片段、以及**空值**（`k\x01\x02`）都会被丢掉。这里必须照抄，别「顺手修好」。
pub fn string_2_message_properties(properties_str: &str) -> StringMap {
    let mut result = StringMap::new();
    if properties_str.is_empty() {
        return result;
    }
    let chars: Vec<char> = properties_str.chars().collect();
    let length = chars.len();
    let mut index = 0usize;
    while index < length {
        let new_index = find_char(&chars, index, length, PROPERTY_SEPARATOR).unwrap_or(length);
        if new_index >= index + 3 {
            if let Some(kv_sep) = find_char(&chars, index, length, NAME_VALUE_SEPARATOR) {
                if kv_sep > index && kv_sep < new_index - 1 {
                    let key: String = chars[index..kv_sep].iter().collect();
                    let value: String = chars[kv_sep + 1..new_index].iter().collect();
                    result.insert(key, value);
                }
            }
        }
        index = new_index + 1;
    }
    result
}

fn find_char(chars: &[char], from: usize, length: usize, target: char) -> Option<usize> {
    (from..length).find(|&i| chars[i] == target)
}

/// `MessageDecoder#decode` 的四个开关（Python `decode_message` 的同名参数）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeOptions {
    /// false 时跳过 BODY 且 body 保持 `None`（Java `readBody=false` 同语义）。
    pub read_body: bool,
    /// 带 `COMPRESSED_FLAG` 时是否解压。
    pub decompress_body: bool,
    /// true 时 `msgId` 同时写进 `offset_msg_id`（客户端视角）。
    pub is_client: bool,
    /// 校验 BODYCRC。
    pub check_crc: bool,
}

impl Default for DecodeOptions {
    fn default() -> DecodeOptions {
        DecodeOptions {
            read_body: true,
            decompress_body: true,
            is_client: true,
            check_crc: false,
        }
    }
}

/// 一条消息编码后的字节数（对应 Java `MessageDecoder#encode` 里 storeSize 的算式，
/// 即 Python 的 `computed_size`）。发送前用它可以避免真的编码一次。
///
/// ⚠ 名字里的 `messageBytesSize` 在本仓库 Java 快照中没有对应方法，语义取自
/// Python 的 `computed_size`。
pub fn message_bytes_size(
    topic: &str,
    body_len: usize,
    properties: &StringMap,
    sys_flag: i32,
) -> usize {
    let bornhost_length = host_length(sys_flag, MessageSysFlag::BORNHOST_V6_FLAG);
    let storehost_length = host_length(sys_flag, MessageSysFlag::STOREHOSTADDRESS_V6_FLAG);
    HEADER_WITHOUT_BODY_LEN
        + bornhost_length
        + storehost_length
        + 4
        + body_len
        + 1
        + topic.len()
        + message_string_size(&message_properties_2_string(properties))
}

/// 字符串在线上格式里占的字节数：UTF-8 字节数 + 2 字节长度前缀。
///
/// ⚠ 同 `message_bytes_size`：Java 快照里没有 `messageStringSize`，按 Python 的
/// `len(string2bytes(s)) + 2` 口径实现。
pub fn message_string_size(s: &str) -> usize {
    2 + s.len()
}

fn host_length(sys_flag: i32, v6_flag: i32) -> usize {
    if sys_flag & v6_flag != 0 {
        20
    } else {
        8
    }
}

/// 对应 Java `MessageDecoder#encode(MessageExt, boolean needCompress)` /
/// Python `encode_message_ext`。
///
/// Java 侧 topic 长度固定写 1 字节、魔数固定写 v1（`MESSAGE_MAGIC_CODE`），
/// `storeSize > 0` 时直接按其分配缓冲（尾部不足会按需补齐）。
pub fn encode_message_ext(message_ext: &MessageExt, need_compress: bool) -> Result<Vec<u8>> {
    let mut body: Vec<u8> = message_ext.body.clone().unwrap_or_default();
    let sys_flag = message_ext.sys_flag;
    if need_compress && (sys_flag & MessageSysFlag::COMPRESSED_FLAG) == MessageSysFlag::COMPRESSED_FLAG
    {
        let compression_type = MessageSysFlag::get_compression_type(sys_flag);
        body = compression::compress(&body, compression_type, compression::DEFAULT_COMPRESS_LEVEL)?;
    }
    let body_length = body.len();

    let topic_bytes = string2bytes(&message_ext.topic);
    let topic_len = topic_bytes.len();
    if topic_len > u8::MAX as usize {
        // Java 会静默截断成 (byte) len，Python 直接抛 struct.error；这里按「报错」处理
        return Err(Error::Encode(format!(
            "topic length {topic_len} exceeds 1-byte field, needs magic code v2"
        )));
    }
    let properties_bytes = string2bytes(&message_properties_2_string(&message_ext.properties));
    let properties_length = properties_bytes.len();

    let bornhost_length = host_length(sys_flag, MessageSysFlag::BORNHOST_V6_FLAG);
    let storehost_length = host_length(sys_flag, MessageSysFlag::STOREHOSTADDRESS_V6_FLAG);
    let computed_size = HEADER_WITHOUT_BODY_LEN
        + bornhost_length
        + storehost_length
        + 4
        + body_length
        + 1
        + topic_len
        + 2
        + properties_length;
    let store_size = if message_ext.store_size > 0 {
        message_ext.store_size.max(computed_size as i32) as usize
    } else {
        computed_size
    };

    let born_host = message_ext
        .born_host
        .clone()
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let store_host = message_ext
        .store_host
        .clone()
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let born_addr = ip_and_port_to_bytes(&born_host, message_ext.born_host_port, bornhost_length == 20)?;
    let store_addr =
        ip_and_port_to_bytes(&store_host, message_ext.store_host_port, storehost_length == 20)?;

    let mut writer = Writer::with_capacity(store_size);
    writer.i32(store_size as i32); // 1 TOTALSIZE
    writer.i32(MESSAGE_MAGIC_CODE); // 2 MAGICCODE
    writer.u32(message_ext.body_crc); // 3 BODYCRC
    writer.i32(message_ext.queue_id); // 4 QUEUEID
    writer.i32(message_ext.flag); // 5 FLAG
    writer.i64(message_ext.queue_offset); // 6 QUEUEOFFSET
    writer.i64(message_ext.commit_log_offset); // 7 PHYSICALOFFSET
    writer.i32(sys_flag); // 8 SYSFLAG
    writer.i64(message_ext.born_timestamp); // 9 BORNTIMESTAMP
    writer.bytes(&born_addr); // 10 BORNHOST
    writer.i64(message_ext.store_timestamp); // 11 STORETIMESTAMP
    writer.bytes(&store_addr); // 12 STOREHOST
    writer.i32(message_ext.reconsume_times); // 13 RECONSUMETIMES
    writer.i64(message_ext.prepared_transaction_offset); // 14
    writer.i32(body_length as i32); // 15 BODY
    writer.bytes(&body);
    writer.u8(topic_len as u8); // 16 TOPIC
    writer.bytes(&topic_bytes);
    writer.u16(properties_length as u32); // 17 PROPERTIES
    writer.bytes(&properties_bytes);
    if store_size > writer.len() {
        writer.zeros(store_size - writer.len());
    }
    Ok(writer.into_inner())
}

/// 对应 Java `MessageDecoder#decode(ByteBuffer)`（默认参数）。
pub fn decode_message(raw: &[u8]) -> Result<MessageExt> {
    decode_message_with(raw, &DecodeOptions::default())
}

/// 对应 Java `MessageDecoder#decode(bb, readBody, deCompressBody, isClient, checkCRC)`
/// / Python `decode_message`。
///
/// Java 里整段被 `try/catch` 包住、失败返回 null；Rust 用 `Error::Decode` 表达同一语义。
pub fn decode_message_with(raw: &[u8], options: &DecodeOptions) -> Result<MessageExt> {
    let mut reader = Reader::new(raw);
    let mut msg_ext = MessageExt::new();

    let store_size = reader.i32()?;
    let magic_code = reader.i32()?;
    // Java MessageVersion.valueOfMagicCode 对未知魔数抛异常 -> decode 返回 null
    let use_v2 = match magic_code {
        MESSAGE_MAGIC_CODE => false,
        MESSAGE_MAGIC_CODE_V2 => true,
        other => return Err(Error::Decode(format!("unknown magic code: {other}"))),
    };
    let body_crc = reader.u32()?;
    let queue_id = reader.i32()?;
    let flag = reader.i32()?;
    let queue_offset = reader.i64()?;
    let physic_offset = reader.i64()?;
    let mut sys_flag = reader.i32()?;
    let born_timestamp = reader.i64()?;

    let bornhost_length = host_length(sys_flag, MessageSysFlag::BORNHOST_V6_FLAG);
    let born_addr = reader.bytes(bornhost_length)?;
    let (born_host, born_port) = bytes_to_ip_and_port(born_addr)?;
    let store_timestamp = reader.i64()?;
    let storehost_length = host_length(sys_flag, MessageSysFlag::STOREHOSTADDRESS_V6_FLAG);
    let store_addr = reader.bytes(storehost_length)?;
    let (store_host, store_port) = bytes_to_ip_and_port(store_addr)?;
    let reconsume_times = reader.i32()?;
    let prepared_transaction_offset = reader.i64()?;

    msg_ext.store_size = store_size;
    msg_ext.body_crc = body_crc;
    msg_ext.queue_id = queue_id;
    msg_ext.flag = flag;
    msg_ext.queue_offset = queue_offset;
    msg_ext.commit_log_offset = physic_offset;
    msg_ext.sys_flag = sys_flag;
    msg_ext.born_timestamp = born_timestamp;
    msg_ext.born_host = Some(born_host);
    msg_ext.born_host_port = born_port;
    msg_ext.store_timestamp = store_timestamp;
    msg_ext.store_host = Some(store_host.clone());
    msg_ext.store_host_port = store_port;
    msg_ext.reconsume_times = reconsume_times;
    msg_ext.prepared_transaction_offset = prepared_transaction_offset;

    // 15 BODY
    let body_len = reader.i32()?;
    if body_len > 0 {
        let body_bytes = reader.bytes(body_len as usize)?;
        if options.read_body {
            let mut body = body_bytes.to_vec();
            if options.check_crc && crc32(&body) != body_crc {
                return Err(Error::Decode("Msg crc is error".into()));
            }
            if options.decompress_body && (sys_flag & MessageSysFlag::COMPRESSED_FLAG) != 0 {
                let compression_type = MessageSysFlag::get_compression_type(sys_flag);
                body = compression::decompress(&body, compression_type)?;
                sys_flag &= !MessageSysFlag::COMPRESSED_FLAG;
                msg_ext.sys_flag = sys_flag;
            }
            msg_ext.body = Some(body);
        } else {
            // Java/Python readBody=false：跳过字节且不赋值，body 保持 null
            msg_ext.body = None;
        }
    } else {
        msg_ext.body = None;
    }

    // 16 TOPIC
    let topic_len = if use_v2 {
        reader.u16()? as usize
    } else {
        reader.u8()? as usize
    };
    let topic_bytes = reader.bytes(topic_len)?;
    msg_ext.topic = String::from_utf8_lossy(topic_bytes).to_string();

    // 17 PROPERTIES
    let properties_length = reader.u16()? as usize;
    if properties_length > 0 {
        let properties_bytes = reader.bytes(properties_length)?;
        let text = String::from_utf8_lossy(properties_bytes).to_string();
        msg_ext.properties = string_2_message_properties(&text);
    }

    // msgId = storeHost(ip+port) + commitLogOffset
    let store_addr = ip_and_port_to_bytes(&store_host, store_port, storehost_length == 20)?;
    msg_ext.msg_id = Some(create_message_id(&store_addr, physic_offset));
    if options.is_client {
        msg_ext.offset_msg_id = msg_ext.msg_id.clone();
    }
    Ok(msg_ext)
}

/// 对应 Java `MessageDecoder.decodes` / Python `decode_messages`：pull 结果里的
/// 17 段消息流 -> 列表。**逐条切分，遇到解不开的就停**（Java 的 `break` 同语义）。
pub fn decode_messages(raw: &[u8]) -> Vec<MessageExt> {
    decode_messages_with(raw, &DecodeOptions::default())
}

/// [`decode_messages`] 的带选项版本。
pub fn decode_messages_with(raw: &[u8], options: &DecodeOptions) -> Vec<MessageExt> {
    let mut result = Vec::new();
    let mut pos = 0usize;
    let total = raw.len();
    while pos < total {
        if total - pos < 4 {
            break;
        }
        let store_size = i32::from_be_bytes([
            raw[pos],
            raw[pos + 1],
            raw[pos + 2],
            raw[pos + 3],
        ]);
        if store_size <= 0 || store_size as usize > total - pos {
            break;
        }
        let end = pos + store_size as usize;
        match decode_message_with(&raw[pos..end], options) {
            Ok(msg) => result.push(msg),
            Err(_) => break,
        }
        pos = end;
    }
    result
}

/// 对应 Java `MessageDecoder#encodeMessage(Message)`：批量消息的单条编码。
///
/// 只写 TOTALSIZE / MAGICCODE(0) / BODYCRC(0) / FLAG / BODY / PROPERTIES。
pub fn encode_message(message: &Message) -> Vec<u8> {
    let body: &[u8] = message.body.as_deref().unwrap_or(&[]);
    let properties_bytes = string2bytes(&message_properties_2_string(&message.properties));
    let properties_length = properties_bytes.len();
    let store_size = 4 + 4 + 4 + 4 + 4 + body.len() + 2 + properties_length;

    let mut writer = Writer::with_capacity(store_size);
    writer.i32(store_size as i32); // 1 TOTALSIZE
    writer.i32(0); // 2 MAGICCODE（批量场景固定 0）
    writer.i32(0); // 3 BODYCRC
    writer.i32(message.flag); // 4 FLAG
    writer.i32(body.len() as i32); // 5 BODY
    writer.bytes(body);
    writer.u16(properties_length as u32); // 6 PROPERTIES
    writer.bytes(&properties_bytes);
    writer.into_inner()
}

/// 对应 Java `MessageDecoder.encodeMessages(List<Message>)`：拼成批量消息 body。
pub fn encode_messages(messages: &[Message]) -> Vec<u8> {
    let mut out = Writer::new();
    for msg in messages {
        out.bytes(&encode_message(msg));
    }
    out.into_inner()
}

/// 对应 Java `MessageDecoder.decodeMessage(ByteBuffer)`：单条批量单元 -> `Message`。
pub fn decode_batch_message(raw: &[u8]) -> Result<Message> {
    let mut reader = Reader::new(raw);
    let _store_size = reader.i32()?; // TOTALSIZE
    let _magic_code = reader.i32()?; // MAGICCODE
    let _body_crc = reader.i32()?; // BODYCRC
    let flag = reader.i32()?;
    let body_len = reader.i32()?;
    let body = reader.bytes(body_len.max(0) as usize)?.to_vec();
    let properties_len = reader.u16()? as usize;
    let properties_bytes = reader.bytes(properties_len)?;
    let text = String::from_utf8_lossy(properties_bytes).to_string();
    let mut msg = Message::new("", Some(&body));
    msg.flag = flag;
    msg.properties = string_2_message_properties(&text);
    Ok(msg)
}

/// 对应 Java `MessageDecoder.decodeMessages(ByteBuffer)`：批量 body -> `Vec<Message>`。
pub fn decode_batch_messages(raw: &[u8]) -> Vec<Message> {
    let mut result = Vec::new();
    let mut pos = 0usize;
    let total = raw.len();
    while pos < total {
        if total - pos < 4 {
            break;
        }
        let store_size = i32::from_be_bytes([raw[pos], raw[pos + 1], raw[pos + 2], raw[pos + 3]]);
        if store_size <= 0 || store_size as usize > total - pos {
            break;
        }
        let end = pos + store_size as usize;
        match decode_batch_message(&raw[pos..end]) {
            Ok(msg) => result.push(msg),
            Err(_) => break,
        }
        pos = end;
    }
    result
}

/// 对应 Java `MessageDecoder.countInnerMsgNum`。
pub fn count_inner_msg_num(raw: &[u8]) -> i32 {
    let mut count = 0i32;
    let mut pos = 0usize;
    let total = raw.len();
    while pos < total {
        count += 1;
        if total - pos < 4 {
            break;
        }
        let size = i32::from_be_bytes([raw[pos], raw[pos + 1], raw[pos + 2], raw[pos + 3]]);
        if size <= 0 || size as usize > total - pos {
            break;
        }
        pos += size as usize;
    }
    count
}

/// 把 admin `ConsumeQueueData.bitMap`（Java `BitsArray#toString` 输出的二进制字符串，
/// 高位在前）解成按位可读的向量。
///
/// ⚠ Python 参考实现里 bitMap 只是原样透传的字符串，Java 快照的 `MessageDecoder` 也没有
/// 这个方法（位图属于 broker 的 ConsumeQueueExt 过滤位图）；这里补齐，上层不必再手写位运算。
pub fn parse_topic_filter_bitmap(bitmap: &str) -> Result<Vec<bool>> {
    let mut out = Vec::with_capacity(bitmap.len());
    for ch in bitmap.chars() {
        match ch {
            '0' => out.push(false),
            '1' => out.push(true),
            other => {
                return Err(Error::Decode(format!(
                    "illegal bitMap char {other:?} at index {}",
                    out.len()
                )))
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message_const::PROPERTY_WAIT_STORE_MSG_OK;

    fn build_ext(overrides: &[(&str, &str)]) -> MessageExt {
        let mut ext = MessageExt::new();
        ext.set_topic("TopicTest");
        ext.set_body(Some(b"hello rocketmq"));
        ext.set_flag(2);
        ext.sys_flag = 0;
        ext.body_crc = crc32(ext.body.as_deref().unwrap_or(&[]));
        ext.queue_id = 3;
        ext.queue_offset = 88;
        ext.commit_log_offset = 1024;
        ext.born_timestamp = 1_700_000_000_000;
        ext.store_timestamp = 1_700_000_000_123;
        ext.set_born_host(Some("127.0.0.1"));
        ext.born_host_port = 54321;
        ext.set_store_host(Some("127.0.0.1"));
        ext.store_host_port = 10911;
        ext.reconsume_times = 1;
        ext.prepared_transaction_offset = 0;
        let mut props = StringMap::new();
        props.insert("TAGS", "TagA");
        props.insert("KEYS", "key1 key2");
        ext.set_properties(props);
        for (key, value) in overrides {
            match *key {
                "topic" => ext.set_topic(value),
                "sys_flag" => ext.sys_flag = value.parse::<i32>().unwrap(),
                "store_size" => ext.set_store_size(value.parse::<i32>().unwrap()),
                other => panic!("unknown override {other}"),
            }
        }
        ext
    }

    /// `test_message_codec.py::build_ext` 编码出来的 139 字节帧（Python 生成的真值）。
    const EXT_HEX: &str = "0000008bdaa320a7f3307b09000000030000000200000000000000580000000000000400\
                           000000000000018bcfe568007f0000010000d4310000018bcfe5687b7f00000100002a9f\
                           0000000100000000000000000000000e68656c6c6f20726f636b65746d7109546f706963\
                           546573740019544147530154616741024b455953016b657931206b65793202";
    const EXT_MSG_ID: &str = "7F00000100002A9F0000000000000400";

    fn unhex(hex: &str) -> Vec<u8> {
        util_all::string_2_bytes(&hex.replace(['\n', ' '], "")).unwrap()
    }

    // ---------------------------------------------------------- 属性串

    #[test]
    fn properties_round_trip() {
        let mut props = StringMap::new();
        props.insert("TAGS", "TagA");
        props.insert("KEYS", "k1 k2 k3");
        props.insert("UNIQ_KEY", "0A0A0A0A");
        let raw = message_properties_2_string(&props);
        assert_eq!(raw.chars().filter(|c| *c == NAME_VALUE_SEPARATOR).count(), 3);
        assert_eq!(raw.chars().filter(|c| *c == PROPERTY_SEPARATOR).count(), 3);
        assert_eq!(string_2_message_properties(&raw), props);
    }

    #[test]
    fn properties_empty_and_garbage() {
        assert!(string_2_message_properties("").is_empty());
        assert_eq!(message_string_size(""), 2);
        // Java：长度 < 3 或没有 kv 分隔符的片段会被跳过
        assert!(string_2_message_properties("a\u{2}").is_empty());
        assert!(string_2_message_properties("ab\u{2}").is_empty());
        assert!(string_2_message_properties("no-separator\u{2}").is_empty());
        let one = string_2_message_properties("k\u{1}v\u{2}");
        assert_eq!(one.get("k"), Some("v"));
        // 空值 k\x01\x02 是「非法片段」，会被丢掉 —— 锁住该行为
        let mut wait = StringMap::new();
        wait.insert(PROPERTY_WAIT_STORE_MSG_OK, "");
        let raw = message_properties_2_string(&wait);
        assert_eq!(raw, "WAIT\u{1}\u{2}");
        assert!(string_2_message_properties(&raw).is_empty());
    }

    #[test]
    fn properties_unicode_round_trip() {
        let mut props = StringMap::new();
        props.insert("中文键", "中文值 带空格");
        assert_eq!(
            string_2_message_properties(&message_properties_2_string(&props)),
            props
        );
    }

    #[test]
    fn properties_none_value_never_encoded() {
        // Rust 的 StringMap 存 String，「null 值」等价于「键不存在」
        let empty = StringMap::new();
        assert_eq!(message_properties_2_string(&empty), "");
    }

    // ------------------------------------------------------ 17 段编解码

    #[test]
    fn encode_matches_python_bytes() {
        let raw = encode_message_ext(&build_ext(&[]), false).unwrap();
        assert_eq!(bytes2string(&raw), bytes2string(&unhex(EXT_HEX)));
        assert_eq!(raw.len(), 139);
    }

    #[test]
    fn full_round_trip() {
        let raw = encode_message_ext(&build_ext(&[]), false).unwrap();
        let got = decode_message(&raw).unwrap();
        assert_eq!(got.get_topic(), "TopicTest");
        assert_eq!(got.get_body(), b"hello rocketmq");
        assert_eq!(got.get_flag(), 2);
        assert_eq!(got.get_queue_id(), 3);
        assert_eq!(got.queue_offset, 88);
        assert_eq!(got.commit_log_offset, 1024);
        assert_eq!(got.get_body_crc(), crc32(b"hello rocketmq"));
        assert_eq!(got.get_born_timestamp(), 1_700_000_000_000);
        assert_eq!(got.get_store_timestamp(), 1_700_000_000_123);
        assert_eq!(got.get_born_host(), Some("127.0.0.1"));
        assert_eq!(got.born_host_port, 54321);
        assert_eq!(got.get_store_host(), Some("127.0.0.1"));
        assert_eq!(got.store_host_port, 10911);
        assert_eq!(got.get_reconsume_times(), 1);
        assert_eq!(got.get_property("TAGS"), Some("TagA"));
        assert_eq!(got.get_property("KEYS"), Some("key1 key2"));
    }

    #[test]
    fn totalsize_is_authoritative() {
        let raw = encode_message_ext(&build_ext(&[]), false).unwrap();
        let store_size = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
        let magic = i32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]);
        assert_eq!(store_size as usize, raw.len());
        assert_eq!(magic, MESSAGE_MAGIC_CODE);
    }

    #[test]
    fn field_offsets() {
        let raw = encode_message_ext(&build_ext(&[]), false).unwrap();
        let long_at = |p: usize| i64::from_be_bytes(raw[p..p + 8].try_into().unwrap());
        let int_at = |p: usize| i32::from_be_bytes(raw[p..p + 4].try_into().unwrap());
        assert_eq!(long_at(QUEUE_OFFSET_POSITION), 88);
        assert_eq!(long_at(PHY_POS_POSITION), 1024);
        assert_eq!(int_at(SYSFLAG_POSITION), 0);
        assert_eq!(long_at(MESSAGE_STORE_TIMESTAMP_POSITION), 1_700_000_000_123);
        assert_eq!(int_at(MESSAGE_FLAG_POSITION), 2);
        assert_eq!(MESSAGE_MAGIC_CODE_POSITION, 4);
    }

    #[test]
    fn store_size_respects_existing_value() {
        let mut raw = encode_message_ext(&build_ext(&[("store_size", "1")]), false).unwrap();
        let declared = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
        assert_eq!(declared as usize, raw.len(), "storeSize=1 应被 max 兜底");
        // storeSize 比实际大时按 storeSize 分配（尾部补 0，Java ByteBuffer.allocate 同语义）
        raw = encode_message_ext(&build_ext(&[("store_size", "200")]), false).unwrap();
        assert_eq!(raw.len(), 200);
        assert!(raw[139..].iter().all(|b| *b == 0));
    }

    #[test]
    fn msg_id_from_store_host_and_offset() {
        let raw = encode_message_ext(&build_ext(&[]), false).unwrap();
        let got = decode_message(&raw).unwrap();
        let expected =
            create_message_id(&ip_and_port_to_bytes("127.0.0.1", 10911, false).unwrap(), 1024);
        assert_eq!(expected, EXT_MSG_ID);
        assert_eq!(got.get_msg_id(), Some(EXT_MSG_ID));
        assert_eq!(got.get_offset_msg_id(), Some(EXT_MSG_ID));
    }

    #[test]
    fn unknown_magic_code_is_an_error() {
        let mut raw = encode_message_ext(&build_ext(&[]), false).unwrap();
        raw[4..8].copy_from_slice(&BLANK_MAGIC_CODE.to_be_bytes());
        let err = decode_message(&raw).unwrap_err();
        assert!(matches!(err, Error::Decode(m) if m.contains("unknown magic code")));
    }

    #[test]
    fn truncated_frame_is_an_error() {
        let raw = encode_message_ext(&build_ext(&[]), false).unwrap();
        assert!(matches!(
            decode_message(&raw[..20]),
            Err(Error::Decode(_))
        ));
        assert!(matches!(decode_message(&[]), Err(Error::Decode(_))));
    }

    #[test]
    fn read_body_false_keeps_body_none() {
        let raw = encode_message_ext(&build_ext(&[]), false).unwrap();
        let options = DecodeOptions {
            read_body: false,
            ..DecodeOptions::default()
        };
        let got = decode_message_with(&raw, &options).unwrap();
        assert!(got.body.is_none());
        assert_eq!(got.get_topic(), "TopicTest");
        assert_eq!(got.get_property("TAGS"), Some("TagA"));
    }

    #[test]
    fn check_crc_detects_corruption() {
        let mut raw = encode_message_ext(&build_ext(&[]), false).unwrap();
        // 固定头 68 字节，再加两个 IPv4 地址各 8 字节 = 84，body 再往后 4 字节长度前缀
        assert_eq!(HEADER_WITHOUT_BODY_LEN, 68);
        assert_eq!(HEADER_WITHOUT_BODY_LEN + 8 + 8, 84);
        raw[88] ^= 0xFF;
        let options = DecodeOptions {
            check_crc: true,
            ..DecodeOptions::default()
        };
        let err = decode_message_with(&raw, &options).unwrap_err();
        assert!(matches!(err, Error::Decode(m) if m.contains("crc")));
        // 不校验 CRC 时照样能解开（body 已是坏数据）
        assert!(decode_message(&raw).is_ok());
    }

    #[test]
    fn decodes_multiple_messages() {
        let mut stream = encode_message_ext(&build_ext(&[]), false).unwrap();
        let mut second = build_ext(&[]);
        second.set_body(Some(b"two"));
        stream.extend_from_slice(&encode_message_ext(&second, false).unwrap());
        let msgs = decode_messages(&stream);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].get_body(), b"hello rocketmq");
        assert_eq!(msgs[1].get_body(), b"two");
    }

    #[test]
    fn decodes_stops_on_garbage() {
        let mut stream = encode_message_ext(&build_ext(&[]), false).unwrap();
        stream.extend_from_slice(b"\x00\x01");
        let msgs = decode_messages(&stream);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].get_body(), b"hello rocketmq");
    }

    #[test]
    fn empty_body_decodes_to_none_body() {
        // Python 真值：body 长度为 0 的 100 字节帧
        let hex = "00000064DAA320A700000000000000030000000200000000000000580000000000000400\
                   000000000000018BCFE568007F0000010000D4310000018BCFE5687B7F00000100002A9F\
                   0000000100000000000000000000000009546F706963546573740000";
        let raw = unhex(hex);
        assert_eq!(raw.len(), 100);
        let got = decode_message(&raw).unwrap();
        assert!(got.body.is_none());
        assert_eq!(got.get_topic(), "TopicTest");
        assert!(got.properties.is_empty());
    }

    #[test]
    fn count_inner_msg_num_cases() {
        let mut stream = Vec::new();
        for i in 0..3 {
            let mut ext = build_ext(&[]);
            ext.set_body(Some(format!("m{i}").as_bytes()));
            stream.extend_from_slice(&encode_message_ext(&ext, false).unwrap());
        }
        assert_eq!(count_inner_msg_num(&stream), 3);
        assert_eq!(count_inner_msg_num(&[]), 0);
        assert_eq!(count_inner_msg_num(b"\x00\x00"), 1);
    }

    #[test]
    fn ipv6_hosts() {
        let mut ext = build_ext(&[]);
        ext.sys_flag = MessageSysFlag::BORNHOST_V6_FLAG | MessageSysFlag::STOREHOSTADDRESS_V6_FLAG;
        ext.set_born_host(Some("2001:db8::1"));
        ext.set_store_host(Some("2001:db8::2"));
        ext.commit_log_offset = 64;
        let raw = encode_message_ext(&ext, false).unwrap();
        let got = decode_message(&raw).unwrap();
        assert_eq!(got.get_born_host(), Some("2001:db8::1"));
        assert_eq!(got.get_store_host(), Some("2001:db8::2"));
        assert_eq!(
            got.get_msg_id(),
            Some(create_message_id(
                &ip_and_port_to_bytes("2001:db8::2", 10911, true).unwrap(),
                64
            ).as_str())
        );
        // Python 真值（156 字节）
        assert_eq!(
            got.get_msg_id(),
            Some("20010DB800000000000000000000000200002A9F0000000000000040")
        );
    }

    #[test]
    fn ip_port_bytes() {
        let raw = ip_and_port_to_bytes("10.0.0.1", 9876, false).unwrap();
        assert_eq!(raw.len(), 8);
        assert_eq!(bytes_to_ip_and_port(&raw).unwrap(), ("10.0.0.1".to_string(), 9876));
        let raw6 = ip_and_port_to_bytes("2001:db8::1", 9876, true).unwrap();
        assert_eq!(raw6.len(), 20);
        assert_eq!(
            bytes_to_ip_and_port(&raw6).unwrap(),
            ("2001:db8::1".to_string(), 9876)
        );
        assert!(matches!(ip_and_port_to_bytes("not-an-ip", 1, false), Err(Error::Decode(_))));
        assert!(matches!(
            ip_and_port_to_bytes("2001:db8::1", 1, false),
            Err(Error::Decode(_))
        ));
        assert!(matches!(bytes_to_ip_and_port(&[0u8; 6]), Err(Error::Decode(_))));
    }

    // --------------------------------------------------------- msgId

    #[test]
    fn msg_id_create_and_decode_v4() {
        let msg_id =
            create_message_id(&ip_and_port_to_bytes("10.0.0.1", 10911, false).unwrap(), 2048);
        assert_eq!(msg_id.len(), 32);
        assert_eq!(msg_id, msg_id.to_ascii_uppercase());
        let (ip, port, offset) = decode_message_id(&msg_id).unwrap();
        assert_eq!((ip.as_str(), port, offset), ("10.0.0.1", 10911, 2048));
    }

    #[test]
    fn msg_id_create_and_decode_v6() {
        let msg_id =
            create_message_id(&ip_and_port_to_bytes("2001:db8::1", 10911, true).unwrap(), 4096);
        assert_eq!(msg_id.len(), 56);
        let (ip, port, offset) = decode_message_id(&msg_id).unwrap();
        assert_eq!((ip.as_str(), port, offset), ("2001:db8::1", 10911, 4096));
    }

    #[test]
    fn msg_id_bad_input() {
        assert!(matches!(decode_message_id("zz"), Err(Error::Decode(_))));
        assert!(matches!(decode_message_id(""), Err(Error::Decode(_))));
        assert!(matches!(
            decode_message_id(&"0A".repeat(8)),
            Err(Error::Decode(_))
        ));
    }

    #[test]
    fn bytes2string_matches_java_hex() {
        assert_eq!(bytes2string(b"\x00\x0f\xab\xff"), "000FABFF");
        assert_eq!(string2bytes_hex("000FABFF"), Some(vec![0x00, 0x0F, 0xAB, 0xFF]));
        assert_eq!(string2bytes_hex(""), None);
        assert_eq!(string2bytes_hex("XYZ"), None);
    }

    // ------------------------------------------------------------ V2

    fn build_v2_frame(topic: &str, body: &[u8], properties_bytes: &[u8]) -> Vec<u8> {
        let topic_bytes = string2bytes(topic);
        let store_size = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 8
            + 4
            + body.len()
            + 2
            + topic_bytes.len()
            + 2
            + properties_bytes.len();
        let mut w = Writer::with_capacity(store_size);
        w.i32(store_size as i32);
        w.i32(MESSAGE_MAGIC_CODE_V2);
        w.u32(crc32(body));
        w.i32(0);
        w.i32(0);
        w.i64(0);
        w.i64(2048);
        w.i32(0);
        w.i64(1_700_000_000_000);
        w.bytes(&ip_and_port_to_bytes("127.0.0.1", 54321, false).unwrap());
        w.i64(1_700_000_000_123);
        w.bytes(&ip_and_port_to_bytes("127.0.0.1", 10911, false).unwrap());
        w.i32(0);
        w.i64(0);
        w.i32(body.len() as i32);
        w.bytes(body);
        w.u16(topic_bytes.len() as u32);
        w.bytes(&topic_bytes);
        w.u16(properties_bytes.len() as u32);
        w.bytes(properties_bytes);
        w.into_inner()
    }

    #[test]
    fn v2_long_topic_decode() {
        let topic = format!("TopicTestLong{}", "x".repeat(130));
        let raw = build_v2_frame(&topic, b"batch-body", b"TAGS\x01TagB\x02");
        let msgs = decode_messages(&raw);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].get_topic(), topic);
        assert_eq!(msgs[0].get_body(), b"batch-body");
        assert_eq!(msgs[0].get_property("TAGS"), Some("TagB"));
    }

    #[test]
    fn v2_msg_id_and_python_fixture() {
        let topic = "L".repeat(200);
        let raw = build_v2_frame(&topic, b"x", b"");
        let got = decode_message(&raw).unwrap();
        assert_eq!(
            got.get_msg_id(),
            Some(create_message_id(&ip_and_port_to_bytes("127.0.0.1", 10911, false).unwrap(), 2048)
                .as_str())
        );
        // V2 帧用 u16 长度字段，长 topic 也能表达；totalLength 必须与解码出的 storeSize 一致。
        let expected = build_v2_frame(&topic, b"batch-body", b"TAGS\x01TagB\x02");
        assert_eq!(expected.len(), 84 + 4 + 10 + 2 + 200 + 2 + 10);
        assert_eq!(
            i32::from_be_bytes(expected[..4].try_into().unwrap()) as usize,
            expected.len(),
            "totalLength 与实际字节数一致"
        );
        let reparsed = decode_message(&expected).unwrap();
        assert_eq!(reparsed.get_store_size(), expected.len() as i32);
        assert_eq!(reparsed.get_topic(), topic);
        assert_eq!(reparsed.get_property("TAGS"), Some("TagB"));
    }

    #[test]
    fn v1_topic_too_long_is_an_error() {
        let mut ext = build_ext(&[]);
        ext.set_topic(&"T".repeat(256));
        assert!(matches!(
            encode_message_ext(&ext, false),
            Err(Error::Encode(_))
        ));
    }

    // -------------------------------------------------------- 批量格式

    #[test]
    fn batch_encode_layout() {
        let mut msg = Message::with_tags_and_keys("T", Some(b"body"), Some("TagA"), None, 0);
        msg.set_flag(7);
        let raw = encode_message(&msg);
        let int_at = |p: usize| i32::from_be_bytes(raw[p..p + 4].try_into().unwrap());
        assert_eq!(int_at(0) as usize, raw.len());
        assert_eq!(int_at(4), 0, "MAGICCODE 固定 0");
        assert_eq!(int_at(8), 0, "BODYCRC 固定 0");
        assert_eq!(int_at(12), 7, "FLAG");
        assert_eq!(int_at(16), 4, "BODY LEN");
    }

    #[test]
    fn batch_round_trip() {
        let msg = Message::with_tags_and_keys("T", Some(b"body"), Some("TagA"), Some("k1"), 7);
        let got = decode_batch_message(&encode_message(&msg)).unwrap();
        assert_eq!(got.get_body(), b"body");
        assert_eq!(got.get_flag(), msg.get_flag());
        assert_eq!(got.get_properties(), msg.get_properties());
        // Python 真值
        assert_eq!(
            bytes2string(&encode_message(&msg)),
            "0000002C00000000000000000000000700000004626F64790012544147530154616741024B455953016B\
             3102"
        );
    }

    #[test]
    fn batch_round_trip_multiple() {
        let msgs = vec![
            Message::new("T", Some(b"m1")),
            Message::with_tags_and_keys("T", Some(b"m2"), Some("TagA"), None, 0),
        ];
        let raw = encode_messages(&msgs);
        let got = decode_batch_messages(&raw);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].get_body(), b"m1");
        assert_eq!(got[1].get_body(), b"m2");
        assert_eq!(got[1].get_property("TAGS"), Some("TagA"));
        assert_eq!(count_inner_msg_num(&raw), 2);
    }

    #[test]
    fn batch_messages_fixture_matches_python() {
        let msgs = vec![
            Message::new("T", Some(b"m1")),
            Message::with_tags_and_keys("T", Some(b"m2"), Some("TagA"), None, 0),
        ];
        // Python: encode_messages([Message("T", b"m1"), Message("T", b"m2", tags=TagA)])
        assert_eq!(
            bytes2string(&encode_messages(&msgs)),
            "00000018000000000000000000000000000000026D310000000000220000000000000000000000000000\
             00026D32000A54414753015461674102"
        );
    }

    #[test]
    fn batch_decode_stops_on_garbage() {
        let mut raw = encode_messages(&[Message::new("T", Some(b"m1"))]);
        raw.extend_from_slice(b"\x00\x00");
        assert_eq!(decode_batch_messages(&raw).len(), 1);
    }

    // --------------------------------------------------------- crc32

    #[test]
    fn crc32_is_standard() {
        assert_eq!(crc32(b""), 0);
        assert_eq!(crc32(b"a"), 0xE8B7_BE43);
        assert_eq!(crc32(b"abc"), 0x3524_41C2);
        assert_eq!(crc32(b"1234567890"), 639_479_525);
    }

    #[test]
    fn message_bytes_size_matches_encode_len() {
        let ext = build_ext(&[]);
        let body_len = ext.body.as_deref().map(<[u8]>::len).unwrap_or(0);
        let size = message_bytes_size(&ext.topic, body_len, &ext.properties, ext.sys_flag);
        assert_eq!(size, encode_message_ext(&ext, false).unwrap().len());
        let v6 = MessageSysFlag::BORNHOST_V6_FLAG | MessageSysFlag::STOREHOSTADDRESS_V6_FLAG;
        assert_eq!(
            message_bytes_size(&ext.topic, body_len, &ext.properties, v6),
            size + 24,
            "两个 v6 地址各多 12 字节"
        );
    }

    // --------------------------------------------------------- 压缩路径

    #[test]
    fn compressed_message_round_trip() {
        let plain = b"compress-me".repeat(50);
        let sys_flag = MessageSysFlag::COMPRESSED_FLAG
            | MessageSysFlag::set_compression_type(0, MessageSysFlag::ZLIB_TYPE);
        let mut ext = build_ext(&[]);
        ext.set_body(Some(&plain));
        ext.body_crc = crc32(&plain);
        ext.sys_flag = sys_flag;

        let raw = encode_message_ext(&ext, true).unwrap();
        assert!(raw.len() < plain.len());
        let got = decode_message(&raw).unwrap();
        assert_eq!(got.get_body(), plain);
        // Java: sysFlag &= ~COMPRESSED_FLAG，只清标志位，压缩类型位（bit8~10）保留
        assert_eq!(got.sys_flag, MessageSysFlag::clear_compressed_flag(sys_flag));
        assert_eq!(
            got.sys_flag,
            MessageSysFlag::set_compression_type(0, MessageSysFlag::ZLIB_TYPE)
        );

        // 不解压路径：body 仍是压缩字节，标志位保留
        let options = DecodeOptions {
            decompress_body: false,
            ..DecodeOptions::default()
        };
        let kept = decode_message_with(&raw, &options).unwrap();
        assert_ne!(kept.get_body(), plain.as_slice());
        assert_eq!(kept.sys_flag, sys_flag);
        assert_eq!(
            compression::decompress(kept.get_body(), MessageSysFlag::ZLIB_TYPE).unwrap(),
            plain
        );
    }

    #[test]
    fn legacy_type_zero_is_decompressed_as_zlib() {
        let plain = b"legacy-body".repeat(20);
        let mut ext = build_ext(&[]);
        ext.body_crc = crc32(&plain);
        ext.sys_flag = MessageSysFlag::COMPRESSED_FLAG; // 类型位为 0：老客户端
        ext.set_body(Some(&compression::compress(&plain, 0, 5).unwrap()));
        let raw = encode_message_ext(&ext, false).unwrap();
        let got = decode_message(&raw).unwrap();
        assert_eq!(got.get_body(), plain);
        assert_eq!(got.sys_flag, 0);
    }

    #[test]
    fn unsupported_compression_fails_instead_of_passthrough() {
        let plain = b"payload-that-should-never-be-returned-as-is".repeat(20);
        let sys_flag = MessageSysFlag::COMPRESSED_FLAG
            | MessageSysFlag::set_compression_type(0, MessageSysFlag::SNAPPY_TYPE);
        let mut ext = build_ext(&[]);
        ext.body_crc = crc32(&plain);
        ext.sys_flag = sys_flag;
        ext.set_body(Some(&compression::compress(&plain, 3, 5).unwrap()));
        let raw = encode_message_ext(&ext, false).unwrap();
        // 消息被丢弃（Java decode 的 catch -> null），而不是交回压缩流
        assert!(matches!(decode_message(&raw), Err(Error::Decode(_))));
        // 反向确认：同一帧只把类型位改成 ZLIB 就能正常解
        let mut ok = raw.clone();
        let ok_flag = MessageSysFlag::COMPRESSED_FLAG
            | MessageSysFlag::set_compression_type(0, MessageSysFlag::ZLIB_TYPE);
        ok[SYSFLAG_POSITION..SYSFLAG_POSITION + 4].copy_from_slice(&ok_flag.to_be_bytes());
        let good = decode_message(&ok).unwrap();
        assert_eq!(good.get_body(), plain);
    }

    #[test]
    fn filter_bitmap_parsing() {
        assert_eq!(
            parse_topic_filter_bitmap("10110").unwrap(),
            vec![true, false, true, true, false]
        );
        assert!(parse_topic_filter_bitmap("").unwrap().is_empty());
        assert!(matches!(
            parse_topic_filter_bitmap("10x"),
            Err(Error::Decode(_))
        ));
    }
}
