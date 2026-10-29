//! 1) 17-field storage format `MessageDecoder#encode(MessageExt, needCompress)` /
//!    `decode(ByteBuffer)`, used for the message body the broker writes and pull/get returns:
//!
//!    | No. | Field | Encoding |
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
//!    | 17 | PROPERTIES | short(2) len + `k\x01v\x02` string |
//!
//! 2) 6-field lightweight format `MessageDecoder#encodeMessage(Message)` / `decodeMessage(ByteBuffer)`,
//!    used only for **batch messages** (the body of `MessageBatch`), without topic / crc:
//!    `TOTALSIZE(4) | MAGICCODE(4, fixed 0) | BODYCRC(4, fixed 0) | FLAG(4) | BODY(4+len) |
//!    PROPERTIES(2+len)`

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::common::buffer::{Reader, Writer};
use crate::common::message::{Message, MessageExt};
use crate::common::sysflag::MessageSysFlag;
use crate::common::{compression, util_all};
use crate::error::{Error, Result};
use crate::remoting::protocol::ext_fields::StringMap;

pub const NAME_VALUE_SEPARATOR: char = '\u{1}';
pub const PROPERTY_SEPARATOR: char = '\u{2}';

pub const MESSAGE_MAGIC_CODE: i32 = -626_843_481;
/// v2 magic code: the topic length field grows from 1 byte to 2 bytes.
pub const MESSAGE_MAGIC_CODE_V2: i32 = -626_843_477;
/// Magic code of a "blank record" in the commitlog, not a valid message frame.
pub const BLANK_MAGIC_CODE: i32 = -875_286_124;

pub const MESSAGE_MAGIC_CODE_POSITION: usize = 4;
pub const MESSAGE_FLAG_POSITION: usize = 16;
pub const PHY_POS_POSITION: usize = 4 + 4 + 4 + 4 + 4 + 8;
pub const QUEUE_OFFSET_POSITION: usize = 4 + 4 + 4 + 4 + 4;
pub const SYSFLAG_POSITION: usize = 4 + 4 + 4 + 4 + 4 + 8 + 8;
pub const MESSAGE_STORE_TIMESTAMP_POSITION: usize = 56;

/// Fixed header (including the BODY length field) shared by v1/v2 frames apart from the differing topic length width.
const HEADER_WITHOUT_BODY_LEN: usize = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + 8 + 4 + 8;

pub use crate::common::util_all::{crc32, crc32_raw};

/// Converts a string straight to bytes as UTF-8 (both the protocol properties section and the topic field use this encoding).
pub fn string2bytes(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

pub fn bytes2string(bytes: &[u8]) -> String {
    util_all::bytes_2_string(bytes)
}

pub fn string2bytes_hex(hex_string: &str) -> Option<Vec<u8>> {
    util_all::string_2_bytes(hex_string)
}

/// In-frame host field: ip bytes + 4-byte big-endian port.
pub fn ip_and_port_to_bytes(ip: &str, port: u32, v6: bool) -> Result<Vec<u8>> {
    let mut out = ip_to_bytes(ip, v6)?;
    out.extend_from_slice(&port.to_be_bytes());
    Ok(out)
}

/// Parses an ip into 4 or 16 bytes with inet_pton semantics.
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

/// Reverse parse of the in-frame host field: 8 bytes = ipv4+port, 20 bytes = ipv6+port.
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

/// Message ID: `addr(8|20B) + commitLogOffset(8B)` concatenated into bytes, then rendered as uppercase hex.
pub fn create_message_id(addr_bytes: &[u8], offset: i64) -> String {
    let mut raw = addr_bytes.to_vec();
    raw.extend_from_slice(&offset.to_be_bytes());
    bytes2string(&raw)
}

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

/// The four switches used when decoding a 17-field frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeOptions {
    pub read_body: bool,
    /// Whether to decompress when `COMPRESSED_FLAG` is set.
    pub decompress_body: bool,
    /// When true, `msgId` is also written into `offset_msg_id` (client view).
    pub is_client: bool,
    /// Validates the BODYCRC field (the stored CRC-32 of the body bytes with the
    /// sign bit cleared) against the body actually read.
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

/// Byte size a string occupies in the wire format: UTF-8 byte count + 2-byte length prefix.
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

pub fn decode_message(raw: &[u8]) -> Result<MessageExt> {
    decode_message_with(raw, &DecodeOptions::default())
}

pub fn decode_message_with(raw: &[u8], options: &DecodeOptions) -> Result<MessageExt> {
    let mut reader = Reader::new(raw);
    let mut msg_ext = MessageExt::new();

    let store_size = reader.i32()?;
    let magic_code = reader.i32()?;
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
            // crc32() returns the same sign-bit-cleared value the store stamps into BODYCRC,
            // so a valid frame passes this check whatever the width of its checksum.
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

pub fn decode_messages(raw: &[u8]) -> Vec<MessageExt> {
    decode_messages_with(raw, &DecodeOptions::default())
}

/// Option-taking variant of [`decode_messages`].
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

/// Writes only TOTALSIZE / MAGICCODE(0) / BODYCRC(0) / FLAG / BODY / PROPERTIES.
pub fn encode_message(message: &Message) -> Vec<u8> {
    let body: &[u8] = message.body.as_deref().unwrap_or(&[]);
    let properties_bytes = string2bytes(&message_properties_2_string(&message.properties));
    let properties_length = properties_bytes.len();
    let store_size = 4 + 4 + 4 + 4 + 4 + body.len() + 2 + properties_length;

    let mut writer = Writer::with_capacity(store_size);
    writer.i32(store_size as i32); // 1 TOTALSIZE
    writer.i32(0); // 2 MAGICCODE (fixed 0 in the batch scenario)
    writer.i32(0); // 3 BODYCRC
    writer.i32(message.flag); // 4 FLAG
    writer.i32(body.len() as i32); // 5 BODY
    writer.bytes(body);
    writer.u16(properties_length as u32); // 6 PROPERTIES
    writer.bytes(&properties_bytes);
    writer.into_inner()
}

pub fn encode_messages(messages: &[Message]) -> Vec<u8> {
    let mut out = Writer::new();
    for msg in messages {
        out.bytes(&encode_message(msg));
    }
    out.into_inner()
}

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

/// Replaces every pulled message that carries both the inner-batch and the unwrap flag with one
/// message per member of its body (a message batch); the envelope fields travel to each member.
/// A body that does not decode completely leaves the envelope in place.
pub(crate) fn unwrap_inner_batch(messages: Vec<MessageExt>) -> Vec<MessageExt> {
    use crate::common::message_const::{
        PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, PROPERTY_WAIT_STORE_MSG_OK,
    };
    use crate::rmq_error;

    let mut out = Vec::with_capacity(messages.len());
    for envelope in messages {
        let flagged = envelope.sys_flag & MessageSysFlag::INNER_BATCH_FLAG != 0
            && envelope.sys_flag & MessageSysFlag::NEED_UNWRAP_FLAG != 0;
        if !flagged {
            out.push(envelope);
            continue;
        }
        let decoded = envelope
            .body
            .as_deref()
            .ok_or_else(|| Error::Decode("inner batch has no body".into()))
            .and_then(decode_inner_batch_members);
        let members = match decoded {
            Ok(members) if !members.is_empty() => members,
            Ok(_) => {
                rmq_error!("inner batch of {} has no member", envelope.topic);
                out.push(envelope);
                continue;
            }
            Err(e) => {
                rmq_error!("decode inner batch of {} failed: {e}", envelope.topic);
                out.push(envelope);
                continue;
            }
        };
        for (i, member) in members.into_iter().enumerate() {
            let mut ext = MessageExt::new();
            ext.topic = envelope.topic.clone();
            ext.queue_offset = envelope.queue_offset + i as i64;
            ext.queue_id = envelope.queue_id;
            ext.flag = member.flag;
            ext.properties = member.properties;
            ext.body = Some(member.body.unwrap_or_default());
            ext.store_host = envelope.store_host.clone();
            ext.store_host_port = envelope.store_host_port;
            ext.born_host = envelope.born_host.clone();
            ext.born_host_port = envelope.born_host_port;
            ext.born_timestamp = envelope.born_timestamp;
            ext.store_timestamp = envelope.store_timestamp;
            ext.sys_flag = envelope.sys_flag;
            ext.commit_log_offset = envelope.commit_log_offset;
            ext.broker_name = envelope.broker_name.clone();
            if let Some(ok) = envelope.get_wait_store_msg_ok() {
                ext.put_property(PROPERTY_WAIT_STORE_MSG_OK, ok);
            }
            // A member has no store offset of its own, so its identity comes from the property.
            ext.msg_id = ext
                .get_property(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                .map(str::to_string);
            out.push(ext);
        }
    }
    out
}

fn decode_inner_batch_members(raw: &[u8]) -> Result<Vec<Message>> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < raw.len() {
        if raw.len() - pos < 4 {
            return Err(Error::Decode("truncated inner batch record".into()));
        }
        let store_size =
            i32::from_be_bytes([raw[pos], raw[pos + 1], raw[pos + 2], raw[pos + 3]]);
        if store_size <= 0 || store_size as usize > raw.len() - pos {
            return Err(Error::Decode(format!(
                "illegal inner batch record size: {store_size}"
            )));
        }
        let end = pos + store_size as usize;
        out.push(decode_batch_message(&raw[pos..end])?);
        pos = end;
    }
    Ok(out)
}

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

    /// A 139-byte extended-message frame: ground truth of the v1 17-field encoding, BODYCRC
    /// written the way the store writes it (0x7330_7B09 = CRC-32 of "hello rocketmq" with the
    /// sign bit cleared, not the full 0xF330_7B09).
    const EXT_HEX: &str = "0000008bdaa320a773307b09000000030000000200000000000000580000000000000400\
                           000000000000018bcfe568007f0000010000d4310000018bcfe5687b7f00000100002a9f\
                           0000000100000000000000000000000e68656c6c6f20726f636b65746d7109546f706963\
                           546573740019544147530154616741024b455953016b657931206b65793202";
    const EXT_MSG_ID: &str = "7F00000100002A9F0000000000000400";

    fn unhex(hex: &str) -> Vec<u8> {
        util_all::string_2_bytes(&hex.replace(['\n', ' '], "")).unwrap()
    }

    // ---------------------------------------------------------- property string

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
        assert!(string_2_message_properties("a\u{2}").is_empty());
        assert!(string_2_message_properties("ab\u{2}").is_empty());
        assert!(string_2_message_properties("no-separator\u{2}").is_empty());
        let one = string_2_message_properties("k\u{1}v\u{2}");
        assert_eq!(one.get("k"), Some("v"));
        // An empty value k\x01\x02 is an "illegal segment" and gets dropped -- locks in that behavior
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
        // StringMap stores String, so a "null value" is equivalent to "key absent"
        let empty = StringMap::new();
        assert_eq!(message_properties_2_string(&empty), "");
    }

    // ------------------------------------------------------ 17-field codec

    #[test]
    fn encode_matches_golden_bytes() {
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
        assert_eq!(got.get_body_crc(), 0x7330_7B09, "stored BODYCRC field");
        assert_eq!(crc32(b"hello rocketmq"), 0x7330_7B09);
        assert_ne!(crc32_raw(b"hello rocketmq"), got.get_body_crc());
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
        assert_eq!(declared as usize, raw.len(), "storeSize=1 should be floored by the max fallback");
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
        // Fixed header is 68 bytes, plus two IPv4 addresses at 8 bytes each = 84, with the 4-byte body length prefix after that
        assert_eq!(HEADER_WITHOUT_BODY_LEN, 68);
        assert_eq!(HEADER_WITHOUT_BODY_LEN + 8 + 8, 84);
        raw[88] ^= 0xFF;
        let options = DecodeOptions {
            check_crc: true,
            ..DecodeOptions::default()
        };
        let err = decode_message_with(&raw, &options).unwrap_err();
        assert!(matches!(err, Error::Decode(m) if m.contains("crc")));
        // Without the CRC check the frame still decodes (the body is already corrupt data)
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
        // Ground truth captured from the wire: a 100-byte frame with a 0-length body
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
        // Ground truth captured from the wire (156 bytes)
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
    fn bytes2string_matches_wire_hex() {
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
    fn v2_msg_id_and_golden_fixture() {
        let topic = "L".repeat(200);
        let raw = build_v2_frame(&topic, b"x", b"");
        let got = decode_message(&raw).unwrap();
        assert_eq!(
            got.get_msg_id(),
            Some(create_message_id(&ip_and_port_to_bytes("127.0.0.1", 10911, false).unwrap(), 2048)
                .as_str())
        );
        // V2 frames use a u16 length field, so long topics can also be expressed; totalLength must match the decoded storeSize.
        let expected = build_v2_frame(&topic, b"batch-body", b"TAGS\x01TagB\x02");
        assert_eq!(expected.len(), 84 + 4 + 10 + 2 + 200 + 2 + 10);
        assert_eq!(
            i32::from_be_bytes(expected[..4].try_into().unwrap()) as usize,
            expected.len(),
            "totalLength matches the actual byte count"
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

    // -------------------------------------------------------- batch format

    #[test]
    fn batch_encode_layout() {
        let mut msg = Message::with_tags_and_keys("T", Some(b"body"), Some("TagA"), None, 0);
        msg.set_flag(7);
        let raw = encode_message(&msg);
        let int_at = |p: usize| i32::from_be_bytes(raw[p..p + 4].try_into().unwrap());
        assert_eq!(int_at(0) as usize, raw.len());
        assert_eq!(int_at(4), 0, "MAGICCODE fixed 0");
        assert_eq!(int_at(8), 0, "BODYCRC fixed 0");
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
        // Byte string captured from the wire
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
    fn batch_messages_fixture_matches_golden() {
        let msgs = vec![
            Message::new("T", Some(b"m1")),
            Message::with_tags_and_keys("T", Some(b"m2"), Some("TagA"), None, 0),
        ];
        // Batch frame byte string captured from the wire: m1 without properties + m2 with TagA
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

    fn inner_batch_envelope(body: &[u8]) -> MessageExt {
        let flags = MessageSysFlag::INNER_BATCH_FLAG | MessageSysFlag::NEED_UNWRAP_FLAG;
        let flags = flags.to_string();
        let mut envelope = build_ext(&[("sys_flag", flags.as_str())]);
        envelope.set_body(Some(body));
        envelope.set_wait_store_msg_ok(true);
        envelope
    }

    #[test]
    fn inner_batch_is_replaced_by_one_message_per_member() {
        let members = vec![
            Message::with_tags_and_keys("", Some(b"m1"), Some("TagA"), None, 11),
            Message::with_tags_and_keys("", Some(b"m2"), Some("TagB"), None, 12),
        ];
        let envelope = inner_batch_envelope(&encode_messages(&members));
        let out = unwrap_inner_batch(vec![envelope]);
        assert_eq!(out.len(), 2);
        for (i, (msg, member)) in out.iter().zip(members.iter()).enumerate() {
            assert_eq!(msg.get_topic(), "TopicTest");
            assert_eq!(msg.get_queue_offset(), 88 + i as i64);
            assert_eq!(msg.get_queue_id(), 3);
            assert_eq!(msg.get_flag(), member.get_flag());
            assert_eq!(msg.get_body(), member.get_body());
            assert_eq!(msg.get_property("TAGS"), member.get_property("TAGS"));
            assert_eq!(msg.get_store_host(), Some("127.0.0.1"));
            assert_eq!(msg.get_born_host(), Some("127.0.0.1"));
            assert_eq!(msg.born_timestamp, 1_700_000_000_000);
            assert_eq!(msg.store_timestamp, 1_700_000_000_123);
            assert_eq!(msg.sys_flag, envelope_flag());
            assert_eq!(msg.commit_log_offset, 1024);
            assert_eq!(msg.get_wait_store_msg_ok(), Some("true"));
        }
    }

    fn envelope_flag() -> i32 {
        MessageSysFlag::INNER_BATCH_FLAG | MessageSysFlag::NEED_UNWRAP_FLAG
    }

    #[test]
    fn an_undecodable_inner_batch_keeps_the_envelope() {
        let raw = encode_messages(&[Message::new("", Some(b"m1"))]);
        let truncated = &raw[..raw.len() - 1];
        let out = unwrap_inner_batch(vec![inner_batch_envelope(truncated)]);
        assert_eq!(out.len(), 1, "a broken batch must not lose the message");
        assert_eq!(out[0].get_queue_offset(), 88);
        assert_eq!(out[0].sys_flag, envelope_flag());
    }

    #[test]
    fn a_message_without_the_unwrap_pair_is_left_alone() {
        let mut envelope = build_ext(&[]);
        envelope.sys_flag = MessageSysFlag::INNER_BATCH_FLAG;
        envelope.set_body(Some(&encode_messages(&[Message::new("", Some(b"m1"))])));
        let out = unwrap_inner_batch(vec![envelope]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].get_queue_offset(), 88);
    }

    // --------------------------------------------------------- crc32

    /// BODYCRC sits right after TOTALSIZE(4) and MAGICCODE(4).
    const BODY_CRC_POSITION: usize = 8;

    /// Chosen because its unmasked checksum has the sign bit set.
    const HIGH_BIT_BODY: &[u8] = b"crc-highbit-2";

    /// Full CRC-32 of [`HIGH_BIT_BODY`] before the store clears the sign bit.
    const HIGH_BIT_CRC_RAW: u32 = 0x9BD9_A717;

    /// The BODYCRC field a store frame carries for [`HIGH_BIT_BODY`].
    const HIGH_BIT_CRC_STORED: u32 = 0x1BD9_A717;

    #[test]
    fn body_crc_is_crc32_with_sign_bit_cleared() {
        assert_eq!(crc32(b""), 0);
        // Sign bit set: the stored field and the unmasked checksum diverge.
        assert_eq!(crc32_raw(b"a"), 0xE8B7_BE43);
        assert_eq!(crc32(b"a"), 0x68B7_BE43);
        // Below 2^31 the two coincide, which is exactly why both are pinned here.
        assert_eq!(crc32_raw(b"abc"), 0x3524_41C2);
        assert_eq!(crc32(b"abc"), 0x3524_41C2);
        assert_eq!(crc32_raw(b"1234567890"), 639_479_525);
        assert_eq!(crc32(b"1234567890"), 639_479_525);
        // The fixture pair, pinned from both sides.
        assert_eq!(crc32_raw(HIGH_BIT_BODY), HIGH_BIT_CRC_RAW);
        assert_eq!(crc32(HIGH_BIT_BODY), HIGH_BIT_CRC_STORED);
        assert_eq!(HIGH_BIT_CRC_RAW & 0x7FFF_FFFF, HIGH_BIT_CRC_STORED);
    }

    #[test]
    fn check_crc_accepts_high_bit_body_whose_stored_crc_is_masked() {
        let mut ext = build_ext(&[]);
        ext.set_body(Some(HIGH_BIT_BODY));
        // Field taken from the stored constant, never derived through crc32().
        ext.set_body_crc(HIGH_BIT_CRC_STORED);
        let frame = encode_message_ext(&ext, false).unwrap();
        let options = DecodeOptions {
            check_crc: true,
            ..DecodeOptions::default()
        };
        let got = decode_message_with(&frame, &options)
            .expect("valid frame whose CRC has the sign bit set must pass");
        assert_eq!(got.get_body(), HIGH_BIT_BODY);
        assert_eq!(got.get_body_crc(), HIGH_BIT_CRC_STORED);
    }

    #[test]
    fn check_crc_rejects_a_wrong_stored_crc_for_the_same_body() {
        let mut ext = build_ext(&[]);
        ext.set_body(Some(HIGH_BIT_BODY));
        ext.set_body_crc(HIGH_BIT_CRC_STORED ^ 0x0000_0001);
        let frame = encode_message_ext(&ext, false).unwrap();
        let options = DecodeOptions {
            check_crc: true,
            ..DecodeOptions::default()
        };
        let err = decode_message_with(&frame, &options).unwrap_err();
        assert!(matches!(err, Error::Decode(m) if m.contains("crc")));
        // The identical frame decodes once the opt-in check is off.
        assert_eq!(decode_message(&frame).unwrap().get_body(), HIGH_BIT_BODY);
    }

    #[test]
    fn encoder_stamps_body_crc_with_sign_bit_cleared() {
        let mut ext = build_ext(&[]);
        ext.set_body(Some(HIGH_BIT_BODY));
        ext.body_crc = crc32(ext.body.as_deref().unwrap_or(&[]));
        let frame = encode_message_ext(&ext, false).unwrap();
        let field_at = |p: usize| u32::from_be_bytes(frame[p..p + 4].try_into().unwrap());
        assert_eq!(field_at(BODY_CRC_POSITION), HIGH_BIT_CRC_STORED);
        assert_ne!(field_at(BODY_CRC_POSITION), HIGH_BIT_CRC_RAW);
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
            "each of the two v6 addresses adds 12 bytes"
        );
    }

    // --------------------------------------------------------- compression path

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
        assert_eq!(got.sys_flag, MessageSysFlag::clear_compressed_flag(sys_flag));
        assert_eq!(
            got.sys_flag,
            MessageSysFlag::set_compression_type(0, MessageSysFlag::ZLIB_TYPE)
        );

        // No-decompression path: the body stays as compressed bytes and the flag bits are preserved
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
        ext.sys_flag = MessageSysFlag::COMPRESSED_FLAG; // type bits are 0: legacy client
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
        assert!(matches!(decode_message(&raw), Err(Error::Decode(_))));
        // Cross-check: the same frame decodes normally once only the type bits are switched to ZLIB
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
