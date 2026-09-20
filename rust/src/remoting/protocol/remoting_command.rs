//! `RemotingCommand`：RocketMQ 远程命令。
//!
//! 线格式：`totalLength(4) | headerLength(高 8 位为序列化类型, 4) | header | body`
//! - JSON header：`RemotingSerializable` 的 JSON 编码
//! - ROCKETMQ header：`code(2) language(1) version(2) opaque(4) flag(4) remark(int+utf8) extFields(int + key(short+utf8) value(int+utf8))`

use std::any::Any;
use std::sync::atomic::{AtomicI32, Ordering};

use serde_json::{Map, Value};

use super::ext_fields::{CustomHeader, ExtFields};
use super::serialize::{
    header_length_of, mark_protocol_type, protocol_type_of, serialize_type_from_env,
    RemotingSerializable, RocketMQSerializable,
};
use crate::error::{Error, Result};
use crate::remoting::protocol::codes::{
    language_code, remoting_command_type, serialize_type,
};

pub const RPC_TYPE: i32 = 0;
pub const RPC_ONEWAY: i32 = 1;

pub const REMOTING_VERSION_KEY: &str = "rocketmq.remoting.version";

/// Java `MQVersion.CURRENT_VERSION`（= `Version.V5_5_1.ordinal()`，本机 5.5.1 集群）。
///
/// 这个值不只是好看：broker 用它决定能不能把管理请求转发回客户端——
/// `AdminBrokerProcessor#callConsumer` 要求 ≥ `V3_1_8_SNAPSHOT`（ordinal 62），
/// `Broker2Client#getConsumeStatus` 要求 ≥ `V3_0_7_SNAPSHOT`（ordinal 28）。
/// 之前默认 0（= `V3_0_0_SNAPSHOT`），于是 `examineConsumerRunningInfo` /
/// `getConsumeStatus` 一律回 `code=1 too low to finish`。
pub const CURRENT_VERSION: i32 = 515;

static REQUEST_ID: AtomicI32 = AtomicI32::new(0);

pub fn next_opaque() -> i32 {
    REQUEST_ID.fetch_add(1, Ordering::SeqCst)
}

pub struct RemotingCommand {
    pub code: i32,
    pub language: i32,
    pub version: i32,
    pub opaque: i32,
    pub flag: i32,
    pub remark: Option<String>,
    pub serialize_type_current_rpc: i32,
    pub body: Option<Vec<u8>>,
    ext_fields: ExtFields,
    custom_header: Option<Box<dyn CustomHeader>>,
}

impl Clone for RemotingCommand {
    fn clone(&self) -> RemotingCommand {
        RemotingCommand {
            code: self.code,
            language: self.language,
            version: self.version,
            opaque: self.opaque,
            flag: self.flag,
            remark: self.remark.clone(),
            serialize_type_current_rpc: self.serialize_type_current_rpc,
            body: self.body.clone(),
            ext_fields: self.ext_fields.clone(),
            custom_header: self.custom_header.as_ref().map(|h| h.boxed_clone()),
        }
    }
}

impl Default for RemotingCommand {
    fn default() -> RemotingCommand {
        RemotingCommand::new()
    }
}

impl RemotingCommand {
    pub fn new() -> RemotingCommand {
        RemotingCommand {
            code: 0,
            language: language_code::RUST,
            version: 0,
            opaque: next_opaque(),
            flag: 0,
            remark: None,
            serialize_type_current_rpc: serialize_type_from_env(),
            body: None,
            ext_fields: ExtFields::new(),
            custom_header: None,
        }
    }

    pub fn create_request_command(code: i32, header: Option<Box<dyn CustomHeader>>) -> RemotingCommand {
        let mut cmd = RemotingCommand::new();
        cmd.code = code;
        cmd.custom_header = header;
        cmd.set_cmd_version();
        cmd
    }

    pub fn create_response_command_with_header(
        code: i32,
        header: Option<Box<dyn CustomHeader>>,
    ) -> RemotingCommand {
        let mut cmd = RemotingCommand::new();
        cmd.code = code;
        cmd.custom_header = header;
        cmd.mark_response_type();
        cmd.set_cmd_version();
        cmd
    }

    /// 对应 Java `createResponseCommand(int code, String remark, Class classHeader)`。
    pub fn create_response(code: i32, remark: Option<String>) -> RemotingCommand {
        let mut cmd = RemotingCommand::new();
        cmd.code = code;
        cmd.remark = remark;
        cmd.mark_response_type();
        cmd.set_cmd_version();
        cmd
    }

    pub fn build_error_response(code: i32, remark: &str) -> RemotingCommand {
        RemotingCommand::create_response(code, Some(remark.to_string()))
    }

    fn set_cmd_version(&mut self) {
        let raw = std::env::var(REMOTING_VERSION_KEY)
            .or_else(|_| std::env::var("ROCKETMQ_REMOTING_VERSION"))
            .ok();
        self.version = raw
            .as_deref()
            .and_then(|v| v.trim().parse::<i32>().ok())
            .unwrap_or(CURRENT_VERSION);
    }

    // ---------------- 标志位 ----------------
    pub fn mark_response_type(&mut self) {
        self.flag |= 1 << RPC_TYPE;
    }

    pub fn is_response_type(&self) -> bool {
        self.flag & (1 << RPC_TYPE) != 0
    }

    pub fn mark_oneway_rpc(&mut self) {
        self.flag |= 1 << RPC_ONEWAY;
    }

    pub fn is_oneway_rpc(&self) -> bool {
        self.flag & (1 << RPC_ONEWAY) != 0
    }

    pub fn command_type(&self) -> &'static str {
        if self.is_response_type() {
            remoting_command_type::RESPONSE_COMMAND
        } else {
            remoting_command_type::REQUEST_COMMAND
        }
    }

    // ---------------- 字段访问 ----------------
    pub fn ext_fields(&self) -> &ExtFields {
        &self.ext_fields
    }

    pub fn ext_fields_mut(&mut self) -> &mut ExtFields {
        &mut self.ext_fields
    }

    pub fn add_ext_field(&mut self, key: &str, value: &str) {
        self.ext_fields.insert(key, value);
    }

    pub fn get_ext_field(&self, key: &str) -> Option<&str> {
        self.ext_fields.get(key)
    }

    pub fn set_body(&mut self, body: Option<Vec<u8>>) {
        self.body = body;
    }

    pub fn body(&self) -> Option<&[u8]> {
        self.body.as_deref()
    }

    pub fn set_custom_header(&mut self, header: Option<Box<dyn CustomHeader>>) {
        self.custom_header = header;
    }

    pub fn custom_header(&self) -> Option<&dyn CustomHeader> {
        self.custom_header.as_deref()
    }

    pub fn take_custom_header(&mut self) -> Option<Box<dyn CustomHeader>> {
        self.custom_header.take()
    }

    /// 把 custom_header 的非 None 字段并入 extFields（对应 Java 的反射写入）。
    pub fn make_custom_header_to_net(&mut self) {
        if let Some(header) = self.custom_header.as_ref() {
            let mut out = ExtFields::new();
            header.to_ext_fields(&mut out);
            for (k, v) in out.iter() {
                self.ext_fields.insert(k.clone(), v.clone());
            }
        }
    }

    /// 从 extFields 还原 header 对象。优先走 `from_ext_fields`（含 V2 短字段名映射）。
    pub fn decode_command_custom_header<T>(&self) -> Result<T>
    where
        T: CustomHeader + Default + Any,
    {
        let mut header = T::default();
        header.from_ext_fields(&self.ext_fields);
        Ok(header)
    }

    pub fn decode_ext_field<T: std::str::FromStr>(&self, key: &str) -> Result<Option<T>>
    where
        T::Err: std::fmt::Display,
    {
        match self.ext_fields.get(key) {
            None => Ok(None),
            Some(raw) => raw
                .parse::<T>()
                .map(Some)
                .map_err(|e| Error::Decode(format!("extField {key}={raw:?} invalid: {e}"))),
        }
    }

    // ---------------- 编解码 ----------------
    pub fn header_encode(&mut self) -> Vec<u8> {
        self.make_custom_header_to_net();
        if self.serialize_type_current_rpc == serialize_type::ROCKETMQ {
            return RocketMQSerializable::rocket_mq_protocol_encode(self);
        }
        RemotingSerializable::encode(&self.to_json_value())
    }

    /// 整帧编码（含 4 字节 totalLength）。
    pub fn encode(&mut self) -> Vec<u8> {
        let header_data = self.header_encode();
        let mut out = Vec::with_capacity(8 + header_data.len() + self.body.as_ref().map_or(0, Vec::len));
        let header_len = header_data.len();
        let total_len = 4 + header_len + self.body.as_ref().map_or(0, Vec::len);
        out.extend_from_slice(&(total_len as i32).to_be_bytes());
        out.extend_from_slice(
            &mark_protocol_type(header_len as i32, self.serialize_type_current_rpc).to_be_bytes(),
        );
        out.extend_from_slice(&header_data);
        if let Some(body) = &self.body {
            out.extend_from_slice(body);
        }
        out
    }

    /// 只编码 header（body 由调用方另行写出），对应 Java `encodeHeader(int bodyLength)`。
    pub fn encode_header(&mut self, body_length: usize) -> Vec<u8> {
        let header_data = self.header_encode();
        let header_len = header_data.len();
        let total_len = 4 + header_len + body_length;
        let mut out = Vec::with_capacity(8 + header_len);
        out.extend_from_slice(&(total_len as i32).to_be_bytes());
        out.extend_from_slice(
            &mark_protocol_type(header_len as i32, self.serialize_type_current_rpc).to_be_bytes(),
        );
        out.extend_from_slice(&header_data);
        out
    }

    /// 解码整帧（`data` 从 totalLength 开始）。
    pub fn decode(data: &[u8]) -> Result<RemotingCommand> {
        let mut r = crate::common::buffer::Reader::new(data);
        let total_length = r.i32()?;
        if total_length > data.len() as i32 - 4 {
            return Err(Error::RemotingCommand(format!(
                "decode error, bad total length: {total_length}"
            )));
        }
        let ori_header_len = r.i32()?;
        let header_length = header_length_of(ori_header_len);
        if header_length > r.remaining() as i32 {
            return Err(Error::RemotingCommand(format!(
                "decode error, bad header length: {header_length}"
            )));
        }
        let protocol_type = protocol_type_of(ori_header_len);
        let header_data = r.bytes(header_length as usize)?.to_vec();
        let body = {
            let rest = data[r.pos()..].to_vec();
            if rest.is_empty() {
                None
            } else {
                Some(rest)
            }
        };

        let mut cmd = RemotingCommand::new();
        cmd.code = 0;
        cmd.opaque = -1;
        cmd.remark = None;
        cmd.flag = 0;
        cmd.version = 0;
        cmd.body = body;
        cmd.ext_fields = ExtFields::new();

        if protocol_type == serialize_type::ROCKETMQ {
            let header = RocketMQSerializable::rocket_mq_protocol_decode(&header_data)?;
            cmd.code = header.code;
            cmd.language = header.language;
            cmd.version = header.version;
            cmd.opaque = header.opaque;
            cmd.flag = header.flag;
            cmd.remark = header.remark;
            cmd.ext_fields = header.ext_fields;
        } else {
            let value = RemotingSerializable::decode(&header_data)?;
            cmd.code = json_i32(&value, "code", 0);
            cmd.language = match value.get("language") {
                Some(Value::String(s)) => language_code::name_to_code(s),
                Some(v) => number_as_i32(v).unwrap_or(language_code::RUST),
                None => language_code::RUST,
            };
            cmd.version = json_i32(&value, "version", 0);
            cmd.opaque = json_i32(&value, "opaque", -1);
            cmd.flag = json_i32(&value, "flag", 0);
            cmd.remark = value
                .get("remark")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if let Some(ext) = value.get("extFields") {
                cmd.ext_fields = ExtFields::from_json(ext);
            }
        }
        cmd.serialize_type_current_rpc = protocol_type;
        Ok(cmd)
    }

    pub fn to_json_value(&self) -> Value {
        let mut map = Map::new();
        map.insert("code".into(), Value::from(self.code));
        map.insert("language".into(), Value::from(self.language));
        map.insert("version".into(), Value::from(self.version));
        map.insert("opaque".into(), Value::from(self.opaque));
        map.insert("flag".into(), Value::from(self.flag));
        if let Some(remark) = &self.remark {
            map.insert("remark".into(), Value::String(remark.clone()));
        }
        if !self.ext_fields.is_empty() {
            map.insert("extFields".into(), self.ext_fields.to_json());
        }
        Value::Object(map)
    }
}

fn number_as_i32(value: &Value) -> Option<i32> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .map(|v| v as i32)
            .or_else(|| n.as_f64().map(|v| v as i32)),
        Value::String(s) => s.trim().parse::<i32>().ok(),
        _ => None,
    }
}

fn json_i32(value: &Value, key: &str, default: i32) -> i32 {
    value.get(key).and_then(number_as_i32).unwrap_or(default)
}

impl std::fmt::Debug for RemotingCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RemotingCommand {{ code: {}, language: {}, version: {}, opaque: {}, flag: {}, remark: {:?}, extFields: {}, bodyLen: {}, serializeType: {}, header: {} }}",
            self.code,
            self.language,
            self.version,
            self.opaque,
            self.flag,
            self.remark,
            self.ext_fields.len(),
            self.body.as_ref().map_or(0, Vec::len),
            self.serialize_type_current_rpc,
            self.custom_header.as_ref().map_or("none", |_| "custom"),
        )
    }
}

impl std::fmt::Display for RemotingCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RemotingCommand [code={}, language={}, version={}, opaque={}, flag(B)={:b}, remark={:?}, extFields={:?}, serializeTypeCurrentRPC={}]",
            self.code,
            self.language,
            self.version,
            self.opaque,
            self.flag,
            self.remark,
            self.ext_fields
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(","),
            self.serialize_type_current_rpc
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 出站请求必须带上 Java 口径的协议版本，否则 broker 的管理转发（307/221）会
    /// 直接以 "too low to finish" 拒掉；`new()` 是解码用的裸对象，仍保持 0。
    #[test]
    fn outgoing_commands_carry_the_protocol_version() {
        assert_eq!(RemotingCommand::new().version, 0);
        assert_eq!(
            RemotingCommand::create_request_command(0, None).version,
            CURRENT_VERSION
        );
        assert_eq!(
            RemotingCommand::create_response(0, None).version,
            CURRENT_VERSION
        );
    }
}
