//! 协议序列化：JSON（`RemotingSerializable`）与 RocketMQ 私有二进制（`RocketMQSerializable`）。
//!
//! JSON 侧必须容忍 fastjson2 的非标准输出：Map 的裸数字键、以对象为键（`offsetTable`
//! 以 MessageQueue 为键）、`NaN` / `Infinity`、尾随逗号。

use serde_json::{Map, Number, Value};

use super::ext_fields::ExtFields;
use super::remoting_command::RemotingCommand;
use crate::common::buffer::{Reader, Writer};
use crate::error::{Error, Result};
use crate::remoting::protocol::codes::serialize_type;

const WS: &[u8] = b" \t\r\n";

pub struct RemotingSerializable;

impl RemotingSerializable {
    pub fn encode(value: &Value) -> Vec<u8> {
        if value.is_null() {
            return Vec::new();
        }
        serde_json::to_vec(value).unwrap_or_default()
    }

    pub fn to_json_string(value: &Value) -> String {
        serde_json::to_string(value).unwrap_or_else(|_| "null".into())
    }

    pub fn to_json_pretty(value: &Value) -> String {
        serde_json::to_string_pretty(value).unwrap_or_else(|_| "null".into())
    }

    /// 严格 JSON 优先，失败后走 fastjson2 容忍解析。
    pub fn decode(data: &[u8]) -> Result<Value> {
        if data.is_empty() {
            return Ok(Value::Null);
        }
        let text = std::str::from_utf8(data)
            .map_err(|e| Error::Decode(format!("json header is not valid utf8: {e}")))?;
        fastjson::from_str(text)
    }
}

pub struct FastJsonError {
    pub message: String,
    pub offset: usize,
}

/// fastjson2 兼容子集解析器（允许非字符串 map 键）。
pub mod fastjson {
    use super::*;

    pub fn from_str(text: &str) -> Result<Value> {
        let mut parser = FastJsonParser::new(text);
        let value = parser.parse()?;
        parser.skip_ws();
        if !parser.eof() {
            return Err(Error::Decode(format!(
                "json trailing data at offset {}",
                parser.pos
            )));
        }
        Ok(value)
    }

    pub fn from_slice(data: &[u8]) -> Result<Value> {
        let text = std::str::from_utf8(data)
            .map_err(|e| Error::Decode(format!("json is not valid utf8: {e}")))?;
        from_str(text)
    }

    /// 把 fastjson2 写出的内联对象键（`{"brokerName":"b","queueId":3}`）还原成 `Value`。
    pub fn decode_map_key(key: &str) -> Option<Value> {
        let text = key.trim();
        if !text.starts_with('{') {
            return None;
        }
        from_str(text).ok()
    }

    struct FastJsonParser<'a> {
        bytes: &'a [u8],
        text: &'a str,
        pos: usize,
    }

    impl<'a> FastJsonParser<'a> {
        fn new(text: &'a str) -> FastJsonParser<'a> {
            FastJsonParser { bytes: text.as_bytes(), text, pos: 0 }
        }

        fn eof(&self) -> bool {
            self.pos >= self.bytes.len()
        }

        fn peek(&self) -> Option<u8> {
            self.bytes.get(self.pos).copied()
        }

        fn skip_ws(&mut self) {
            while let Some(c) = self.peek() {
                if WS.contains(&c) {
                    self.pos += 1;
                } else {
                    break;
                }
            }
        }

        fn parse(&mut self) -> Result<Value> {
            self.value()
        }

        fn value(&mut self) -> Result<Value> {
            self.skip_ws();
            match self.peek() {
                Some(b'{') => self.object().map(Value::Object),
                Some(b'[') => self.array().map(Value::Array),
                Some(b'"') => self.string().map(Value::String),
                Some(_) => self.literal(),
                None => Err(Error::Decode(format!(
                    "unexpected end of input at offset {}",
                    self.pos
                ))),
            }
        }

        fn object(&mut self) -> Result<Map<String, Value>> {
            self.pos += 1;
            let mut result = Map::new();
            self.skip_ws();
            if self.peek() == Some(b'}') {
                self.pos += 1;
                return Ok(result);
            }
            loop {
                let key = self.key()?;
                self.skip_ws();
                if self.peek() != Some(b':') {
                    return Err(Error::Decode(format!(
                        "expected ':' at offset {}",
                        self.pos
                    )));
                }
                self.pos += 1;
                let value = self.value()?;
                result.insert(key, value);
                self.skip_ws();
                match self.peek() {
                    Some(b',') => {
                        self.pos += 1;
                        self.skip_ws();
                        if self.peek() == Some(b'}') {
                            self.pos += 1;
                            return Ok(result);
                        }
                    }
                    Some(b'}') => {
                        self.pos += 1;
                        return Ok(result);
                    }
                    _ => {
                        return Err(Error::Decode(format!(
                            "expected ',' or '}}' at offset {}",
                            self.pos
                        )))
                    }
                }
            }
        }

        fn array(&mut self) -> Result<Vec<Value>> {
            self.pos += 1;
            let mut items = Vec::new();
            self.skip_ws();
            if self.peek() == Some(b']') {
                self.pos += 1;
                return Ok(items);
            }
            loop {
                items.push(self.value()?);
                self.skip_ws();
                match self.peek() {
                    Some(b',') => {
                        self.pos += 1;
                        self.skip_ws();
                        if self.peek() == Some(b']') {
                            self.pos += 1;
                            return Ok(items);
                        }
                    }
                    Some(b']') => {
                        self.pos += 1;
                        return Ok(items);
                    }
                    _ => {
                        return Err(Error::Decode(format!(
                            "expected ',' or ']' at offset {}",
                            self.pos
                        )))
                    }
                }
            }
        }

        fn key(&mut self) -> Result<String> {
            self.skip_ws();
            match self.peek() {
                Some(b'"') => self.string(),
                Some(b'{') | Some(b'[') => {
                    // 非字符串键保留原始文本，调用方用 decode_map_key 再解析。
                    let start = self.pos;
                    self.value()?;
                    Ok(self.text[start..self.pos].to_string())
                }
                _ => {
                    let start = self.pos;
                    while let Some(c) = self.peek() {
                        if c == b':' || WS.contains(&c) {
                            break;
                        }
                        self.pos += 1;
                    }
                    Ok(self.text[start..self.pos].to_string())
                }
            }
        }

        fn string(&mut self) -> Result<String> {
            self.pos += 1;
            let mut out = String::new();
            loop {
                if self.pos >= self.bytes.len() {
                    return Err(Error::Decode("unterminated string".into()));
                }
                let c = self.bytes[self.pos];
                match c {
                    b'"' => {
                        self.pos += 1;
                        return Ok(out);
                    }
                    b'\\' => {
                        self.pos += 1;
                        let e = *self.bytes.get(self.pos).ok_or_else(|| {
                            Error::Decode("unterminated escape".into())
                        })?;
                        match e {
                            b'u' => {
                                if self.pos + 4 >= self.bytes.len() {
                                    return Err(Error::Decode("bad \\u escape".into()));
                                }
                                let digits = std::str::from_utf8(&self.bytes[self.pos + 1..self.pos + 5])
                                    .map_err(|_| Error::Decode("bad \\u escape".into()))?;
                                let code = u32::from_str_radix(digits, 16)
                                    .map_err(|_| Error::Decode("bad \\u escape".into()))?;
                                // 代理对：fastjson2 输出的是标准 \uXXXX\uXXXX
                                let mut scalar = code;
                                if (0xD800..0xDC00).contains(&code)
                                    && self.bytes.get(self.pos + 5) == Some(&b'\\')
                                        && self.bytes.get(self.pos + 6) == Some(&b'u')
                                    {
                                        let low_str = &self.bytes[self.pos + 7..self.pos + 11];
                                        let low = u32::from_str_radix(
                                            std::str::from_utf8(low_str)
                                                .map_err(|_| Error::Decode("bad \\u escape".into()))?,
                                            16,
                                        )
                                        .map_err(|_| Error::Decode("bad \\u escape".into()))?;
                                        if (0xDC00..0xE000).contains(&low) {
                                            scalar = 0x10000
                                                + ((code - 0xD800) << 10)
                                                + (low - 0xDC00);
                                            self.pos += 6;
                                        }
                                    }
                                out.push(
                                    char::from_u32(scalar)
                                        .ok_or_else(|| Error::Decode("bad \\u escape".into()))?,
                                );
                                self.pos += 5;
                                continue;
                            }
                            b'"' => out.push('"'),
                            b'\\' => out.push('\\'),
                            b'/' => out.push('/'),
                            b'b' => out.push('\u{8}'),
                            b'f' => out.push('\u{c}'),
                            b'n' => out.push('\n'),
                            b'r' => out.push('\r'),
                            b't' => out.push('\t'),
                            other => {
                                // \x 后跟 ASCII 之外的字节时按 UTF-8 原样搬运，避免撕裂多字节字符
                                if other < 0x80 {
                                    out.push(other as char);
                                } else {
                                    out.push('\\');
                                    out.push('\\');
                                    continue;
                                }
                            }
                        }
                        self.pos += 1;
                    }
                    _ => {
                        // 非 ASCII 字节按整个 UTF-8 序列搬运
                        let len = utf8_len(c);
                        let end = (self.pos + len).min(self.bytes.len());
                        match std::str::from_utf8(&self.bytes[self.pos..end]) {
                            Ok(s) => out.push_str(s),
                            Err(_) => out.push(c as char),
                        }
                        self.pos = end;
                    }
                }
            }
        }

        fn literal(&mut self) -> Result<Value> {
            let rest = &self.text[self.pos..];
            for token in ["true", "false", "null", "-Infinity", "Infinity", "NaN"] {
                if let Some(tail) = rest.get(token.len()..token.len() + 1) {
                    if !is_token_boundary(tail.as_bytes()[0]) {
                        continue;
                    }
                } else if rest.len() != token.len() {
                    continue;
                }
                if rest.starts_with(token) {
                    self.pos += token.len();
                    return Ok(match token {
                        "true" => Value::Bool(true),
                        "false" => Value::Bool(false),
                        // serde_json 的 Number 装不下 NaN / Infinity
                        "NaN" | "Infinity" | "-Infinity" => Value::Null,
                        _ => Value::Null,
                    });
                }
            }
            let end = number_end(rest);
            if end == 0 {
                return Err(Error::Decode(format!(
                    "unexpected token at offset {}: {:?}",
                    self.pos,
                    &rest.chars().take(20).collect::<String>()
                )));
            }
            let raw = &rest[..end];
            self.pos += end;
            Ok(number_value(raw))
        }
    }

    fn is_token_boundary(c: u8) -> bool {
        c == b',' || c == b'}' || c == b']' || WS.contains(&c)
    }

    fn utf8_len(first: u8) -> usize {
        if first < 0x80 {
            1
        } else if first >= 0xF0 {
            4
        } else if first >= 0xE0 {
            3
        } else if first >= 0xC0 {
            2
        } else {
            1
        }
    }

    /// 返回数字字面量的结束偏移（0 表示不是数字）。
    fn number_end(rest: &str) -> usize {
        let b = rest.as_bytes();
        let mut i = 0usize;
        if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
            i += 1;
        }
        let digits_start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == digits_start {
            return 0;
        }
        if i < b.len() && b[i] == b'.' {
            let frac_start = i + 1;
            let mut j = frac_start;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            if j > frac_start {
                i = j;
            }
        }
        if i < b.len() && (b[i] | 0x20) == b'e' {
            let mut j = i + 1;
            if j < b.len() && (b[j] == b'-' || b[j] == b'+') {
                j += 1;
            }
            let exp_start = j;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            if j > exp_start {
                i = j;
            }
        }
        i
    }

    fn number_value(raw: &str) -> Value {
        if raw.contains('.') || raw.contains('e') || raw.contains('E') {
            return raw
                .parse::<f64>()
                .ok()
                .and_then(f64_to_value)
                .unwrap_or(Value::Null);
        }
        if let Ok(v) = raw.parse::<i64>() {
            return Value::Number(Number::from(v));
        }
        if let Ok(v) = raw.parse::<u64>() {
            return Value::Number(Number::from(v));
        }
        raw.parse::<f64>().ok().and_then(f64_to_value).unwrap_or(Value::Null)
    }

    fn f64_to_value(v: f64) -> Option<Value> {
        Number::from_f64(v).map(Value::Number)
    }
}

/// `RocketMQSerializable`：4.x 私有二进制 header。
pub struct RocketMqHeader {
    pub code: i32,
    pub language: i32,
    pub version: i32,
    pub opaque: i32,
    pub flag: i32,
    pub remark: Option<String>,
    pub ext_fields: ExtFields,
}

pub struct RocketMQSerializable;

impl RocketMQSerializable {
    pub fn map_serialize(map: &ExtFields) -> Option<Vec<u8>> {
        if map.is_empty() {
            return None;
        }
        let mut w = Writer::new();
        for (k, v) in map.iter() {
            w.string(true, Some(k));
            w.string(false, Some(v));
        }
        Some(w.into_inner())
    }

    pub fn map_deserialize(buf: &[u8], mut offset: usize, length: usize) -> Result<(ExtFields, usize)> {
        let mut map = ExtFields::new();
        let end = offset + length;
        if end > buf.len() {
            return Err(Error::Decode(format!(
                "extFields length {length} overruns header buffer ({} bytes)",
                buf.len()
            )));
        }
        while offset < end {
            let mut r = Reader::with_pos(buf, offset);
            let k = r.string(true)?;
            let v = r.string(false)?;
            offset = r.pos();
            if let (Some(k), Some(v)) = (k, v) {
                map.insert(k, v);
            }
        }
        Ok((map, offset))
    }

    /// 对应 `calTotalLen`。
    pub fn cal_total_len(remark: Option<&str>, ext: Option<&[u8]>) -> usize {
        let remark_len = remark.map(str::as_bytes).unwrap_or(&[][..]).len();
        let ext_len = ext.unwrap_or(&[][..]).len();
        2 + 1 + 2 + 4 + 4 + 4 + remark_len + 4 + ext_len
    }

    pub fn rocket_mq_protocol_encode(cmd: &RemotingCommand) -> Vec<u8> {
        let ext_bytes = RocketMQSerializable::map_serialize(cmd.ext_fields());
        let total_len = RocketMQSerializable::cal_total_len(cmd.remark.as_deref(), ext_bytes.as_deref());
        let mut w = Writer::with_capacity(total_len);
        w.i16(cmd.code);
        w.u8((cmd.language & 0xFF) as u8);
        w.i16(cmd.version);
        w.i32(cmd.opaque);
        w.i32(cmd.flag);
        match cmd.remark.as_deref().filter(|r| !r.is_empty()) {
            Some(remark) => {
                w.i32(remark.len() as i32);
                w.bytes(remark.as_bytes());
            }
            None => {
                w.i32(0);
            }
        }
        match ext_bytes {
            Some(ext) => {
                w.i32(ext.len() as i32);
                w.bytes(&ext);
            }
            None => {
                w.i32(0);
            }
        }
        debug_assert_eq!(w.len(), total_len, "rocketmq protocol encode length mismatch");
        w.into_inner()
    }

    pub fn rocket_mq_protocol_decode(header: &[u8]) -> Result<RocketMqHeader> {
        let mut r = Reader::new(header);
        let code = r.i16()?;
        let language = r.u8()? as i32;
        let version = r.i16()?;
        let opaque = r.i32()?;
        let flag = r.i32()?;
        let remark = r.string(false)?;
        let ext_len = r.i32()?;
        let ext_fields = if ext_len > 0 {
            let (map, _) =
                RocketMQSerializable::map_deserialize(header, r.pos(), ext_len as usize)?;
            map
        } else {
            ExtFields::new()
        };
        Ok(RocketMqHeader {
            code,
            language,
            version,
            opaque,
            flag,
            remark,
            ext_fields,
        })
    }
}

/// header 长度低 24 位 + 序列化类型高 8 位（对应 Java `markProtocolType`）。
pub fn mark_protocol_type(source: i32, stype: i32) -> i32 {
    ((stype & 0xFF) << 24) | (source & 0x00FF_FFFF)
}

pub fn protocol_type_of(source: i32) -> i32 {
    (source >> 24) & 0xFF
}

pub fn header_length_of(source: i32) -> i32 {
    source & 0x00FF_FFFF
}

pub fn serialize_type_from_env() -> i32 {
    let raw = std::env::var("ROCKETMQ_SERIALIZE_TYPE")
        .or_else(|_| std::env::var("rocketmq.serialize.type"))
        .unwrap_or_default();
    if raw.trim().eq_ignore_ascii_case("ROCKETMQ") {
        serialize_type::ROCKETMQ
    } else {
        serialize_type::JSON
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tolerant_json_handles_unquoted_numeric_keys() {
        let v = fastjson::from_str(r#"{"brokerAddrs":{0:"127.0.0.1:10911","1":"b"}}"#).unwrap();
        assert_eq!(v["brokerAddrs"]["0"], "127.0.0.1:10911");
        assert_eq!(v["brokerAddrs"]["1"], "b");
    }

    #[test]
    fn tolerant_json_handles_object_keys() {
        let text = r#"{"offsetTable":{{"brokerName":"b","queueId":3,"topic":"t"}:{"brokerOffset":9}}}"#;
        let v = fastjson::from_str(text).unwrap();
        let table = v["offsetTable"].as_object().unwrap();
        assert_eq!(table.len(), 1);
        let (key, value) = table.iter().next().unwrap();
        let mq = fastjson::decode_map_key(key).unwrap();
        assert_eq!(mq["topic"], "t");
        assert_eq!(mq["queueId"], 3);
        assert_eq!(value["brokerOffset"], 9);
    }

    #[test]
    fn tolerant_json_accepts_trailing_commas_and_literals() {
        let v = fastjson::from_str("{\"a\":1, \"b\":[1,2,], \"c\":null, \"d\":true}").unwrap();
        assert_eq!(v["a"], 1);
        assert_eq!(v["b"].as_array().unwrap().len(), 2);
        assert!(v["c"].is_null());
        assert_eq!(v["d"], Value::Bool(true));
    }

    #[test]
    fn tolerant_json_parses_numbers_like_python() {
        let v = fastjson::from_str(r#"{"i":-7,"f":1.5,"e":1e3,"big":18446744073709551615}"#).unwrap();
        assert_eq!(v["i"], -7);
        assert_eq!(v["f"].as_f64(), Some(1.5));
        assert_eq!(v["e"].as_f64(), Some(1000.0));
        assert!(v["big"].is_number());
    }

    #[test]
    fn tolerant_json_does_not_misread_prefix_literals() {
        let v = fastjson::from_str(r#"{"a":"nullish","b":1}"#).unwrap();
        assert_eq!(v["a"], "nullish");
        assert_eq!(v["b"], 1);
    }

    #[test]
    fn escapes_match_java_json() {
        let v = fastjson::from_str(r#"{"a":"A\u4e2dB\t\n\"\\\/"}"#).unwrap();
        assert_eq!(v["a"], "A\u{4e2d}B\t\n\"\\/");
        let v = fastjson::from_str(r#"{"a":"\ud83d\ude00"}"#).unwrap();
        assert_eq!(v["a"], "\u{1f600}");
    }

    #[test]
    fn rocketmq_protocol_round_trip() {
        let mut cmd = RemotingCommand::create_request_command(310, None);
        cmd.opaque = 42;
        cmd.flag = 3;
        cmd.remark = Some("hello".into());
        cmd.ext_fields_mut().insert("topic", "T");
        cmd.ext_fields_mut().insert("queueId", "7");
        let encoded = RocketMQSerializable::rocket_mq_protocol_encode(&cmd);
        assert_eq!(encoded.len(), 2 + 1 + 2 + 4 + 4 + 4 + 5 + 4 + (3 + 5 + 4 + 1) + (7 + 1 + 4 + 1));
        let decoded = RocketMQSerializable::rocket_mq_protocol_decode(&encoded).unwrap();
        assert_eq!(decoded.code, 310);
        assert_eq!(decoded.opaque, 42);
        assert_eq!(decoded.flag, 3);
        assert_eq!(decoded.remark.as_deref(), Some("hello"));
        assert_eq!(decoded.ext_fields.get("topic"), Some("T"));
        assert_eq!(decoded.ext_fields.get("queueId"), Some("7"));
    }

    #[test]
    fn protocol_type_bits() {
        assert_eq!(mark_protocol_type(100, serialize_type::ROCKETMQ), 0x0100_0064);
        assert_eq!(protocol_type_of(0x0100_0064), serialize_type::ROCKETMQ);
        assert_eq!(header_length_of(0x0100_0064), 100);
        assert_eq!(mark_protocol_type(100, serialize_type::JSON), 100);
    }
}
