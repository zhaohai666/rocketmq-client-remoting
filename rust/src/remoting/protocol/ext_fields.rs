//! `RocketMQSerializable` 用到的 extFields 容器。
//!
//! 必须保序：`map_serialize` 按插入顺序写二进制，ACL 签名按 key 字典序拼接，两条路径
//! 都依赖同一份内容，用 `HashMap` 会让逐字节对拍失效。

use std::any::Any;

use serde_json::{Map, Value};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtFields {
    entries: Vec<(String, String)>,
}

impl ExtFields {
    pub fn new() -> ExtFields {
        ExtFields { entries: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, (String, String)> {
        self.entries.iter()
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .rev()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// 覆盖已有 key 时保持原位置（与 Java `HashMap.put` 的迭代顺序差异不影响签名，
    /// 因为签名前会按 key 排序；这里保持与 Python `dict` 赋值一致的语义）。
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();
        match self.entries.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value,
            None => self.entries.push((key, value)),
        }
    }

    pub fn insert_opt(&mut self, key: &str, value: Option<String>) {
        if let Some(v) = value {
            self.insert(key, v);
        }
    }

    pub fn remove(&mut self, key: &str) -> Option<String> {
        match self.entries.iter().position(|(k, _)| k == key) {
            Some(i) => Some(self.entries.remove(i).1),
            None => None,
        }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn extend(&mut self, other: &ExtFields) {
        for (k, v) in other.iter() {
            self.insert(k.clone(), v.clone());
        }
    }

    /// 按 key 字典序排列的 (key, value) 列表，ACL 签名用。
    pub fn sorted(&self) -> Vec<(&str, &str)> {
        let mut out: Vec<(&str, &str)> = self
            .entries
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        out.sort_unstable_by(|a, b| a.0.cmp(b.0));
        out
    }

    pub fn to_json(&self) -> Value {
        let mut map = Map::new();
        for (k, v) in &self.entries {
            map.insert(k.clone(), Value::String(v.clone()));
        }
        Value::Object(map)
    }

    pub fn from_json(value: &Value) -> ExtFields {
        let mut out = ExtFields::new();
        if let Value::Object(map) = value {
            for (k, v) in map {
                let s = match v {
                    Value::String(s) => s.clone(),
                    Value::Null => continue,
                    other => other.to_string(),
                };
                out.insert(k.clone(), s);
            }
        }
        out
    }

    pub fn get_i32(&self, key: &str) -> Result<Option<i32>> {
        self.parse_num(key, |v| v.parse::<i32>(), "int")
    }

    pub fn get_i64(&self, key: &str) -> Result<Option<i64>> {
        self.parse_num(key, |v| v.parse::<i64>(), "long")
    }

    pub fn get_f64(&self, key: &str) -> Result<Option<f64>> {
        self.parse_num(key, |v| v.parse::<f64>(), "double")
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.get(key).map(|v| v.eq_ignore_ascii_case("true") || v == "1")
    }

    fn parse_num<T, F, E>(
        &self,
        key: &str,
        parse: F,
        kind: &str,
    ) -> Result<Option<T>>
    where
        F: FnOnce(&str) -> std::result::Result<T, E>,
        E: std::fmt::Display,
    {
        match self.get(key) {
            None => Ok(None),
            Some(raw) => parse(raw)
                .map(Some)
                .map_err(|e| Error::Decode(format!("extField {key}={raw:?} is not a {kind}: {e}"))),
        }
    }
}

/// 消息属性等「字符串 -> 字符串」容器与 extFields 结构相同，共用一个保序实现。
pub type StringMap = ExtFields;

/// 自定义 header（对应 Java `CommandCustomHeader` 的反射读写）。
///
/// `to_ext_fields` 只写非 `None` 的字段，对齐 Java 反射里 `field.get() != null` 的过滤。
pub trait CustomHeader: Send + Sync + Any {
    fn to_ext_fields(&self, out: &mut ExtFields);
    // 名字对齐 Java `FastCodesHeader#decode(ExtFields)` / Python 的 `fill_header_from_ext`：
    // 从 extFields 读回自身，故带 &mut self。
    #[allow(clippy::wrong_self_convention)]
    fn from_ext_fields(&mut self, ext: &ExtFields);
    fn as_any(&self) -> &dyn Any;
    fn boxed_clone(&self) -> Box<dyn CustomHeader>;
}

impl ExtFields {
    pub fn from_header(header: &dyn CustomHeader) -> ExtFields {
        let mut out = ExtFields::new();
        header.to_ext_fields(&mut out);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_insertion_order_and_overwrites_in_place() {
        let mut ext = ExtFields::new();
        ext.insert("a", "1");
        ext.insert("b", "2");
        ext.insert("a", "3");
        assert_eq!(
            ext.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect::<Vec<_>>(),
            vec![("a", "3"), ("b", "2")]
        );
        assert_eq!(ext.get("a"), Some("3"));
        assert_eq!(ext.get("missing"), None);
    }

    #[test]
    fn sorted_output_matches_acl_signing_order() {
        let mut ext = ExtFields::new();
        for k in ["Signature", "AccessKey", "OnsChannel", "abc"] {
            ext.insert(k, k);
        }
        let keys: Vec<&str> = ext.sorted().into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec!["AccessKey", "OnsChannel", "Signature", "abc"]);
    }

    #[test]
    fn json_round_trip_keeps_strings() {
        let mut ext = ExtFields::new();
        ext.insert("queueId", "3");
        ext.insert("brokerName", "broker-a");
        let json = ext.to_json();
        assert_eq!(json["queueId"], "3");
        assert_eq!(ExtFields::from_json(&json), ext);
    }
}
