//! 时间戳查位点的边界语义（对应 Java `org.apache.rocketmq.common.BoundaryType`，
//! 参考实现 `python/rocketmq/common/boundary_type.py`）。
//!
//! Java 锚点：
//!
//! * 枚举声明 `LOWER("lower")` / `UPPER("upper")`（`BoundaryType.java:23/:28`），
//!   但入网的值是 `Enum.toString()`（`RemotingCommand.makeCustomHeaderToNet:430`
//!   的 `value.toString()`）—— **大写枚举名** `LOWER` / `UPPER`；`getName()` 的小写名
//!   只喂给 `getType`，从不上报文。
//! * `getType(String)`:41 是**宽松**解析：只有 `"upper".equalsIgnoreCase(name)`
//!   才回 UPPER，其余（null / 空串 / 未知值 / 小写 "lower"）一律 LOWER。broker 解析
//!   `SearchOffsetRequestHeader.boundaryType` 走的就是它。

/// 对应 Java `BoundaryType`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum BoundaryType {
    /// Java `BoundaryType.LOWER`。
    #[default]
    Lower,
    /// Java `BoundaryType.UPPER`。
    Upper,
}

impl BoundaryType {
    /// Java `Enum.name()`（`BoundaryType` 没覆写 `toString()`，两者同值）—— 入网文本。
    pub fn name(self) -> &'static str {
        match self {
            BoundaryType::Lower => "LOWER",
            BoundaryType::Upper => "UPPER",
        }
    }

    /// Java `BoundaryType.getName()`：小写名，只用于比对与日志。
    pub fn lowercase_name(self) -> &'static str {
        match self {
            BoundaryType::Lower => "lower",
            BoundaryType::Upper => "upper",
        }
    }

    /// Java `BoundaryType.getType(String)`：非 "upper"（大小写不敏感）一律 LOWER。
    pub fn get_type(name: &str) -> BoundaryType {
        if name.eq_ignore_ascii_case("upper") {
            BoundaryType::Upper
        } else {
            BoundaryType::Lower
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_name_is_the_java_enum_name() {
        assert_eq!(BoundaryType::Lower.name(), "LOWER");
        assert_eq!(BoundaryType::Upper.name(), "UPPER");
        assert_eq!(BoundaryType::default(), BoundaryType::Lower);
    }

    #[test]
    fn get_type_is_lenient_like_java() {
        assert_eq!(BoundaryType::get_type("UPPER"), BoundaryType::Upper);
        assert_eq!(BoundaryType::get_type("upper"), BoundaryType::Upper);
        assert_eq!(BoundaryType::get_type("Upper"), BoundaryType::Upper);
        assert_eq!(BoundaryType::get_type("LOWER"), BoundaryType::Lower);
        assert_eq!(BoundaryType::get_type("lower"), BoundaryType::Lower);
        assert_eq!(BoundaryType::get_type(""), BoundaryType::Lower);
        assert_eq!(BoundaryType::get_type("junk"), BoundaryType::Lower);
        assert_eq!(BoundaryType::Lower.lowercase_name(), "lower");
        assert_eq!(BoundaryType::Upper.lowercase_name(), "upper");
    }
}
