//! * 枚举声明 `LOWER("lower")` / `UPPER("upper")`，
//!   但入网的值是 `Enum.toString()`（`RemotingCommand.makeCustomHeaderToNet:430`
//!   的 `value.toString()`）—— **大写枚举名** `LOWER` / `UPPER`；`getName()` 的小写名
//!   只喂给 `getType`，从不上报文。
//! * `getType(String)`:41 是**宽松**解析：只有 `"upper".equalsIgnoreCase(name)`
//!   才回 UPPER，其余（null / 空串 / 未知值 / 小写 "lower"）一律 LOWER。broker 解析
//!   `SearchOffsetRequestHeader.boundaryType` 走的就是它。

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum BoundaryType {
    #[default]
    Lower,
    Upper,
}

impl BoundaryType {
    pub fn name(self) -> &'static str {
        match self {
            BoundaryType::Lower => "LOWER",
            BoundaryType::Upper => "UPPER",
        }
    }

    pub fn lowercase_name(self) -> &'static str {
        match self {
            BoundaryType::Lower => "lower",
            BoundaryType::Upper => "upper",
        }
    }

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
    fn wire_name_is_the_server_enum_name() {
        assert_eq!(BoundaryType::Lower.name(), "LOWER");
        assert_eq!(BoundaryType::Upper.name(), "UPPER");
        assert_eq!(BoundaryType::default(), BoundaryType::Lower);
    }

    #[test]
    fn get_type_is_lenient_per_contract() {
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
