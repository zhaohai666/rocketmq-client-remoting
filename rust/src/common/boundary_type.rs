//! * The enum declares `LOWER("lower")` / `UPPER("upper")`,
//!   but the value that goes on the wire is the default string form of the variant -- the
//!   **uppercase enum names** `LOWER` / `UPPER`; the lowercase names
//!   from `lowercase_name()` only feed `get_type` and never go on the wire.
//! * `get_type` is **lenient** parsing: only `"upper"` compared case-insensitively
//!   yields Upper; everything else (null / empty string / unknown values / lowercase "lower") yields Lower. The broker parses
//!   `SearchOffsetRequestHeader.boundaryType` with this same rule.

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
