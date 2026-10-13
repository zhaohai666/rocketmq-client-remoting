
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum MessageType {
    #[default]
    NormalMsg = 0,
    TransMsgHalf = 1,
    TransMsgCommit = 2,
    DelayMsg = 3,
    OrderMsg = 4,
}

impl MessageType {
    pub fn value(self) -> i32 {
        self as i32
    }

    /// `value()` 的逆运算；越界返回 `None`，不 panic。
    pub fn from_value(value: i32) -> Option<MessageType> {
        match value {
            0 => Some(MessageType::NormalMsg),
            1 => Some(MessageType::TransMsgHalf),
            2 => Some(MessageType::TransMsgCommit),
            3 => Some(MessageType::DelayMsg),
            4 => Some(MessageType::OrderMsg),
            _ => None,
        }
    }

    pub fn short_name(self) -> &'static str {
        match self {
            MessageType::NormalMsg => "Normal",
            MessageType::TransMsgHalf => "Trans",
            MessageType::TransMsgCommit => "TransCommit",
            MessageType::DelayMsg => "Delay",
            MessageType::OrderMsg => "Order",
        }
    }

    pub fn get_by_short_name(short_name: &str) -> MessageType {
        match short_name {
            "Normal" => MessageType::NormalMsg,
            "Trans" => MessageType::TransMsgHalf,
            "TransCommit" => MessageType::TransMsgCommit,
            "Delay" => MessageType::DelayMsg,
            "Order" => MessageType::OrderMsg,
            _ => MessageType::NormalMsg,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinals_are_contract_ordinals() {
        assert_eq!(MessageType::NormalMsg.value(), 0);
        assert_eq!(MessageType::TransMsgHalf.value(), 1);
        assert_eq!(MessageType::TransMsgCommit.value(), 2);
        assert_eq!(MessageType::DelayMsg.value(), 3);
        assert_eq!(MessageType::OrderMsg.value(), 4);
        assert_eq!(MessageType::default(), MessageType::NormalMsg);
    }

    #[test]
    fn short_names_round_trip() {
        for t in [
            MessageType::NormalMsg,
            MessageType::TransMsgHalf,
            MessageType::TransMsgCommit,
            MessageType::DelayMsg,
            MessageType::OrderMsg,
        ] {
            assert_eq!(MessageType::get_by_short_name(t.short_name()), t);
            assert_eq!(MessageType::from_value(t.value()), Some(t));
        }
        assert_eq!(
            MessageType::NormalMsg.short_name(),
            "Normal"
        );
        assert_eq!(MessageType::TransMsgHalf.short_name(), "Trans");
        assert_eq!(MessageType::TransMsgCommit.short_name(), "TransCommit");
    }

    #[test]
    fn unknown_short_name_falls_back_to_normal() {
        assert_eq!(MessageType::get_by_short_name(""), MessageType::NormalMsg);
        assert_eq!(MessageType::get_by_short_name("nonsense"), MessageType::NormalMsg);
        assert_eq!(MessageType::from_value(5), None);
        assert_eq!(MessageType::from_value(-1), None);
    }
}
