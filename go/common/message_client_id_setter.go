package common

// MessageClientIDSetter assigns and reads the UNIQ_KEY message id property.

// SetUniqID writes UNIQ_KEY only when it is absent. The "only when absent"
// part is what makes
// the id STABLE across send retries: every attempt goes through here, and the
// broker, the trace subsystem and END_TRANSACTION all key off the first value.
// Overwriting it on retry would make SendResult.msgId differ from the message
// that actually landed.
func SetUniqID(msg *Message) {
	if msg == nil {
		return
	}
	if _, ok := msg.GetProperty(PropertyUniqKey); !ok {
		msg.PutProperty(PropertyUniqKey, CreateUniqID())
	}
}

// GetUniqID reads UNIQ_KEY back.
func GetUniqID(msg *Message) (string, bool) {
	if msg == nil {
		return "", false
	}
	return msg.GetProperty(PropertyUniqKey)
}

// ClientMsgID mirrors Java MessageClientExt#getMsgId() on a consumed message:
// the id is the UNIQ_KEY property, and only falls back to the broker offset id
// when that property is absent or empty. Client-side decoding keeps the offset
// id in MsgID (and copies it into OffsetMsgID), so every consumer-side call site
// that in Java reads msg.getMsgId() must go through this helper instead of
// reading MsgID directly.
func ClientMsgID(msg *MessageExt) string {
	if msg == nil {
		return ""
	}
	if v, ok := msg.GetProperty(PropertyUniqKey); ok && v != "" {
		return v
	}
	return msg.MsgID
}
