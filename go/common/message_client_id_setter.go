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
