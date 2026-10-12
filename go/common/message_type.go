package common

// MessageType is the message-type classification carried in trace records.
//
// The INTEGER values are load-bearing: trace records store msgType as the
// enum's ordinal, so the declaration order is itself the wire contract —
// Normal_Msg=0, Trans_Msg_Half=1, Trans_msg_Commit=2, Delay_Msg=3, Order_Msg=4.
// Reordering these would silently corrupt every trace record.
type MessageType int

const (
	NormalMsg MessageType = iota
	TransMsgHalf
	TransMsgCommit
	DelayMsg
	OrderMsg
)

// messageTypeShortNames maps each type to its wire short name. The values are
// wire literals too: the trace HTTP/JSON views read them back by name,
// and an unknown name falls back to Normal_Msg rather than failing.
var messageTypeShortNames = map[MessageType]string{
	NormalMsg:      "Normal",
	TransMsgHalf:   "Trans",
	TransMsgCommit: "TransCommit",
	DelayMsg:       "Delay",
	OrderMsg:       "Order",
}

// ShortName returns the wire short name; an unknown type reports Normal.
func (t MessageType) ShortName() string {
	if name, ok := messageTypeShortNames[t]; ok {
		return name
	}
	return messageTypeShortNames[NormalMsg]
}

// MessageTypeByShortName maps a wire short name back to a type; an
// unrecognised name degrades to NormalMsg, it does not error.
func MessageTypeByShortName(shortName string) MessageType {
	for t, name := range messageTypeShortNames {
		if name == shortName {
			return t
		}
	}
	return NormalMsg
}

// String keeps the type printable in logs.
func (t MessageType) String() string { return t.ShortName() }
