package common

// MessageType mirrors Java org.apache.rocketmq.common.message.MessageType.
//
// The INTEGER values are load-bearing: the trace codec writes msgType as the
// enum's ordinal() (TraceDataEncoder's Pub and EndTransaction branches), so the
// declaration order must match Java's exactly —
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

// messageTypeShortNames is Java MessageType's short name table. The values are
// wire literals too: the trace HTTP/JSON views read them back by name
// (MessageType.getByShortName), and an unknown name falls back to Normal_Msg
// rather than failing.
var messageTypeShortNames = map[MessageType]string{
	NormalMsg:      "Normal",
	TransMsgHalf:   "Trans",
	TransMsgCommit: "TransCommit",
	DelayMsg:       "Delay",
	OrderMsg:       "Order",
}

// ShortName is Java MessageType.getShortName().
func (t MessageType) ShortName() string {
	if name, ok := messageTypeShortNames[t]; ok {
		return name
	}
	return messageTypeShortNames[NormalMsg]
}

// MessageTypeByShortName is Java MessageType.getByShortName: an unrecognised
// name degrades to NormalMsg, it does not error.
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
