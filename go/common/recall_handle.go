package common

import (
	"encoding/base64"
	"strings"
)

// The recall handle is the opaque token a producer receives in a timer/delay
// message's send result and later hands back to RecallMessage. A v1 handle
// packs five space-separated fields — version, topic, brokerName, deliverMs,
// messageID — and base64url-encodes the lot. Java builds it with a PADDED url
// encoder but accepts both paddings on decode; this port mirrors both.
const (
	recallHandleSeparator   = " "
	recallHandleVersionV1   = "v1"
	recallHandleV1MinFields = 5

	RecallHandleInvalid = "recall handle is invalid"
)

// RecallHandleV1 is the decoded payload of a v1 handle. TimestampStr stays a
// string on purpose: the broker parses the deliver time leniently and the
// client never needs it as a number.
type RecallHandleV1 struct {
	Topic        string
	BrokerName   string
	TimestampStr string
	MessageID    string
}

// BuildRecallHandle produces the base64url form of a v1 handle, WITH padding
// (Java Base64.getUrlEncoder()).
func BuildRecallHandle(topic, brokerName, timestampStr, messageID string) string {
	raw := strings.Join([]string{
		recallHandleVersionV1, topic, brokerName, timestampStr, messageID,
	}, recallHandleSeparator)
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

// DecodeRecallHandle accepts the padded form Java writes and the unpadded form
// other clients write (trailing '=' is stripped before decoding). Anything
// else — empty input, bad base64, a different version, fewer than five
// segments — is "recall handle is invalid"; extra segments are ignored
// (items[4] keeps the messageID).
func DecodeRecallHandle(handle string) (*RecallHandleV1, error) {
	if handle == "" {
		return nil, ClientError(RecallHandleInvalid)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(handle, "="))
	if err != nil {
		return nil, ClientError(RecallHandleInvalid)
	}
	items := strings.Split(string(raw), recallHandleSeparator)
	if len(items) < recallHandleV1MinFields || items[0] != recallHandleVersionV1 {
		return nil, ClientError(RecallHandleInvalid)
	}
	return &RecallHandleV1{
		Topic:        items[1],
		BrokerName:   items[2],
		TimestampStr: items[3],
		MessageID:    items[4],
	}, nil
}
