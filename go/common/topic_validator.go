package common

import "strings"

// Topic / group name legality (Java TopicValidator). The Java whitelist
// `^[%|a-zA-Z0-9_-]+$` is implemented as a byte check: code points >= 0x80 are
// always illegal, and any non-ASCII UTF-8 leading byte is >= 0x80, so the
// byte-wise walk agrees with Java's char-based check on real input. The broker
// runs the same table; letting a bad name through here only moves the failure
// to topic-creation time on a path that should have failed locally.
const (
	TopicMaxLength           = 127
	GroupMaxLength           = 120
	RetryOrDLQTopicMaxLength = 255

	ValidCharPattern = "^[%|a-zA-Z0-9_-]+$"

	AutoCreateTopicKeyTopic       = "TBW102"
	RmqSysScheduleTopic           = "SCHEDULE_TOPIC_XXXX"
	RmqSysBenchmarkTopic          = "BenchmarkTest"
	RmqSysTransHalfTopic          = "RMQ_SYS_TRANS_HALF_TOPIC"
	RmqSysTraceTopic              = "RMQ_SYS_TRACE_TOPIC"
	RmqSysTransOpHalfTopic        = "RMQ_SYS_TRANS_OP_HALF_TOPIC"
	RmqSysTransCheckMaxTimeTopic  = "TRANS_CHECK_MAX_TIME_TOPIC"
	RmqSysSelfTestTopic           = "SELF_TEST_TOPIC"
	RmqSysOffsetMovedEvent        = "OFFSET_MOVED_EVENT"
	RmqSysRocksdbTransHalfTopic   = "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC"
	RmqSysRocksdbTransOpHalfTopic = "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC"
	RmqSysRocksdbOffsetTopic      = "CHECKPOINT_TOPIC"
)

// systemTopicSet mirrors Java TopicValidator.getSystemTopicSet (12 entries).
var systemTopicSet = map[string]struct{}{
	AutoCreateTopicKeyTopic:       {},
	RmqSysScheduleTopic:           {},
	RmqSysBenchmarkTopic:          {},
	RmqSysTransHalfTopic:          {},
	RmqSysTraceTopic:              {},
	RmqSysTransOpHalfTopic:        {},
	RmqSysTransCheckMaxTimeTopic:  {},
	RmqSysSelfTestTopic:           {},
	RmqSysOffsetMovedEvent:        {},
	RmqSysRocksdbOffsetTopic:      {},
	RmqSysRocksdbTransHalfTopic:   {},
	RmqSysRocksdbTransOpHalfTopic: {},
}

// notAllowedSendTopicSet: broker-internal state flows; sending into them would
// pollute the broker's transaction/delay/validation logic. %RETRY% is
// deliberately NOT here — sendMessageBack writes into %RETRY%<group>.
var notAllowedSendTopicSet = map[string]struct{}{
	RmqSysScheduleTopic:           {},
	RmqSysTransHalfTopic:          {},
	RmqSysTransOpHalfTopic:        {},
	RmqSysTransCheckMaxTimeTopic:  {},
	RmqSysSelfTestTopic:           {},
	RmqSysOffsetMovedEvent:        {},
	RmqSysRocksdbTransHalfTopic:   {},
	RmqSysRocksdbTransOpHalfTopic: {},
}

func allowedChar(b byte) bool {
	if b >= '0' && b <= '9' {
		return true
	}
	if b >= 'a' && b <= 'z' {
		return true
	}
	if b >= 'A' && b <= 'Z' {
		return true
	}
	return b == '%' || b == '-' || b == '_' || b == '|'
}

// IsTopicOrGroupIllegal walks the whitelist; the empty string is legal here
// (blank names are rejected earlier by the is_blank step, same order as Java).
func IsTopicOrGroupIllegal(name string) bool {
	for i := 0; i < len(name); i++ {
		b := name[i]
		if b >= 0x80 || !allowedChar(b) {
			return true
		}
	}
	return false
}

func IsSystemTopic(topic string) bool {
	if _, ok := systemTopicSet[topic]; ok {
		return true
	}
	return strings.HasPrefix(topic, SystemTopicPrefix)
}

func IsNotAllowedSendTopic(topic string) bool {
	_, ok := notAllowedSendTopicSet[topic]
	return ok
}
