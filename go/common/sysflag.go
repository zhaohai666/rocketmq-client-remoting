package common

import (
	"strconv"
	"strings"
)

// MessageSysFlag bit layout (low -> high):
//
//	bit0     COMPRESSED
//	bit1     MULTI_TAGS
//	bit2     TRANSACTION_PREPARED
//	bit2-3   TRANSACTION_COMMIT(0x2<<2) / TRANSACTION_ROLLBACK(0x3<<2) share the mask
//	bit4     BORNHOST_V6
//	bit5     STOREHOSTADDRESS_V6
//	bit6     NEED_UNWRAP
//	bit7     INNER_BATCH
//	bit8~10  COMPRESSION_TYPE (mask 0x7<<8)
const (
	MessageSysFlagCompressed          int32 = 0x1
	MessageSysFlagMultiTags           int32 = 0x1 << 1
	MessageSysFlagTransactionNotType  int32 = 0
	MessageSysFlagTransactionPrepared int32 = 0x1 << 2
	MessageSysFlagTransactionCommit   int32 = 0x2 << 2
	MessageSysFlagTransactionRollback int32 = 0x3 << 2
	MessageSysFlagBornhostV6          int32 = 0x1 << 4
	MessageSysFlagStorehostV6         int32 = 0x1 << 5
	MessageSysFlagNeedUnwrap          int32 = 0x1 << 6
	MessageSysFlagInnerBatch          int32 = 0x1 << 7

	MessageSysFlagCompressionLz4      int32 = 0x1 << 8
	MessageSysFlagCompressionZstd     int32 = 0x2 << 8
	MessageSysFlagCompressionZlib     int32 = 0x3 << 8
	MessageSysFlagCompressionTypeMask int32 = 0x7 << 8

	// CompressionTypeShift is the bit offset of the compression type field.
	CompressionTypeShift = 8

	// Historical names: the compression type VALUE (not shifted).
	Lz4Type    int32 = 1
	ZstdType   int32 = 2
	ZlibType   int32 = 3
	SnappyType int32 = 4
)

func GetCompressionType(sysFlag int32) int32 {
	return (sysFlag & MessageSysFlagCompressionTypeMask) >> CompressionTypeShift
}

// SetCompressionType overwrites the type bits, leaving all other bits intact.
func SetCompressionType(sysFlag, compressionType int32) int32 {
	return (sysFlag &^ MessageSysFlagCompressionTypeMask) |
		((compressionType << CompressionTypeShift) & MessageSysFlagCompressionTypeMask)
}

func IsCompressed(sysFlag int32) bool {
	return sysFlag&MessageSysFlagCompressed == MessageSysFlagCompressed
}

// ClearCompressedFlag only clears COMPRESSED_FLAG; the compression type bits
// (bit8~10) are kept after decompression, matching Java.
func ClearCompressedFlag(sysFlag int32) int32 { return sysFlag &^ MessageSysFlagCompressed }

func GetTransactionValue(flag int32) int32 { return flag & MessageSysFlagTransactionRollback }

func ResetTransactionValue(flag, transactionType int32) int32 {
	return (flag &^ MessageSysFlagTransactionRollback) | transactionType
}

func CheckFlag(flag, expected int32) bool { return flag&expected != 0 }

// CommitLogFlag (broker-side commitlog flags; clients only read derived info).
const (
	CommitLogFlagCommit   int32 = 0x1 << 0
	CommitLogFlagFlush    int32 = 0x1 << 1
	CommitLogFlagRollback int32 = 0x1 << 2
)

// ConsumeInitMode (QueryConsumerOffsetResponseHeader.mode).
const (
	ConsumeInitModeMin int32 = 0
	ConsumeInitModeMax int32 = 1
)

// PullSysFlag bits carried in the pull request sysFlag.
const (
	PullSysFlagCommitOffset                   int32 = 0x1
	PullSysFlagSuspend                        int32 = 0x1 << 1
	PullSysFlagSubscription                   int32 = 0x1 << 2
	PullSysFlagClassFilter                    int32 = 0x1 << 3
	PullSysFlagLitePullMessage                int32 = 0x1 << 4
	PullSysFlagProxyBlock                     int32 = 0x1 << 5
	PullSysFlagExtBrokerGroup                 int32 = 0x1 << 6
	PullSysFlagInnerSql                       int32 = 0x1 << 7
	PullSysFlagMultiTag                       int32 = 0x1 << 8
	PullSysFlagStartOffset                    int32 = 0x1 << 9
	PullSysFlagSupportFilterAndComputeHashing int32 = 0x1 << 10
)

func BuildSysFlag(commitOffset, suspend, subscription, classFilter, litePull bool) int32 {
	var flag int32
	if commitOffset {
		flag |= PullSysFlagCommitOffset
	}
	if suspend {
		flag |= PullSysFlagSuspend
	}
	if subscription {
		flag |= PullSysFlagSubscription
	}
	if classFilter {
		flag |= PullSysFlagClassFilter
	}
	if litePull {
		flag |= PullSysFlagLitePullMessage
	}
	return flag
}

// BuildSysFlagBasic is the four-arg Java overload (no litePull).
func BuildSysFlagBasic(commitOffset, suspend, subscription, classFilter bool) int32 {
	return BuildSysFlag(commitOffset, suspend, subscription, classFilter, false)
}

func BuildSysFlagWithHashing(sysFlag int32, support bool) int32 {
	if support {
		return sysFlag | PullSysFlagSupportFilterAndComputeHashing
	}
	return sysFlag
}

func HasSupportFilterAndComputeHashingFlag(sysFlag int32) bool {
	return sysFlag&PullSysFlagSupportFilterAndComputeHashing == PullSysFlagSupportFilterAndComputeHashing
}

func ClearCommitOffsetFlag(sysFlag int32) int32 { return sysFlag &^ PullSysFlagCommitOffset }
func HasCommitOffsetFlag(sysFlag int32) bool {
	return sysFlag&PullSysFlagCommitOffset == PullSysFlagCommitOffset
}
func HasSuspendFlag(sysFlag int32) bool {
	return sysFlag&PullSysFlagSuspend == PullSysFlagSuspend
}
func ClearSuspendFlag(sysFlag int32) int32 { return sysFlag &^ PullSysFlagSuspend }
func HasSubscriptionFlag(sysFlag int32) bool {
	return sysFlag&PullSysFlagSubscription == PullSysFlagSubscription
}
func BuildSysFlagWithSubscription(sysFlag int32) int32 { return sysFlag | PullSysFlagSubscription }
func HasClassFilterFlag(sysFlag int32) bool {
	return sysFlag&PullSysFlagClassFilter == PullSysFlagClassFilter
}
func HasLitePullFlag(sysFlag int32) bool {
	return sysFlag&PullSysFlagLitePullMessage == PullSysFlagLitePullMessage
}
func HasStartOffsetFlag(sysFlag int32) bool {
	return sysFlag&PullSysFlagStartOffset == PullSysFlagStartOffset
}

// PermName (Java org.apache.rocketmq.common.sysflag.PermName).
const (
	PermPriority int32 = 0x1 << 3
	PermRead     int32 = 0x4
	PermWrite    int32 = 0x2
	PermInherit  int32 = 0x1
	PermOwner    int32 = 0x1 << 4
)

// PermIsValid mirrors Java PermName.isValid: perm >= 0 && perm < PERM_PRIORITY.
func PermIsValid(perm int32) bool { return perm >= 0 && perm < PermPriority }

// PermIsValidStr: non-numeric input is invalid (Java would throw
// NumberFormatException; the ports agree on false).
func PermIsValidStr(perm string) bool {
	v, err := strconv.ParseInt(strings.TrimSpace(perm), 10, 32)
	if err != nil {
		return false
	}
	return PermIsValid(int32(v))
}

// Perm2String renders the R/W/X three-character form.
func Perm2String(perm int32) string {
	var sb strings.Builder
	if PermRead == perm&PermRead {
		sb.WriteByte('R')
	} else {
		sb.WriteByte('-')
	}
	if PermWrite == perm&PermWrite {
		sb.WriteByte('W')
	} else {
		sb.WriteByte('-')
	}
	if PermInherit == perm&PermInherit {
		sb.WriteByte('X')
	} else {
		sb.WriteByte('-')
	}
	return sb.String()
}

func CheckPerm(perm, wantedPerm int32) bool { return perm&wantedPerm == wantedPerm }

// SubscriptionMode (Python side; a subset of Java ConsumeMode).
const (
	SubscriptionModeGroup        int32 = 0
	SubscriptionModeBroadcasting int32 = 1
)
