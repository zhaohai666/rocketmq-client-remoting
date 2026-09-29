// Package remoting implements the RocketMQ remoting protocol wire layer:
// request/response codes, the fastjson2-tolerant JSON codec, the RocketMQ
// private binary header codec and the RemotingCommand frame.
package remoting

import "strings"

// RequestCode mirrors org.apache.rocketmq.remoting.protocol.RequestCode.
const (
	ReqSendMessage                      = int32(10)
	ReqPullMessage                      = int32(11)
	ReqQueryMessage                     = int32(12)
	ReqQueryBrokerOffset                = int32(13)
	ReqQueryConsumerOffset              = int32(14)
	ReqUpdateConsumerOffset             = int32(15)
	ReqUpdateAndCreateTopic             = int32(17)
	ReqGetAllTopicConfig                = int32(21)
	ReqGetTopicConfigList               = int32(22)
	ReqGetTopicNameList                 = int32(23)
	ReqUpdateBrokerConfig               = int32(25)
	ReqGetBrokerConfig                  = int32(26)
	ReqSearchOffsetByTimestamp          = int32(29)
	ReqGetMaxOffset                     = int32(30)
	ReqGetMinOffset                     = int32(31)
	ReqGetEarliestMsgStoretime          = int32(32)
	ReqViewMessageByID                  = int32(33)
	ReqHeartBeat                        = int32(34)
	ReqUnregisterClient                 = int32(35)
	ReqConsumerSendMsgBack              = int32(36)
	ReqEndTransaction                   = int32(37)
	ReqGetConsumerListByGroup           = int32(38)
	ReqCheckTransactionState            = int32(39)
	ReqNotifyConsumerIDsChanged         = int32(40)
	ReqLockBatchMQ                      = int32(41)
	ReqUnlockBatchMQ                    = int32(42)
	ReqGetAllConsumerOffset             = int32(43)
	ReqGetAllDelayOffset                = int32(45)
	ReqCheckClientConfig                = int32(46)
	ReqPutKVConfig                      = int32(100)
	ReqGetKVConfig                      = int32(101)
	ReqDeleteKVConfig                   = int32(102)
	ReqRegisterBroker                   = int32(103)
	ReqUnregisterBroker                 = int32(104)
	ReqGetRouteInfoByTopic              = int32(105)
	ReqGetBrokerClusterInfo             = int32(106)
	ReqUpdateAndCreateSubscriptionGroup = int32(200)
	ReqGetAllSubscriptionGroupConfig    = int32(201)
	ReqGetTopicStatsInfo                = int32(202)
	ReqGetConsumerConnectionList        = int32(203)
	ReqGetProducerConnectionList        = int32(204)
	ReqWipeWritePermOfBroker            = int32(205)
	ReqGetAllTopicListFromNameServer    = int32(206)
	ReqDeleteSubscriptionGroup          = int32(207)
	ReqGetConsumeStats                  = int32(208)
	ReqResetConsumerClientOffset        = int32(220)
	ReqGetConsumerStatusFromClient      = int32(221)
	ReqInvokeBrokerToResetOffset        = int32(222)
	ReqInvokeBrokerToGetConsumerStatus  = int32(223)
	ReqQueryTopicConsumeByWho           = int32(300)
	ReqGetTopicsByCluster               = int32(224)
	ReqQueryTopicsByConsumer            = int32(343)
	ReqQuerySubscriptionByConsumer      = int32(345)
	ReqGetSystemTopicListFromBroker     = int32(305)
	ReqCleanExpiredConsumeQueue         = int32(306)
	ReqGetConsumerRunningInfo           = int32(307)
	ReqConsumeMessageDirectly           = int32(309)
	ReqSendMessageV2                    = int32(310)
	ReqCloneGroupOffset                 = int32(314)
	ReqCleanUnusedTopic                 = int32(316)
	ReqGetBrokerConsumeStats            = int32(317)
	ReqSendBatchMessage                 = int32(320)
	ReqQueryConsumeQueue                = int32(321)
	ReqQueryDataVersion                 = int32(322)
	ReqSendReplyMessage                 = int32(324)
	ReqSendReplyMessageV2               = int32(325)
	ReqPushReplyMessageToClient         = int32(326)
	ReqGetTopicConfig                   = int32(351)
	ReqGetSubscriptionGroupConfig       = int32(352)
	ReqGetBrokerMemberGroup             = int32(901)
	ReqBrokerHeartbeat                  = int32(904)
	ReqLitePullMessage                  = int32(361)
	ReqRecallMessage                    = int32(370)
	ReqQueryAssignment                  = int32(400)
	ReqSetMessageRequestMode            = int32(401)
	ReqPopMessage                       = int32(200050)
	ReqAckMessage                       = int32(200051)
	ReqBatchAckMessage                  = int32(200151)
	ReqChangeMessageInvisibleTime       = int32(200053)
	ReqNotification                     = int32(200054)
	ReqPollingInfo                      = int32(200055)
)

// ResponseCode mirrors RemotingSysResponseCode + ResponseCode.
const (
	RespSuccess                   = int32(0)
	RespSystemError               = int32(1)
	RespSystemBusy                = int32(2)
	RespRequestCodeNotSupported   = int32(3)
	RespTransactionFailed         = int32(4)
	RespFlushDiskTimeout          = int32(10)
	RespSlaveNotAvailable         = int32(11)
	RespFlushSlaveTimeout         = int32(12)
	RespMessageIllegal            = int32(13)
	RespServiceNotAvailable       = int32(14)
	RespVersionNotSupported       = int32(15)
	RespNoPermission              = int32(16)
	RespTopicNotExist             = int32(17)
	RespTopicExistAlready         = int32(18)
	RespPullNotFound              = int32(19)
	RespPullRetryImmediately      = int32(20)
	RespPullOffsetMoved           = int32(21)
	RespQueryNotFound             = int32(22)
	RespSubscriptionParseFailed   = int32(23)
	RespSubscriptionNotExist      = int32(24)
	RespSubscriptionNotLatest     = int32(25)
	RespSubscriptionGroupNotExist = int32(26)
	RespFilterDataNotExist        = int32(27)
	RespInvalidParameter          = int32(29)
	RespTransactionShouldCommit   = int32(200)
	RespTransactionShouldRollback = int32(201)
	RespTransactionStateUnknow    = int32(202)
	RespNoBuyerId                 = int32(204)
	RespNotInCurrentUnit          = int32(205)
	RespConsumerNotOnline         = int32(206)
	RespConsumeMsgTimeout         = int32(207)
	RespNoMessage                 = int32(208)
	RespPollingFull               = int32(209)
	RespPollingTimeout            = int32(210)
	RespBrokerNotExist            = int32(211)
	RespBrokerDispatchNotComplete = int32(212)
	RespFlowControl               = int32(215)
	RespNotLeaderForQueue         = int32(501)
	RespIllegalOperation          = int32(604)
	RespGoAway                    = int32(1500)

	// Client-side pseudo response codes (Java RpcResponseCode).
	RespRPCUnknown             = int32(-1000)
	RespRPCAddrIsNull          = int32(-1002)
	RespRPCSendToChannelFailed = int32(-1004)
	RespRPCTimeOut             = int32(-1006)
)

// LanguageCode values (Java enum order).
const (
	LangJava   = int32(0)
	LangCPP    = int32(1)
	LangDotNet = int32(2)
	LangPython = int32(3)
	LangGo     = int32(9)
	LangRust   = int32(12)
	LangNodeJS = int32(13)
	LangOther  = int32(7)
)

// LanguageCodeFromName maps a Java enum name back to the code; unknown -> OTHER.
// 5.x name servers serialize the language as the enum name string.
func LanguageCodeFromName(name string) int32 {
	switch strings.ToUpper(name) {
	case "JAVA":
		return LangJava
	case "CPP":
		return LangCPP
	case "DOTNET":
		return LangDotNet
	case "PYTHON":
		return LangPython
	case "DELPHI":
		return 4
	case "ERLANG":
		return 5
	case "RUBY":
		return 6
	case "HTTP":
		return 8
	case "GO":
		return LangGo
	case "PHP":
		return 10
	case "OMS":
		return 11
	case "RUST":
		return LangRust
	case "NODEJS", "NODE_JS":
		return LangNodeJS
	}
	return LangOther
}

// LanguageName maps a language code to the Java enum name; unknown -> OTHER.
func LanguageName(code int32) string {
	switch code & 0xFF {
	case 0:
		return "JAVA"
	case 1:
		return "CPP"
	case 2:
		return "DOTNET"
	case 3:
		return "PYTHON"
	case 4:
		return "DELPHI"
	case 5:
		return "ERLANG"
	case 6:
		return "RUBY"
	case 8:
		return "HTTP"
	case 9:
		return "GO"
	case 10:
		return "PHP"
	case 11:
		return "OMS"
	case 12:
		return "RUST"
	case 13:
		return "NODE_JS"
	}
	return "OTHER"
}

// SerializeType: JSON header or the RocketMQ private binary header.
const (
	SerializeTypeJSON     = int32(0)
	SerializeTypeRocketMQ = int32(1)
)

// Request/response flag bits (RemotingCommand).
const (
	FlagRPCType   = int32(0) // bit 0: response
	FlagOnewayRPC = int32(1) // bit 1: oneway
)

// RequestSource mirrors Java RequestSource.
const (
	RequestSourceSDK               = int32(-1)
	RequestSourceProxyForOrder     = int32(0)
	RequestSourceProxyForBroadcast = int32(1)
	RequestSourceProxyForStream    = int32(2)
)

// ForbiddenType mirrors Java ForbiddenType.
const (
	ForbiddenBroker              = int32(1)
	ForbiddenGroup               = int32(2)
	ForbiddenTopic               = int32(3)
	ForbiddenBroadcastingDisable = int32(4)
	ForbiddenSubscription        = int32(5)
)
