// 请求码 / 响应码 / 语言码 / 序列化类型（对应 org.apache.rocketmq.remoting.protocol 枚举与常量）。
//
// 取值与 cpp/include/rocketmq/remoting/protocol/codes.h、python/.../codes.py 逐项对齐，
// 并由 tests 里的 Java 对齐用例在 ROCKETMQ_JAVA_SRC 指向 Java 源码树时做回归守卫。
namespace RocketMQ.Remoting.Protocol;

/// <summary>org.apache.rocketmq.remoting.protocol.RequestCode（166 项，这里收录客户端用到的全集）。</summary>
public static class RequestCode
{
    public const int SendMessage = 10;
    public const int PullMessage = 11;
    public const int QueryMessage = 12;
    public const int QueryBrokerOffset = 13;
    public const int QueryConsumerOffset = 14;
    public const int UpdateConsumerOffset = 15;
    public const int UpdateAndCreateTopic = 17;
    public const int UpdateAndCreateTopicList = 18;
    public const int GetAllTopicConfig = 21;
    public const int GetTopicConfigList = 22;
    public const int GetTopicNameList = 23;
    public const int UpdateBrokerConfig = 25;
    public const int GetBrokerConfig = 26;
    public const int TriggerDeleteFiles = 27;
    public const int GetBrokerRuntimeInfo = 28;
    public const int SearchOffsetByTimestamp = 29;
    public const int GetMaxOffset = 30;
    public const int GetMinOffset = 31;
    public const int GetEarliestMsgStoretime = 32;
    public const int ViewMessageById = 33;
    public const int HeartBeat = 34;
    public const int UnregisterClient = 35;
    public const int ConsumerSendMsgBack = 36;
    public const int EndTransaction = 37;
    public const int GetConsumerListByGroup = 38;
    public const int CheckTransactionState = 39;
    public const int NotifyConsumerIdsChanged = 40;
    public const int LockBatchMq = 41;
    public const int UnlockBatchMq = 42;
    public const int GetAllConsumerOffset = 43;
    public const int GetAllDelayOffset = 45;
    public const int CheckClientConfig = 46;
    public const int GetClientConfig = 47;
    public const int GetTimerCheckPoint = 60;
    public const int GetTimerMetrics = 61;
    public const int PutKvConfig = 100;
    public const int GetKvConfig = 101;
    public const int DeleteKvConfig = 102;
    public const int RegisterBroker = 103;
    public const int UnregisterBroker = 104;
    public const int GetRouteinfoByTopic = 105;
    public const int GetBrokerClusterInfo = 106;
    public const int UpdateAndCreateSubscriptiongroup = 200;
    public const int GetAllSubscriptiongroupConfig = 201;
    public const int GetTopicStatsInfo = 202;
    public const int GetConsumerConnectionList = 203;
    public const int GetProducerConnectionList = 204;
    public const int WipeWritePermOfBroker = 205;
    public const int GetAllTopicListFromNameserver = 206;
    public const int DeleteSubscriptiongroup = 207;
    public const int GetConsumeStats = 208;
    public const int SuspendConsumer = 209;
    public const int ResumeConsumer = 210;
    public const int ResetConsumerOffsetInConsumer = 211;
    public const int ResetConsumerOffsetInBroker = 212;
    public const int AdjustConsumerThreadPool = 213;
    public const int WhoConsumeTheMessage = 214;
    public const int DeleteTopicInBroker = 215;
    public const int DeleteTopicInNamesrv = 216;
    public const int RegisterTopicInNamesrv = 217;
    public const int GetKvlistByNamespace = 219;
    public const int ResetConsumerClientOffset = 220;
    public const int GetConsumerStatusFromClient = 221;
    public const int InvokeBrokerToResetOffset = 222;
    public const int InvokeBrokerToGetConsumerStatus = 223;
    public const int GetTopicsByCluster = 224;
    public const int UpdateAndCreateSubscriptiongroupList = 225;
    public const int QueryTopicConsumeByWho = 300;
    public const int RegisterFilterServer = 301;
    public const int RegisterMessageFilterClass = 302;
    public const int QueryConsumeTimeSpan = 303;
    public const int GetSystemTopicListFromNs = 304;
    public const int GetSystemTopicListFromBroker = 305;
    public const int CleanExpiredConsumequeue = 306;
    public const int GetConsumerRunningInfo = 307;
    public const int QueryCorrectionOffset = 308;
    public const int ConsumeMessageDirectly = 309;
    public const int SendMessageV2 = 310;
    public const int GetUnitTopicList = 311;
    public const int GetHasUnitSubTopicList = 312;
    public const int GetHasUnitSubUnunitTopicList = 313;
    public const int CloneGroupOffset = 314;
    public const int ViewBrokerStatsData = 315;
    public const int CleanUnusedTopic = 316;
    public const int GetBrokerConsumeStats = 317;
    public const int UpdateNamesrvConfig = 318;
    public const int GetNamesrvConfig = 319;
    public const int SendBatchMessage = 320;
    public const int QueryConsumeQueue = 321;
    public const int QueryDataVersion = 322;
    public const int ResumeCheckHalfMessage = 323;
    public const int SendReplyMessage = 324;
    public const int SendReplyMessageV2 = 325;
    public const int PushReplyMessageToClient = 326;
    public const int AddWritePermOfBroker = 327;
    public const int GetAllProducerInfo = 328;
    public const int DeleteExpiredCommitlog = 329;
    public const int QueryTopicsByConsumer = 343;
    public const int QuerySubscriptionByConsumer = 345;
    public const int GetTopicConfig = 351;
    public const int GetSubscriptiongroupConfig = 352;
    public const int UpdateAndGetGroupForbidden = 353;
    public const int CheckRocksdbCqWriteProgress = 354;
    public const int ExportRocksdbConfigToJson = 355;
    public const int LitePullMessage = 361;
    public const int RecallMessage = 370;
    public const int QueryAssignment = 400;
    public const int SetMessageRequestMode = 401;
    public const int GetAllMessageRequestMode = 402;
    public const int UpdateAndCreateStaticTopic = 513;
    public const int GetBrokerMemberGroup = 901;
    public const int AddBroker = 902;
    public const int RemoveBroker = 903;
    public const int BrokerHeartbeat = 904;
    public const int NotifyMinBrokerIdChange = 905;
    public const int ExchangeBrokerHaInfo = 906;
    public const int GetBrokerHaStatus = 907;
    public const int ResetMasterFlushOffset = 908;
    public const int ControllerAlterSyncStateSet = 1001;
    public const int ControllerElectMaster = 1002;
    public const int ControllerRegisterBroker = 1003;
    public const int ControllerGetReplicaInfo = 1004;
    public const int ControllerGetMetadataInfo = 1005;
    public const int ControllerGetSyncStateData = 1006;
    public const int GetBrokerEpochCache = 1007;
    public const int NotifyBrokerRoleChanged = 1008;
    public const int UpdateControllerConfig = 1009;
    public const int GetControllerConfig = 1010;
    public const int CleanBrokerData = 1011;
    public const int ControllerGetNextBrokerId = 1012;
    public const int ControllerApplyBrokerId = 1013;
    public const int UpdateColdDataFlowCtrConfig = 2001;
    public const int RemoveColdDataFlowCtrConfig = 2002;
    public const int GetColdDataFlowCtrInfo = 2003;
    public const int SetCommitlogReadMode = 2004;
    public const int AuthCreateUser = 3001;
    public const int AuthUpdateUser = 3002;
    public const int AuthDeleteUser = 3003;
    public const int AuthGetUser = 3004;
    public const int AuthListUser = 3005;
    public const int AuthCreateAcl = 3006;
    public const int AuthUpdateAcl = 3007;
    public const int AuthDeleteAcl = 3008;
    public const int AuthGetAcl = 3009;
    public const int AuthListAcl = 3010;
    public const int SwitchTimerEngine = 5001;
    public const int DeleteTopicInBrokerList = 5002;
    public const int DeleteSubscriptionGroupList = 5003;
    public const int PopMessage = 200050;
    public const int AckMessage = 200051;
    public const int PeekMessage = 200052;
    public const int ChangeMessageInvisibletime = 200053;
    public const int Notification = 200054;
    public const int PollingInfo = 200055;
    public const int PopRollback = 200056;
    public const int PopLiteMessage = 200070;
    public const int LiteSubscriptionCtl = 200071;
    public const int AckLiteMessage = 200072;
    public const int NotifyUnsubscribeLite = 200073;
    public const int GetBrokerLiteInfo = 200074;
    public const int GetParentTopicInfo = 200075;
    public const int GetLiteTopicInfo = 200076;
    public const int GetLiteClientInfo = 200077;
    public const int GetLiteGroupInfo = 200078;
    public const int TriggerLiteDispatch = 200079;
    public const int BatchAckMessage = 200151;
}

/// <summary>org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode（5 项）。</summary>
public static class RemotingSysResponseCode
{
    public const int Success = 0;
    public const int SystemError = 1;
    public const int SystemBusy = 2;
    public const int RequestCodeNotSupported = 3;
    public const int TransactionFailed = 4;
}

/// <summary>org.apache.rocketmq.remoting.protocol.ResponseCode（含客户端侧 RPC_* 伪响应码）。</summary>
public static class ResponseCode
{
    // RemotingSysResponseCode
    public const int Success = RemotingSysResponseCode.Success;
    public const int SystemError = RemotingSysResponseCode.SystemError;
    public const int SystemBusy = RemotingSysResponseCode.SystemBusy;
    public const int RequestCodeNotSupported = RemotingSysResponseCode.RequestCodeNotSupported;
    public const int TransactionFailed = RemotingSysResponseCode.TransactionFailed;

    public const int RpcTimeOut = -1006;
    public const int RpcSendToChannelFailed = -1004;
    public const int RpcAddrIsNull = -1002;
    public const int RpcUnknown = -1000;
    public const int FlushDiskTimeout = 10;
    public const int SlaveNotAvailable = 11;
    public const int FlushSlaveTimeout = 12;
    public const int MessageIllegal = 13;
    public const int ServiceNotAvailable = 14;
    public const int VersionNotSupported = 15;
    public const int NoPermission = 16;
    public const int TopicNotExist = 17;
    public const int TopicExistAlready = 18;
    public const int PullNotFound = 19;
    public const int PullRetryImmediately = 20;
    public const int PullOffsetMoved = 21;
    public const int QueryNotFound = 22;
    public const int SubscriptionParseFailed = 23;
    public const int SubscriptionNotExist = 24;
    public const int SubscriptionNotLatest = 25;
    public const int SubscriptionGroupNotExist = 26;
    public const int FilterDataNotExist = 27;
    public const int FilterDataNotLatest = 28;
    public const int InvalidParameter = 29;
    public const int TransactionShouldCommit = 200;
    public const int TransactionShouldRollback = 201;
    public const int TransactionStateUnknow = 202;
    public const int TransactionStateGroupWrong = 203;
    public const int NoBuyerId = 204;
    public const int NotInCurrentUnit = 205;
    public const int ConsumerNotOnline = 206;
    public const int ConsumeMsgTimeout = 207;
    public const int NoMessage = 208;
    public const int PollingFull = 209;
    public const int PollingTimeout = 210;
    public const int BrokerNotExist = 211;
    public const int BrokerDispatchNotComplete = 212;
    public const int BroadcastConsumption = 213;
    public const int FlowControl = 215;
    public const int NotLeaderForQueue = 501;
    public const int IllegalOperation = 604;
    public const int GoAway = 1500;
    public const int ControllerFencedMasterEpoch = 2000;
    public const int ControllerFencedSyncStateSetEpoch = 2001;
    public const int ControllerInvalidMaster = 2002;
    public const int ControllerInvalidReplicas = 2003;
    public const int ControllerMasterNotAvailable = 2004;
    public const int ControllerInvalidRequest = 2005;
    public const int ControllerBrokerNotAlive = 2006;
    public const int ControllerNotLeader = 2007;
    public const int ControllerBrokerMetadataNotExist = 2008;
    public const int ControllerInvalidCleanBrokerMetadata = 2009;
    public const int ControllerBrokerNeedToBeRegistered = 2010;
    public const int ControllerMasterStillExist = 2011;
    public const int ControllerElectMasterFailed = 2012;
    public const int ControllerAlterSyncStateSetFailed = 2013;
    public const int ControllerBrokerIdInvalid = 2014;
    public const int ControllerJraftInternalError = 2015;
    public const int ControllerBrokerLiveInfoNotExists = 2016;
    public const int LmqQuotaExceeded = 2017;
    public const int LiteSubscriptionQuotaExceeded = 2018;
    public const int UserNotExist = 3001;
    public const int PolicyNotExist = 3002;
}

/// <summary>org.apache.rocketmq.remoting.protocol.LanguageCode。</summary>
public static class LanguageCode
{
    public const byte Java = 0;
    public const byte Cpp = 1;
    public const byte Dotnet = 2;
    public const byte Python = 3;
    public const byte Delphi = 4;
    public const byte Erlang = 5;
    public const byte Ruby = 6;
    public const byte Other = 7;
    public const byte Http = 8;
    public const byte Go = 9;
    public const byte Php = 10;
    public const byte Oms = 11;
    public const byte Rust = 12;
    public const byte NodeJs = 13;

    /// <summary>按码值反查名字；未知返回 null。</summary>
    public static string? ValueOf(int code) => code switch
    {
        0 => "JAVA",
        1 => "CPP",
        2 => "DOTNET",
        3 => "PYTHON",
        4 => "DELPHI",
        5 => "ERLANG",
        6 => "RUBY",
        7 => "OTHER",
        8 => "HTTP",
        9 => "GO",
        10 => "PHP",
        11 => "OMS",
        12 => "RUST",
        13 => "NODE_JS",
        _ => null,
    };

    /// <summary>按名字反查码值；未知返回 null（5.x 服务端会把 language 序列化成**枚举名**）。</summary>
    public static byte? FromName(string name) => name switch
    {
        "JAVA" => Java,
        "CPP" => Cpp,
        "DOTNET" => Dotnet,
        "PYTHON" => Python,
        "DELPHI" => Delphi,
        "ERLANG" => Erlang,
        "RUBY" => Ruby,
        "OTHER" => Other,
        "HTTP" => Http,
        "GO" => Go,
        "PHP" => Php,
        "OMS" => Oms,
        "RUST" => Rust,
        "NODE_JS" => NodeJs,
        _ => null,
    };
}

/// <summary>org.apache.rocketmq.remoting.protocol.SerializeType。</summary>
public static class SerializeType
{
    public const byte Json = 0;
    public const byte Rocketmq = 1;

    public static string? ValueOf(int code) => code switch
    {
        0 => "JSON",
        1 => "ROCKETMQ",
        _ => null,
    };
}

public static class RemotingCommandType
{
    public const string RequestCommand = "REQUEST_COMMAND";
    public const string ResponseCommand = "RESPONSE_COMMAND";
}

public static class ForbiddenType
{
    public const int BrokerForbidden = 1;
    public const int GroupForbidden = 2;
    public const int TopicForbidden = 3;
    public const int BroadcastingDisableForbidden = 4;
    public const int SubscriptionForbidden = 5;
}

public static class RequestType
{
    public const int Stream = 0;
}

public static class RequestSource
{
    public const int Sdk = -1;
    public const int ProxyForOrder = 0;
    public const int ProxyForBroadcast = 1;
    public const int ProxyForStream = 2;
}
