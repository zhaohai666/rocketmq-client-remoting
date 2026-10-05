<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

/**
 * 请求码 / 响应码 / 语言码 / 序列化类型（对应 org.apache.rocketmq.remoting.protocol 枚举与常量，
 * 移植自 protocol/codes.py）。
 *
 * RequestCode / ResponseCode 用 UPPER_SNAKE 常量名，与 Java/Python 一致。
 */

/** 对应 org.apache.rocketmq.remoting.protocol.RequestCode（全量）。 */
final class RequestCode
{
    public const SEND_MESSAGE = 10;
    public const PULL_MESSAGE = 11;
    public const QUERY_MESSAGE = 12;
    public const QUERY_BROKER_OFFSET = 13;
    public const QUERY_CONSUMER_OFFSET = 14;
    public const UPDATE_CONSUMER_OFFSET = 15;
    public const UPDATE_AND_CREATE_TOPIC = 17;
    public const UPDATE_AND_CREATE_TOPIC_LIST = 18;
    public const GET_ALL_TOPIC_CONFIG = 21;
    public const GET_TOPIC_CONFIG_LIST = 22;
    public const GET_TOPIC_NAME_LIST = 23;
    public const UPDATE_BROKER_CONFIG = 25;
    public const GET_BROKER_CONFIG = 26;
    public const TRIGGER_DELETE_FILES = 27;
    public const GET_BROKER_RUNTIME_INFO = 28;
    public const SEARCH_OFFSET_BY_TIMESTAMP = 29;
    public const GET_MAX_OFFSET = 30;
    public const GET_MIN_OFFSET = 31;
    public const GET_EARLIEST_MSG_STORETIME = 32;
    public const VIEW_MESSAGE_BY_ID = 33;
    public const HEART_BEAT = 34;
    public const UNREGISTER_CLIENT = 35;
    public const CONSUMER_SEND_MSG_BACK = 36;
    public const END_TRANSACTION = 37;
    public const GET_CONSUMER_LIST_BY_GROUP = 38;
    public const CHECK_TRANSACTION_STATE = 39;
    public const NOTIFY_CONSUMER_IDS_CHANGED = 40;
    public const LOCK_BATCH_MQ = 41;
    public const UNLOCK_BATCH_MQ = 42;
    public const GET_ALL_CONSUMER_OFFSET = 43;
    public const GET_ALL_DELAY_OFFSET = 45;
    public const CHECK_CLIENT_CONFIG = 46;
    public const GET_CLIENT_CONFIG = 47;
    public const GET_TIMER_CHECK_POINT = 60;
    public const GET_TIMER_METRICS = 61;
    public const POP_MESSAGE = 200050;
    public const ACK_MESSAGE = 200051;
    public const BATCH_ACK_MESSAGE = 200151;
    public const PEEK_MESSAGE = 200052;
    public const CHANGE_MESSAGE_INVISIBLETIME = 200053;
    public const NOTIFICATION = 200054;
    public const POLLING_INFO = 200055;
    public const POP_ROLLBACK = 200056;
    public const POP_LITE_MESSAGE = 200070;
    public const LITE_SUBSCRIPTION_CTL = 200071;
    public const ACK_LITE_MESSAGE = 200072;
    public const NOTIFY_UNSUBSCRIBE_LITE = 200073;
    public const GET_BROKER_LITE_INFO = 200074;
    public const GET_PARENT_TOPIC_INFO = 200075;
    public const GET_LITE_TOPIC_INFO = 200076;
    public const GET_LITE_CLIENT_INFO = 200077;
    public const GET_LITE_GROUP_INFO = 200078;
    public const TRIGGER_LITE_DISPATCH = 200079;
    public const PUT_KV_CONFIG = 100;
    public const GET_KV_CONFIG = 101;
    public const DELETE_KV_CONFIG = 102;
    public const REGISTER_BROKER = 103;
    public const UNREGISTER_BROKER = 104;
    public const GET_ROUTEINFO_BY_TOPIC = 105;
    public const GET_BROKER_CLUSTER_INFO = 106;
    public const UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200;
    public const GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201;
    public const GET_TOPIC_STATS_INFO = 202;
    public const GET_CONSUMER_CONNECTION_LIST = 203;
    public const GET_PRODUCER_CONNECTION_LIST = 204;
    public const WIPE_WRITE_PERM_OF_BROKER = 205;
    public const GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206;
    public const DELETE_SUBSCRIPTIONGROUP = 207;
    public const GET_CONSUME_STATS = 208;
    public const SUSPEND_CONSUMER = 209;
    public const RESUME_CONSUMER = 210;
    public const RESET_CONSUMER_OFFSET_IN_CONSUMER = 211;
    public const RESET_CONSUMER_OFFSET_IN_BROKER = 212;
    public const ADJUST_CONSUMER_THREAD_POOL = 213;
    public const WHO_CONSUME_THE_MESSAGE = 214;
    public const DELETE_TOPIC_IN_BROKER = 215;
    public const DELETE_TOPIC_IN_NAMESRV = 216;
    public const REGISTER_TOPIC_IN_NAMESRV = 217;
    public const GET_KVLIST_BY_NAMESPACE = 219;
    public const RESET_CONSUMER_CLIENT_OFFSET = 220;
    public const GET_CONSUMER_STATUS_FROM_CLIENT = 221;
    public const INVOKE_BROKER_TO_RESET_OFFSET = 222;
    public const INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223;
    public const QUERY_TOPIC_CONSUME_BY_WHO = 300;
    public const GET_TOPICS_BY_CLUSTER = 224;
    public const UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST = 225;
    public const QUERY_TOPICS_BY_CONSUMER = 343;
    public const QUERY_SUBSCRIPTION_BY_CONSUMER = 345;
    public const REGISTER_FILTER_SERVER = 301;
    public const REGISTER_MESSAGE_FILTER_CLASS = 302;
    public const QUERY_CONSUME_TIME_SPAN = 303;
    public const GET_SYSTEM_TOPIC_LIST_FROM_NS = 304;
    public const GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305;
    public const CLEAN_EXPIRED_CONSUMEQUEUE = 306;
    public const GET_CONSUMER_RUNNING_INFO = 307;
    public const QUERY_CORRECTION_OFFSET = 308;
    public const CONSUME_MESSAGE_DIRECTLY = 309;
    public const SEND_MESSAGE_V2 = 310;
    public const GET_UNIT_TOPIC_LIST = 311;
    public const GET_HAS_UNIT_SUB_TOPIC_LIST = 312;
    public const GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313;
    public const CLONE_GROUP_OFFSET = 314;
    public const VIEW_BROKER_STATS_DATA = 315;
    public const CLEAN_UNUSED_TOPIC = 316;
    public const GET_BROKER_CONSUME_STATS = 317;
    public const UPDATE_NAMESRV_CONFIG = 318;
    public const GET_NAMESRV_CONFIG = 319;
    public const SEND_BATCH_MESSAGE = 320;
    public const QUERY_CONSUME_QUEUE = 321;
    public const QUERY_DATA_VERSION = 322;
    public const RESUME_CHECK_HALF_MESSAGE = 323;
    public const SEND_REPLY_MESSAGE = 324;
    public const SEND_REPLY_MESSAGE_V2 = 325;
    public const PUSH_REPLY_MESSAGE_TO_CLIENT = 326;
    public const ADD_WRITE_PERM_OF_BROKER = 327;
    public const GET_ALL_PRODUCER_INFO = 328;
    public const DELETE_EXPIRED_COMMITLOG = 329;
    public const GET_TOPIC_CONFIG = 351;
    public const GET_SUBSCRIPTIONGROUP_CONFIG = 352;
    public const UPDATE_AND_GET_GROUP_FORBIDDEN = 353;
    public const GET_BROKER_MEMBER_GROUP = 901;
    public const BROKER_HEARTBEAT = 904;
    // ---- 以下由 rocketmq/remoting RequestCode.java 全量补齐（含 controller / acl / 冷数据 / 静态 topic）----
    public const CHECK_ROCKSDB_CQ_WRITE_PROGRESS = 354;
    public const EXPORT_ROCKSDB_CONFIG_TO_JSON = 355;
    public const LITE_PULL_MESSAGE = 361;
    public const RECALL_MESSAGE = 370;
    public const QUERY_ASSIGNMENT = 400;
    public const SET_MESSAGE_REQUEST_MODE = 401;
    public const GET_ALL_MESSAGE_REQUEST_MODE = 402;
    public const UPDATE_AND_CREATE_STATIC_TOPIC = 513;
    public const ADD_BROKER = 902;
    public const REMOVE_BROKER = 903;
    public const NOTIFY_MIN_BROKER_ID_CHANGE = 905;
    public const EXCHANGE_BROKER_HA_INFO = 906;
    public const GET_BROKER_HA_STATUS = 907;
    public const RESET_MASTER_FLUSH_OFFSET = 908;
    public const CONTROLLER_ALTER_SYNC_STATE_SET = 1001;
    public const CONTROLLER_ELECT_MASTER = 1002;
    public const CONTROLLER_REGISTER_BROKER = 1003;
    public const CONTROLLER_GET_REPLICA_INFO = 1004;
    public const CONTROLLER_GET_METADATA_INFO = 1005;
    public const CONTROLLER_GET_SYNC_STATE_DATA = 1006;
    public const GET_BROKER_EPOCH_CACHE = 1007;
    public const NOTIFY_BROKER_ROLE_CHANGED = 1008;
    public const UPDATE_CONTROLLER_CONFIG = 1009;
    public const GET_CONTROLLER_CONFIG = 1010;
    public const CLEAN_BROKER_DATA = 1011;
    public const CONTROLLER_GET_NEXT_BROKER_ID = 1012;
    public const CONTROLLER_APPLY_BROKER_ID = 1013;
    public const UPDATE_COLD_DATA_FLOW_CTR_CONFIG = 2001;
    public const REMOVE_COLD_DATA_FLOW_CTR_CONFIG = 2002;
    public const GET_COLD_DATA_FLOW_CTR_INFO = 2003;
    public const SET_COMMITLOG_READ_MODE = 2004;
    public const AUTH_CREATE_USER = 3001;
    public const AUTH_UPDATE_USER = 3002;
    public const AUTH_DELETE_USER = 3003;
    public const AUTH_GET_USER = 3004;
    public const AUTH_LIST_USER = 3005;
    public const AUTH_CREATE_ACL = 3006;
    public const AUTH_UPDATE_ACL = 3007;
    public const AUTH_DELETE_ACL = 3008;
    public const AUTH_GET_ACL = 3009;
    public const AUTH_LIST_ACL = 3010;
    public const SWITCH_TIMER_ENGINE = 5001;
    public const DELETE_TOPIC_IN_BROKER_LIST = 5002;
    public const DELETE_SUBSCRIPTION_GROUP_LIST = 5003;
}

/** 对应 org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode。 */
final class RemotingSysResponseCode
{
    public const SUCCESS = 0;
    public const SYSTEM_ERROR = 1;
    public const SYSTEM_BUSY = 2;
    public const REQUEST_CODE_NOT_SUPPORTED = 3;
    public const TRANSACTION_FAILED = 4;
}

/**
 * 对应 Java ResponseCode extends RemotingSysResponseCode。
 * PHP 常量不能继承，这里把父类五个系统码原样再声明一份。
 */
final class ResponseCode
{
    // ---- RemotingSysResponseCode（继承部分）----
    public const SUCCESS = RemotingSysResponseCode::SUCCESS;
    public const SYSTEM_ERROR = RemotingSysResponseCode::SYSTEM_ERROR;
    public const SYSTEM_BUSY = RemotingSysResponseCode::SYSTEM_BUSY;
    public const REQUEST_CODE_NOT_SUPPORTED = RemotingSysResponseCode::REQUEST_CODE_NOT_SUPPORTED;
    public const TRANSACTION_FAILED = RemotingSysResponseCode::TRANSACTION_FAILED;

    public const FLUSH_DISK_TIMEOUT = 10;
    public const SLAVE_NOT_AVAILABLE = 11;
    public const FLUSH_SLAVE_TIMEOUT = 12;
    public const MESSAGE_ILLEGAL = 13;
    public const SERVICE_NOT_AVAILABLE = 14;
    public const VERSION_NOT_SUPPORTED = 15;
    public const NO_PERMISSION = 16;
    public const TOPIC_NOT_EXIST = 17;
    public const TOPIC_EXIST_ALREADY = 18;
    public const PULL_NOT_FOUND = 19;
    public const PULL_RETRY_IMMEDIATELY = 20;
    public const PULL_OFFSET_MOVED = 21;
    public const QUERY_NOT_FOUND = 22;
    public const SUBSCRIPTION_PARSE_FAILED = 23;
    public const SUBSCRIPTION_NOT_EXIST = 24;
    public const SUBSCRIPTION_NOT_LATEST = 25;
    public const SUBSCRIPTION_GROUP_NOT_EXIST = 26;
    public const FILTER_DATA_NOT_EXIST = 27;
    public const FILTER_DATA_NOT_LATEST = 28;
    public const INVALID_PARAMETER = 29;
    public const TRANSACTION_SHOULD_COMMIT = 200;
    public const TRANSACTION_SHOULD_ROLLBACK = 201;
    public const TRANSACTION_STATE_UNKNOW = 202;
    public const TRANSACTION_STATE_GROUP_WRONG = 203;
    public const NO_BUYER_ID = 204;
    public const NOT_IN_CURRENT_UNIT = 205;
    public const CONSUMER_NOT_ONLINE = 206;
    public const CONSUME_MSG_TIMEOUT = 207;
    public const NO_MESSAGE = 208;
    public const POLLING_FULL = 209;
    public const POLLING_TIMEOUT = 210;
    public const BROKER_NOT_EXIST = 211;
    public const BROKER_DISPATCH_NOT_COMPLETE = 212;
    public const BROADCAST_CONSUMPTION = 213;
    public const FLOW_CONTROL = 215;
    public const NOT_LEADER_FOR_QUEUE = 501;
    public const ILLEGAL_OPERATION = 604;
    public const GO_AWAY = 1500;
    public const CONTROLLER_FENCED_MASTER_EPOCH = 2000;
    public const CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH = 2001;
    public const CONTROLLER_INVALID_MASTER = 2002;
    public const CONTROLLER_INVALID_REPLICAS = 2003;
    public const CONTROLLER_MASTER_NOT_AVAILABLE = 2004;
    public const CONTROLLER_INVALID_REQUEST = 2005;
    public const CONTROLLER_BROKER_NOT_ALIVE = 2006;
    public const CONTROLLER_NOT_LEADER = 2007;
    public const CONTROLLER_BROKER_METADATA_NOT_EXIST = 2008;
    public const CONTROLLER_INVALID_CLEAN_BROKER_METADATA = 2009;
    public const CONTROLLER_BROKER_NEED_TO_BE_REGISTERED = 2010;
    public const CONTROLLER_MASTER_STILL_EXIST = 2011;
    public const CONTROLLER_ELECT_MASTER_FAILED = 2012;
    public const CONTROLLER_ALTER_SYNC_STATE_SET_FAILED = 2013;
    public const CONTROLLER_BROKER_ID_INVALID = 2014;
    public const CONTROLLER_JRAFT_INTERNAL_ERROR = 2015;
    public const CONTROLLER_BROKER_LIVE_INFO_NOT_EXISTS = 2016;
    public const LMQ_QUOTA_EXCEEDED = 2017;
    public const LITE_SUBSCRIPTION_QUOTA_EXCEEDED = 2018;
    public const USER_NOT_EXIST = 3001;
    public const POLICY_NOT_EXIST = 3002;
    // 客户端侧伪响应码（Java ResponseCode 中的负值常量）
    public const RPC_UNKNOWN = -1000;
    public const RPC_ADDR_IS_NULL = -1002;
    public const RPC_SEND_TO_CHANNEL_FAILED = -1004;
    public const RPC_TIME_OUT = -1006;
}

/**
 * 语言枚举（与 Java LanguageCode 的 byte 码一致）。
 * 对齐 Python：Python 自增了 PYTHON=3，PHP 端同样已有 PHP=10
 * （broker 只做展示，不校验枚举值）。
 */
enum LanguageCode: int
{
    case JAVA = 0;
    case CPP = 1;
    case DOTNET = 2;
    case PYTHON = 3;
    case DELPHI = 4;
    case ERLANG = 5;
    case RUBY = 6;
    case OTHER = 7;
    case HTTP = 8;
    case GO = 9;
    case PHP = 10;
    case OMS = 11;
    case RUST = 12;
    case NODE_JS = 13;

    /** 对应 Python LanguageCode.value_of：码值（低 8 位）→ 枚举名。 */
    public static function valueOf(int $code): ?string
    {
        $c = $code & 0xFF;
        foreach (self::cases() as $case) {
            if ($case->value === $c) {
                return $case->name;
            }
        }
        return null;
    }

    /** 对应 Python LanguageCode.name_to_code：枚举名（大小写不敏感）→ 码值，未知回 OTHER。 */
    public static function nameToCode(string $name): int
    {
        $upper = strtoupper($name);
        foreach (self::cases() as $case) {
            if ($case->name === $upper) {
                return $case->value;
            }
        }
        return self::OTHER->value;
    }
}

/** 对应 org.apache.rocketmq.remoting.protocol.SerializeType。 */
enum SerializeType: int
{
    case JSON = 0;
    case ROCKETMQ = 1;

    /** 对应 Python SerializeType.value_of：码值（低 8 位）→ 名字。 */
    public static function nameOf(int $code): ?string
    {
        return match ($code & 0xFF) {
            0 => 'JSON',
            1 => 'ROCKETMQ',
            default => null,
        };
    }
}

/** 对应 org.apache.rocketmq.remoting.protocol.RemotingCommandType。 */
enum RemotingCommandType: string
{
    case REQUEST_COMMAND = 'REQUEST_COMMAND';
    case RESPONSE_COMMAND = 'RESPONSE_COMMAND';
}

/** 对应 org.apache.rocketmq.remoting.protocol.ForbiddenType。 */
final class ForbiddenType
{
    public const BROKER_FORBIDDEN = 1;
    public const GROUP_FORBIDDEN = 2;
    public const TOPIC_FORBIDDEN = 3;
    public const BROADCASTING_DISABLE_FORBIDDEN = 4;
    public const SUBSCRIPTION_FORBIDDEN = 5;
}

/** 对应 org.apache.rocketmq.remoting.protocol.RequestType。 */
final class RequestType
{
    public const STREAM = 0;
}

/** 对应 org.apache.rocketmq.remoting.protocol.RequestSource。 */
final class RequestSource
{
    public const SDK = -1;
    public const PROXY_FOR_ORDER = 0;
    public const PROXY_FOR_BROADCAST = 1;
    public const PROXY_FOR_STREAM = 2;
}
