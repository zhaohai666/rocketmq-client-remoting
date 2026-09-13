// 请求码 / 响应码 / 语言码 / 序列化类型（对应 org.apache.rocketmq.remoting.protocol 枚举与常量）。
//
// 本文件由工具脚本依据 Java 源码生成，并与 python/rocketmq/remoting/protocol/codes.py
// 交叉校验，取值零漂移；tests/test_java_alignment.cpp 会在 ROCKETMQ_JAVA_SRC 指向
// Java 源码树时做同样的回归守卫。
#ifndef ROCKETMQ_REMOTING_PROTOCOL_CODES_H
#define ROCKETMQ_REMOTING_PROTOCOL_CODES_H

#include <cstdint>

namespace rocketmq {

// org.apache.rocketmq.remoting.protocol.RequestCode（166 项）
struct RequestCode {
    static constexpr int32_t SEND_MESSAGE = 10;
    static constexpr int32_t PULL_MESSAGE = 11;
    static constexpr int32_t QUERY_MESSAGE = 12;
    static constexpr int32_t QUERY_BROKER_OFFSET = 13;
    static constexpr int32_t QUERY_CONSUMER_OFFSET = 14;
    static constexpr int32_t UPDATE_CONSUMER_OFFSET = 15;
    static constexpr int32_t UPDATE_AND_CREATE_TOPIC = 17;
    static constexpr int32_t UPDATE_AND_CREATE_TOPIC_LIST = 18;
    static constexpr int32_t GET_ALL_TOPIC_CONFIG = 21;
    static constexpr int32_t GET_TOPIC_CONFIG_LIST = 22;
    static constexpr int32_t GET_TOPIC_NAME_LIST = 23;
    static constexpr int32_t UPDATE_BROKER_CONFIG = 25;
    static constexpr int32_t GET_BROKER_CONFIG = 26;
    static constexpr int32_t TRIGGER_DELETE_FILES = 27;
    static constexpr int32_t GET_BROKER_RUNTIME_INFO = 28;
    static constexpr int32_t SEARCH_OFFSET_BY_TIMESTAMP = 29;
    static constexpr int32_t GET_MAX_OFFSET = 30;
    static constexpr int32_t GET_MIN_OFFSET = 31;
    static constexpr int32_t GET_EARLIEST_MSG_STORETIME = 32;
    static constexpr int32_t VIEW_MESSAGE_BY_ID = 33;
    static constexpr int32_t HEART_BEAT = 34;
    static constexpr int32_t UNREGISTER_CLIENT = 35;
    static constexpr int32_t CONSUMER_SEND_MSG_BACK = 36;
    static constexpr int32_t END_TRANSACTION = 37;
    static constexpr int32_t GET_CONSUMER_LIST_BY_GROUP = 38;
    static constexpr int32_t CHECK_TRANSACTION_STATE = 39;
    static constexpr int32_t NOTIFY_CONSUMER_IDS_CHANGED = 40;
    static constexpr int32_t LOCK_BATCH_MQ = 41;
    static constexpr int32_t UNLOCK_BATCH_MQ = 42;
    static constexpr int32_t GET_ALL_CONSUMER_OFFSET = 43;
    static constexpr int32_t GET_ALL_DELAY_OFFSET = 45;
    static constexpr int32_t CHECK_CLIENT_CONFIG = 46;
    static constexpr int32_t GET_CLIENT_CONFIG = 47;
    static constexpr int32_t GET_TIMER_CHECK_POINT = 60;
    static constexpr int32_t GET_TIMER_METRICS = 61;
    static constexpr int32_t PUT_KV_CONFIG = 100;
    static constexpr int32_t GET_KV_CONFIG = 101;
    static constexpr int32_t DELETE_KV_CONFIG = 102;
    static constexpr int32_t REGISTER_BROKER = 103;
    static constexpr int32_t UNREGISTER_BROKER = 104;
    static constexpr int32_t GET_ROUTEINFO_BY_TOPIC = 105;
    static constexpr int32_t GET_BROKER_CLUSTER_INFO = 106;
    static constexpr int32_t UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200;
    static constexpr int32_t GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201;
    static constexpr int32_t GET_TOPIC_STATS_INFO = 202;
    static constexpr int32_t GET_CONSUMER_CONNECTION_LIST = 203;
    static constexpr int32_t GET_PRODUCER_CONNECTION_LIST = 204;
    static constexpr int32_t WIPE_WRITE_PERM_OF_BROKER = 205;
    static constexpr int32_t GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206;
    static constexpr int32_t DELETE_SUBSCRIPTIONGROUP = 207;
    static constexpr int32_t GET_CONSUME_STATS = 208;
    static constexpr int32_t SUSPEND_CONSUMER = 209;
    static constexpr int32_t RESUME_CONSUMER = 210;
    static constexpr int32_t RESET_CONSUMER_OFFSET_IN_CONSUMER = 211;
    static constexpr int32_t RESET_CONSUMER_OFFSET_IN_BROKER = 212;
    static constexpr int32_t ADJUST_CONSUMER_THREAD_POOL = 213;
    static constexpr int32_t WHO_CONSUME_THE_MESSAGE = 214;
    static constexpr int32_t DELETE_TOPIC_IN_BROKER = 215;
    static constexpr int32_t DELETE_TOPIC_IN_NAMESRV = 216;
    static constexpr int32_t REGISTER_TOPIC_IN_NAMESRV = 217;
    static constexpr int32_t GET_KVLIST_BY_NAMESPACE = 219;
    static constexpr int32_t RESET_CONSUMER_CLIENT_OFFSET = 220;
    static constexpr int32_t GET_CONSUMER_STATUS_FROM_CLIENT = 221;
    static constexpr int32_t INVOKE_BROKER_TO_RESET_OFFSET = 222;
    static constexpr int32_t INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223;
    static constexpr int32_t GET_TOPICS_BY_CLUSTER = 224;
    static constexpr int32_t UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST = 225;
    static constexpr int32_t QUERY_TOPIC_CONSUME_BY_WHO = 300;
    static constexpr int32_t REGISTER_FILTER_SERVER = 301;
    static constexpr int32_t REGISTER_MESSAGE_FILTER_CLASS = 302;
    static constexpr int32_t QUERY_CONSUME_TIME_SPAN = 303;
    static constexpr int32_t GET_SYSTEM_TOPIC_LIST_FROM_NS = 304;
    static constexpr int32_t GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305;
    static constexpr int32_t CLEAN_EXPIRED_CONSUMEQUEUE = 306;
    static constexpr int32_t GET_CONSUMER_RUNNING_INFO = 307;
    static constexpr int32_t QUERY_CORRECTION_OFFSET = 308;
    static constexpr int32_t CONSUME_MESSAGE_DIRECTLY = 309;
    static constexpr int32_t SEND_MESSAGE_V2 = 310;
    static constexpr int32_t GET_UNIT_TOPIC_LIST = 311;
    static constexpr int32_t GET_HAS_UNIT_SUB_TOPIC_LIST = 312;
    static constexpr int32_t GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313;
    static constexpr int32_t CLONE_GROUP_OFFSET = 314;
    static constexpr int32_t VIEW_BROKER_STATS_DATA = 315;
    static constexpr int32_t CLEAN_UNUSED_TOPIC = 316;
    static constexpr int32_t GET_BROKER_CONSUME_STATS = 317;
    static constexpr int32_t UPDATE_NAMESRV_CONFIG = 318;
    static constexpr int32_t GET_NAMESRV_CONFIG = 319;
    static constexpr int32_t SEND_BATCH_MESSAGE = 320;
    static constexpr int32_t QUERY_CONSUME_QUEUE = 321;
    static constexpr int32_t QUERY_DATA_VERSION = 322;
    static constexpr int32_t RESUME_CHECK_HALF_MESSAGE = 323;
    static constexpr int32_t SEND_REPLY_MESSAGE = 324;
    static constexpr int32_t SEND_REPLY_MESSAGE_V2 = 325;
    static constexpr int32_t PUSH_REPLY_MESSAGE_TO_CLIENT = 326;
    static constexpr int32_t ADD_WRITE_PERM_OF_BROKER = 327;
    static constexpr int32_t GET_ALL_PRODUCER_INFO = 328;
    static constexpr int32_t DELETE_EXPIRED_COMMITLOG = 329;
    static constexpr int32_t QUERY_TOPICS_BY_CONSUMER = 343;
    static constexpr int32_t QUERY_SUBSCRIPTION_BY_CONSUMER = 345;
    static constexpr int32_t GET_TOPIC_CONFIG = 351;
    static constexpr int32_t GET_SUBSCRIPTIONGROUP_CONFIG = 352;
    static constexpr int32_t UPDATE_AND_GET_GROUP_FORBIDDEN = 353;
    static constexpr int32_t CHECK_ROCKSDB_CQ_WRITE_PROGRESS = 354;
    static constexpr int32_t EXPORT_ROCKSDB_CONFIG_TO_JSON = 355;
    static constexpr int32_t LITE_PULL_MESSAGE = 361;
    static constexpr int32_t RECALL_MESSAGE = 370;
    static constexpr int32_t QUERY_ASSIGNMENT = 400;
    static constexpr int32_t SET_MESSAGE_REQUEST_MODE = 401;
    static constexpr int32_t GET_ALL_MESSAGE_REQUEST_MODE = 402;
    static constexpr int32_t UPDATE_AND_CREATE_STATIC_TOPIC = 513;
    static constexpr int32_t GET_BROKER_MEMBER_GROUP = 901;
    static constexpr int32_t ADD_BROKER = 902;
    static constexpr int32_t REMOVE_BROKER = 903;
    static constexpr int32_t BROKER_HEARTBEAT = 904;
    static constexpr int32_t NOTIFY_MIN_BROKER_ID_CHANGE = 905;
    static constexpr int32_t EXCHANGE_BROKER_HA_INFO = 906;
    static constexpr int32_t GET_BROKER_HA_STATUS = 907;
    static constexpr int32_t RESET_MASTER_FLUSH_OFFSET = 908;
    static constexpr int32_t CONTROLLER_ALTER_SYNC_STATE_SET = 1001;
    static constexpr int32_t CONTROLLER_ELECT_MASTER = 1002;
    static constexpr int32_t CONTROLLER_REGISTER_BROKER = 1003;
    static constexpr int32_t CONTROLLER_GET_REPLICA_INFO = 1004;
    static constexpr int32_t CONTROLLER_GET_METADATA_INFO = 1005;
    static constexpr int32_t CONTROLLER_GET_SYNC_STATE_DATA = 1006;
    static constexpr int32_t GET_BROKER_EPOCH_CACHE = 1007;
    static constexpr int32_t NOTIFY_BROKER_ROLE_CHANGED = 1008;
    static constexpr int32_t UPDATE_CONTROLLER_CONFIG = 1009;
    static constexpr int32_t GET_CONTROLLER_CONFIG = 1010;
    static constexpr int32_t CLEAN_BROKER_DATA = 1011;
    static constexpr int32_t CONTROLLER_GET_NEXT_BROKER_ID = 1012;
    static constexpr int32_t CONTROLLER_APPLY_BROKER_ID = 1013;
    static constexpr int32_t UPDATE_COLD_DATA_FLOW_CTR_CONFIG = 2001;
    static constexpr int32_t REMOVE_COLD_DATA_FLOW_CTR_CONFIG = 2002;
    static constexpr int32_t GET_COLD_DATA_FLOW_CTR_INFO = 2003;
    static constexpr int32_t SET_COMMITLOG_READ_MODE = 2004;
    static constexpr int32_t AUTH_CREATE_USER = 3001;
    static constexpr int32_t AUTH_UPDATE_USER = 3002;
    static constexpr int32_t AUTH_DELETE_USER = 3003;
    static constexpr int32_t AUTH_GET_USER = 3004;
    static constexpr int32_t AUTH_LIST_USER = 3005;
    static constexpr int32_t AUTH_CREATE_ACL = 3006;
    static constexpr int32_t AUTH_UPDATE_ACL = 3007;
    static constexpr int32_t AUTH_DELETE_ACL = 3008;
    static constexpr int32_t AUTH_GET_ACL = 3009;
    static constexpr int32_t AUTH_LIST_ACL = 3010;
    static constexpr int32_t SWITCH_TIMER_ENGINE = 5001;
    static constexpr int32_t DELETE_TOPIC_IN_BROKER_LIST = 5002;
    static constexpr int32_t DELETE_SUBSCRIPTION_GROUP_LIST = 5003;
    static constexpr int32_t POP_MESSAGE = 200050;
    static constexpr int32_t ACK_MESSAGE = 200051;
    static constexpr int32_t PEEK_MESSAGE = 200052;
    static constexpr int32_t CHANGE_MESSAGE_INVISIBLETIME = 200053;
    static constexpr int32_t NOTIFICATION = 200054;
    static constexpr int32_t POLLING_INFO = 200055;
    static constexpr int32_t POP_ROLLBACK = 200056;
    static constexpr int32_t POP_LITE_MESSAGE = 200070;
    static constexpr int32_t LITE_SUBSCRIPTION_CTL = 200071;
    static constexpr int32_t ACK_LITE_MESSAGE = 200072;
    static constexpr int32_t NOTIFY_UNSUBSCRIBE_LITE = 200073;
    static constexpr int32_t GET_BROKER_LITE_INFO = 200074;
    static constexpr int32_t GET_PARENT_TOPIC_INFO = 200075;
    static constexpr int32_t GET_LITE_TOPIC_INFO = 200076;
    static constexpr int32_t GET_LITE_CLIENT_INFO = 200077;
    static constexpr int32_t GET_LITE_GROUP_INFO = 200078;
    static constexpr int32_t TRIGGER_LITE_DISPATCH = 200079;
    static constexpr int32_t BATCH_ACK_MESSAGE = 200151;
};

// org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode（5 项）
struct RemotingSysResponseCode {
    static constexpr int32_t SUCCESS = 0;
    static constexpr int32_t SYSTEM_ERROR = 1;
    static constexpr int32_t SYSTEM_BUSY = 2;
    static constexpr int32_t REQUEST_CODE_NOT_SUPPORTED = 3;
    static constexpr int32_t TRANSACTION_FAILED = 4;
};

// org.apache.rocketmq.remoting.protocol.ResponseCode（含客户端侧 RPC_* 伪响应码）
struct ResponseCode : public RemotingSysResponseCode {
    static constexpr int32_t RPC_TIME_OUT = -1006;
    static constexpr int32_t RPC_SEND_TO_CHANNEL_FAILED = -1004;
    static constexpr int32_t RPC_ADDR_IS_NULL = -1002;
    static constexpr int32_t RPC_UNKNOWN = -1000;
    static constexpr int32_t FLUSH_DISK_TIMEOUT = 10;
    static constexpr int32_t SLAVE_NOT_AVAILABLE = 11;
    static constexpr int32_t FLUSH_SLAVE_TIMEOUT = 12;
    static constexpr int32_t MESSAGE_ILLEGAL = 13;
    static constexpr int32_t SERVICE_NOT_AVAILABLE = 14;
    static constexpr int32_t VERSION_NOT_SUPPORTED = 15;
    static constexpr int32_t NO_PERMISSION = 16;
    static constexpr int32_t TOPIC_NOT_EXIST = 17;
    static constexpr int32_t TOPIC_EXIST_ALREADY = 18;
    static constexpr int32_t PULL_NOT_FOUND = 19;
    static constexpr int32_t PULL_RETRY_IMMEDIATELY = 20;
    static constexpr int32_t PULL_OFFSET_MOVED = 21;
    static constexpr int32_t QUERY_NOT_FOUND = 22;
    static constexpr int32_t SUBSCRIPTION_PARSE_FAILED = 23;
    static constexpr int32_t SUBSCRIPTION_NOT_EXIST = 24;
    static constexpr int32_t SUBSCRIPTION_NOT_LATEST = 25;
    static constexpr int32_t SUBSCRIPTION_GROUP_NOT_EXIST = 26;
    static constexpr int32_t FILTER_DATA_NOT_EXIST = 27;
    static constexpr int32_t FILTER_DATA_NOT_LATEST = 28;
    static constexpr int32_t INVALID_PARAMETER = 29;
    static constexpr int32_t TRANSACTION_SHOULD_COMMIT = 200;
    static constexpr int32_t TRANSACTION_SHOULD_ROLLBACK = 201;
    static constexpr int32_t TRANSACTION_STATE_UNKNOW = 202;
    static constexpr int32_t TRANSACTION_STATE_GROUP_WRONG = 203;
    static constexpr int32_t NO_BUYER_ID = 204;
    static constexpr int32_t NOT_IN_CURRENT_UNIT = 205;
    static constexpr int32_t CONSUMER_NOT_ONLINE = 206;
    static constexpr int32_t CONSUME_MSG_TIMEOUT = 207;
    static constexpr int32_t NO_MESSAGE = 208;
    static constexpr int32_t POLLING_FULL = 209;
    static constexpr int32_t POLLING_TIMEOUT = 210;
    static constexpr int32_t BROKER_NOT_EXIST = 211;
    static constexpr int32_t BROKER_DISPATCH_NOT_COMPLETE = 212;
    static constexpr int32_t BROADCAST_CONSUMPTION = 213;
    static constexpr int32_t FLOW_CONTROL = 215;
    static constexpr int32_t NOT_LEADER_FOR_QUEUE = 501;
    static constexpr int32_t ILLEGAL_OPERATION = 604;
    static constexpr int32_t GO_AWAY = 1500;
    static constexpr int32_t CONTROLLER_FENCED_MASTER_EPOCH = 2000;
    static constexpr int32_t CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH = 2001;
    static constexpr int32_t CONTROLLER_INVALID_MASTER = 2002;
    static constexpr int32_t CONTROLLER_INVALID_REPLICAS = 2003;
    static constexpr int32_t CONTROLLER_MASTER_NOT_AVAILABLE = 2004;
    static constexpr int32_t CONTROLLER_INVALID_REQUEST = 2005;
    static constexpr int32_t CONTROLLER_BROKER_NOT_ALIVE = 2006;
    static constexpr int32_t CONTROLLER_NOT_LEADER = 2007;
    static constexpr int32_t CONTROLLER_BROKER_METADATA_NOT_EXIST = 2008;
    static constexpr int32_t CONTROLLER_INVALID_CLEAN_BROKER_METADATA = 2009;
    static constexpr int32_t CONTROLLER_BROKER_NEED_TO_BE_REGISTERED = 2010;
    static constexpr int32_t CONTROLLER_MASTER_STILL_EXIST = 2011;
    static constexpr int32_t CONTROLLER_ELECT_MASTER_FAILED = 2012;
    static constexpr int32_t CONTROLLER_ALTER_SYNC_STATE_SET_FAILED = 2013;
    static constexpr int32_t CONTROLLER_BROKER_ID_INVALID = 2014;
    static constexpr int32_t CONTROLLER_JRAFT_INTERNAL_ERROR = 2015;
    static constexpr int32_t CONTROLLER_BROKER_LIVE_INFO_NOT_EXISTS = 2016;
    static constexpr int32_t LMQ_QUOTA_EXCEEDED = 2017;
    static constexpr int32_t LITE_SUBSCRIPTION_QUOTA_EXCEEDED = 2018;
    static constexpr int32_t USER_NOT_EXIST = 3001;
    static constexpr int32_t POLICY_NOT_EXIST = 3002;
};

// org.apache.rocketmq.remoting.protocol.LanguageCode（应用层可选 identity 提示）
struct LanguageCode {
    static constexpr uint8_t JAVA = 0;
    static constexpr uint8_t CPP = 1;
    static constexpr uint8_t DOTNET = 2;
    static constexpr uint8_t PYTHON = 3;
    static constexpr uint8_t DELPHI = 4;
    static constexpr uint8_t ERLANG = 5;
    static constexpr uint8_t RUBY = 6;
    static constexpr uint8_t OTHER = 7;
    static constexpr uint8_t HTTP = 8;
    static constexpr uint8_t GO = 9;
    static constexpr uint8_t PHP = 10;
    static constexpr uint8_t OMS = 11;
    static constexpr uint8_t RUST = 12;
    static constexpr uint8_t NODE_JS = 13;
    // CPP = 1 即本实现使用的语种代号（见 RemotingCommand 的 language 字段）
    struct Values {
        static constexpr const char* JAVA = "JAVA";
        static constexpr const char* CPP = "CPP";
        static constexpr const char* DOTNET = "DOTNET";
        static constexpr const char* PYTHON = "PYTHON";
        static constexpr const char* DELPHI = "DELPHI";
        static constexpr const char* ERLANG = "ERLANG";
        static constexpr const char* RUBY = "RUBY";
        static constexpr const char* OTHER = "OTHER";
        static constexpr const char* HTTP = "HTTP";
        static constexpr const char* GO = "GO";
        static constexpr const char* PHP = "PHP";
        static constexpr const char* OMS = "OMS";
        static constexpr const char* RUST = "RUST";
        static constexpr const char* NODE_JS = "NODE_JS";
    };
    // 按码值反查名字；未知返回 nullptr
    static const char* valueOf(int32_t code) {
        switch (code) {
            case 0: return "JAVA";
            case 1: return "CPP";
            case 2: return "DOTNET";
            case 3: return "PYTHON";
            case 4: return "DELPHI";
            case 5: return "ERLANG";
            case 6: return "RUBY";
            case 7: return "OTHER";
            case 8: return "HTTP";
            case 9: return "GO";
            case 10: return "PHP";
            case 11: return "OMS";
            case 12: return "RUST";
            case 13: return "NODE_JS";
            default: return nullptr;
        }
    }

};

// org.apache.rocketmq.remoting.protocol.SerializeType
struct SerializeType {
    static constexpr uint8_t JSON = 0;
    static constexpr uint8_t ROCKETMQ = 1;
    struct Values {
        static constexpr const char* JSON = "JSON";
        static constexpr const char* ROCKETMQ = "ROCKETMQ";
    };
    // 按码值反查名字；未知返回 nullptr
    static const char* valueOf(int32_t code) {
        switch (code) {
            case 0: return "JSON";
            case 1: return "ROCKETMQ";
            default: return nullptr;
        }
    }

};

struct RemotingCommandType {
    static constexpr const char* REQUEST_COMMAND = "REQUEST_COMMAND";
    static constexpr const char* RESPONSE_COMMAND = "RESPONSE_COMMAND";
};

struct ForbiddenType {
    static constexpr int32_t BROKER_FORBIDDEN = 1;
    static constexpr int32_t GROUP_FORBIDDEN = 2;
    static constexpr int32_t TOPIC_FORBIDDEN = 3;
    static constexpr int32_t BROADCASTING_DISABLE_FORBIDDEN = 4;
    static constexpr int32_t SUBSCRIPTION_FORBIDDEN = 5;
};

struct RequestType {
    static constexpr int32_t STREAM = 0;
};

struct RequestSource {
    static constexpr int32_t SDK = -1;
    static constexpr int32_t PROXY_FOR_ORDER = 0;
    static constexpr int32_t PROXY_FOR_BROADCAST = 1;
    static constexpr int32_t PROXY_FOR_STREAM = 2;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_CODES_H
