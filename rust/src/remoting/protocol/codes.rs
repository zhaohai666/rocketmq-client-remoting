//! 请求码 / 响应码 / 语言码 / 序列化类型
//! （对应 `org.apache.rocketmq.remoting.protocol.RequestCode`、`ResponseCode`、`LanguageCode`、`SerializeType`）。

/// `RequestCode`
pub mod request_code {
    pub const SEND_MESSAGE: i32 = 10;
    pub const PULL_MESSAGE: i32 = 11;
    pub const QUERY_MESSAGE: i32 = 12;
    pub const QUERY_BROKER_OFFSET: i32 = 13;
    pub const QUERY_CONSUMER_OFFSET: i32 = 14;
    pub const UPDATE_CONSUMER_OFFSET: i32 = 15;
    pub const UPDATE_AND_CREATE_TOPIC: i32 = 17;
    pub const UPDATE_AND_CREATE_TOPIC_LIST: i32 = 18;
    pub const GET_ALL_TOPIC_CONFIG: i32 = 21;
    pub const GET_TOPIC_CONFIG_LIST: i32 = 22;
    pub const GET_TOPIC_NAME_LIST: i32 = 23;
    pub const UPDATE_BROKER_CONFIG: i32 = 25;
    pub const GET_BROKER_CONFIG: i32 = 26;
    pub const TRIGGER_DELETE_FILES: i32 = 27;
    pub const GET_BROKER_RUNTIME_INFO: i32 = 28;
    pub const SEARCH_OFFSET_BY_TIMESTAMP: i32 = 29;
    pub const GET_MAX_OFFSET: i32 = 30;
    pub const GET_MIN_OFFSET: i32 = 31;
    pub const GET_EARLIEST_MSG_STORETIME: i32 = 32;
    pub const VIEW_MESSAGE_BY_ID: i32 = 33;
    pub const HEART_BEAT: i32 = 34;
    pub const UNREGISTER_CLIENT: i32 = 35;
    pub const CONSUMER_SEND_MSG_BACK: i32 = 36;
    pub const END_TRANSACTION: i32 = 37;
    pub const GET_CONSUMER_LIST_BY_GROUP: i32 = 38;
    pub const CHECK_TRANSACTION_STATE: i32 = 39;
    pub const NOTIFY_CONSUMER_IDS_CHANGED: i32 = 40;
    pub const LOCK_BATCH_MQ: i32 = 41;
    pub const UNLOCK_BATCH_MQ: i32 = 42;
    pub const GET_ALL_CONSUMER_OFFSET: i32 = 43;
    pub const GET_ALL_DELAY_OFFSET: i32 = 45;
    pub const CHECK_CLIENT_CONFIG: i32 = 46;
    pub const GET_CLIENT_CONFIG: i32 = 47;
    pub const GET_TIMER_CHECK_POINT: i32 = 60;
    pub const GET_TIMER_METRICS: i32 = 61;
    pub const POP_MESSAGE: i32 = 200050;
    pub const ACK_MESSAGE: i32 = 200051;
    pub const BATCH_ACK_MESSAGE: i32 = 200151;
    pub const PEEK_MESSAGE: i32 = 200052;
    pub const CHANGE_MESSAGE_INVISIBLETIME: i32 = 200053;
    pub const NOTIFICATION: i32 = 200054;
    pub const POLLING_INFO: i32 = 200055;
    pub const POP_ROLLBACK: i32 = 200056;
    pub const POP_LITE_MESSAGE: i32 = 200070;
    pub const LITE_SUBSCRIPTION_CTL: i32 = 200071;
    pub const ACK_LITE_MESSAGE: i32 = 200072;
    pub const NOTIFY_UNSUBSCRIBE_LITE: i32 = 200073;
    pub const GET_BROKER_LITE_INFO: i32 = 200074;
    pub const GET_PARENT_TOPIC_INFO: i32 = 200075;
    pub const GET_LITE_TOPIC_INFO: i32 = 200076;
    pub const GET_LITE_CLIENT_INFO: i32 = 200077;
    pub const GET_LITE_GROUP_INFO: i32 = 200078;
    pub const TRIGGER_LITE_DISPATCH: i32 = 200079;
    pub const PUT_KV_CONFIG: i32 = 100;
    pub const GET_KV_CONFIG: i32 = 101;
    pub const DELETE_KV_CONFIG: i32 = 102;
    pub const REGISTER_BROKER: i32 = 103;
    pub const UNREGISTER_BROKER: i32 = 104;
    pub const GET_ROUTEINFO_BY_TOPIC: i32 = 105;
    pub const GET_BROKER_CLUSTER_INFO: i32 = 106;
    pub const UPDATE_AND_CREATE_SUBSCRIPTIONGROUP: i32 = 200;
    pub const GET_ALL_SUBSCRIPTIONGROUP_CONFIG: i32 = 201;
    pub const GET_TOPIC_STATS_INFO: i32 = 202;
    pub const GET_CONSUMER_CONNECTION_LIST: i32 = 203;
    pub const GET_PRODUCER_CONNECTION_LIST: i32 = 204;
    pub const WIPE_WRITE_PERM_OF_BROKER: i32 = 205;
    pub const GET_ALL_TOPIC_LIST_FROM_NAMESERVER: i32 = 206;
    pub const DELETE_SUBSCRIPTIONGROUP: i32 = 207;
    pub const GET_CONSUME_STATS: i32 = 208;
    pub const SUSPEND_CONSUMER: i32 = 209;
    pub const RESUME_CONSUMER: i32 = 210;
    pub const RESET_CONSUMER_OFFSET_IN_CONSUMER: i32 = 211;
    pub const RESET_CONSUMER_OFFSET_IN_BROKER: i32 = 212;
    pub const ADJUST_CONSUMER_THREAD_POOL: i32 = 213;
    pub const WHO_CONSUME_THE_MESSAGE: i32 = 214;
    pub const DELETE_TOPIC_IN_BROKER: i32 = 215;
    pub const DELETE_TOPIC_IN_NAMESRV: i32 = 216;
    pub const REGISTER_TOPIC_IN_NAMESRV: i32 = 217;
    pub const GET_KVLIST_BY_NAMESPACE: i32 = 219;
    pub const RESET_CONSUMER_CLIENT_OFFSET: i32 = 220;
    pub const GET_CONSUMER_STATUS_FROM_CLIENT: i32 = 221;
    pub const INVOKE_BROKER_TO_RESET_OFFSET: i32 = 222;
    pub const INVOKE_BROKER_TO_GET_CONSUMER_STATUS: i32 = 223;
    pub const QUERY_TOPIC_CONSUME_BY_WHO: i32 = 300;
    pub const GET_TOPICS_BY_CLUSTER: i32 = 224;
    pub const UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST: i32 = 225;
    pub const QUERY_TOPICS_BY_CONSUMER: i32 = 343;
    pub const QUERY_SUBSCRIPTION_BY_CONSUMER: i32 = 345;
    pub const REGISTER_FILTER_SERVER: i32 = 301;
    pub const REGISTER_MESSAGE_FILTER_CLASS: i32 = 302;
    pub const QUERY_CONSUME_TIME_SPAN: i32 = 303;
    pub const GET_SYSTEM_TOPIC_LIST_FROM_NS: i32 = 304;
    pub const GET_SYSTEM_TOPIC_LIST_FROM_BROKER: i32 = 305;
    pub const CLEAN_EXPIRED_CONSUMEQUEUE: i32 = 306;
    pub const GET_CONSUMER_RUNNING_INFO: i32 = 307;
    pub const QUERY_CORRECTION_OFFSET: i32 = 308;
    pub const CONSUME_MESSAGE_DIRECTLY: i32 = 309;
    pub const SEND_MESSAGE_V2: i32 = 310;
    pub const GET_UNIT_TOPIC_LIST: i32 = 311;
    pub const GET_HAS_UNIT_SUB_TOPIC_LIST: i32 = 312;
    pub const GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST: i32 = 313;
    pub const CLONE_GROUP_OFFSET: i32 = 314;
    pub const VIEW_BROKER_STATS_DATA: i32 = 315;
    pub const CLEAN_UNUSED_TOPIC: i32 = 316;
    pub const GET_BROKER_CONSUME_STATS: i32 = 317;
    pub const UPDATE_NAMESRV_CONFIG: i32 = 318;
    pub const GET_NAMESRV_CONFIG: i32 = 319;
    pub const SEND_BATCH_MESSAGE: i32 = 320;
    pub const QUERY_CONSUME_QUEUE: i32 = 321;
    pub const QUERY_DATA_VERSION: i32 = 322;
    pub const RESUME_CHECK_HALF_MESSAGE: i32 = 323;
    pub const SEND_REPLY_MESSAGE: i32 = 324;
    pub const SEND_REPLY_MESSAGE_V2: i32 = 325;
    pub const PUSH_REPLY_MESSAGE_TO_CLIENT: i32 = 326;
    pub const ADD_WRITE_PERM_OF_BROKER: i32 = 327;
    pub const GET_ALL_PRODUCER_INFO: i32 = 328;
    pub const DELETE_EXPIRED_COMMITLOG: i32 = 329;
    pub const GET_TOPIC_CONFIG: i32 = 351;
    pub const GET_SUBSCRIPTIONGROUP_CONFIG: i32 = 352;
    pub const UPDATE_AND_GET_GROUP_FORBIDDEN: i32 = 353;
    pub const GET_BROKER_MEMBER_GROUP: i32 = 901;
    pub const BROKER_HEARTBEAT: i32 = 904;
    pub const CHECK_ROCKSDB_CQ_WRITE_PROGRESS: i32 = 354;
    pub const EXPORT_ROCKSDB_CONFIG_TO_JSON: i32 = 355;
    pub const LITE_PULL_MESSAGE: i32 = 361;
    pub const RECALL_MESSAGE: i32 = 370;
    pub const QUERY_ASSIGNMENT: i32 = 400;
    pub const SET_MESSAGE_REQUEST_MODE: i32 = 401;
    pub const GET_ALL_MESSAGE_REQUEST_MODE: i32 = 402;
    pub const UPDATE_AND_CREATE_STATIC_TOPIC: i32 = 513;
    pub const ADD_BROKER: i32 = 902;
    pub const REMOVE_BROKER: i32 = 903;
    pub const NOTIFY_MIN_BROKER_ID_CHANGE: i32 = 905;
    pub const EXCHANGE_BROKER_HA_INFO: i32 = 906;
    pub const GET_BROKER_HA_STATUS: i32 = 907;
    pub const RESET_MASTER_FLUSH_OFFSET: i32 = 908;
    pub const CONTROLLER_ALTER_SYNC_STATE_SET: i32 = 1001;
    pub const CONTROLLER_ELECT_MASTER: i32 = 1002;
    pub const CONTROLLER_REGISTER_BROKER: i32 = 1003;
    pub const CONTROLLER_GET_REPLICA_INFO: i32 = 1004;
    pub const CONTROLLER_GET_METADATA_INFO: i32 = 1005;
    pub const CONTROLLER_GET_SYNC_STATE_DATA: i32 = 1006;
    pub const GET_BROKER_EPOCH_CACHE: i32 = 1007;
    pub const NOTIFY_BROKER_ROLE_CHANGED: i32 = 1008;
    pub const UPDATE_CONTROLLER_CONFIG: i32 = 1009;
    pub const GET_CONTROLLER_CONFIG: i32 = 1010;
    pub const CLEAN_BROKER_DATA: i32 = 1011;
    pub const CONTROLLER_GET_NEXT_BROKER_ID: i32 = 1012;
    pub const CONTROLLER_APPLY_BROKER_ID: i32 = 1013;
    pub const UPDATE_COLD_DATA_FLOW_CTR_CONFIG: i32 = 2001;
    pub const REMOVE_COLD_DATA_FLOW_CTR_CONFIG: i32 = 2002;
    pub const GET_COLD_DATA_FLOW_CTR_INFO: i32 = 2003;
    pub const SET_COMMITLOG_READ_MODE: i32 = 2004;
    pub const AUTH_CREATE_USER: i32 = 3001;
    pub const AUTH_UPDATE_USER: i32 = 3002;
    pub const AUTH_DELETE_USER: i32 = 3003;
    pub const AUTH_GET_USER: i32 = 3004;
    pub const AUTH_LIST_USER: i32 = 3005;
    pub const AUTH_CREATE_ACL: i32 = 3006;
    pub const AUTH_UPDATE_ACL: i32 = 3007;
    pub const AUTH_DELETE_ACL: i32 = 3008;
    pub const AUTH_GET_ACL: i32 = 3009;
    pub const AUTH_LIST_ACL: i32 = 3010;
    pub const SWITCH_TIMER_ENGINE: i32 = 5001;
    pub const DELETE_TOPIC_IN_BROKER_LIST: i32 = 5002;
    pub const DELETE_SUBSCRIPTION_GROUP_LIST: i32 = 5003;

    /// 可读名字，日志与 `codes.h` 常量守卫测试用。
    pub fn name_of(code: i32) -> Option<&'static str> {
        Some(match code {
            SEND_MESSAGE => "SEND_MESSAGE",
            PULL_MESSAGE => "PULL_MESSAGE",
            SEND_MESSAGE_V2 => "SEND_MESSAGE_V2",
            GET_ROUTEINFO_BY_TOPIC => "GET_ROUTEINFO_BY_TOPIC",
            HEART_BEAT => "HEART_BEAT",
            UNREGISTER_CLIENT => "UNREGISTER_CLIENT",
            CONSUMER_SEND_MSG_BACK => "CONSUMER_SEND_MSG_BACK",
            END_TRANSACTION => "END_TRANSACTION",
            GET_CONSUMER_LIST_BY_GROUP => "GET_CONSUMER_LIST_BY_GROUP",
            CHECK_TRANSACTION_STATE => "CHECK_TRANSACTION_STATE",
            LOCK_BATCH_MQ => "LOCK_BATCH_MQ",
            UNLOCK_BATCH_MQ => "UNLOCK_BATCH_MQ",
            POP_MESSAGE => "POP_MESSAGE",
            ACK_MESSAGE => "ACK_MESSAGE",
            BATCH_ACK_MESSAGE => "BATCH_ACK_MESSAGE",
            LITE_PULL_MESSAGE => "LITE_PULL_MESSAGE",
            UPDATE_CONSUMER_OFFSET => "UPDATE_CONSUMER_OFFSET",
            QUERY_CONSUMER_OFFSET => "QUERY_CONSUMER_OFFSET",
            GET_MAX_OFFSET => "GET_MAX_OFFSET",
            GET_MIN_OFFSET => "GET_MIN_OFFSET",
            SEARCH_OFFSET_BY_TIMESTAMP => "SEARCH_OFFSET_BY_TIMESTAMP",
            VIEW_MESSAGE_BY_ID => "VIEW_MESSAGE_BY_ID",
            SEND_BATCH_MESSAGE => "SEND_BATCH_MESSAGE",
            SEND_REPLY_MESSAGE => "SEND_REPLY_MESSAGE",
            SEND_REPLY_MESSAGE_V2 => "SEND_REPLY_MESSAGE_V2",
            PUSH_REPLY_MESSAGE_TO_CLIENT => "PUSH_REPLY_MESSAGE_TO_CLIENT",
            GET_CONSUME_STATS => "GET_CONSUME_STATS",
            UPDATE_AND_CREATE_TOPIC => "UPDATE_AND_CREATE_TOPIC",
            DELETE_TOPIC_IN_BROKER => "DELETE_TOPIC_IN_BROKER",
            DELETE_TOPIC_IN_NAMESRV => "DELETE_TOPIC_IN_NAMESRV",
            UPDATE_AND_CREATE_SUBSCRIPTIONGROUP => "UPDATE_AND_CREATE_SUBSCRIPTIONGROUP",
            DELETE_SUBSCRIPTIONGROUP => "DELETE_SUBSCRIPTIONGROUP",
            GET_ALL_TOPIC_CONFIG => "GET_ALL_TOPIC_CONFIG",
            GET_TOPIC_STATS_INFO => "GET_TOPIC_STATS_INFO",
            _ => return None,
        })
    }
}

/// `RemotingSysResponseCode`
pub mod remoting_sys_response_code {
    pub const SUCCESS: i32 = 0;
    pub const SYSTEM_ERROR: i32 = 1;
    pub const SYSTEM_BUSY: i32 = 2;
    pub const REQUEST_CODE_NOT_SUPPORTED: i32 = 3;
    pub const TRANSACTION_FAILED: i32 = 4;
}

/// `ResponseCode extends RemotingSysResponseCode`
pub mod response_code {
    pub use super::remoting_sys_response_code::*;

    pub const FLUSH_DISK_TIMEOUT: i32 = 10;
    pub const SLAVE_NOT_AVAILABLE: i32 = 11;
    pub const FLUSH_SLAVE_TIMEOUT: i32 = 12;
    pub const MESSAGE_ILLEGAL: i32 = 13;
    pub const SERVICE_NOT_AVAILABLE: i32 = 14;
    pub const VERSION_NOT_SUPPORTED: i32 = 15;
    pub const NO_PERMISSION: i32 = 16;
    pub const TOPIC_NOT_EXIST: i32 = 17;
    pub const TOPIC_EXIST_ALREADY: i32 = 18;
    pub const PULL_NOT_FOUND: i32 = 19;
    pub const PULL_RETRY_IMMEDIATELY: i32 = 20;
    pub const PULL_OFFSET_MOVED: i32 = 21;
    pub const QUERY_NOT_FOUND: i32 = 22;
    pub const SUBSCRIPTION_PARSE_FAILED: i32 = 23;
    pub const SUBSCRIPTION_NOT_EXIST: i32 = 24;
    pub const SUBSCRIPTION_NOT_LATEST: i32 = 25;
    pub const SUBSCRIPTION_GROUP_NOT_EXIST: i32 = 26;
    pub const FILTER_DATA_NOT_EXIST: i32 = 27;
    pub const FILTER_DATA_NOT_LATEST: i32 = 28;
    pub const INVALID_PARAMETER: i32 = 29;
    pub const TRANSACTION_SHOULD_COMMIT: i32 = 200;
    pub const TRANSACTION_SHOULD_ROLLBACK: i32 = 201;
    pub const TRANSACTION_STATE_UNKNOW: i32 = 202;
    pub const TRANSACTION_STATE_GROUP_WRONG: i32 = 203;
    pub const NO_BUYER_ID: i32 = 204;
    pub const NOT_IN_CURRENT_UNIT: i32 = 205;
    pub const CONSUMER_NOT_ONLINE: i32 = 206;
    pub const CONSUME_MSG_TIMEOUT: i32 = 207;
    pub const NO_MESSAGE: i32 = 208;
    pub const POLLING_FULL: i32 = 209;
    pub const POLLING_TIMEOUT: i32 = 210;
    pub const BROKER_NOT_EXIST: i32 = 211;
    pub const BROKER_DISPATCH_NOT_COMPLETE: i32 = 212;
    pub const BROADCAST_CONSUMPTION: i32 = 213;
    pub const FLOW_CONTROL: i32 = 215;
    pub const NOT_LEADER_FOR_QUEUE: i32 = 501;
    pub const ILLEGAL_OPERATION: i32 = 604;
    pub const GO_AWAY: i32 = 1500;
    pub const CONTROLLER_FENCED_MASTER_EPOCH: i32 = 2000;
    pub const CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH: i32 = 2001;
    pub const CONTROLLER_INVALID_MASTER: i32 = 2002;
    pub const CONTROLLER_INVALID_REPLICAS: i32 = 2003;
    pub const CONTROLLER_MASTER_NOT_AVAILABLE: i32 = 2004;
    pub const CONTROLLER_INVALID_REQUEST: i32 = 2005;
    pub const CONTROLLER_BROKER_NOT_ALIVE: i32 = 2006;
    pub const CONTROLLER_NOT_LEADER: i32 = 2007;
    pub const CONTROLLER_BROKER_METADATA_NOT_EXIST: i32 = 2008;
    pub const CONTROLLER_INVALID_CLEAN_BROKER_METADATA: i32 = 2009;
    pub const CONTROLLER_BROKER_NEED_TO_BE_REGISTERED: i32 = 2010;
    pub const CONTROLLER_MASTER_STILL_EXIST: i32 = 2011;
    pub const CONTROLLER_ELECT_MASTER_FAILED: i32 = 2012;
    pub const CONTROLLER_ALTER_SYNC_STATE_SET_FAILED: i32 = 2013;
    pub const CONTROLLER_BROKER_ID_INVALID: i32 = 2014;
    pub const CONTROLLER_JRAFT_INTERNAL_ERROR: i32 = 2015;
    pub const CONTROLLER_BROKER_LIVE_INFO_NOT_EXISTS: i32 = 2016;
    pub const LMQ_QUOTA_EXCEEDED: i32 = 2017;
    pub const LITE_SUBSCRIPTION_QUOTA_EXCEEDED: i32 = 2018;
    pub const USER_NOT_EXIST: i32 = 3001;
    pub const POLICY_NOT_EXIST: i32 = 3002;

    // 客户端侧伪响应码
    pub const RPC_UNKNOWN: i32 = -1000;
    pub const RPC_ADDR_IS_NULL: i32 = -1002;
    pub const RPC_SEND_TO_CHANNEL_FAILED: i32 = -1004;
    pub const RPC_TIME_OUT: i32 = -1006;
}

/// `LanguageCode`（byte 码与 Java 枚举顺序一致）
pub mod language_code {
    pub const JAVA: i32 = 0;
    pub const CPP: i32 = 1;
    pub const DOTNET: i32 = 2;
    pub const PYTHON: i32 = 3;
    pub const DELPHI: i32 = 4;
    pub const ERLANG: i32 = 5;
    pub const RUBY: i32 = 6;
    pub const OTHER: i32 = 7;
    pub const HTTP: i32 = 8;
    pub const GO: i32 = 9;
    pub const PHP: i32 = 10;
    pub const OMS: i32 = 11;
    pub const RUST: i32 = 12;
    pub const NODE_JS: i32 = 13;

    pub fn value_of(code: i32) -> Option<&'static str> {
        Some(match code & 0xFF {
            JAVA => "JAVA",
            CPP => "CPP",
            DOTNET => "DOTNET",
            PYTHON => "PYTHON",
            DELPHI => "DELPHI",
            ERLANG => "ERLANG",
            RUBY => "RUBY",
            OTHER => "OTHER",
            HTTP => "HTTP",
            GO => "GO",
            PHP => "PHP",
            OMS => "OMS",
            RUST => "RUST",
            NODE_JS => "NODE_JS",
            _ => return None,
        })
    }

    /// 5.x NameServer 会把 language 序列化成枚举名字符串。
    pub fn name_to_code(name: &str) -> i32 {
        match name.to_ascii_uppercase().as_str() {
            "JAVA" => JAVA,
            "CPP" => CPP,
            "DOTNET" => DOTNET,
            "PYTHON" => PYTHON,
            "DELPHI" => DELPHI,
            "ERLANG" => ERLANG,
            "RUBY" => RUBY,
            "HTTP" => HTTP,
            "GO" => GO,
            "PHP" => PHP,
            "OMS" => OMS,
            "RUST" => RUST,
            "NODEJS" | "NODE_JS" => NODE_JS,
            _ => OTHER,
        }
    }
}

/// `SerializeType`
pub mod serialize_type {
    pub const JSON: i32 = 0;
    pub const ROCKETMQ: i32 = 1;

    pub fn value_of(code: i32) -> Option<&'static str> {
        match code & 0xFF {
            JSON => Some("JSON"),
            ROCKETMQ => Some("ROCKETMQ"),
            _ => None,
        }
    }
}

/// `RemotingCommandType`
pub mod remoting_command_type {
    pub const REQUEST_COMMAND: &str = "REQUEST_COMMAND";
    pub const RESPONSE_COMMAND: &str = "RESPONSE_COMMAND";
}

/// `ForbiddenType`
pub mod forbidden_type {
    pub const BROKER_FORBIDDEN: i32 = 1;
    pub const GROUP_FORBIDDEN: i32 = 2;
    pub const TOPIC_FORBIDDEN: i32 = 3;
    pub const BROADCASTING_DISABLE_FORBIDDEN: i32 = 4;
    pub const SUBSCRIPTION_FORBIDDEN: i32 = 5;
}

pub mod request_type {
    pub const STREAM: i32 = 0;
}

pub mod request_source {
    pub const SDK: i32 = -1;
    pub const PROXY_FOR_ORDER: i32 = 0;
    pub const PROXY_FOR_BROADCAST: i32 = 1;
    pub const PROXY_FOR_STREAM: i32 = 2;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn language_codes_match_java() {
        assert_eq!(language_code::JAVA, 0);
        assert_eq!(language_code::PYTHON, 3);
        assert_eq!(language_code::RUST, 12);
        assert_eq!(language_code::name_to_code("java"), language_code::JAVA);
        assert_eq!(language_code::value_of(13), Some("NODE_JS"));
        assert_eq!(language_code::value_of(99), None);
    }

    #[test]
    fn key_codes_match_java() {
        assert_eq!(request_code::SEND_MESSAGE_V2, 310);
        assert_eq!(request_code::GET_ROUTEINFO_BY_TOPIC, 105);
        assert_eq!(request_code::POP_MESSAGE, 200050);
        assert_eq!(response_code::PULL_NOT_FOUND, 19);
        assert_eq!(response_code::SUCCESS, 0);
        assert_eq!(response_code::RPC_TIME_OUT, -1006);
        assert_eq!(serialize_type::value_of(1), Some("ROCKETMQ"));
    }
}
