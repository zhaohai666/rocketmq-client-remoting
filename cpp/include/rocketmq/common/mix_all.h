#ifndef ROCKETMQ_COMMON_MIX_ALL_H
#define ROCKETMQ_COMMON_MIX_ALL_H

#include <cstdint>
#include <string>

#include "rocketmq/common/types.h"

namespace rocketmq {

struct MixAll {
    static constexpr const char* NAMESRV_ADDR_PROPERTY = "rocketmq.namesrv.addr";
    static constexpr const char* NAMESRV_ADDR_ENV = "NAMESRV_ADDR";
    static constexpr const char* MESSAGE_COMPRESS_LEVEL = "rocketmq.message.compressLevel";
    static constexpr const char* DEFAULT_TOPIC = "TBW102";
    static constexpr const char* BENCHMARK_TOPIC = "BenchmarkTest";
    static constexpr const char* DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER";
    // Default value of instanceName (config rocketmq.client.name, "DEFAULT" when absent)
    static constexpr const char* DEFAULT_INSTANCE_NAME = "DEFAULT";
    static constexpr const char* DEFAULT_CONSUMER_GROUP = "DEFAULT_CONSUMER";
    static constexpr const char* CLIENT_INNER_PRODUCER_GROUP = "CLIENT_INNER_PRODUCER";
    static constexpr const char* SELF_TEST_PRODUCER_GROUP = "SELF_TEST_P_GROUP";
    static constexpr const char* SELF_TEST_CONSUMER_GROUP = "SELF_TEST_C_GROUP";
    static constexpr const char* ONS_ADDR = "ONS_ADDR";
    static constexpr const char* CID_RMQ_SYS_PREFIX = "CID_RMQ_SYS_";
    static constexpr const char* CID_ONSAPI_PREFIX = "CID_ONSAPI_";
    static constexpr const char* PROXY_NAME = "MQProxy";

    static constexpr const char* RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";
    static constexpr const char* DLQ_GROUP_TOPIC_PREFIX = "%DLQ%";
    static constexpr const char* REPLY_TOPIC_PREFIX = "%REPLY%";
    // Request-Reply (5.x): the reply topic name = <clusterName>_REPLY_TOPIC.
    // ⚠ Note this is not the same as REPLY_TOPIC_PREFIX ("%REPLY%") above: the latter is the
    // older convention where "a consumer sends the reply back to its own %REPLY%<topic>"; 5.x request-reply uses the one below.
    static constexpr const char* REPLY_TOPIC_POSTFIX = "REPLY_TOPIC";
    // MSG_TYPE property value of a reply message (protocol MixAll.REPLY_MESSAGE_FLAG)
    static constexpr const char* REPLY_MESSAGE_FLAG = "reply";
    static constexpr const char* SYSTEM_TOPIC_PREFIX = "rmq_sys_";
    static constexpr const char* TOOLS_CONSUMER_GROUP = "TOOLS_CONSUMER";
    static constexpr const char* FILTERSRV_CONSUMER_GROUP = "FILTERSRV_CONSUMER";
    static constexpr const char* MONITOR_CONSUMER_GROUP = "__MONITOR_CONSUMER";
    static constexpr const char* CLIENT_INNER_CONSUMER_GROUP = "CLIENT_INNER_CONSUMER";
    static constexpr const char* ONS_NAMESPACE = "namespace";
    static constexpr const char* SCHEDULE_CONSUMER_GROUP = "SCHEDULE_CONSUMER";
    static constexpr const char* ONS_HTTP_PROXY_GROUP = "CID_ONSHTTP_PROXY";
    static constexpr const char* CID_ONSAPI_PERMISSION_GROUP = "CID_ONSAPI_PERMISSION";
    static constexpr const char* CID_ONSAPI_OWNER_GROUP = "CID_ONSAPI_OWNER";
    static constexpr const char* CID_ONSAPI_PULL_GROUP = "CID_ONSAPI_PULL";
    static constexpr const char* CID_SYS_RMQ_TRANS = "CID_SYS_RMQ_TRANS";

    // ⚠ In the protocol UNIQUE_MSG_QUERY_FLAG is the **key name** in extFields (value "true"/"false"),
    // not an integer flag bit: sent as an integer the key becomes a numeric key and a uniqKey query cannot route to the RocksDB index branch.
    // The three modes of querying messages by key are in QueryMsgType.
    static constexpr const char* UNIQUE_MSG_QUERY_FLAG = "_UNIQUE_KEY_QUERY";
    static constexpr const char* SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    static constexpr const char* LMQ_PREFIX = "%LMQ%";
    static constexpr int32_t LMQ_QUEUE_ID = 0;
    static constexpr const char* TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC";
    static constexpr const char* REAL_TRACE_TOPIC = "rmq_sys_TRACE_DATA";
    static constexpr const char* RMQ_SYS_TRANS_HALF_TOPIC = "RMQ_SYS_TRANS_HALF_TOPIC";
    static constexpr const char* RMQ_SYS_TRANS_OP_HALF_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    static constexpr int32_t TRANS_CHECK_MAX_TIME = 15;
    static constexpr const char* UNIT_PREFIX = "unit_";
    // Protocol MixAll.REQ_T: the extFields key marking the request type, written by StreamTypeRPCHook.
    // Only the broker-side proxy/stream chain reads it; a plain broker ignores it.
    static constexpr const char* REQ_T = "ReqT";
    // The clientId string uses `RequestType.STREAM.name()`, i.e. the literal "STREAM"
    // (⚠ different from that **enum code** in extFields, see StreamTypeRPCHook).
    static constexpr const char* STREAM_REQUEST_TYPE = "STREAM";
    // The two extFields keys of the 5.x namespace (namespaceV2, Alibaba Cloud instance-id shaped),
    // protocol MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD / _NAMESPACE_FIELD
    // , written by NamespaceRpcHook.
    // ⚠ These are **two mechanisms** distinct from setNamespace above (the old namespace, where the client rewrites the topic into "ns%topic"):
    // this one leaves the topic name untouched and the broker resolves the resource into the instance from the request header.
    static constexpr const char* RPC_REQUEST_HEADER_NAMESPACED_FIELD = "nsd";
    static constexpr const char* RPC_REQUEST_HEADER_NAMESPACE_FIELD = "ns";

    static constexpr int32_t DEFAULT_TOPIC_QUEUE_NUMS = 4;
    static constexpr int32_t DEFAULT_TOPIC_READ_QUEUE_NUMS = 4;
    static constexpr int32_t DEFAULT_TOPIC_WRITE_QUEUE_NUMS = 4;
    static constexpr int32_t MAX_TOPIC_LENGTH = 127;
    static constexpr int32_t MAX_GROUP_LENGTH = 255;
    static constexpr int32_t CHARACTER_MAX_LENGTH = 255;

    static constexpr int32_t MASTER_ID = 0;
    static constexpr int32_t READ_PERM_BY_DEFAULT = 4 | 2;  // PERM_READ | PERM_WRITE

    static constexpr const char* NAMESPACE_ORDER_TOPIC_CONFIG = "ORDER_TOPIC_CONFIG";

    static std::string getRetryTopic(const std::string& consumerGroup) {
        return std::string(RETRY_GROUP_TOPIC_PREFIX) + consumerGroup;
    }

    // Protocol MixAll.brokerVIPChannel: the VIP channel = port - 2.
    // When the port cannot be parsed the address is returned unchanged (so the transport never crashes over it).
    static std::string brokerVipChannel(bool isChange, const std::string& brokerAddr) {
        if (!isChange) return brokerAddr;
        size_t colon = brokerAddr.rfind(':');
        if (colon == std::string::npos) return brokerAddr;
        try {
            int64_t port = std::stoll(brokerAddr.substr(colon + 1));
            return brokerAddr.substr(0, colon) + ":" + std::to_string(port - 2);
        } catch (...) {
            return brokerAddr;
        }
    }

    static bool isRetryTopic(const std::string& topic) {
        return topic.rfind(RETRY_GROUP_TOPIC_PREFIX, 0) == 0;
    }

    static std::string getDlqTopic(const std::string& consumerGroup) {
        return std::string(DLQ_GROUP_TOPIC_PREFIX) + consumerGroup;
    }

    static bool isDlqTopic(const std::string& topic) {
        return topic.rfind(DLQ_GROUP_TOPIC_PREFIX, 0) == 0;
    }

    // Protocol MixAll.getReplyTopic(clusterName) = clusterName + "_REPLY_TOPIC".
    // The broker registers this topic as a **system topic** at startup (TopicConfigManager.init), so
    // a client must not createTopic it (rejected with INVALID_PARAMETER "conflict with system topic").
    static std::string getReplyTopic(const std::string& clusterName) {
        return clusterName + "_" + REPLY_TOPIC_POSTFIX;
    }

    static bool isSysTopic(const std::string& topic) {
        return topic.rfind(SYSTEM_TOPIC_PREFIX, 0) == 0;
    }

    // Protocol MixAll.isLmq (an LMQ topic starts with %LMQ%)
    static bool isLmq(const std::string& lmqMetaData) {
        return lmqMetaData.rfind(LMQ_PREFIX, 0) == 0;
    }

    // Protocol MixAll.isSysConsumerGroup (CID_RMQ_SYS_ prefix)
    static bool isSysConsumerGroup(const std::string& consumerGroup) {
        return consumerGroup.rfind(CID_RMQ_SYS_PREFIX, 0) == 0;
    }

    // Protocol MixAll.isPredefinedGroup (a hit in PREDEFINE_GROUP_SET)
    static bool isPredefinedGroup(const std::string& consumerGroup);

    static std::string resetRetryAndDlqTopic(const std::string& topic) {
        if (isRetryTopic(topic)) {
            return topic.substr(std::string(RETRY_GROUP_TOPIC_PREFIX).size());
        }
        if (isDlqTopic(topic)) {
            return topic.substr(std::string(DLQ_GROUP_TOPIC_PREFIX).size());
        }
        return topic;
    }

    // Local egress IP (probed with a UDP connect; falls back to gethostname -> 127.0.0.1 when there is no external network)
    static std::string getIpStr();
    // Local IP probed only once per process: fixed after the first probe, so the clientId of a client stays stable
    // (re-probing every time is both slower and, after a NIC change, could give a restarting client a different clientId).
    static const std::string& cachedIpStr();
    static int32_t pid();

    // ---------------- Properties <-> String ----------------
    // Protocol MixAll.properties2String / string2Properties.
    //
    // This is the easiest trap on the admin side: **the GET_BROKER_CONFIG response body is properties text**
    // (each line "key=value"), not JSON and not a KVTable. Parsing it with a JSON parser must fail.
    //
    // Note: PropertyMap is a std::map, naturally ordered by key, so the output order is deterministic;
    // the broker parses line by line, so the order carries no semantics.
    static std::string properties2String(const PropertyMap& properties);
    static PropertyMap string2Properties(const std::string& text);
};

// QueryMsgType: the three modes of querying messages by key.
// Not the same thing as MixAll::UNIQUE_MSG_QUERY_FLAG (an extFields key name).
struct QueryMsgType {
    static constexpr int32_t ALL_MESSAGE = 0;
    static constexpr int32_t UNIQUE_KEY = 1;
    static constexpr int32_t NORMAL = 2;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_MIX_ALL_H
