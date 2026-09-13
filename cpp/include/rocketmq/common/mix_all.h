// org.apache.rocketmq.common.MixAll 的 C++ 对应：全局常量与静态工具。
#ifndef ROCKETMQ_COMMON_MIX_ALL_H
#define ROCKETMQ_COMMON_MIX_ALL_H

#include <cstdint>
#include <string>

namespace rocketmq {

struct MixAll {
    static constexpr const char* NAMESRV_ADDR_PROPERTY = "rocketmq.namesrv.addr";
    static constexpr const char* NAMESRV_ADDR_ENV = "NAMESRV_ADDR";
    static constexpr const char* MESSAGE_COMPRESS_LEVEL = "rocketmq.message.compressLevel";
    static constexpr const char* DEFAULT_TOPIC = "TBW102";
    static constexpr const char* BENCHMARK_TOPIC = "BenchmarkTest";
    static constexpr const char* DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER";
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
    static constexpr const char* SYSTEM_TOPIC_PREFIX = "rmq_sys_";
    static constexpr const char* TOOLS_CONSUMER_GROUP = "TOOLS_CONSUMER";
    static constexpr const char* FILTERSRV_CONSUMER_GROUP = "FILTERSRV_CONSUMER";
    static constexpr const char* MONITOR_CONSUMER_GROUP = "__MONITOR_CONSUMER";
    static constexpr const char* CLIENT_INNER_CONSUMER_GROUP = "CLIENT_INNER_CONSUMER";
    static constexpr const char* ONS_NAMESPACE = "namespace";

    static constexpr int32_t ALL_MESSAGE_QUERY_FLAG = 0;
    static constexpr int32_t UNIQUE_MSG_QUERY_FLAG = 1;
    static constexpr int32_t NORMAL_MSG_QUERY_FLAG = 2;
    static constexpr const char* TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC";
    static constexpr const char* REAL_TRACE_TOPIC = "rmq_sys_TRACE_DATA";
    static constexpr const char* RMQ_SYS_TRANS_HALF_TOPIC = "RMQ_SYS_TRANS_HALF_TOPIC";
    static constexpr const char* RMQ_SYS_TRANS_OP_HALF_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    static constexpr int32_t TRANS_CHECK_MAX_TIME = 15;
    static constexpr const char* UNIT_PREFIX = "unit_";

    static constexpr int32_t DEFAULT_TOPIC_QUEUE_NUMS = 4;
    static constexpr int32_t DEFAULT_TOPIC_READ_QUEUE_NUMS = 4;
    static constexpr int32_t DEFAULT_TOPIC_WRITE_QUEUE_NUMS = 4;
    static constexpr int32_t MAX_TOPIC_LENGTH = 127;
    static constexpr int32_t MAX_GROUP_LENGTH = 255;
    static constexpr int32_t CHARACTER_MAX_LENGTH = 255;

    static constexpr int32_t MASTER_ID = 0;
    static constexpr int32_t READ_PERM_BY_DEFAULT = 4 | 2;  // PERM_READ | PERM_WRITE

    static std::string getRetryTopic(const std::string& consumerGroup) {
        return std::string(RETRY_GROUP_TOPIC_PREFIX) + consumerGroup;
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

    static std::string getReplyTopic(const std::string& topic) {
        return std::string(REPLY_TOPIC_PREFIX) + topic;
    }

    static bool isSysTopic(const std::string& topic) {
        return topic.rfind(SYSTEM_TOPIC_PREFIX, 0) == 0;
    }

    static std::string resetRetryAndDlqTopic(const std::string& topic) {
        if (isRetryTopic(topic)) {
            return topic.substr(std::string(RETRY_GROUP_TOPIC_PREFIX).size());
        }
        if (isDlqTopic(topic)) {
            return topic.substr(std::string(DLQ_GROUP_TOPIC_PREFIX).size());
        }
        return topic;
    }

    // 本机出口 IP（UDP connect 探测，无外网回落 gethostname -> 127.0.0.1）
    static std::string getIpStr();
    static int32_t pid();
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_MIX_ALL_H
