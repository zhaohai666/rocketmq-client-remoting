// org.apache.rocketmq.common.MixAll 的 C++ 对应：全局常量与静态工具。
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
    static constexpr const char* SCHEDULE_CONSUMER_GROUP = "SCHEDULE_CONSUMER";
    static constexpr const char* ONS_HTTP_PROXY_GROUP = "CID_ONSHTTP_PROXY";
    static constexpr const char* CID_ONSAPI_PERMISSION_GROUP = "CID_ONSAPI_PERMISSION";
    static constexpr const char* CID_ONSAPI_OWNER_GROUP = "CID_ONSAPI_OWNER";
    static constexpr const char* CID_ONSAPI_PULL_GROUP = "CID_ONSAPI_PULL";
    static constexpr const char* CID_SYS_RMQ_TRANS = "CID_SYS_RMQ_TRANS";

    // ⚠ Java 的 MixAll.UNIQUE_MSG_QUERY_FLAG 是 extFields 里的**键名**（取值 "true"/"false"），
    // 不是一个整数标志位。早期 C++ 实现把它当 1 用是错的：发给 broker 后 key 变成数字键，
    // uniqKey 查询路由不到 RocksDB 索引分支。按 key 查消息的三种模式见 QueryMsgType。
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

    // 对应 Java MixAll.isLmq（LMQ topic 以 %LMQ% 开头）
    static bool isLmq(const std::string& lmqMetaData) {
        return lmqMetaData.rfind(LMQ_PREFIX, 0) == 0;
    }

    // 对应 Java MixAll.isSysConsumerGroup（CID_RMQ_SYS_ 前缀）
    static bool isSysConsumerGroup(const std::string& consumerGroup) {
        return consumerGroup.rfind(CID_RMQ_SYS_PREFIX, 0) == 0;
    }

    // 对应 Java MixAll.isPredefinedGroup（PREDEFINE_GROUP_SET 命中）
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

    // 本机出口 IP（UDP connect 探测，无外网回落 gethostname -> 127.0.0.1）
    static std::string getIpStr();
    static int32_t pid();

    // ---------------- Properties <-> String ----------------
    // 对应 Java MixAll.properties2String / string2Properties。
    //
    // 这是管理端最容易踩的坑：**GET_BROKER_CONFIG 的响应体是 properties 文本**
    // （每行 "key=value"），不是 JSON、也不是 KVTable。用 JSON 解析器去解必然失败。
    //
    // 注：PropertyMap 是 std::map，天然按 key 有序，因此输出顺序确定（比 Java 的
    // HashMap 遍历顺序更稳定）；broker 侧按行解析，顺序不影响语义。
    static std::string properties2String(const PropertyMap& properties);
    static PropertyMap string2Properties(const std::string& text);
};

// 对应 Java QueryMsgByKeySubCommand.QueryMsgType：按 key 查消息的三种模式。
// 与 MixAll::UNIQUE_MSG_QUERY_FLAG（extFields 键名）不是一回事。
struct QueryMsgType {
    static constexpr int32_t ALL_MESSAGE = 0;
    static constexpr int32_t UNIQUE_KEY = 1;
    static constexpr int32_t NORMAL = 2;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_MIX_ALL_H
