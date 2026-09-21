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
    // Java `ClientConfig#instanceName` 的默认值（`System.getProperty("rocketmq.client.name", "DEFAULT")`）
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
    // Request-Reply（5.x）：应答 topic 名 = <clusterName>_REPLY_TOPIC。
    // ⚠ 注意与上面的 REPLY_TOPIC_PREFIX("%REPLY%") 不是一回事：后者是旧版
    // 「消费者把消息回投到自己的 %REPLY%<topic>」的约定，5.x 的 request-reply 用的是下面这个。
    static constexpr const char* REPLY_TOPIC_POSTFIX = "REPLY_TOPIC";
    // 应答消息的 MSG_TYPE 属性值（Java MixAll.REPLY_MESSAGE_FLAG）
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
    // 对应 Java `MixAll.REQ_T`：请求类型标记的 extFields 键，由 StreamTypeRPCHook 写入。
    // broker 侧只有 proxy/stream 链路读它，普通 broker 忽略。
    static constexpr const char* REQ_T = "ReqT";
    // Java 拼 clientId 用的是 `RequestType.STREAM.name()`，即字面量 "STREAM"
    // （⚠ 与 extFields 里那个**枚举 code** 不同，见 StreamTypeRPCHook）。
    static constexpr const char* STREAM_REQUEST_TYPE = "STREAM";

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

    // 对应 Java MixAll.getReplyTopic(clusterName) = clusterName + "_REPLY_TOPIC"。
    // 该 topic 由 broker 在启动时注册为**系统 topic**（TopicConfigManager.init），
    // 客户端不可 createTopic 建它（会被 INVALID_PARAMETER「conflict with system topic」拒）。
    static std::string getReplyTopic(const std::string& clusterName) {
        return clusterName + "_" + REPLY_TOPIC_POSTFIX;
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
    // 进程内只探测一次的本机 IP。对应 Java `ClientConfig#clientIP`：它在 ClientConfig
    // 构造时就定下来，同一个客户端的 clientId 因此稳定（每次重新探测既慢，又可能在
    // 换网卡后让重启的客户端换一个 clientId）。
    static const std::string& cachedIpStr();
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
