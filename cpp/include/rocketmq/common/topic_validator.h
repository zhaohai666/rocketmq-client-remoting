#ifndef ROCKETMQ_COMMON_TOPIC_VALIDATOR_H
#define ROCKETMQ_COMMON_TOPIC_VALIDATOR_H

#include <cstdint>
#include <set>
#include <string>

namespace rocketmq {

struct TopicValidator {
    static constexpr int32_t TOPIC_MAX_LENGTH = 127;
    static constexpr int32_t GROUP_MAX_LENGTH = 120;
    static constexpr int32_t RETRY_OR_DLQ_TOPIC_MAX_LENGTH = 255;

    static constexpr const char* VALID_CHAR_PATTERN = "^[%|a-zA-Z0-9_-]+$";

    static constexpr const char* AUTO_CREATE_TOPIC_KEY_TOPIC = "TBW102";
    static constexpr const char* RMQ_SYS_SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    static constexpr const char* RMQ_SYS_BENCHMARK_TOPIC = "BenchmarkTest";
    static constexpr const char* RMQ_SYS_TRANS_HALF_TOPIC = "RMQ_SYS_TRANS_HALF_TOPIC";
    static constexpr const char* RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC = "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC";
    static constexpr const char* RMQ_SYS_TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC";
    static constexpr const char* RMQ_SYS_TRANS_OP_HALF_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    static constexpr const char* RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC = "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC";
    static constexpr const char* RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC = "TRANS_CHECK_MAX_TIME_TOPIC";
    static constexpr const char* RMQ_SYS_SELF_TEST_TOPIC = "SELF_TEST_TOPIC";
    static constexpr const char* RMQ_SYS_OFFSET_MOVED_EVENT = "OFFSET_MOVED_EVENT";
    static constexpr const char* RMQ_SYS_ROCKSDB_OFFSET_TOPIC = "CHECKPOINT_TOPIC";

    static constexpr const char* SYSTEM_TOPIC_PREFIX = "rmq_sys_";

    // 字符表判定：**空串返回 false**（空/纯空白由 UtilAll::isBlank 单独管）。
    static bool isTopicOrGroupIllegal(const std::string& name);

    // 命中系统 topic 名单或 rmq_sys_ 前缀
    static bool isSystemTopic(const std::string& topic);

    // 客户端**不能直接发**的 topic：这几个是 broker 内部状态流水（半消息、延迟、
    // 轨迹校验…），用户发进去会污染 broker 的事务/延迟/校验逻辑。
    // ⚠ %RETRY% 前缀 topic **不在**名单里：sendMessageBack 就是往 %RETRY%group 写的。
    static bool isNotAllowedSendTopic(const std::string& topic);

    static const std::set<std::string>& systemTopicSet();
    static const std::set<std::string>& notAllowedSendTopicSet();
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_TOPIC_VALIDATOR_H
