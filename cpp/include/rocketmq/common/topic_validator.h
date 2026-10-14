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

    // Character-table check: **an empty string returns false** (empty/pure whitespace is handled separately by UtilAll::isBlank).
    static bool isTopicOrGroupIllegal(const std::string& name);

    // Hit the system topic list or the rmq_sys_ prefix
    static bool isSystemTopic(const std::string& topic);

    // Topics the client **must not send to directly**: these are broker-internal state streams (half
    // messages, delay, trace check-back, ...); sending into them would corrupt the broker's transaction/delay/check logic.
    // ⚠ %RETRY% prefixed topics are **not** in the list: sendMessageBack writes to %RETRY%group on purpose.
    static bool isNotAllowedSendTopic(const std::string& topic);

    static const std::set<std::string>& systemTopicSet();
    static const std::set<std::string>& notAllowedSendTopicSet();
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_TOPIC_VALIDATOR_H
