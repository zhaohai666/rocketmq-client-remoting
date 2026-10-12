#include "rocketmq/common/topic_validator.h"

#include <array>
#include <cstddef>

namespace rocketmq {

namespace {

const std::array<bool, 256>& validCharTable() {
    static const std::array<bool, 256> kTable = [] {
        std::array<bool, 256> table{};
        table[static_cast<unsigned char>('%')] = true;
        table[static_cast<unsigned char>('-')] = true;
        table[static_cast<unsigned char>('_')] = true;
        table[static_cast<unsigned char>('|')] = true;
        for (int c = '0'; c <= '9'; ++c) table[static_cast<std::size_t>(c)] = true;
        for (int c = 'A'; c <= 'Z'; ++c) table[static_cast<std::size_t>(c)] = true;
        for (int c = 'a'; c <= 'z'; ++c) table[static_cast<std::size_t>(c)] = true;
        return table;
    }();
    return kTable;
}

}  // namespace

bool TopicValidator::isTopicOrGroupIllegal(const std::string& name) {
    const std::array<bool, 256>& valid = validCharTable();
    for (char c : name) {
        const unsigned char ch = static_cast<unsigned char>(c);
        if (ch >= 128 || !valid[ch]) {
            return true;
        }
    }
    return false;
}

bool TopicValidator::isSystemTopic(const std::string& topic) {
    return systemTopicSet().count(topic) > 0 ||
           topic.rfind(SYSTEM_TOPIC_PREFIX, 0) == 0;
}

bool TopicValidator::isNotAllowedSendTopic(const std::string& topic) {
    return notAllowedSendTopicSet().count(topic) > 0;
}

const std::set<std::string>& TopicValidator::systemTopicSet() {
    static const std::set<std::string> kSystemTopicSet = {
        AUTO_CREATE_TOPIC_KEY_TOPIC,
        RMQ_SYS_SCHEDULE_TOPIC,
        RMQ_SYS_BENCHMARK_TOPIC,
        RMQ_SYS_TRANS_HALF_TOPIC,
        RMQ_SYS_TRACE_TOPIC,
        RMQ_SYS_TRANS_OP_HALF_TOPIC,
        RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
        RMQ_SYS_SELF_TEST_TOPIC,
        RMQ_SYS_OFFSET_MOVED_EVENT,
        RMQ_SYS_ROCKSDB_OFFSET_TOPIC,
        RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
        RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
    };
    return kSystemTopicSet;
}

const std::set<std::string>& TopicValidator::notAllowedSendTopicSet() {
    static const std::set<std::string> kNotAllowedSendTopicSet = {
        RMQ_SYS_SCHEDULE_TOPIC,
        RMQ_SYS_TRANS_HALF_TOPIC,
        RMQ_SYS_TRANS_OP_HALF_TOPIC,
        RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
        RMQ_SYS_SELF_TEST_TOPIC,
        RMQ_SYS_OFFSET_MOVED_EVENT,
        RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
        RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
    };
    return kNotAllowedSendTopicSet;
}

}  // namespace rocketmq
