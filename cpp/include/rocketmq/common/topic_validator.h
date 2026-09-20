// org.apache.rocketmq.common.topic.TopicValidator 的 C++ 对应：
// topic / group 名字的合法性判定（字符表、长度上限、系统 topic 名单）。
//
// 与 Python 参考实现（python/rocketmq/common/topic_validator.py）逐条对齐：
// Java 的字符表白名单是 ^[%|a-zA-Z0-9_-]+$，实现方式是一张 128 长的
// VALID_CHAR_BIT_MAP——**码点 >= 128 一律非法**。这里按字节实现（UTF-8 的非 ASCII
// 字节必然 >= 0x80，因此与 Java 按 char 判定在真实输入上等价）。客户端放行而
// broker 拒绝只会把错误推迟到发送/建 topic 那一刻，所以本地必须先把关。
//
// ⚠ 与 Java 的差异（有意为之）：Java TopicValidator 还有 validateTopic/validateGroup
// 这类返回 ValidateResult 的管理端入口，本仓库的三语言端口都未移植（客户端只用
// 抛异常式 Validators），这里保持同一口径，只移植三个判定函数。
#ifndef ROCKETMQ_COMMON_TOPIC_VALIDATOR_H
#define ROCKETMQ_COMMON_TOPIC_VALIDATOR_H

#include <cstdint>
#include <set>
#include <string>

namespace rocketmq {

struct TopicValidator {
    // 对应 Java TopicValidator 的三档长度上限。group 名要参与拼
    // %RETRY%group_topic / %DLQ%group_topic，所以比 topic 更短。
    static constexpr int32_t TOPIC_MAX_LENGTH = 127;
    static constexpr int32_t GROUP_MAX_LENGTH = 120;
    static constexpr int32_t RETRY_OR_DLQ_TOPIC_MAX_LENGTH = 255;

    // 对应 Java 注释里的 regex（判定本身用查表，不用正则，与 Java 一致）
    static constexpr const char* VALID_CHAR_PATTERN = "^[%|a-zA-Z0-9_-]+$";

    // ---------------- 系统 topic 名单（与 Java/Python 逐一对应） ----------------
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

    // 字符表判定：**空串返回 false**（空/纯空白由 UtilAll::isBlank 单独管，
    // 与 Python is_topic_or_group_illegal 的口径一致）。
    static bool isTopicOrGroupIllegal(const std::string& name);

    // 命中系统 topic 名单或 rmq_sys_ 前缀
    static bool isSystemTopic(const std::string& topic);

    // 客户端**不能直接发**的 topic：这几个是 broker 内部状态流水（半消息、延迟、
    // 轨迹校验…），用户发进去会污染 broker 的事务/延迟/校验逻辑。
    // ⚠ %RETRY% 前缀 topic **不在**名单里：sendMessageBack 就是往 %RETRY%group 写的。
    static bool isNotAllowedSendTopic(const std::string& topic);

    // 名单本体（对应 Java getSystemTopicSet / getNotAllowedSendTopicSet）。
    // 用函数内 static 暴露，避免头文件里放可改状态的容器。
    static const std::set<std::string>& systemTopicSet();
    static const std::set<std::string>& notAllowedSendTopicSet();
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_TOPIC_VALIDATOR_H
