// org.apache.rocketmq.common.topic.TopicValidator 的 C# 对应：topic / group 名字合法性判定。
namespace RocketMQ.Common;

/// <summary>
/// topic / group 名字的字符表与保留名判定。
///
/// Java 的字符表白名单是 <c>^[%|a-zA-Z0-9_-]+$</c>，实现方式是一张 128 长的
/// <c>VALID_CHAR_BIT_MAP</c>：<b>码点 &gt;= 128 一律非法</b>。这里逐字节照抄，因为 broker 侧
/// （<c>TopicConfigValidator</c>）用的是同一张表，客户端放行而 broker 拒绝只会把错误推迟到
/// 建 topic 那一刻，而名字是<b>发送路径</b>上就该定的东西。
/// </summary>
public static class TopicValidator
{
    public const int TopicMaxLength = 127;

    /// <summary>
    /// group 名要参与拼 %RETRY%group_topic / %DLQ%group_topic，所以比 topic 更短。
    /// </summary>
    public const int GroupMaxLength = 120;

    /// <summary>仅 TopicValidator.ValidateTopic 使用，checkTopic 不用（Java 同）。</summary>
    public const int RetryOrDlqTopicMaxLength = 255;

    /// <summary>Java 报错文案里引用的正则原文。</summary>
    public const string ValidCharPattern = "^[%|a-zA-Z0-9_-]+$";

    public const string AutoCreateTopicKeyTopic = "TBW102";
    public const string RmqSysScheduleTopic = "SCHEDULE_TOPIC_XXXX";
    public const string RmqSysBenchmarkTopic = "BenchmarkTest";
    public const string RmqSysTransHalfTopic = "RMQ_SYS_TRANS_HALF_TOPIC";
    public const string RmqSysRocksdbTransHalfTopic = "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC";
    public const string RmqSysTraceTopic = "RMQ_SYS_TRACE_TOPIC";
    public const string RmqSysTransOpHalfTopic = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    public const string RmqSysRocksdbTransOpHalfTopic = "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC";
    public const string RmqSysTransCheckMaxTimeTopic = "TRANS_CHECK_MAX_TIME_TOPIC";
    public const string RmqSysSelfTestTopic = "SELF_TEST_TOPIC";
    public const string RmqSysOffsetMovedEvent = "OFFSET_MOVED_EVENT";
    public const string RmqSysRocksdbOffsetTopic = "CHECKPOINT_TOPIC";

    public const string SystemTopicPrefix = "rmq_sys_";

    public static readonly HashSet<string> SystemTopicSet = new()
    {
        AutoCreateTopicKeyTopic,
        RmqSysScheduleTopic,
        RmqSysBenchmarkTopic,
        RmqSysTransHalfTopic,
        RmqSysTraceTopic,
        RmqSysTransOpHalfTopic,
        RmqSysTransCheckMaxTimeTopic,
        RmqSysSelfTestTopic,
        RmqSysOffsetMovedEvent,
        RmqSysRocksdbOffsetTopic,
        RmqSysRocksdbTransHalfTopic,
        RmqSysRocksdbTransOpHalfTopic,
    };

    /// <summary>
    /// 客户端<b>不能直接发</b>的 topic：这几个是 broker 内部状态流水（半消息、延迟、轨迹校验……），
    /// 用户发进去会污染 broker 的事务/延迟/校验逻辑。
    /// 注意 <c>%RETRY%</c> 前缀<b>不在</b>此列 —— sendMessageBack 就是往 %RETRY%group 写的。
    /// </summary>
    public static readonly HashSet<string> NotAllowedSendTopicSet = new()
    {
        RmqSysScheduleTopic,
        RmqSysTransHalfTopic,
        RmqSysTransOpHalfTopic,
        RmqSysTransCheckMaxTimeTopic,
        RmqSysSelfTestTopic,
        RmqSysOffsetMovedEvent,
        RmqSysRocksdbTransHalfTopic,
        RmqSysRocksdbTransOpHalfTopic,
    };

    /// <summary>Java 的 VALID_CHAR_BIT_MAP 展开成集合：ASCII 数字、大小写字母和 % - _ |。</summary>
    private static bool AllowedChar(char c) =>
        c is >= '0' and <= '9' or >= 'a' and <= 'z' or >= 'A' and <= 'Z'
        or '%' or '-' or '_' or '|';

    /// <summary>
    /// 对应 <c>TopicValidator.isTopicOrGroupIllegal</c>。
    /// 空串返回 false —— 空由 checkTopic/checkGroup 的 isBlank 那一步管，与 Java 一致。
    /// </summary>
    public static bool IsTopicOrGroupIllegal(string? name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return false;
        }

        foreach (char c in name)
        {
            // Java 的位图只有 128 位：非 ASCII 码点一律非法，不做 Unicode 归类。
            if (c >= 128 || !AllowedChar(c))
            {
                return true;
            }
        }

        return false;
    }

    public static bool IsSystemTopic(string topic) =>
        SystemTopicSet.Contains(topic) || topic.StartsWith(SystemTopicPrefix, StringComparison.Ordinal);

    public static bool IsNotAllowedSendTopic(string topic) => NotAllowedSendTopicSet.Contains(topic);
}
