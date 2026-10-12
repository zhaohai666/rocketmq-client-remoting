namespace RocketMQ.Common;

/// <summary>
/// topic / group 名字的字符表与保留名判定。
public static class TopicValidator
{
    public const int TopicMaxLength = 127;

    /// <summary>
    /// group 名要参与拼 %RETRY%group_topic / %DLQ%group_topic，所以比 topic 更短。
    /// </summary>
    public const int GroupMaxLength = 120;

    public const int RetryOrDlqTopicMaxLength = 255;

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

    private static bool AllowedChar(char c) =>
        c is >= '0' and <= '9' or >= 'a' and <= 'z' or >= 'A' and <= 'Z'
        or '%' or '-' or '_' or '|';

    public static bool IsTopicOrGroupIllegal(string? name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return false;
        }

        foreach (char c in name)
        {
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
