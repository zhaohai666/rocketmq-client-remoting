namespace RocketMQ.Common;

/// <summary>
/// Character table and reserved-name checks for topic / group names.
public static class TopicValidator
{
    public const int TopicMaxLength = 127;

    /// <summary>
    /// The group name is used to build %RETRY%group_topic / %DLQ%group_topic, so it is shorter than the topic limit.
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
    /// Topics the client <b>must not send to directly</b>: these are the broker's internal state streams (half messages, delays, trace checks, ...),
    /// and if a user sends into them it pollutes the broker's transaction/delay/check logic.
    /// Note the <c>%RETRY%</c> prefix is <b>not</b> in this list — sendMessageBack writes to %RETRY%group.
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
