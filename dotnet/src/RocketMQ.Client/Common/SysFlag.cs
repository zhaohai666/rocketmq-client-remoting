// org.apache.rocketmq.common.sysflag.* 的 C# 对应：消息/拉取标志位与权限位。
//
// 位含义（低 -> 高），与 RocketMQ 4.9.x Java 实现逐位核对：
//   bit0  COMPRESSED
//   bit1  MULTI_TAGS
//   bit2  TRANSACTION_PREPARED
//   bit2~3 TRANSACTION_COMMIT(0x2&lt;&lt;2) / TRANSACTION_ROLLBACK(0x3&lt;&lt;2) 共用掩码
//   bit4  BORNHOST_V6
//   bit5  STOREHOSTADDRESS_V6
//   bit6  NEED_UNWRAP
//   bit7  INNER_BATCH
//   bit8~10 COMPRESSION_TYPE（掩码 0x7 &lt;&lt; 8）
namespace RocketMQ.Common;

public static class MessageSysFlag
{
    public const int CompressedFlag = 0x1;
    public const int MultiTagsFlag = 0x1 << 1;
    public const int TransactionNotType = 0;
    public const int TransactionPreparedType = 0x1 << 2;
    public const int TransactionCommitType = 0x2 << 2;
    public const int TransactionRollbackType = 0x3 << 2;
    public const int BornhostV6Flag = 0x1 << 4;
    public const int StorehostaddressV6Flag = 0x1 << 5;
    public const int NeedUnwrapFlag = 0x1 << 6;
    public const int InnerBatchFlag = 0x1 << 7;

    public const int CompressionLz4Type = 0x1 << 8;
    public const int CompressionZstdType = 0x2 << 8;
    public const int CompressionZlibType = 0x3 << 8;
    public const int CompressionTypeComparator = 0x7 << 8;
    public const int CompressionTypeShift = 8;

    // compression_type 数值（getCompressionType 的返回值）
    public const int Lz4Type = 1;
    public const int ZstdType = 2;
    public const int ZlibType = 3;
    public const int SnappyType = 4;

    /// <summary>Java: (flag &amp; COMPRESSION_TYPE_COMPARATOR) &gt;&gt; 8</summary>
    public static int GetCompressionType(int sysFlag) =>
        (sysFlag & CompressionTypeComparator) >> CompressionTypeShift;

    public static int SetCompressionType(int sysFlag, int compressionType) =>
        (sysFlag & ~CompressionTypeComparator)
        | ((compressionType << CompressionTypeShift) & CompressionTypeComparator);

    public static bool IsCompressed(int sysFlag) => (sysFlag & CompressedFlag) == CompressedFlag;

    public static int ClearCompressedFlag(int sysFlag) => sysFlag & ~CompressedFlag;

    /// <summary>Java: flag &amp; TRANSACTION_ROLLBACK_TYPE</summary>
    public static int GetTransactionValue(int flag) => flag & TransactionRollbackType;

    public static int ResetTransactionValue(int flag, int transactionType) =>
        (flag & ~TransactionRollbackType) | transactionType;

    public static bool Check(int flag, int expectedFlag) => (flag & expectedFlag) != 0;
}

public static class PullSysFlag
{
    public const int FlagCommitOffset = 0x1;
    public const int FlagSuspend = 0x1 << 1;
    public const int FlagSubscription = 0x1 << 2;
    public const int FlagClassFilter = 0x1 << 3;
    public const int FlagLitePullMessage = 0x1 << 4;
    public const int FlagProxyBlock = 0x1 << 5;
    public const int FlagExtBrokerGroup = 0x1 << 6;
    public const int FlagInnerSql = 0x1 << 7;
    public const int FlagMultiTag = 0x1 << 8;
    public const int FlagStartOffset = 0x1 << 9;

    public static int BuildSysFlag(
        bool commitOffset, bool suspend, bool subscription, bool classFilter, bool litePull = false)
    {
        int flag = 0;
        if (commitOffset)
        {
            flag |= FlagCommitOffset;
        }

        if (suspend)
        {
            flag |= FlagSuspend;
        }

        if (subscription)
        {
            flag |= FlagSubscription;
        }

        if (classFilter)
        {
            flag |= FlagClassFilter;
        }

        if (litePull)
        {
            flag |= FlagLitePullMessage;
        }

        return flag;
    }

    public static int ClearCommitOffsetFlag(int sysFlag) => sysFlag & ~FlagCommitOffset;

    public static bool HasCommitOffsetFlag(int sysFlag) =>
        (sysFlag & FlagCommitOffset) == FlagCommitOffset;

    public static bool HasSuspendFlag(int sysFlag) => (sysFlag & FlagSuspend) == FlagSuspend;

    public static int ClearSuspendFlag(int sysFlag) => sysFlag & ~FlagSuspend;

    public static bool HasSubscriptionFlag(int sysFlag) =>
        (sysFlag & FlagSubscription) == FlagSubscription;

    public static int BuildSysFlagWithSubscription(int sysFlag) => sysFlag | FlagSubscription;

    public static bool HasClassFilterFlag(int sysFlag) =>
        (sysFlag & FlagClassFilter) == FlagClassFilter;

    public static bool HasLitePullFlag(int sysFlag) =>
        (sysFlag & FlagLitePullMessage) == FlagLitePullMessage;
}

public static class PermName
{
    public const int PermPriority = 0x1 << 3;
    public const int PermRead = 0x4;
    public const int PermWrite = 0x2;
    public const int PermInherit = 0x1;
    public const int PermOwner = 0x1 << 4;

    /// <summary>
    /// 对应 Java PermName.permToString：三位标志 "RWX"，不满足的位写 '-'。
    /// （C++ 侧曾只声明未定义——「编译能过、一链接就炸」的坑。）
    /// </summary>
    public static string PermToString(int perm)
    {
        string s = string.Empty;
        s += (perm & PermRead) == PermRead ? 'R' : '-';
        s += (perm & PermWrite) == PermWrite ? 'W' : '-';
        s += (perm & PermInherit) == PermInherit ? 'X' : '-';
        return s;
    }

    public static bool CheckPerm(int perm, int wantedPerm) => (perm & wantedPerm) == wantedPerm;

    /// <summary>
    /// 对应 Java PermName.isValid(value)：合法区间为 [0, PERM_PRIORITY)。
    /// 管理端 UpdateBrokerConfig 用它校验 brokerPermission。
    /// </summary>
    public static bool IsValid(int value) => value >= 0 && value < PermPriority;
}

public static class SubscriptionMode
{
    public const int Group = 0;
    public const int Broadcasting = 1;
}
