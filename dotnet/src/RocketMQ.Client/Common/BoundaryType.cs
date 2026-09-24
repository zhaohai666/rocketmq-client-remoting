namespace RocketMQ.Common;

/// <summary>
/// 时间戳查位点的边界语义（对应 Java org.apache.rocketmq.common.BoundaryType，
/// 参考实现 python/rocketmq/common/boundary_type.py）。
///
/// Java 锚点：
///   * 枚举声明 LOWER("lower") / UPPER("upper")（BoundaryType.java:23/:28），
///     但入网的值是 Enum.toString()（RemotingCommand.makeCustomHeaderToNet 的
///     value.toString()）—— 也就是**大写枚举名** "LOWER"/"UPPER"；
///     getName() 那个小写名只喂给 getType 做解析，从不上报文。
///   * getType(String):41 是**宽松**解析：只有 "upper".equalsIgnoreCase(name)
///     才回 UPPER，其余（null / 空串 / 未知值 / 小写 "lower"）一律 LOWER。
///     broker 侧解 SearchOffsetRequestHeader.boundaryType 走的就是它。
/// </summary>
public enum BoundaryType
{
    Lower = 0,
    Upper = 1,
}

public static class BoundaryTypeNames
{
    /// <summary>Java Enum.name()（BoundaryType 没覆写 toString()，两者同值）—— 入网文本。</summary>
    public static string Name(BoundaryType t) => t == BoundaryType.Upper ? "UPPER" : "LOWER";

    /// <summary>Java BoundaryType.getName()：小写名，只用于比对与日志。</summary>
    public static string LowercaseName(BoundaryType t) => t == BoundaryType.Upper ? "upper" : "lower";

    /// <summary>Java BoundaryType.getType(String)：非 "upper"（大小写不敏感）一律 LOWER。</summary>
    public static BoundaryType GetType(string? name) =>
        string.Equals(name, "upper", StringComparison.OrdinalIgnoreCase)
            ? BoundaryType.Upper
            : BoundaryType.Lower;
}
