namespace RocketMQ.Common;

public enum BoundaryType
{
    Lower = 0,
    Upper = 1,
}

public static class BoundaryTypeNames
{
    public static string Name(BoundaryType t) => t == BoundaryType.Upper ? "UPPER" : "LOWER";

    public static string LowercaseName(BoundaryType t) => t == BoundaryType.Upper ? "upper" : "lower";

    public static BoundaryType GetType(string? name) =>
        string.Equals(name, "upper", StringComparison.OrdinalIgnoreCase)
            ? BoundaryType.Upper
            : BoundaryType.Lower;
}
