// 时间戳查位点的边界语义（对应 org.apache.rocketmq.common.BoundaryType，
// 参考实现 python/rocketmq/common/boundary_type.py）。
//
// Java 锚点：
//   * 枚举声明 LOWER("lower") / UPPER("upper")（BoundaryType.java:23/:28），
//     但入网的值是 Enum.toString()（RemotingCommand.makeCustomHeaderToNet 的
//     value.toString()）—— 也就是**大写枚举名** "LOWER"/"UPPER"；
//     getName() 那个小写名只喂给 getType 做解析，从不上报文。
//   * getType(String):41 是**宽松**解析：只有 "upper".equalsIgnoreCase(name)
//     才回 UPPER，其余（null / 空串 / 未知值 / 小写 "lower"）一律 LOWER。
//     broker 侧解 SearchOffsetRequestHeader.boundaryType 走的就是它。
#ifndef ROCKETMQ_COMMON_BOUNDARY_TYPE_H
#define ROCKETMQ_COMMON_BOUNDARY_TYPE_H

#include <string>

namespace rocketmq {

/// 对应 Java BoundaryType。
enum class BoundaryType {
    LOWER,
    UPPER,
};

/// Java Enum.name()（BoundaryType 没覆写 toString()，两者同值）—— 入网文本。
inline std::string boundaryTypeName(BoundaryType t) {
    return t == BoundaryType::UPPER ? "UPPER" : "LOWER";
}

/// Java BoundaryType.getName()：小写名，只用于比对与日志。
inline std::string boundaryTypeLowercaseName(BoundaryType t) {
    return t == BoundaryType::UPPER ? "upper" : "lower";
}

/// Java BoundaryType.getType(String)：非 "upper"（大小写不敏感）一律 LOWER。
inline BoundaryType boundaryTypeFromString(const std::string& name) {
    if (name.size() == 5) {
        static const char kUpper[] = "upper";
        bool same = true;
        for (std::size_t i = 0; i < 5; ++i) {
            char c = name[i];
            if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
            if (c != kUpper[i]) {
                same = false;
                break;
            }
        }
        if (same) return BoundaryType::UPPER;
    }
    return BoundaryType::LOWER;
}

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_BOUNDARY_TYPE_H
