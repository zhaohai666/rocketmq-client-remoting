#ifndef ROCKETMQ_COMMON_BOUNDARY_TYPE_H
#define ROCKETMQ_COMMON_BOUNDARY_TYPE_H

#include <cstddef>
#include <string>

namespace rocketmq {

enum class BoundaryType {
    LOWER,
    UPPER,
};

inline std::string boundaryTypeName(BoundaryType t) {
    return t == BoundaryType::UPPER ? "UPPER" : "LOWER";
}

inline std::string boundaryTypeLowercaseName(BoundaryType t) {
    return t == BoundaryType::UPPER ? "upper" : "lower";
}

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
