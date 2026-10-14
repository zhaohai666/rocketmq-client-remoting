#ifndef ROCKETMQ_COMMON_SYSFLAG_H
#define ROCKETMQ_COMMON_SYSFLAG_H

#include <cstdint>
#include <string>

namespace rocketmq {

struct MessageSysFlag {
    static constexpr int32_t COMPRESSED_FLAG = 0x1;
    static constexpr int32_t MULTI_TAGS_FLAG = 0x1 << 1;
    static constexpr int32_t TRANSACTION_NOT_TYPE = 0;
    static constexpr int32_t TRANSACTION_PREPARED_TYPE = 0x1 << 2;
    static constexpr int32_t TRANSACTION_COMMIT_TYPE = 0x2 << 2;
    static constexpr int32_t TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;
    static constexpr int32_t BORNHOST_V6_FLAG = 0x1 << 4;
    static constexpr int32_t STOREHOSTADDRESS_V6_FLAG = 0x1 << 5;
    static constexpr int32_t NEED_UNWRAP_FLAG = 0x1 << 6;
    static constexpr int32_t INNER_BATCH_FLAG = 0x1 << 7;

    static constexpr int32_t COMPRESSION_LZ4_TYPE = 0x1 << 8;
    static constexpr int32_t COMPRESSION_ZSTD_TYPE = 0x2 << 8;
    static constexpr int32_t COMPRESSION_ZLIB_TYPE = 0x3 << 8;
    static constexpr int32_t COMPRESSION_TYPE_COMPARATOR = 0x7 << 8;
    static constexpr int32_t COMPRESSION_TYPE_SHIFT = 8;

    // compression_type values (return value of getCompressionType)
    static constexpr int32_t LZ4_TYPE = 1;
    static constexpr int32_t ZSTD_TYPE = 2;
    static constexpr int32_t ZLIB_TYPE = 3;
    static constexpr int32_t SNAPPY_TYPE = 4;

    static int32_t getCompressionType(int32_t sysFlag) {
        return (sysFlag & COMPRESSION_TYPE_COMPARATOR) >> COMPRESSION_TYPE_SHIFT;
    }

    static int32_t setCompressionType(int32_t sysFlag, int32_t compressionType) {
        return (sysFlag & ~COMPRESSION_TYPE_COMPARATOR)
             | ((compressionType << COMPRESSION_TYPE_SHIFT) & COMPRESSION_TYPE_COMPARATOR);
    }

    static bool isCompressed(int32_t sysFlag) {
        return (sysFlag & COMPRESSED_FLAG) == COMPRESSED_FLAG;
    }

    static int32_t clearCompressedFlag(int32_t sysFlag) {
        return sysFlag & ~COMPRESSED_FLAG;
    }

    static int32_t getTransactionValue(int32_t flag) {
        return flag & TRANSACTION_ROLLBACK_TYPE;
    }

    static int32_t resetTransactionValue(int32_t flag, int32_t transactionType) {
        return (flag & ~TRANSACTION_ROLLBACK_TYPE) | transactionType;
    }

    static bool check(int32_t flag, int32_t expectedFlag) {
        return (flag & expectedFlag) != 0;
    }
};

struct PullSysFlag {
    static constexpr int32_t FLAG_COMMIT_OFFSET = 0x1;
    static constexpr int32_t FLAG_SUSPEND = 0x1 << 1;
    static constexpr int32_t FLAG_SUBSCRIPTION = 0x1 << 2;
    static constexpr int32_t FLAG_CLASS_FILTER = 0x1 << 3;
    static constexpr int32_t FLAG_LITE_PULL_MESSAGE = 0x1 << 4;
    static constexpr int32_t FLAG_PROXY_BLOCK = 0x1 << 5;
    static constexpr int32_t FLAG_EXT_BROKER_GROUP = 0x1 << 6;
    static constexpr int32_t FLAG_INNER_SQL = 0x1 << 7;
    static constexpr int32_t FLAG_MULTI_TAG = 0x1 << 8;
    static constexpr int32_t FLAG_START_OFFSET = 0x1 << 9;

    static int32_t buildSysFlag(bool commitOffset, bool suspend, bool subscription,
                                bool classFilter, bool litePull = false) {
        int32_t flag = 0;
        if (commitOffset) flag |= FLAG_COMMIT_OFFSET;
        if (suspend) flag |= FLAG_SUSPEND;
        if (subscription) flag |= FLAG_SUBSCRIPTION;
        if (classFilter) flag |= FLAG_CLASS_FILTER;
        if (litePull) flag |= FLAG_LITE_PULL_MESSAGE;
        return flag;
    }

    static int32_t clearCommitOffsetFlag(int32_t sysFlag) { return sysFlag & ~FLAG_COMMIT_OFFSET; }

    static bool hasCommitOffsetFlag(int32_t sysFlag) {
        return (sysFlag & FLAG_COMMIT_OFFSET) == FLAG_COMMIT_OFFSET;
    }

    static bool hasSuspendFlag(int32_t sysFlag) {
        return (sysFlag & FLAG_SUSPEND) == FLAG_SUSPEND;
    }

    static int32_t clearSuspendFlag(int32_t sysFlag) { return sysFlag & ~FLAG_SUSPEND; }

    static bool hasSubscriptionFlag(int32_t sysFlag) {
        return (sysFlag & FLAG_SUBSCRIPTION) == FLAG_SUBSCRIPTION;
    }

    static int32_t buildSysFlagWithSubscription(int32_t sysFlag) {
        return sysFlag | FLAG_SUBSCRIPTION;
    }

    static bool hasClassFilterFlag(int32_t sysFlag) {
        return (sysFlag & FLAG_CLASS_FILTER) == FLAG_CLASS_FILTER;
    }

    static bool hasLitePullFlag(int32_t sysFlag) {
        return (sysFlag & FLAG_LITE_PULL_MESSAGE) == FLAG_LITE_PULL_MESSAGE;
    }
};

struct PermName {
    static constexpr int32_t PERM_PRIORITY = 0x1 << 3;
    static constexpr int32_t PERM_READ = 0x4;
    static constexpr int32_t PERM_WRITE = 0x2;
    static constexpr int32_t PERM_INHERIT = 0x1;
    static constexpr int32_t PERM_OWNER = 0x1 << 4;

    static std::string permToString(int32_t perm) {
        std::string s;
        s += (perm & PERM_READ) == PERM_READ ? 'R' : '-';
        s += (perm & PERM_WRITE) == PERM_WRITE ? 'W' : '-';
        s += (perm & PERM_INHERIT) == PERM_INHERIT ? 'X' : '-';
        return s;
    }

    static bool checkPerm(int32_t perm, int32_t wantedPerm) {
        return (perm & wantedPerm) == wantedPerm;
    }

    static bool isValid(int32_t value) { return value >= 0 && value < PERM_PRIORITY; }
};

struct SubscriptionMode {
    static constexpr int32_t GROUP = 0;
    static constexpr int32_t BROADCASTING = 1;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_SYSFLAG_H
