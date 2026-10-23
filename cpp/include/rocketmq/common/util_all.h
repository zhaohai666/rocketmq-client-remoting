#ifndef ROCKETMQ_COMMON_UTIL_ALL_H
#define ROCKETMQ_COMMON_UTIL_ALL_H

#include <cstdint>
#include <string>

#include "rocketmq/common/byte_buffer.h"

namespace rocketmq {

struct UtilAll {
    static constexpr const char* YYYY_MM_DD_HH_MM_SS = "%Y-%m-%d %H:%M:%S";
    static constexpr const char* YYYY_MM_DD_HH_MM_SS_SSS = "%Y-%m-%d %H:%M:%S.%03d";

    static constexpr const char* HEX_ARRAY = "0123456789ABCDEF";

    static int64_t currentTimeMillis();
    static int64_t currentTimeSeconds();
    static std::string offset2FileName(int64_t offset);
    static int64_t computeElapseTimeMillis(int64_t lastTime);
    static std::string timeToHumanString(int64_t ts, const std::string& pattern = YYYY_MM_DD_HH_MM_SS);

    static std::string timeMillisToHumanString3(int64_t ts);

    // The inverse of timeMillisToHumanString3. A malformed value is a hard error: a consumer that
    // silently fell back to "now" would skip the backlog it was configured to replay.
    static int64_t parseConsumeTimestamp(const std::string& ts);

    static bool isBlank(const std::string& s);
    static bool isNotBlank(const std::string& s);

    static int32_t pid();

    static bool isIpv4(const std::string& addr);

    // IP string -> 4B(v4) / 16B(v6) bytes in network order
    static bool ipToBytes(const std::string& ip, bool v6, Bytes& out);

    // Network-order bytes -> IP string (v4: 4B, v6: 16B)
    static bool bytesToIp(const Bytes& raw, std::string& ip);

    // Value the store writes into the BODYCRC field: CRC-32 with the sign bit dropped.
    // Comparing a full 32-bit CRC against that field disagrees for roughly half of all bodies,
    // i.e. every body whose CRC has bit 31 set, so such an opt-in check rejects valid messages.
    static uint32_t crc32(const Bytes& data);

    static int32_t charToByte(char c);

    // Byte-by-byte conversion to upper-case hexadecimal
    static std::string bytes2String(const Bytes& bs);

    // Hexadecimal string -> byte string; returns an empty string on invalid characters
    static Bytes string2Bytes(const std::string& hexString);

    static std::string localIp();

    static std::string userHome();

    // Cross-platform warmup/pretty name
    static std::string nextMillisString();
};

// Uniq client message id: IP(4|16B) + PID(2B) + hash(4B) + millis of the current day(4B) + counter(2B)
struct InnerIdGenerator {
    static std::string createUniqId();
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_UTIL_ALL_H
