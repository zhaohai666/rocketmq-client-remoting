// org.apache.rocketmq.common.UtilAll 的 C++ 对应：通用工具。
//
// 关键点：bytes2string 必须输出**大写**十六进制（Java HEX_ARRAY = "0123456789ABCDEF"），
// msgId 依赖该大小写；string2bytes 是十六进制解码，不是 UTF-8 编码。
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

    // Java UtilAll.timeMillisToHumanString3：本地时区的 14 位 "yyyyMMddHHmmss"，
    // consumeTimestamp 的默认值与展示都靠它。
    static std::string timeMillisToHumanString3(int64_t ts);

    static bool isBlank(const std::string& s);
    static bool isNotBlank(const std::string& s);

    static int32_t pid();

    static bool isIpv4(const std::string& addr);

    // IP 字符串 -> 4B(v4) / 16B(v6) 网络序字节
    static bool ipToBytes(const std::string& ip, bool v6, Bytes& out);

    // 网络序字节 -> IP 字符串（v4: 4B, v6: 16B）
    static bool bytesToIp(const Bytes& raw, std::string& ip);

    // Java UtilAll.crc32：标准 CRC32（poly 0xEDB88320）
    static uint32_t crc32(const Bytes& data);

    static int32_t charToByte(char c);

    // 逐字节转大写十六进制
    static std::string bytes2String(const Bytes& bs);

    // 十六进制字符串 -> 字节串；非法字符返回空串
    static Bytes string2Bytes(const std::string& hexString);

    static std::string localIp();

    // Java System.getProperty("user.home")：POSIX 取 HOME，Windows 取 USERPROFILE。
    // 只读 HOME 的话，Windows 上日志与本地位点文件会静默落不到用户目录。
    static std::string userHome();

    // 跨平台的 warmup/pretty 名字
    static std::string nextMillisString();
};

// MessageClientIDSetter 的 C++ 对应：IP(4|16B) + PID(2B) + hash(4B) + 当日毫秒(4B) + 自增(2B)
struct InnerIdGenerator {
    static std::string createUniqId();
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_UTIL_ALL_H
