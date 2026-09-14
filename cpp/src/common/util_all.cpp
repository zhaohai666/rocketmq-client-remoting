// org.apache.rocketmq.common.UtilAll 的 C++ 实现。
//
// 对齐 python/rocketmq/common/util_all.py：
//   - bytes2String 输出**大写**十六进制（msgId 依赖大小写）；
//   - string2Bytes 是十六进制解码（非 UTF-8），非法字符返回空串；
//   - crc32 为标准 CRC32（poly 0xEDB88320），与 Java/zlib 一致。
#include "rocketmq/common/util_all.h"

#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>

#include "rocketmq/common/net_compat.h"

#if defined(_WIN32)
#include <process.h>  // _getpid
#endif

namespace rocketmq {

namespace {

// 标准 CRC32 查表（poly 0xEDB88320）
struct Crc32Table {
    uint32_t t[256];
    Crc32Table() {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) {
                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }
            t[i] = c;
        }
    }
};

const Crc32Table& crcTable() {
    static const Crc32Table table;
    return table;
}

}  // namespace

int64_t UtilAll::currentTimeMillis() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

int64_t UtilAll::currentTimeSeconds() {
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

std::string UtilAll::offset2FileName(int64_t offset) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%020lld", static_cast<long long>(offset));
    return std::string(buf);
}

int64_t UtilAll::computeElapseTimeMillis(int64_t lastTime) {
    return currentTimeMillis() - lastTime;
}

std::string UtilAll::timeToHumanString(int64_t ts, const std::string& pattern) {
    if (ts <= 0) return "-";
    std::time_t secs = static_cast<std::time_t>(ts / 1000);
    int64_t ms = ts % 1000;
    if (ms < 0) ms = 0;
    std::tm tmv{};
#if defined(_WIN32)
    localtime_s(&tmv, &secs);
#else
    localtime_r(&secs, &tmv);
#endif
    std::string pat = pattern;
    std::string suffix;
    bool hasMillis = false;
    size_t pos = pat.find("%03d");
    if (pos != std::string::npos) {
        hasMillis = true;
        suffix = pat.substr(pos + 4);
        pat = pat.substr(0, pos);
    }
    char buf[160];
    buf[0] = '\0';
    std::strftime(buf, sizeof(buf), pat.c_str(), &tmv);
    std::string out(buf);
    if (hasMillis) {
        char mb[8];
        std::snprintf(mb, sizeof(mb), "%03d", static_cast<int>(ms));
        out += mb;
        out += suffix;
    }
    return out;
}

bool UtilAll::isBlank(const std::string& s) {
    for (char c : s) {
        if (!std::isspace(static_cast<unsigned char>(c))) return false;
    }
    return true;
}

bool UtilAll::isNotBlank(const std::string& s) { return !isBlank(s); }

int32_t UtilAll::pid() {
#if defined(_WIN32)
    return static_cast<int32_t>(_getpid());
#else
    return static_cast<int32_t>(getpid());
#endif
}

bool UtilAll::isIpv4(const std::string& addr) {
    struct in_addr a4{};
    return inet_pton(AF_INET, addr.c_str(), &a4) == 1;
}

bool UtilAll::ipToBytes(const std::string& ip, bool v6, Bytes& out) {
    out.clear();
    if (v6) {
        struct in6_addr a6{};
        if (inet_pton(AF_INET6, ip.c_str(), &a6) != 1) return false;
        out.assign(reinterpret_cast<const char*>(&a6), 16);
        return true;
    }
    struct in_addr a4{};
    if (inet_pton(AF_INET, ip.c_str(), &a4) != 1) return false;
    out.assign(reinterpret_cast<const char*>(&a4), 4);
    return true;
}

bool UtilAll::bytesToIp(const Bytes& raw, std::string& ip) {
    char buf[64];
    if (raw.size() == 4) {
        if (!inet_ntop(AF_INET, raw.data(), buf, sizeof(buf))) return false;
        ip = buf;
        return true;
    }
    if (raw.size() == 16) {
        if (!inet_ntop(AF_INET6, raw.data(), buf, sizeof(buf))) return false;
        ip = buf;
        return true;
    }
    return false;
}

uint32_t UtilAll::crc32(const Bytes& data) {
    const Crc32Table& table = crcTable();
    uint32_t crc = 0xFFFFFFFFu;
    for (unsigned char c : data) {
        crc = table.t[(crc ^ c) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

int32_t UtilAll::charToByte(char c) {
    const char* hex = HEX_ARRAY;
    for (int i = 0; i < 16; ++i) {
        if (hex[i] == c) return i;
    }
    // 兼容小写输入
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    if (c >= '0' && c <= '9') return c - '0';
    return -1;
}

std::string UtilAll::bytes2String(const Bytes& bs) {
    std::string out;
    out.reserve(bs.size() * 2);
    for (unsigned char b : bs) {
        out.push_back(HEX_ARRAY[(b >> 4) & 0x0F]);
        out.push_back(HEX_ARRAY[b & 0x0F]);
    }
    return out;
}

Bytes UtilAll::string2Bytes(const std::string& hexString) {
    Bytes out;
    if (hexString.empty()) return out;
    size_t len = hexString.size();
    if (len % 2 != 0) return out;  // 非法长度
    out.resize(len / 2);
    for (size_t i = 0; i < len / 2; ++i) {
        int hi = charToByte(hexString[i * 2]);
        int lo = charToByte(hexString[i * 2 + 1]);
        if (hi < 0 || lo < 0) return Bytes();  // 非法字符 -> 空串
        out[i] = static_cast<char>((hi << 4) | lo);
    }
    return out;
}

std::string UtilAll::localIp() {
    // 注意：Windows 的 socket 句柄是 SOCKET(UINT_PTR)，**不能**用 int 接（64 位会截断），
    // 关闭也必须用 closesocket 而不是 ::close（::close 在 Windows 上根本不存在）。
    // 这些差异统一由 netcompat 处理。
    netcompat::ensureInitialized();
    netcompat::socket_t fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (netcompat::isValid(fd)) {
        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(80);
        inet_pton(AF_INET, "8.8.8.8", &addr.sin_addr);
        if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
            struct sockaddr_in local{};
            netcompat::socklen_type len = sizeof(local);
            if (::getsockname(fd, reinterpret_cast<struct sockaddr*>(&local), &len) == 0) {
                char buf[64];
                if (inet_ntop(AF_INET, &local.sin_addr, buf, sizeof(buf))) {
                    netcompat::closeSocket(fd);
                    return std::string(buf);
                }
            }
        }
        netcompat::closeSocket(fd);
    }
    char host[256];
    if (::gethostname(host, sizeof(host)) == 0) {
        host[sizeof(host) - 1] = '\0';
        return std::string(host);
    }
    return "127.0.0.1";
}

std::string UtilAll::nextMillisString() {
    return std::to_string(currentTimeMillis());
}

// ---------------------------------------------------------------- InnerIdGenerator

std::string InnerIdGenerator::createUniqId() {
    static std::atomic<uint32_t> counter{0};
    uint32_t c = ++counter;

    std::string ip = UtilAll::localIp();
    Bytes result;
    Bytes ipBytes;
    if (UtilAll::isIpv4(ip) && UtilAll::ipToBytes(ip, false, ipBytes)) {
        result += ipBytes;
    } else if (UtilAll::ipToBytes(ip, true, ipBytes)) {
        result += ipBytes;
    } else {
        result.push_back(0x7F);
        result.push_back(0x00);
        result.push_back(0x00);
        result.push_back(0x01);
    }

    int32_t pidv = UtilAll::pid();
    putInt16(result, pidv & 0xFFFF);

    // 类加载 hash（Java 为 abs(_classLoaderHash)）：此处用稳定的字符串哈希替代进程随机 hash
    int32_t classHash = javaStringHash("RocketMQClient");
    if (classHash < 0) classHash = -classHash;
    putInt32U(result, static_cast<uint32_t>(classHash));

    // 当日毫秒（Java 用"当月毫秒"，Python 实现为当日毫秒，这里保持一致）
    std::time_t now = std::time(nullptr);
    std::tm tmv{};
#if defined(_WIN32)
    localtime_s(&tmv, &now);
#else
    localtime_r(&now, &tmv);
#endif
    uint32_t dayMs = static_cast<uint32_t>(((tmv.tm_hour * 60 + tmv.tm_min) * 60 + tmv.tm_sec) * 1000);
    putInt32U(result, dayMs);

    putInt16(result, static_cast<int32_t>(c & 0xFFFF));

    return UtilAll::bytes2String(result);
}

}  // namespace rocketmq
