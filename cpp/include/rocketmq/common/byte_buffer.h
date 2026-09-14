// RocketMQ 二进制读写工具：全部大端（network byte order），与 Java ByteBuffer 默认序一致。
#ifndef ROCKETMQ_COMMON_BYTE_BUFFER_H
#define ROCKETMQ_COMMON_BYTE_BUFFER_H

#include <cstdint>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

namespace rocketmq {

// 二进制字节串。std::string 二进制安全，且便于与 body/属性串互操作。
using Bytes = std::string;

// 行 format：与 Java DataOutputStream.writeInt/Long/Short/Byte 一致
inline void putInt8(Bytes& buf, uint8_t v) {
    buf.push_back(static_cast<char>(v));
}

inline void putInt16(Bytes& buf, int32_t v) {
    buf.push_back(static_cast<char>((v >> 8) & 0xFF));
    buf.push_back(static_cast<char>(v & 0xFF));
}

inline void putInt16U(Bytes& buf, uint32_t v) {
    buf.push_back(static_cast<char>((v >> 8) & 0xFF));
    buf.push_back(static_cast<char>(v & 0xFF));
}

inline void putInt32(Bytes& buf, int32_t v) {
    buf.push_back(static_cast<char>((v >> 24) & 0xFF));
    buf.push_back(static_cast<char>((v >> 16) & 0xFF));
    buf.push_back(static_cast<char>((v >> 8) & 0xFF));
    buf.push_back(static_cast<char>(v & 0xFF));
}

inline void putInt32U(Bytes& buf, uint32_t v) {
    putInt32(buf, static_cast<int32_t>(v));
}

inline void putInt64(Bytes& buf, int64_t v) {
    buf.push_back(static_cast<char>((v >> 56) & 0xFF));
    buf.push_back(static_cast<char>((v >> 48) & 0xFF));
    buf.push_back(static_cast<char>((v >> 40) & 0xFF));
    buf.push_back(static_cast<char>((v >> 32) & 0xFF));
    buf.push_back(static_cast<char>((v >> 24) & 0xFF));
    buf.push_back(static_cast<char>((v >> 16) & 0xFF));
    buf.push_back(static_cast<char>((v >> 8) & 0xFF));
    buf.push_back(static_cast<char>(v & 0xFF));
}

// Java 的 int/long 溢出语义：结果按 32/64 位有符号回绕
inline int32_t toInt32(int64_t v) {
    return static_cast<int32_t>(static_cast<uint32_t>(v & 0xFFFFFFFFL));
}

inline int32_t javaStringHash(const std::string& s) {
    // Java String.hashCode：逐 char（UTF-16 单元）累加，32 位有符号溢出。
    // 对 ASCII 而言即逐字节；非 ASCII 走 UTF-16 单元换算，避免与 Java 结果不一致。
    int64_t h = 0;
    for (size_t i = 0; i < s.size();) {
        uint32_t cp = 0;
        unsigned char c = static_cast<unsigned char>(s[i]);
        if (c < 0x80) {
            cp = c; i += 1;
        } else if ((c & 0xE0) == 0xC0 && i + 1 < s.size()) {
            cp = ((c & 0x1Fu) << 6) | (static_cast<unsigned char>(s[i + 1]) & 0x3Fu);
            i += 2;
        } else if ((c & 0xF0) == 0xE0 && i + 2 < s.size()) {
            cp = ((c & 0x0Fu) << 12)
               | ((static_cast<unsigned char>(s[i + 1]) & 0x3Fu) << 6)
               | (static_cast<unsigned char>(s[i + 2]) & 0x3Fu);
            i += 3;
        } else if ((c & 0xF8) == 0xF0 && i + 3 < s.size()) {
            cp = ((c & 0x07u) << 18)
               | ((static_cast<unsigned char>(s[i + 1]) & 0x3Fu) << 12)
               | ((static_cast<unsigned char>(s[i + 2]) & 0x3Fu) << 6)
               | (static_cast<unsigned char>(s[i + 3]) & 0x3Fu);
            i += 4;
        } else {
            cp = c; i += 1;
        }
        if (cp > 0xFFFF) {
            uint32_t u = cp - 0x10000;
            uint32_t hi = 0xD800 + (u >> 10);
            uint32_t lo = 0xDC00 + (u & 0x3FF);
            h = (h * 31 + hi) & 0xFFFFFFFFL;
            h = (h * 31 + lo) & 0xFFFFFFFFL;
        } else {
            h = (h * 31 + cp) & 0xFFFFFFFFL;
        }
    }
    return static_cast<int32_t>(h >= 0x80000000L ? h - 0x100000000L : h);
}

// 只读游标。越界抛 std::out_of_range，由上层按"解码失败"处理。
class ByteReader {
public:
    ByteReader(const Bytes& data, size_t offset = 0) : data_(data), pos_(offset) {}

    const Bytes& data() const { return data_; }
    size_t pos() const { return pos_; }
    size_t remaining() const { return data_.size() - pos_; }
    void seek(size_t p) { pos_ = p; }
    void skip(size_t n) { require(n); pos_ += n; }

    void require(size_t n) const {
        if (remaining() < n) {
            throw std::out_of_range("ByteBuffer underflow");
        }
    }

    // 不移动游标的读取（对应 Java ByteBuffer.get(index) 系列）
    int32_t peekInt32(size_t index) const { return getInt32At(data_, index); }
    int64_t peekInt64(size_t index) const { return getInt64At(data_, index); }

    int8_t readInt8() {
        require(1);
        return static_cast<int8_t>(static_cast<unsigned char>(data_[pos_++]));
    }

    int32_t readUnsignedInt8() {
        require(1);
        return static_cast<int32_t>(static_cast<unsigned char>(data_[pos_++]));
    }

    int32_t readInt16() {
        require(2);
        int32_t v = (static_cast<unsigned char>(data_[pos_]) << 8)
                  | static_cast<unsigned char>(data_[pos_ + 1]);
        pos_ += 2;
        return v;
    }

    int32_t readInt32() {
        require(4);
        int32_t v = getInt32At(data_, pos_);
        pos_ += 4;
        return v;
    }

    uint32_t readUnsignedInt32() { return static_cast<uint32_t>(readInt32()); }

    int64_t readInt64() {
        require(8);
        int64_t v = 0;
        for (int i = 0; i < 8; ++i) {
            v = (v << 8) | static_cast<unsigned char>(data_[pos_ + i]);
        }
        pos_ += 8;
        return v;
    }

    Bytes readBytes(size_t n) {
        require(n);
        Bytes out = data_.substr(pos_, n);
        pos_ += n;
        return out;
    }

    // 静态版本：从任意偏移读取，不持有游标
    static int32_t readInt32At(const Bytes& b, size_t offset) { return getInt32At(b, offset); }

    static int32_t getInt32At(const Bytes& b, size_t offset) {
        if (b.size() < offset + 4) {
            throw std::out_of_range("ByteBuffer underflow (int32)");
        }
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            v = (v << 8) | static_cast<unsigned char>(b[offset + i]);
        }
        return static_cast<int32_t>(v);
    }

    static int64_t getInt64At(const Bytes& b, size_t offset) {
        if (b.size() < offset + 8) {
            throw std::out_of_range("ByteBuffer underflow (int64)");
        }
        int64_t v = 0;
        for (int i = 0; i < 8; ++i) {
            v = (v << 8) | static_cast<unsigned char>(b[offset + i]);
        }
        return v;
    }

private:
    const Bytes& data_;
    size_t pos_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_BYTE_BUFFER_H
