// 消息体压缩/解压（对应 org.apache.rocketmq.common.compression.*）。
//
// 与 Java 的对齐要点：
//   1. 类型解析走 CompressionType.findByValue：**0 与 3 都映射到 ZLIB**
//      （类型位为 0 是老版本客户端的压缩消息，必须按 ZLIB 解，否则静默损坏）。
//   2. 压缩格式是 **zlib 流格式**（RFC1950，带 2 字节头与 adler32 校验），不是
//      raw deflate：Java 用 Deflater/Inflater（nowrap=false），Python 用
//      zlib.compress/decompress，C++ 这里用 zlib 的 deflate/inflate 默认封装。
//      lz4 用 LZ4 Frame 格式（Java 是 lz4-java 的 LZ4Frame*Stream）。
//   3. **失败一律抛异常，绝不静默原样返回**。静默透传会把压缩字节流当作正文交给
//      上层——不报错、不抛异常，属不可察觉的数据损坏（Java 抛 IOException /
//      RuntimeException，这里的语义与其一致）。
#ifndef ROCKETMQ_COMMON_COMPRESSION_H
#define ROCKETMQ_COMMON_COMPRESSION_H

#include <cstdint>
#include <string>

#include "rocketmq/common/byte_buffer.h"  // Bytes
#include "rocketmq/common/types.h"

namespace rocketmq {

// 对应 org.apache.rocketmq.common.compression.CompressionType
struct CompressionType {
    static constexpr int32_t LZ4 = 1;
    static constexpr int32_t ZSTD = 2;
    static constexpr int32_t ZLIB = 3;

    // Java CompressionType.findByValue：
    //   case 1 -> LZ4; case 2 -> ZSTD; case 0 / case 3 -> ZLIB; default -> 抛错
    static int32_t findByValue(int32_t value);

    // Java CompressionType.getCompressionFlag：类型 -> sysFlag 的 bit8~10
    static int32_t getCompressionFlag(int32_t value);
};

// 当前构建是否编入了 zlib 支持。
bool hasZlibSupport();

// 对应 org.apache.rocketmq.common.compression.CompressorFactory。
struct CompressorFactory {
    // level 仅在 ZLIB 下有意义（Java 默认 5，也是 Python 侧使用的值）
    static Bytes compress(const Bytes& src, int32_t compressionType, int level = 5);

    static Bytes decompress(const Bytes& src, int32_t compressionType);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_COMPRESSION_H
