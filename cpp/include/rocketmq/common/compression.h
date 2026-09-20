// 消息体压缩/解压（对应 org.apache.rocketmq.common.compression.*）。
//
// 与 Java 的对齐要点：
//   1. 类型解析走 CompressionType.findByValue：**0 与 3 都映射到 ZLIB**
//      （类型位为 0 是老版本客户端的压缩消息，必须按 ZLIB 解，否则静默损坏）。
//   2. 压缩格式是 **zlib 流格式**（RFC1950，带 2 字节头与 adler32 校验），不是
//      raw deflate：Java 用 Deflater/Inflater（nowrap=false），Python 用
//      zlib.compress/decompress，C++ 这里用 zlib 的 deflate/inflate 默认封装。
//      lz4 用 **LZ4 Frame 格式**（magic 0x184D2204，Java 是 lz4-java 的
//      LZ4Frame*Stream，C++ 是 liblz4 的 LZ4F_*）——不是 LZ4_compress_default 的裸 block。
//      zstd 用**标准 zstd 帧**（magic 0xFD2FB528，Java 是 zstd-jni 的
//      ZstdOutputStream/ZstdInputStream，C++ 是 ZSTD_compress/ZSTD_decompress）。
//      两种都是自描述帧，所以只要格式对，与 Java/Python/Rust 的默认参数不同也能互解。
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

// 当前构建是否编入了对应后端。CMake 找不到库时会关掉相应宏，届时
// CompressorFactory 抛异常而不是静默透传压缩字节（见 compression.cpp 顶部说明）。
bool hasZlibSupport();
bool hasZstdSupport();
bool hasLz4Support();

// 对应 org.apache.rocketmq.common.compression.CompressorFactory。
struct CompressorFactory {
    // level 仅在 ZLIB 下有意义（Java 默认 5，也是 Python 侧使用的值）
    static Bytes compress(const Bytes& src, int32_t compressionType, int level = 5);

    static Bytes decompress(const Bytes& src, int32_t compressionType);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_COMPRESSION_H
