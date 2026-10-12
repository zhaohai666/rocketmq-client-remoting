#ifndef ROCKETMQ_COMMON_COMPRESSION_H
#define ROCKETMQ_COMMON_COMPRESSION_H

#include <cstdint>
#include <string>

#include "rocketmq/common/byte_buffer.h"  // Bytes
#include "rocketmq/common/types.h"

namespace rocketmq {

struct CompressionType {
    static constexpr int32_t LZ4 = 1;
    static constexpr int32_t ZSTD = 2;
    static constexpr int32_t ZLIB = 3;

    static int32_t findByValue(int32_t value);

    static int32_t getCompressionFlag(int32_t value);
};

// 当前构建是否编入了对应后端。CMake 找不到库时会关掉相应宏，届时
// CompressorFactory 抛异常而不是静默透传压缩字节（见 compression.cpp 顶部说明）。
bool hasZlibSupport();
bool hasZstdSupport();
bool hasLz4Support();

struct CompressorFactory {
    static Bytes compress(const Bytes& src, int32_t compressionType, int level = 5);

    static Bytes decompress(const Bytes& src, int32_t compressionType);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_COMPRESSION_H
