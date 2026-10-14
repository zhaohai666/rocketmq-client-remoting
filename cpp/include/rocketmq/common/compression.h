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

// Whether this build compiled in the corresponding backend. When CMake cannot find the library it disables
// the macro, and then CompressorFactory throws instead of silently passing compressed bytes through.
bool hasZlibSupport();
bool hasZstdSupport();
bool hasLz4Support();

struct CompressorFactory {
    static Bytes compress(const Bytes& src, int32_t compressionType, int level = 5);

    static Bytes decompress(const Bytes& src, int32_t compressionType);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_COMPRESSION_H
