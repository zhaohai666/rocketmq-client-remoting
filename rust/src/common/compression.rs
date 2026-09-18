//! 消息体压缩（对应 Java `org.apache.rocketmq.common.compression.{Zlib,Lz4,Zstd}Compressor`
//! 与 `CompressorFactory`，参考实现
//! `python/rocketmq/common/message_decoder.py` 的 `_compress/_decompress`）。
//!
//! - ZLIB：Python `zlib.compress(data, level)` = RFC1950 zlib 流；Java 是
//!   `new Deflater(level)` + `DeflaterOutputStream`，同一封装。
//! - LZ4：两边都是 **LZ4 Frame** 格式（Java `LZ4FrameOutputStream`，Python
//!   `lz4.frame`，Rust `lz4_flex::frame`），帧是自描述的，三端互通。
//! - ZSTD：Java `ZstdOutputStream`，Rust `zstd` crate 的 frame 编解码。
//!
//! 未支持的压缩类型**必须报错**，不能原样透传：调用方解压后会清掉
//! `COMPRESSED_FLAG`，透传等于把压缩字节流当正文交出去且事后无法识别。

use std::io::{Read as IoRead, Write as IoWrite};

use crate::common::sysflag::MessageSysFlag;
use crate::error::{Error, Result};

/// 对应 Java `MessageDecoder#encode` 里硬编码的 `compressor.compress(body, 5)`
/// 与 Python `_compress(..., level = 5)`。
pub const DEFAULT_COMPRESS_LEVEL: i32 = 5;

/// Python `zstandard.ZstdCompressor()` 的默认级别（Java 侧则由调用方传入 level）。
pub const ZSTD_DEFAULT_LEVEL: i32 = 3;

/// 对应 Java `CompressionType.findByValue` 的向后兼容映射：
/// `1 -> LZ4`、`2 -> ZSTD`、`0`（老版本没有类型位）与 `3 -> ZLIB`。
///
/// 0 必须映射到 ZLIB：老客户端产的压缩消息类型位就是 0，不映射的话解压会失败，
/// 而外层照样清掉 `COMPRESSED_FLAG`，结果就是静默返回压缩字节流。
pub fn normalize_compression_type(compression_type: i32) -> i32 {
    if compression_type == 0 {
        MessageSysFlag::ZLIB_TYPE
    } else {
        compression_type
    }
}

/// 算法名，仅日志/报错用（对应 Java `CompressionType.name()`）。
pub fn compression_type_name(compression_type: i32) -> Option<&'static str> {
    match normalize_compression_type(compression_type) {
        MessageSysFlag::LZ4_TYPE => Some("LZ4"),
        MessageSysFlag::ZSTD_TYPE => Some("ZSTD"),
        MessageSysFlag::ZLIB_TYPE => Some("ZLIB"),
        _ => None,
    }
}

/// 对应 Java `CompressorFactory.getCompressor` 对未知类型返回 null（调用方 NPE）
/// 与 `CompressionType.findByValue` 的 `RuntimeException`；Python 抛
/// `RuntimeError("unsupported compression type: %d")`。
fn unsupported(compression_type: i32) -> Error {
    Error::Decode(format!("unsupported compression type: {compression_type}"))
}

/// 对应 Java `Compressor#compress(byte[], int level)` / Python `_compress`。
///
/// 与 Python 一致：`level` **只作用于 ZLIB**（`zlib.compress(data, level)`），
/// LZ4 与 ZSTD 用各自默认参数。
/// ⚠ Java 会把 level 透传给 `ZstdOutputStream`，ZSTD 的压缩级别因此可能与 Python 不同
/// （LZ4 两边都忽略 level）。
pub fn compress(data: &[u8], compression_type: i32, level: i32) -> Result<Vec<u8>> {
    match normalize_compression_type(compression_type) {
        MessageSysFlag::ZLIB_TYPE => zlib_compress(data, level),
        MessageSysFlag::LZ4_TYPE => lz4_compress(data),
        MessageSysFlag::ZSTD_TYPE => zstd_compress(data, ZSTD_DEFAULT_LEVEL),
        _ => Err(unsupported(compression_type)),
    }
}

/// 对应 Java `Compressor#decompress(byte[])` / Python `_decompress`。
pub fn decompress(data: &[u8], compression_type: i32) -> Result<Vec<u8>> {
    match normalize_compression_type(compression_type) {
        MessageSysFlag::ZLIB_TYPE => zlib_decompress(data),
        MessageSysFlag::LZ4_TYPE => lz4_decompress(data),
        MessageSysFlag::ZSTD_TYPE => zstd_decompress(data),
        _ => Err(unsupported(compression_type)),
    }
}

/// 对应 Python `decompress_body`：`decompress` 的公开入口。
///
/// 除消息解码路径外还有第二个调用方：Request-Reply 的应答是从
/// `PUSH_REPLY_MESSAGE_TO_CLIENT(326)` 直接推过来的裸包，不走消息解码，
/// 需要自己按 `sysFlag` 判断并解压（对齐 Java
/// `ClientRemotingProcessor#receiveReplyMessage`）。
pub fn decompress_body(data: &[u8], compression_type: i32) -> Result<Vec<u8>> {
    decompress(data, compression_type)
}

/// 对应 Java `ZlibCompressor`（`Deflater(level)` + `DeflaterOutputStream`）。
pub fn zlib_compress(data: &[u8], level: i32) -> Result<Vec<u8>> {
    if !(0..=9).contains(&level) {
        return Err(Error::Encode(format!("invalid zlib level: {level}")));
    }
    let mut encoder = flate2::read::ZlibEncoder::new(data, flate2::Compression::new(level as u32));
    let mut out = Vec::new();
    encoder
        .read_to_end(&mut out)
        .map_err(|e| Error::Encode(format!("zlib compress failed: {e}")))?;
    Ok(out)
}

/// 对应 Java `ZlibCompressor#decompress`（`InflaterInputStream`）。
pub fn zlib_decompress(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = flate2::read::ZlibDecoder::new(data);
    let mut out = Vec::new();
    let err = match decoder.read_to_end(&mut out) {
        Ok(_) => return Ok(out),
        Err(e) => e,
    };
    // Java 截断流时抛 IOException，这里同样报错（绝不返回半截数据）
    Err(Error::Decode(format!("zlib decompress failed: {err}")))
}

/// 对应 Java `Lz4Compressor`（`LZ4FrameOutputStream` 默认参数）。
pub fn lz4_compress(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
    encoder
        .write_all(data)
        .map_err(|e| Error::Encode(format!("lz4 compress failed: {e}")))?;
    encoder
        .finish()
        .map_err(|e| Error::Encode(format!("lz4 compress failed: {e}")))
}

/// 对应 Java `Lz4Compressor#decompress`（`LZ4FrameInputStream`）。
pub fn lz4_decompress(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = lz4_flex::frame::FrameDecoder::new(data);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| Error::Decode(format!("lz4 decompress failed: {e}")))?;
    Ok(out)
}

/// 对应 Java `ZstdCompressor`（`ZstdOutputStream(stream, level)`）。
pub fn zstd_compress(data: &[u8], level: i32) -> Result<Vec<u8>> {
    zstd::encode_all(data, level).map_err(|e| Error::Encode(format!("zstd compress failed: {e}")))
}

/// 对应 Java `ZstdCompressor#decompress`（`ZstdInputStream`）。
///
/// 帧里没写 content-size 时也能解（Java `ZstdInputStream` 同语义，
/// Python 为此特意改用 stream_reader）。
pub fn zstd_decompress(data: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(data).map_err(|e| Error::Decode(format!("zstd decompress failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::util_all;

    /// 与 Python / C++ 侧生成真值时使用的同一段明文（1120 字节）。
    fn payload() -> Vec<u8> {
        b"rocketmq-compressed-payload-".repeat(40)
    }

    /// Python `zlib.compress(b"rocketmq-compressed-payload-" * 40, 5)` 的字节（47B）。
    /// 硬编码外部真值，用来证明我们读的是真正的 RFC1950 流，而不是自压自解。
    const ZLIB_L5_HEX: &str = "785e2bca4fce4e2dc92dd44dcecf2d284a2d2e4e4dd12d48acccc94f4cd12d1a9\
                               51b951b951b95a3400e0058e0b9f0";

    /// 外部（Python `lz4.frame.compress`）产出的 LZ4 Frame，`lz4` CLI 校验可解（66B）。
    const LZ4_FRAME_HEX: &str = "04224d1868406004000000000000482b000000ff0d726f636b65746d712d636f\
                                 6d707265737365642d7061796c6f61642d1c00ffffffff30506c6f61642d00000000";

    /// 同上，空输入的 frame（11B）。
    const LZ4_FRAME_EMPTY_HEX: &str = "04224d1860408200000000";

    /// `zstd -3` CLI 产出：带 content-size 的帧（50B）。
    const ZSTD_WITH_SIZE_HEX: &str = "28b52ffd646003250100e0726f636b65746d712d636f6d70726573736564\
                                      2d7061796c6f61642d010004f1ff740278aa9792";

    /// `zstd -3` CLI 产出：不带 content-size 的帧（49B，Java zstd-jni 默认写法）。
    const ZSTD_NO_SIZE_HEX: &str = "28b52ffd0408250100e0726f636b65746d712d636f6d707265737365642d70\
                                    61796c6f61642d010004f1ff740278aa9792";

    fn unhex(hex: &str) -> Vec<u8> {
        util_all::string_2_bytes(&hex.replace(['\n', ' '], "")).unwrap()
    }

    #[test]
    fn compression_type_mapping() {
        // Java CompressionType.findByValue
        assert_eq!(normalize_compression_type(0), MessageSysFlag::ZLIB_TYPE);
        assert_eq!(normalize_compression_type(3), MessageSysFlag::ZLIB_TYPE);
        assert_eq!(normalize_compression_type(1), MessageSysFlag::LZ4_TYPE);
        assert_eq!(normalize_compression_type(2), MessageSysFlag::ZSTD_TYPE);
        assert_eq!(compression_type_name(0), Some("ZLIB"));
        assert_eq!(compression_type_name(1), Some("LZ4"));
        assert_eq!(compression_type_name(2), Some("ZSTD"));
        assert_eq!(compression_type_name(4), None);
        assert_eq!(DEFAULT_COMPRESS_LEVEL, 5);
        assert_eq!(ZSTD_DEFAULT_LEVEL, 3);
    }

    #[test]
    fn zlib_round_trip_and_external_fixture() {
        let compressed = zlib_compress(&payload(), 5).unwrap();
        assert!(compressed.len() < payload().len(), "重复明文应被压缩");
        assert_eq!(compressed[0], 0x78, "zlib 流第一个字节是 CMF");
        // 互通真值：Python `zlib.compress(data, 5)` 产出的 47 字节必须能解开。
        // 反向不成立——deflate 的具体字节由实现决定（Python 用 C zlib、这里是
        // flate2 的纯 Rust 后端，同级别字节数就会不同），所以只断言「我们产的
        // 流是合法 RFC1950」，与 C++ 端口 `test_compression.cpp` 同一口径。
        let external = unhex(ZLIB_L5_HEX);
        assert_eq!(external.len(), 47, "外部 fixture 长度");
        assert_eq!(zlib_decompress(&external).unwrap(), payload());
        assert_eq!(decompress(&compressed, MessageSysFlag::ZLIB_TYPE).unwrap(), payload());
        // 类型位 0（老版本）按 ZLIB 解
        assert_eq!(decompress(&compressed, 0).unwrap(), payload());
    }

    #[test]
    fn lz4_round_trip_and_external_fixture() {
        let compressed = lz4_compress(&payload()).unwrap();
        assert!(compressed.len() < payload().len());
        assert_eq!(&compressed[..4], &[0x04, 0x22, 0x4D, 0x18], "LZ4 Frame magic");
        // 外部产出的 frame 必须能解
        assert_eq!(lz4_decompress(&unhex(LZ4_FRAME_HEX)).unwrap(), payload());
        assert!(lz4_decompress(&unhex(LZ4_FRAME_EMPTY_HEX)).unwrap().is_empty());
        assert_eq!(decompress(&compressed, MessageSysFlag::LZ4_TYPE).unwrap(), payload());
        // 自己产出的 frame 也要能被「外部」路径解（同一实现，但至少自描述头一致）
        assert_eq!(decompress(&compressed, 1).unwrap(), payload());
        assert_eq!(lz4_compress(&[]).unwrap().as_slice()[..4], [0x04, 0x22, 0x4D, 0x18]);
    }

    #[test]
    fn zstd_round_trip_and_external_fixtures() {
        let compressed = zstd_compress(&payload(), ZSTD_DEFAULT_LEVEL).unwrap();
        assert!(compressed.len() < payload().len());
        assert_eq!(&compressed[..4], &[0x28, 0xB5, 0x2F, 0xFD], "zstd magic number");
        assert_eq!(zstd_decompress(&unhex(ZSTD_WITH_SIZE_HEX)).unwrap(), payload());
        assert_eq!(zstd_decompress(&unhex(ZSTD_NO_SIZE_HEX)).unwrap(), payload());
        assert_eq!(decompress(&compressed, MessageSysFlag::ZSTD_TYPE).unwrap(), payload());
    }

    #[test]
    fn compress_dispatch_uses_sys_flag_type() {
        for ctype in [
            MessageSysFlag::ZLIB_TYPE,
            MessageSysFlag::LZ4_TYPE,
            MessageSysFlag::ZSTD_TYPE,
            0,
        ] {
            let bytes = compress(&payload(), ctype, DEFAULT_COMPRESS_LEVEL).unwrap();
            assert_ne!(bytes, payload());
            assert_eq!(decompress_body(&bytes, ctype).unwrap(), payload());
        }
    }

    #[test]
    fn unsupported_type_fails_loudly() {
        // SNAPPY(4) 以及 5/6/7 在 Java 里 findByValue 直接抛 RuntimeException
        for ctype in [4, 5, 6, 7] {
            let err = compress(&payload(), ctype, 5).unwrap_err();
            assert!(
                matches!(err, Error::Decode(ref m) if m == &format!("unsupported compression type: {ctype}")),
                "{err}"
            );
            assert!(matches!(decompress(&payload(), ctype), Err(Error::Decode(_))));
        }
    }

    #[test]
    fn garbage_input_is_an_error_not_a_panic() {
        assert!(matches!(zlib_decompress(b"not-a-zlib-stream"), Err(Error::Decode(_))));
        assert!(matches!(lz4_decompress(b"not-an-lz4-frame"), Err(Error::Decode(_))));
        assert!(matches!(zstd_decompress(b"not-zstd"), Err(Error::Decode(_))));
        // 截断的 zlib 流必须报错，而不是静默返回半截数据
        let compressed = zlib_compress(&payload(), 5).unwrap();
        let truncated = &compressed[..compressed.len() / 2];
        assert!(matches!(zlib_decompress(truncated), Err(Error::Decode(_))));
        // 截断的 lz4 frame 同理
        let lz = lz4_compress(&payload()).unwrap();
        assert!(matches!(lz4_decompress(&lz[..lz.len() / 2]), Err(Error::Decode(_))));
        assert!(matches!(zlib_compress(b"x", 10), Err(Error::Encode(_))));
    }
}
