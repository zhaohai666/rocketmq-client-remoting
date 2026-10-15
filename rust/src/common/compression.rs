//! Unsupported compression types **must error out**, never pass through unchanged: after decompressing the caller
//! clears `COMPRESSED_FLAG`, so passing through hands the compressed byte stream over as the body with no way to detect it afterwards.

use std::io::{Read as IoRead, Write as IoWrite};

use crate::common::sysflag::MessageSysFlag;
use crate::error::{Error, Result};

pub const DEFAULT_COMPRESS_LEVEL: i32 = 5;

pub const ZSTD_DEFAULT_LEVEL: i32 = 3;

/// 0 must map to ZLIB: messages produced by old clients have a compression-type bit of 0; without this mapping decompression
/// would fail while the outer layer still clears `COMPRESSED_FLAG`, silently returning the compressed byte stream.
pub fn normalize_compression_type(compression_type: i32) -> i32 {
    if compression_type == 0 {
        MessageSysFlag::ZLIB_TYPE
    } else {
        compression_type
    }
}

/// For entry points that persist configuration (e.g. [`crate::client::producer::DefaultMQProducer::set_compress_type`]):
/// prefer rejecting at set time over discovering an unknown algorithm at send time (the old version silently downgraded
/// to no compression at the send stage, giving the user no signal at all).
pub fn validate_compression_type(compression_type: i32) -> Result<i32> {
    match compression_type {
        0 | MessageSysFlag::ZLIB_TYPE => Ok(MessageSysFlag::ZLIB_TYPE),
        MessageSysFlag::LZ4_TYPE => Ok(MessageSysFlag::LZ4_TYPE),
        MessageSysFlag::ZSTD_TYPE => Ok(MessageSysFlag::ZSTD_TYPE),
        _ => Err(Error::Encode(format!(
            "Unknown compress type value: {compression_type}"
        ))),
    }
}

pub fn compression_type_name(compression_type: i32) -> Option<&'static str> {
    match normalize_compression_type(compression_type) {
        MessageSysFlag::LZ4_TYPE => Some("LZ4"),
        MessageSysFlag::ZSTD_TYPE => Some("ZSTD"),
        MessageSysFlag::ZLIB_TYPE => Some("ZLIB"),
        _ => None,
    }
}

fn unsupported(compression_type: i32) -> Error {
    Error::Decode(format!("unsupported compression type: {compression_type}"))
}

pub fn compress(data: &[u8], compression_type: i32, level: i32) -> Result<Vec<u8>> {
    match normalize_compression_type(compression_type) {
        MessageSysFlag::ZLIB_TYPE => zlib_compress(data, level),
        MessageSysFlag::LZ4_TYPE => lz4_compress(data),
        MessageSysFlag::ZSTD_TYPE => zstd_compress(data, ZSTD_DEFAULT_LEVEL),
        _ => Err(unsupported(compression_type)),
    }
}

pub fn decompress(data: &[u8], compression_type: i32) -> Result<Vec<u8>> {
    match normalize_compression_type(compression_type) {
        MessageSysFlag::ZLIB_TYPE => zlib_decompress(data),
        MessageSysFlag::LZ4_TYPE => lz4_decompress(data),
        MessageSysFlag::ZSTD_TYPE => zstd_decompress(data),
        _ => Err(unsupported(compression_type)),
    }
}

/// Public entry point of `decompress`, dispatched by the message-body compression bits.
pub fn decompress_body(data: &[u8], compression_type: i32) -> Result<Vec<u8>> {
    decompress(data, compression_type)
}

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

pub fn zlib_decompress(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = flate2::read::ZlibDecoder::new(data);
    let mut out = Vec::new();
    let err = match decoder.read_to_end(&mut out) {
        Ok(_) => return Ok(out),
        Err(e) => e,
    };
    Err(Error::Decode(format!("zlib decompress failed: {err}")))
}

pub fn lz4_compress(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
    encoder
        .write_all(data)
        .map_err(|e| Error::Encode(format!("lz4 compress failed: {e}")))?;
    encoder
        .finish()
        .map_err(|e| Error::Encode(format!("lz4 compress failed: {e}")))
}

pub fn lz4_decompress(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = lz4_flex::frame::FrameDecoder::new(data);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| Error::Decode(format!("lz4 decompress failed: {e}")))?;
    Ok(out)
}

pub fn zstd_compress(data: &[u8], level: i32) -> Result<Vec<u8>> {
    zstd::encode_all(data, level).map_err(|e| Error::Encode(format!("zstd compress failed: {e}")))
}

pub fn zstd_decompress(data: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(data).map_err(|e| Error::Decode(format!("zstd decompress failed: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::util_all;

    /// The same plaintext (1120 bytes) used by the ground-truth fixtures.
    fn payload() -> Vec<u8> {
        b"rocketmq-compressed-payload-".repeat(40)
    }

    /// Bytes produced by external zlib (level 5) compressing the same plaintext (47B).
    /// Hard-coded external ground truth, proving we read a real RFC1950 stream rather than self-compress and self-decompress.
    const ZLIB_L5_HEX: &str = "785e2bca4fce4e2dc92dd44dcecf2d284a2d2e4e4dd12d48acccc94f4cd12d1a9\
                               51b951b951b95a3400e0058e0b9f0";

    /// A frame produced by an external LZ4 Frame tool, verified decodable by the `lz4` CLI (66B).
    const LZ4_FRAME_HEX: &str = "04224d1868406004000000000000482b000000ff0d726f636b65746d712d636f\
                                 6d707265737365642d7061796c6f61642d1c00ffffffff30506c6f61642d00000000";

    /// Same as above, a frame for empty input (11B).
    const LZ4_FRAME_EMPTY_HEX: &str = "04224d1860408200000000";

    /// Produced by the `zstd -3` CLI: a frame with content-size (50B).
    const ZSTD_WITH_SIZE_HEX: &str = "28b52ffd646003250100e0726f636b65746d712d636f6d70726573736564\
                                      2d7061796c6f61642d010004f1ff740278aa9792";

    const ZSTD_NO_SIZE_HEX: &str = "28b52ffd0408250100e0726f636b65746d712d636f6d707265737365642d70\
                                    61796c6f61642d010004f1ff740278aa9792";

    fn unhex(hex: &str) -> Vec<u8> {
        util_all::string_2_bytes(&hex.replace(['\n', ' '], "")).unwrap()
    }

    #[test]
    fn compression_type_mapping() {
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
    fn validate_compression_type_mirrors_find_by_value() {
        assert_eq!(
            validate_compression_type(0).unwrap(),
            MessageSysFlag::ZLIB_TYPE
        );
        assert_eq!(
            validate_compression_type(3).unwrap(),
            MessageSysFlag::ZLIB_TYPE
        );
        assert_eq!(
            validate_compression_type(1).unwrap(),
            MessageSysFlag::LZ4_TYPE
        );
        assert_eq!(
            validate_compression_type(2).unwrap(),
            MessageSysFlag::ZSTD_TYPE
        );
        let err = validate_compression_type(9).unwrap_err();
        assert_eq!(
            err.to_string(),
            "encode error: Unknown compress type value: 9"
        );
        assert!(matches!(
            validate_compression_type(-1),
            Err(Error::Encode(_))
        ));
        assert!(matches!(
            validate_compression_type(4),
            Err(Error::Encode(_))
        ));
    }

    #[test]
    fn zlib_round_trip_and_external_fixture() {
        let compressed = zlib_compress(&payload(), 5).unwrap();
        assert!(compressed.len() < payload().len(), "repeated plaintext should compress");
        assert_eq!(compressed[0], 0x78, "first byte of the zlib stream is CMF");
        // Interop ground truth: the 47 bytes produced by external zlib (level 5) must decompress.
        // The reverse does not hold -- the exact deflate bytes are implementation-defined (this crate's
        // flate2 backend would differ from an external implementation in bytes even at the same level), so we
        // only assert "the stream we produce is valid RFC1950".
        let external = unhex(ZLIB_L5_HEX);
        assert_eq!(external.len(), 47, "external fixture length");
        assert_eq!(zlib_decompress(&external).unwrap(), payload());
        assert_eq!(
            decompress(&compressed, MessageSysFlag::ZLIB_TYPE).unwrap(),
            payload()
        );
        // Type bits 0 (old clients) decompress as ZLIB
        assert_eq!(decompress(&compressed, 0).unwrap(), payload());
    }

    #[test]
    fn lz4_round_trip_and_external_fixture() {
        let compressed = lz4_compress(&payload()).unwrap();
        assert!(compressed.len() < payload().len());
        assert_eq!(
            &compressed[..4],
            &[0x04, 0x22, 0x4D, 0x18],
            "LZ4 Frame magic"
        );
        // Externally produced frames must decompress
        assert_eq!(lz4_decompress(&unhex(LZ4_FRAME_HEX)).unwrap(), payload());
        assert!(lz4_decompress(&unhex(LZ4_FRAME_EMPTY_HEX))
            .unwrap()
            .is_empty());
        assert_eq!(
            decompress(&compressed, MessageSysFlag::LZ4_TYPE).unwrap(),
            payload()
        );
        // Our own frames must also decompress via the "external" path (same implementation, but at least the self-describing header matches)
        assert_eq!(decompress(&compressed, 1).unwrap(), payload());
        assert_eq!(
            lz4_compress(&[]).unwrap().as_slice()[..4],
            [0x04, 0x22, 0x4D, 0x18]
        );
    }

    #[test]
    fn zstd_round_trip_and_external_fixtures() {
        let compressed = zstd_compress(&payload(), ZSTD_DEFAULT_LEVEL).unwrap();
        assert!(compressed.len() < payload().len());
        assert_eq!(
            &compressed[..4],
            &[0x28, 0xB5, 0x2F, 0xFD],
            "zstd magic number"
        );
        assert_eq!(
            zstd_decompress(&unhex(ZSTD_WITH_SIZE_HEX)).unwrap(),
            payload()
        );
        assert_eq!(
            zstd_decompress(&unhex(ZSTD_NO_SIZE_HEX)).unwrap(),
            payload()
        );
        assert_eq!(
            decompress(&compressed, MessageSysFlag::ZSTD_TYPE).unwrap(),
            payload()
        );
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
        for ctype in [4, 5, 6, 7] {
            let err = compress(&payload(), ctype, 5).unwrap_err();
            assert!(
                matches!(err, Error::Decode(ref m) if m == &format!("unsupported compression type: {ctype}")),
                "{err}"
            );
            assert!(matches!(
                decompress(&payload(), ctype),
                Err(Error::Decode(_))
            ));
        }
    }

    #[test]
    fn garbage_input_is_an_error_not_a_panic() {
        assert!(matches!(
            zlib_decompress(b"not-a-zlib-stream"),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            lz4_decompress(b"not-an-lz4-frame"),
            Err(Error::Decode(_))
        ));
        assert!(matches!(
            zstd_decompress(b"not-zstd"),
            Err(Error::Decode(_))
        ));
        // A truncated zlib stream must error out, not silently return half the data
        let compressed = zlib_compress(&payload(), 5).unwrap();
        let truncated = &compressed[..compressed.len() / 2];
        assert!(matches!(zlib_decompress(truncated), Err(Error::Decode(_))));
        // Same for a truncated lz4 frame
        let lz = lz4_compress(&payload()).unwrap();
        assert!(matches!(
            lz4_decompress(&lz[..lz.len() / 2]),
            Err(Error::Decode(_))
        ));
        assert!(matches!(zlib_compress(b"x", 10), Err(Error::Encode(_))));
    }
}
