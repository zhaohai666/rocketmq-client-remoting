//! 大端字节序读写缓冲（对应 Java `ByteBuffer` 的用法与 Python 的 `struct.pack(">…")`）。

use crate::error::{Error, Result};

#[derive(Debug, Default, Clone)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Writer {
        Writer { buf: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Writer {
        Writer { buf: Vec::with_capacity(cap) }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }

    pub fn u8(&mut self, v: u8) -> &mut Writer {
        self.buf.push(v);
        self
    }

    pub fn i8(&mut self, v: i8) -> &mut Writer {
        self.buf.push(v as u8);
        self
    }

    pub fn i16(&mut self, v: i32) -> &mut Writer {
        self.buf.extend_from_slice(&((v as i16) as u16).to_be_bytes());
        self
    }

    pub fn u16(&mut self, v: u32) -> &mut Writer {
        self.buf.extend_from_slice(&(v as u16).to_be_bytes());
        self
    }

    pub fn i32(&mut self, v: i32) -> &mut Writer {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn u32(&mut self, v: u32) -> &mut Writer {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn i64(&mut self, v: i64) -> &mut Writer {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn bytes(&mut self, src: &[u8]) -> &mut Writer {
        self.buf.extend_from_slice(src);
        self
    }

    pub fn zeros(&mut self, n: usize) -> &mut Writer {
        self.buf.resize(self.buf.len() + n, 0);
        self
    }

    /// 覆盖写入位置 `at` 处的 4 字节大端整数（用于先占位后回填长度）。
    pub fn patch_i32(&mut self, at: usize, v: i32) {
        self.buf[at..at + 4].copy_from_slice(&v.to_be_bytes());
    }

    /// 对应 `RocketMQSerializable.writeStr(buf, useShortLength, s)`。
    pub fn string(&mut self, short_length: bool, s: Option<&str>) -> &mut Writer {
        let bytes = s.map(str::as_bytes).unwrap_or(&[][..]);
        if short_length {
            self.u16(bytes.len() as u32);
        } else {
            self.i32(bytes.len() as i32);
        }
        self.bytes(bytes)
    }

    /// 对应 `RocketMQSerializable.writeDecimalLong`：4 字节长度 + 十进制文本。
    pub fn decimal_long(&mut self, value: i64) -> &mut Writer {
        let len_at = self.buf.len();
        self.i32(0);
        let text = value.to_string();
        self.bytes(text.as_bytes());
        self.patch_i32(len_at, (self.buf.len() - len_at - 4) as i32);
        self
    }

    pub fn decimal_int(&mut self, value: i32) -> &mut Writer {
        self.decimal_long(value as i64)
    }
}

pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Reader<'a> {
        Reader { data, pos: 0 }
    }

    pub fn with_pos(data: &'a [u8], pos: usize) -> Reader<'a> {
        Reader { data, pos }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn set_pos(&mut self, pos: usize) {
        self.pos = pos;
    }

    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub fn slice(&self, start: usize, len: usize) -> Result<&'a [u8]> {
        let end = start.checked_add(len).ok_or_else(|| Error::Decode("slice overflow".into()))?;
        if end > self.data.len() {
            return Err(Error::Decode(format!(
                "buffer underflow: need {end} bytes, have {}",
                self.data.len()
            )));
        }
        Ok(&self.data[start..end])
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let part = self.slice(self.pos, n)?;
        self.pos += n;
        Ok(part)
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn i8(&mut self) -> Result<i8> {
        Ok(self.take(1)?[0] as i8)
    }

    pub fn u16(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    /// 读出的是 Java `short` 的无符号解释（与 Python `code & 0xFFFF` 对齐）。
    pub fn i16(&mut self) -> Result<i32> {
        Ok(self.u16()? as i32)
    }

    pub fn i32(&mut self) -> Result<i32> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn i64(&mut self) -> Result<i64> {
        let b = self.take(8)?;
        Ok(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        self.take(n)
    }

    pub fn rest(&mut self) -> &'a [u8] {
        let r = &self.data[self.pos.min(self.data.len())..];
        self.pos = self.data.len();
        r
    }

    /// 对应 `RocketMQSerializable.readStr`：长度为 0 时返回 `None`（而非空串）。
    pub fn string(&mut self, short_length: bool) -> Result<Option<String>> {
        let n = if short_length {
            self.u16()? as usize
        } else {
            let raw = self.i32()?;
            if raw < 0 {
                return Err(Error::Decode(format!("negative string length {raw}")));
            }
            raw as usize
        };
        if n == 0 {
            return Ok(None);
        }
        let bytes = self.bytes(n)?;
        String::from_utf8(bytes.to_vec())
            .map(Some)
            .map_err(|e| Error::Decode(format!("invalid utf8: {e}")))
    }

    /// 对应 `RocketMQSerializable.readDecimalInt/Long`：长度前缀 + 十进制文本。
    pub fn decimal_long(&mut self) -> Result<i64> {
        let n = self.i32()?;
        if n <= 0 {
            return Ok(0);
        }
        let text = String::from_utf8(self.bytes(n as usize)?.to_vec())
            .map_err(|e| Error::Decode(format!("invalid utf8: {e}")))?;
        text.trim().parse::<i64>().map_err(|e| Error::Decode(format!("bad decimal {text:?}: {e}")))
    }

    pub fn decimal_int(&mut self) -> Result<i32> {
        Ok(self.decimal_long()? as i32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn big_endian_round_trip() {
        let mut w = Writer::new();
        w.i16(310).u8(12).i16(428).i32(-7).i32(0x1234_5678).i64(1 << 40);
        let mut r = Reader::new(w.as_slice());
        assert_eq!(r.i16().unwrap(), 310);
        assert_eq!(r.u8().unwrap(), 12);
        assert_eq!(r.i16().unwrap(), 428);
        assert_eq!(r.i32().unwrap(), -7);
        assert_eq!(r.i32().unwrap(), 0x1234_5678);
        assert_eq!(r.i64().unwrap(), 1 << 40);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn code_above_65535_truncates_like_java_short() {
        let mut w = Writer::new();
        w.i16(200_050);
        let mut r = Reader::new(w.as_slice());
        assert_eq!(r.i16().unwrap(), 200_050 & 0xFFFF);
    }

    #[test]
    fn string_none_on_zero_length() {
        let mut w = Writer::new();
        w.string(false, None).string(true, Some("TagA"));
        let mut r = Reader::new(w.as_slice());
        assert_eq!(r.string(false).unwrap(), None);
        assert_eq!(r.string(true).unwrap().as_deref(), Some("TagA"));
    }

    #[test]
    fn decimal_long_matches_python() {
        // python: write_decimal_long(0) -> 0000000130, (12345) -> 00000005 3132333435
        let vectors = [
            (0i64, "0000000130"),
            (1, "0000000131"),
            (9, "0000000139"),
            (12345, "000000053132333435"),
            (-1, "000000022d31"),
            (
                -9223372036854775808,
                "000000142d39323233333732303336383534373735383038",
            ),
            (2147483648, "0000000a32313437343833363438"),
        ];
        for (value, hex) in vectors {
            let mut w = Writer::new();
            w.decimal_long(value);
            assert_eq!(bytes_hex(w.as_slice()), hex, "value {value}");
            let mut r = Reader::new(w.as_slice());
            assert_eq!(r.decimal_long().unwrap(), value);
            assert_eq!(r.remaining(), 0);
        }
    }

    fn bytes_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
