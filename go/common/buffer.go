package common

import (
	"encoding/binary"
	"fmt"
)

// Writer is a big-endian byte sink (Java ByteBuffer writes / Python
// struct.pack(">...")).
type Writer struct {
	buf []byte
}

func NewWriter() *Writer { return &Writer{} }

func NewWriterCap(capacity int) *Writer { return &Writer{buf: make([]byte, 0, capacity)} }

func (w *Writer) Len() int       { return len(w.buf) }
func (w *Writer) IsEmpty() bool  { return len(w.buf) == 0 }
func (w *Writer) Buffer() []byte { return w.buf }

func (w *Writer) IntoInner() []byte { return w.buf }

func (w *Writer) U8(v byte) *Writer {
	w.buf = append(w.buf, v)
	return w
}

func (w *Writer) I8(v int8) *Writer {
	w.buf = append(w.buf, byte(v))
	return w
}

// I16 truncates to the low 16 bits, like writing a Java short.
func (w *Writer) I16(v int32) *Writer {
	w.buf = binary.BigEndian.AppendUint16(w.buf, uint16(v))
	return w
}

func (w *Writer) U16(v uint32) *Writer {
	w.buf = binary.BigEndian.AppendUint16(w.buf, uint16(v))
	return w
}

func (w *Writer) I32(v int32) *Writer {
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v))
	return w
}

func (w *Writer) U32(v uint32) *Writer {
	w.buf = binary.BigEndian.AppendUint32(w.buf, v)
	return w
}

func (w *Writer) I64(v int64) *Writer {
	w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(v))
	return w
}

func (w *Writer) Bytes(src []byte) *Writer {
	w.buf = append(w.buf, src...)
	return w
}

func (w *Writer) Zeros(n int) *Writer {
	for i := 0; i < n; i++ {
		w.buf = append(w.buf, 0)
	}
	return w
}

// PatchI32 overwrites the 4 big-endian bytes at position at (reserve-then-fill).
func (w *Writer) PatchI32(at int, v int32) {
	binary.BigEndian.PutUint32(w.buf[at:at+4], uint32(v))
}

// String mirrors RocketMQSerializable.writeStr: nil writes a zero length.
func (w *Writer) String(shortLength bool, s *string) *Writer {
	var b []byte
	if s != nil {
		b = []byte(*s)
	}
	if shortLength {
		w.U16(uint32(len(b)))
	} else {
		w.I32(int32(len(b)))
	}
	return w.Bytes(b)
}

// DecimalLong mirrors RocketMQSerializable.writeDecimalLong: 4-byte length
// prefix + decimal text.
func (w *Writer) DecimalLong(v int64) *Writer {
	lenAt := len(w.buf)
	w.I32(0)
	text := fmt.Sprintf("%d", v)
	w.Bytes([]byte(text))
	w.PatchI32(lenAt, int32(len(w.buf)-lenAt-4))
	return w
}

func (w *Writer) DecimalInt(v int32) *Writer {
	return w.DecimalLong(int64(v))
}

// Reader walks a byte slice with big-endian primitives; every read that runs
// past the end returns a DecodeError (buffer underflow).
type Reader struct {
	data []byte
	pos  int
}

func NewReader(data []byte) *Reader { return &Reader{data: data} }

func NewReaderAt(data []byte, pos int) *Reader { return &Reader{data: data, pos: pos} }

func (r *Reader) Pos() int       { return r.pos }
func (r *Reader) SetPos(pos int) { r.pos = pos }

func (r *Reader) Remaining() int { return len(r.data) - r.pos }

func (r *Reader) IsEmpty() bool { return r.Remaining() == 0 }

func (r *Reader) Slice(start, length int) ([]byte, error) {
	end := start + length
	if start < 0 || length < 0 || end > len(r.data) || end < start {
		return nil, DecodeError(fmt.Sprintf("buffer underflow: need %d bytes, have %d", end, len(r.data)))
	}
	return r.data[start:end], nil
}

func (r *Reader) take(n int) ([]byte, error) {
	part, err := r.Slice(r.pos, n)
	if err != nil {
		return nil, err
	}
	r.pos += n
	return part, nil
}

func (r *Reader) U8() (byte, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *Reader) I8() (int8, error) {
	v, err := r.U8()
	return int8(v), err
}

func (r *Reader) U16() (uint16, error) {
	b, err := r.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

// I16 reads the unsigned interpretation of a Java short (Python `code & 0xFFFF`).
func (r *Reader) I16() (int32, error) {
	v, err := r.U16()
	return int32(v), err
}

func (r *Reader) I32() (int32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func (r *Reader) U32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *Reader) I64() (int64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

func (r *Reader) Bytes(n int) ([]byte, error) {
	return r.take(n)
}

func (r *Reader) Rest() []byte {
	if r.pos > len(r.data) {
		r.pos = len(r.data)
	}
	rest := r.data[r.pos:]
	r.pos = len(r.data)
	return rest
}

// String mirrors RocketMQSerializable.readStr: zero length yields (nil, nil).
func (r *Reader) String(shortLength bool) (*string, error) {
	var n int
	if shortLength {
		v, err := r.U16()
		if err != nil {
			return nil, err
		}
		n = int(v)
	} else {
		raw, err := r.I32()
		if err != nil {
			return nil, err
		}
		if raw < 0 {
			return nil, DecodeError(fmt.Sprintf("negative string length %d", raw))
		}
		n = int(raw)
	}
	if n == 0 {
		return nil, nil
	}
	b, err := r.Bytes(n)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// DecimalLong mirrors RocketMQSerializable.readDecimalInt/Long: length prefix +
// decimal text; non-positive length yields 0.
func (r *Reader) DecimalLong() (int64, error) {
	n, err := r.I32()
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, nil
	}
	b, err := r.Bytes(int(n))
	if err != nil {
		return 0, err
	}
	text := string(b)
	var v int64
	if _, err := fmt.Sscanf(text, "%d", &v); err != nil {
		return 0, DecodeError(fmt.Sprintf("bad decimal %q: %v", text, err))
	}
	return v, nil
}

func (r *Reader) DecimalInt() (int32, error) {
	v, err := r.DecimalLong()
	return int32(v), err
}
