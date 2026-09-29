package common

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

var monotonicStart = time.Now()

// MonotonicMillis is a monotonic millisecond clock (never goes backwards when
// the wall clock is adjusted); send-retry budgets and broker latency avoidance
// depend on it.
func MonotonicMillis() float64 {
	return time.Since(monotonicStart).Seconds() * 1000
}

// NanoTime mirrors Java System.nanoTime: arbitrary origin, comparisons only.
func NanoTime() int64 {
	return time.Since(monotonicStart).Nanoseconds()
}

func CurrentTimeMillis() int64 { return time.Now().UnixMilli() }

func CurrentTimeSeconds() int64 { return CurrentTimeMillis() / 1000 }

func ComputeElapseTimeMillis(lastTime int64) int64 { return CurrentTimeMillis() - lastTime }

// JavaStringHash mirrors java.lang.String.hashCode: h = 31*h + ch with 32-bit
// wrap (Go int32 arithmetic wraps silently). Reference vectors: TagA=2598919,
// TagB=2598920, P=80, PA=2545, * = 42.
func JavaStringHash(s string) int32 {
	var h int32
	for _, ch := range s {
		h = h*31 + int32(ch)
	}
	return h
}

// Crc32 is the standard IEEE CRC-32 (b"a"=0xE8B7BE43, b"abc"=0x352441C2).
func Crc32(data []byte) uint32 { return crc32.ChecksumIEEE(data) }

// Bytes2String mirrors Java UtilAll.bytes2string: per-byte UPPERCASE hex
// (msgId casing depends on it).
func Bytes2String(bytes []byte) string {
	return strings.ToUpper(fmt.Sprintf("%x", bytes))
}

// String2Bytes mirrors Java UtilAll.string2bytes: hex text -> bytes (not UTF-8
// decoding). Invalid or odd-length input yields nil.
func String2Bytes(hex string) []byte {
	if hex == "" {
		return nil
	}
	b, err := hexDecodeString(hex)
	if err != nil {
		return nil
	}
	return b
}

func hexDecodeString(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd length hex string")
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(out); i++ {
		hi, ok := hexNibble(s[2*i])
		if !ok {
			return nil, fmt.Errorf("bad hex char")
		}
		lo, ok := hexNibble(s[2*i+1])
		if !ok {
			return nil, fmt.Errorf("bad hex char")
		}
		out[i] = hi<<4 | lo
	}
	return out, nil
}

func hexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

func Offset2Filename(offset int64) string { return fmt.Sprintf("%020d", offset) }

// TimeToHumanString formats in the local timezone; ts <= 0 becomes "-".
func TimeToHumanString(ts int64) string {
	if ts <= 0 {
		return "-"
	}
	return time.UnixMilli(ts).Format("2006-01-02 15:04:05")
}

func IsBlank(s string) bool    { return strings.TrimSpace(s) == "" }
func IsNotBlank(s string) bool { return !IsBlank(s) }

func Pid() int { return os.Getpid() }

// UserHome checks HOME first, then USERPROFILE (Windows).
func UserHome() string {
	if v := os.Getenv("HOME"); v != "" {
		return v
	}
	return os.Getenv("USERPROFILE")
}

func IsIPv4(addr string) bool {
	ip := net.ParseIP(addr)
	return ip != nil && ip.To4() != nil && !strings.Contains(addr, ":")
}

func IsIPv6(addr string) bool {
	ip := net.ParseIP(addr)
	return ip != nil && strings.Contains(addr, ":")
}

// LocalIP probes the outbound IP via a UDP socket (no packet is actually
// sent); falls back to 127.0.0.1.
func LocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		return addr.IP.String()
	}
	return "127.0.0.1"
}

// ParseAddr splits host:port, accepting IPv6 written as [::1]:9876.
func ParseAddr(addr string) (string, string) {
	if rest, ok := strings.CutPrefix(addr, "["); ok {
		if host, tail, found := strings.Cut(rest, "]"); found {
			return host, strings.TrimPrefix(tail, ":")
		}
	}
	if i := strings.LastIndexByte(addr, ':'); i >= 0 {
		return addr[:i], addr[i+1:]
	}
	return addr, ""
}

// CurrentDayMillis is the milliseconds elapsed in the local day (the 4-byte
// segment of Java createUniqID: hour/min/sec + millis).
func CurrentDayMillis() uint32 {
	now := time.Now()
	secs := now.Hour()*3600 + now.Minute()*60 + now.Second()
	return uint32(secs*1000 + now.Nanosecond()/1e6)
}

var uniqIDCounter uint32

// CreateUniqID mirrors Java MessageClientIDSetter.createUniqID:
// IP(4B; IPv6 16B) + PID(2B) + class-hash(4B) + day-millis(4B) + counter(2B),
// rendered as UPPERCASE hex — exactly 32 chars under IPv4.
func CreateUniqID() string {
	counter := atomic.AddUint32(&uniqIDCounter, 1)

	var b []byte
	ip := LocalIP()
	switch {
	case IsIPv4(ip):
		b = append(b, net.ParseIP(ip).To4()...)
	case IsIPv6(ip):
		b = append(b, net.ParseIP(ip).To16()...)
	default:
		b = append(b, ip...)
	}
	b = binary.BigEndian.AppendUint16(b, uint16(Pid()))
	b = binary.BigEndian.AppendUint32(b, uint32(JavaStringHash("RocketMQClient")))
	b = binary.BigEndian.AppendUint32(b, CurrentDayMillis())
	b = binary.BigEndian.AppendUint16(b, uint16(counter))
	return Bytes2String(b)
}
