// live_compression_matrix is the Go leg of scripts/compression_matrix.sh: one
// deterministic payload (8192B, compressible), produced by port A and consumed
// by port B, with the verdict in the `match=1` field.
//
//	go run ./examples/live_compression_matrix send <topic> <group> <size> <namesrv> [codec]
//	go run ./examples/live_compression_matrix recv <topic> <group> <size> <namesrv>
//
// The payload is rebuilt locally by every port from the SAME recipe (one fixed
// line repeated, then truncated), so nothing has to be exchanged — and the
// verdict is a CRC of the decompressed body, not the CRC numbers printed by the
// two sides (those differ by 2^31 on purpose: see below).
//
// Why the matrix exists: a unit test proves "I can decode what I encoded". It
// cannot prove "port A's compressor produces bytes port B can inflate". Getting
// that wrong is SILENT data corruption — the compressed stream is handed out as
// the body with no error at all — so it needs a real broker and two real clients.
//
// Go's side of the codec story: the standard library has zlib only, and this
// module takes no third-party dependencies, so LZ4/ZSTD are reported as
// unsupported instead of being passed through (common/compression.go). That is
// deliberate and it is why the script skips every Go leg for those codecs.
//
// Exit codes follow the Python leg so the script can tell them apart:
// 0 ok / 1 generic failure (a failed send included — that is what Python's
// uncaught MQClientException exits with) / 2 bad codec or bad usage / 3 recv
// timeout. Keeping 2 strictly for "bad codec / bad usage" is the point: a plain
// send failure must NOT report 2 or it becomes indistinguishable from an
// unsupported codec.
package main

import (
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/client"
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// payloadLine is the seed the matrix uses in every port — it must be byte
// identical to Java's CompressProbe.buildPayload, otherwise the two sides compute
// different CRCs of "the same" payload and every leg fails for no reason.
const payloadLine = "rocketmq-compress-interop-payload-line-0123456789\n"

func buildPayload(size int) []byte {
	out := make([]byte, 0, size)
	for len(out) < size {
		out = append(out, payloadLine...)
	}
	return out[:size]
}

// crc is the standard CRC-32 (not Java's UtilAll.crc32, which masks the top
// bit) — matching the other four ports. Only `match` is compared across ports.
func crc(data []byte) uint32 { return crc32.ChecksumIEEE(data) }

func main() {
	if len(os.Args) < 2 {
		fmt.Println(usage)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "send":
		// send <topic> <group> <size> <namesrv> [codec]
		if len(os.Args) < 6 {
			fmt.Println(usage)
			os.Exit(2)
		}
		codec := "zlib"
		if len(os.Args) > 6 {
			codec = os.Args[6]
		}
		os.Exit(doSend(os.Args[2], os.Args[3], atoi(os.Args[4]), os.Args[5], codec))
	case "recv":
		// recv <topic> <group> <size> <namesrv>
		if len(os.Args) < 6 {
			fmt.Println(usage)
			os.Exit(2)
		}
		os.Exit(doRecv(os.Args[2], os.Args[3], atoi(os.Args[4]), os.Args[5]))
	default:
		fmt.Println(usage)
		os.Exit(2)
	}
}

const usage = `usage:
  live_compression_matrix send <topic> <group> <size> <namesrv> [codec]
  live_compression_matrix recv <topic> <group> <size> <namesrv>`

// compressType maps the matrix codec name to the sysFlag type bits, using the
// same values as Java CompressionType.findByValue (1=LZ4, 2=ZSTD, 3=ZLIB).
// LZ4/ZSTD resolve to a type here (rather than being rejected as unknown) so
// doSend can turn them into an explicit "unsupported codec" failure below.
func compressType(codec string) (int32, bool) {
	switch codec {
	case "zlib":
		return common.MessageSysFlagCompressionZlib >> common.CompressionTypeShift, true
	case "lz4":
		return common.MessageSysFlagCompressionLz4 >> common.CompressionTypeShift, true
	case "zstd":
		return common.MessageSysFlagCompressionZstd >> common.CompressionTypeShift, true
	}
	return 0, false
}

func doSend(topic, group string, size int, ns, codec string) int {
	ctype, ok := compressType(codec)
	if !ok {
		fmt.Printf("SEND_FAIL unknown codec=%s\n", codec)
		return 2
	}
	payload := buildPayload(size)
	// Pre-flight the codec — the producer must NOT be relied on to catch this.
	// tryToCompressMessage swallows the error and sends the body UNCOMPRESSED
	// (Go mirrors Java exactly; see client/producer.go), so a naive send reports
	// SEND_OK for LZ4/ZSTD while nothing was compressed. A leg claiming "LZ4
	// interop" would then pass vacuously, because the payload arrives intact and
	// the CRC matches. Reject it here instead, as exit 2 = bad codec.
	if _, err := common.Compress(payload, ctype, common.DefaultCompressLevel); err != nil {
		fmt.Printf("SEND_FAIL unsupported codec=%s: %v\n", codec, err)
		return 2
	}

	producer, err := client.NewDefaultMQProducer(group)
	if err != nil {
		fmt.Printf("SEND_FAIL producer=%v\n", err)
		return 1
	}
	producer.SetNameServerAddr(ns)
	producer.SetSendMsgTimeout(10_000)
	// The threshold is compressMsgBodyOverHowmuch (4096 by default); the matrix
	// always passes 8192 so the body really is compressed. Anything below the
	// threshold would make the whole leg vacuous.
	producer.SetCompressType(ctype)
	if err := producer.Start(); err != nil {
		fmt.Printf("SEND_FAIL start=%v\n", err)
		return 1
	}
	sr, err := producer.Send(common.NewMessage(topic, payload))
	producer.Shutdown()
	if err != nil {
		fmt.Printf("SEND_FAIL send=%v\n", err)
		return 1
	}
	fmt.Printf("SEND_OK codec=%s len=%d crc32=%d msgId=%s\n", codec, len(payload), crc(payload), sr.MsgID)
	return 0
}

func doRecv(topic, group string, size int, ns string) int {
	payload := buildPayload(size)

	msg := recvOne(ns, topic, group, 60*time.Second)
	if msg == nil {
		fmt.Println("RECV_TIMEOUT")
		return 3
	}
	body := msg.Body
	// The decode already ran the decompressor (DefaultDecodeOptions), so `body`
	// is the inflated payload — if it were the raw stream the length check would
	// catch it, and if the flag had not been cleared the CRC would not match.
	ok := len(body) == size && crc(body) == crc(payload)
	match := 0
	if ok {
		match = 1
	}
	fmt.Printf("RECV_OK len=%d crc32=%d storeSize=%d match=%d\n", len(body), crc(body), msg.StoreSize, match)
	if ok {
		return 0
	}
	return 1
}

// collector is the concurrent listener: it keeps the first message and returns
// success so the broker does not re-deliver it.
type collector struct {
	mu   sync.Mutex
	msgs []*common.MessageExt
}

func (c *collector) ConsumeMessage(msgs []*common.MessageExt, _ *client.ConsumeConcurrentlyContext) client.ConsumeConcurrentlyStatus {
	c.mu.Lock()
	c.msgs = append(c.msgs, msgs...)
	c.mu.Unlock()
	return client.ConsumeSuccess
}

func (c *collector) take() *common.MessageExt {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.msgs) == 0 {
		return nil
	}
	return c.msgs[0]
}

func recvOne(ns, topic, group string, timeout time.Duration) *common.MessageExt {
	c, err := client.NewDefaultMQPushConsumer(group)
	if err != nil {
		return nil
	}
	c.SetNameServerAddresses([]string{ns})
	c.SetInstanceName(fmt.Sprintf("go-compress-%d", time.Now().UnixNano()))
	c.SetConsumeThreadMin(2)
	c.SetConsumeThreadMax(4)
	// CONSUME_FROM_FIRST_OFFSET: the sender writes before this consumer starts.
	c.SetConsumeFromWhere(client.ConsumeFromWhereFirstOffset)

	l := &collector{}
	if err := c.SetMessageListener(l); err != nil {
		return nil
	}
	if err := c.Subscribe(topic, "*"); err != nil {
		return nil
	}
	if err := c.Start(); err != nil {
		return nil
	}
	defer c.Shutdown()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if m := l.take(); m != nil {
			return m
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}

func atoi(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0
		}
		n = n*10 + int(r-'0')
	}
	return n
}
