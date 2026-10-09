// live_compression is the Node.js leg of scripts/compression_matrix.sh: one
// deterministic payload (8192B, compressible) produced by port A and consumed by
// port B, with the verdict carried by the `match=1` field.
//
//   node --experimental-strip-types examples/live_compression.ts send <topic> <group> <size> <namesrv> [codec]
//   node --experimental-strip-types examples/live_compression.ts recv <topic> <group> <size> <namesrv>
//
// Every port rebuilds the payload locally from the SAME recipe (one fixed line
// repeated, then truncated) — byte-identical to Java's CompressProbe.buildPayload
// — so nothing has to be exchanged. Only `match` is compared across ports: the
// printed CRC numbers differ by 2^31 on purpose, because Java's UtilAll.crc32
// masks the top bit while this port uses the standard CRC-32.
//
// Why the matrix exists at all: a unit test proves "I decode what I encoded". It
// cannot prove "port A's compressor produces bytes port B can inflate", and a
// wrong answer there is SILENT data corruption — the compressed stream is handed
// out as the body without an error. That needs a real broker and two real
// clients.
//
// Node's codec surface: zlib and zstd come from node:zlib (Node's own stdlib),
// LZ4 is the hand-written frame codec — see src/common/compress.ts. ZSTD uses
// the real binding on Node >= 23.8 and the store-only frame codec below that,
// which is why the sender prints its own storeSize: a leg where nothing was
// compressed still passes the CRC, so the number is evidence, not the verdict.
//
// Exit codes match the Python/Go/PHP legs: 0 ok / 1 ordinary failure (including
// a failed send) / 2 bad codec or bad usage / 3 recv timeout. 2 stays strictly
// for "bad codec / bad usage" so a send failure is never mistaken for an
// unsupported codec.
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import { Message } from '../src/common/message.ts';
import { PullStatus } from '../src/client/consumer_result.ts';
import { SendStatus } from '../src/client/send_result.ts';
import { compressionTypeByName } from '../src/common/compress.ts';
import { crc32 } from '../src/common/utilAll.ts';

const PAYLOAD_LINE = 'rocketmq-compress-interop-payload-line-0123456789\n';

function buildPayload(size: number): Buffer {
  const line = Buffer.from(PAYLOAD_LINE, 'utf8');
  const out = Buffer.allocUnsafe(size);
  let written = 0;
  while (written < size) {
    const n = Math.min(line.length, size - written);
    line.copy(out, written, 0, n);
    written += n;
  }
  return out;
}

const USAGE = `usage:
  live_compression.ts send <topic> <group> <size> <namesrv> [codec]
  live_compression.ts recv <topic> <group> <size> <namesrv>`;

function arg(i: number | undefined, def: string): string {
  return i != null && i !== '' ? i : def;
}

function toInt(s: string | undefined): number {
  const n = Number(s);
  return Number.isFinite(n) ? Math.floor(n) : 0;
}

async function doSend(topic: string, group: string, size: number, ns: string, codec: string): Promise<number> {
  let ctype: number;
  try {
    ctype = compressionTypeByName(codec.toUpperCase());
  } catch {
    console.log(`SEND_UNSUPPORTED codec=${codec} (node port supports zlib|lz4|zstd)`);
    return 2;
  }
  const payload = buildPayload(size);
  const producer = new DefaultMQProducer(group);
  producer.setNamesrvAddr(ns);
  producer.setSendMsgTimeout(10000);
  producer.setCompressType(ctype);
  // 8192 >= compressMsgBodyOverHowmuch (4096), so the body really is compressed.
  try {
    await producer.start();
    const r = await producer.send(new Message(topic, payload));
    if (r.sendStatus !== SendStatus.SEND_OK) {
      console.log(`SEND_FAILED status=${r.sendStatus}`);
      return 1;
    }
    console.log(`SEND_OK codec=${codec} len=${payload.length} crc32=${crc32(payload)} msgId=${r.msgId}`);
    return 0;
  } catch (e) {
    console.log(`SEND_FAILED ${(e as Error).message}`);
    return 1;
  } finally {
    try { producer.shutdown(); } catch { /* best effort */ }
  }
}

async function doRecv(topic: string, group: string, size: number, ns: string): Promise<number> {
  const payload = buildPayload(size);
  const expect = crc32(payload);
  const consumer = new DefaultMQPullConsumer(group);
  consumer.setNamesrvAddr(ns);
  // subscribe() MUST precede start() (cross-port rule: the pull consumer
  // registers its subscription at heartbeat time and start() requires it).
  consumer.subscribe(topic, '*');
  const deadline = Date.now() + 120000;
  try {
    await consumer.start();
    while (Date.now() < deadline) {
      let mqs;
      try {
        mqs = await consumer.fetchMessageQueuesInBalance(topic);
      } catch {
        await new Promise((r) => setTimeout(r, 500));
        continue; // route not visible yet
      }
      for (const mq of mqs) {
        let offset = 0; // the sender wrote before this consumer started
        for (let round = 0; round < 40; round++) {
          // SHORT poll: suspend stays false (rule #10).
          const r = await consumer.pull(mq, '*', offset, 32);
          if (r.status !== PullStatus.FOUND) {
            if (r.status === PullStatus.OFFSET_ILLEGAL) { offset = r.nextBeginOffset; continue; }
            break;
          }
          for (const m of r.msgFoundList) {
            // The decode already ran the decompressor and cleared
            // COMPRESSED_FLAG, so getBody() is the inflated payload: a body
            // that came back still compressed would fail both the size check
            // and the CRC below, never match by accident.
            const plain: Buffer = m.getBody() || Buffer.alloc(0);
            if (plain.length !== size) continue; // another leg's message
            const got = crc32(plain);
            if (got === expect) {
              console.log(`RECV_OK len=${plain.length} crc32=${got} storeSize=${m.getStoreSize()} match=1`);
              return 0;
            }
            // Read a message of the right size whose CRC differs: that IS the
            // silent corruption this matrix is built to catch.
            console.log(`RECV_BAD len=${plain.length} crc32=${got} expect=${expect} storeSize=${m.getStoreSize()} match=0`);
            return 1;
          }
          offset = r.nextBeginOffset;
        }
      }
      await new Promise((r) => setTimeout(r, 500));
    }
    console.log(`RECV_TIMEOUT expect=${expect} size=${size}`);
    return 3;
  } catch (e) {
    console.log(`RECV_FAILED ${(e as Error).message}`);
    return 1;
  } finally {
    try { await consumer.shutdown(); } catch { /* best effort */ }
  }
}

const mode = process.argv[2];
const [, , , topic, group, sizeArg, nsArg, codecArg] = process.argv;
const size = toInt(sizeArg);
const ns = arg(nsArg, process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');

if (!topic || !group || size <= 0 || (mode !== 'send' && mode !== 'recv')) {
  console.log(USAGE);
  process.exit(2);
}

const code = mode === 'send'
  ? await doSend(topic, group, size, ns, arg(codecArg, 'zlib'))
  : await doRecv(topic, group, size, ns);
process.exit(code);
