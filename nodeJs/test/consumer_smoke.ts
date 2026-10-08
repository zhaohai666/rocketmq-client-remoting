// Consumer-layer smoke test: allocation strategies, ProcessQueue lifecycle,
// LocalFileOffsetStore round-trip (fastjson2 compact object-key shape),
// trace codec round-trip (incl. the keys-less SubBefore 7-segment quirk), and
// consumer construct sanity. Run: node --experimental-strip-types test/consumer_smoke.ts
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { MessageQueue, MessageExt } from '../src/common/message.ts';
import {
  AllocateMessageQueueAveragely, AllocateMessageQueueAveragelyByCircle,
  AllocateMessageQueueConsistentHash, ALLOCATE_STRATEGIES,
} from '../src/client/allocate.ts';
import { ProcessQueue } from '../src/client/process_queue.ts';
import { LocalFileOffsetStore, mqKey, ReadOffsetMode } from '../src/client/offset_store.ts';
import {
  TraceContext, TraceBean, TraceType, EncodeTraceContext, DecodeTraceDataString,
} from '../src/client/trace_context.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQAdminExt, string2Properties } from '../src/client/admin.ts';
import { ConsumerStatsManager } from '../src/client/consumer_stats.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean) {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name}`); }
}

function makeMqs(topic: string, broker: string, n: number): MessageQueue[] {
  const out: MessageQueue[] = [];
  for (let i = 0; i < n; i++) out.push(new MessageQueue(topic, broker, i));
  return out;
}

function makeMsg(topic: string, broker: string, queueId: number, queueOffset: number): MessageExt {
  const m = new MessageExt(topic, Buffer.from(`body-${queueOffset}`), 'TagA', 'k1');
  m.brokerName = broker;
  m.queueId = queueId;
  m.queueOffset = queueOffset;
  return m;
}

console.log('== allocation ==');
{
  const mqAll = makeMqs('T', 'broker-a', 8);
  const cidAll = ['c1', 'c2'];
  const avg = new AllocateMessageQueueAveragely();
  const r1 = avg.allocate('g', 'c1', mqAll, cidAll);
  const r2 = avg.allocate('g', 'c2', mqAll, cidAll);
  check('AVG 8q/2c -> 4+4', r1.length === 4 && r2.length === 4);
  check('AVG disjoint + total 8',
    new Set([...r1.map((q) => q.getQueueId()), ...r2.map((q) => q.getQueueId())]).size === 8);

  const circle = new AllocateMessageQueueAveragelyByCircle();
  const c1 = circle.allocate('g', 'c1', mqAll, cidAll).map((q) => q.getQueueId());
  check('CIRCLE interleaves', c1.join(',') === '0,2,4,6');

  // 8 queues / 3 clients: AVG gives 3+3+2
  const cid3 = ['c1', 'c2', 'c3'];
  const sizes = cid3.map((c) => avg.allocate('g', c, mqAll, cid3).length);
  check('AVG 8q/3c -> 3+3+2', sizes.join(',') === '3,3,2');

  const chash = new AllocateMessageQueueConsistentHash();
  const all = new Set<string>();
  for (const c of cid3) {
    for (const q of chash.allocate('g', c, mqAll, cid3)) all.add(q.getTopic() + '@' + q.getQueueId());
  }
  check('CONSISTENT_HASH covers all 8 queues', all.size === 8);
  check('ALLOCATE_STRATEGIES registry has 6 entries (Java factory set)',
    Object.keys(ALLOCATE_STRATEGIES).length === 6);
}

console.log('== ProcessQueue ==');
{
  const pq = new ProcessQueue();
  const msgs = [0, 1, 2, 3].map((i) => makeMsg('T', 'b', 0, i));
  check('putMessage true (new)', pq.putMessage(msgs) === true);
  check('putMessage duplicate false', pq.putMessage([msgs[0]]) === false);
  check('queueOffsetMaxValue = 3', pq.queueOffsetMaxValue() === 3);

  const batch = pq.takeBatch(2);
  check('takeBatch(2) -> offsets 0,1', batch.map((m) => m.queueOffset).join(',') === '0,1');

  pq.completeBatch([msgs[0]]);
  const floor = pq.minRemainingExcept([msgs[1]]);
  check('minRemainingExcept = 2', floor === 2);
  pq.requeueBatch([msgs[1]]);
  check('requeue restores batch', pq.takeBatch(4).length === 3);

  const removed = pq.removeMessage(msgs.slice(1));
  check('removeMessage on emptied buffer returns -1 (Go/Java parity)', removed === -1);
  check('bufferedOffsetSpan after remove = (0,0,0)',
    JSON.stringify(pq.bufferedOffsetSpan()) === '[0,0,0]');
}

console.log('== LocalFileOffsetStore ==');
{
  const base = fs.mkdtempSync(path.join(os.tmpdir(), 'rmq_offsets_'));
  const store = new LocalFileOffsetStore('cid1', 'g1', base);
  const mq = new MessageQueue('T', 'broker-a', 0);
  store.updateOffset(mq, 42, false);
  await store.persistAll([mq]);
  // Java fastjson2 writes the compact object-key shape; keys are JSON objects.
  const file = path.join(base, 'cid1', 'g1', 'offsets.json');
  const raw = JSON.parse(fs.readFileSync(file, 'utf8'));
  const keys = Object.keys(raw.offsetTable);
  check('file key is the fastjson2 object-key shape',
    keys.length === 1 && JSON.parse(keys[0]).topic === 'T' && JSON.parse(keys[0]).queueId === 0);

  const store2 = new LocalFileOffsetStore('cid1', 'g1', base);
  await store2.load();
  const back = await store2.readOffset(mq, ReadOffsetMode.READ_FROM_STORE);
  check('load round-trips offset 42', back === 42);

  // Corrupt file → empty table + warning, not an error.
  fs.writeFileSync(file, 'NOT JSON{{{');
  const store3 = new LocalFileOffsetStore('cid1', 'g1', base);
  await store3.load();
  check('corrupt file degrades to empty',
    (await store3.readOffset(mq, ReadOffsetMode.READ_FROM_STORE)) === -1);
  fs.rmSync(base, { recursive: true, force: true });
}

console.log('== trace codec ==');
{
  // Pub round-trip
  const ctx = new TraceContext();
  ctx.traceType = TraceType.PUB;
  ctx.groupName = 'G';
  ctx.timeStamp = 1700000000000;
  ctx.costTime = 12;
  ctx.isSuccess = true;
  const bean = new TraceBean();
  bean.topic = 'T';
  bean.msgId = 'UNIQ_KEY_1';
  bean.tags = 'TagA';
  bean.keys = 'k1';
  bean.storeHost = '127.0.0.1:10911';
  bean.bodyLength = 6;
  ctx.traceBeans = [bean];
  const encoded = EncodeTraceContext(ctx);
  check('encode produces transData with \\x02 terminator', encoded != null && encoded.transData.endsWith('\u0002'));
  const decoded = DecodeTraceDataString(encoded!.transData);
  check('Pub decode: 1 context', decoded.length === 1);
  check('Pub decode round-trip topic/msgId',
    decoded[0].traceType === 'Pub' && decoded[0].traceBeans[0].topic === 'T'
    && decoded[0].traceBeans[0].msgId === 'UNIQ_KEY_1');

  // SubBefore WITHOUT keys → 7 segments (Java AIOOBE quirk) must still decode.
  const sub = new TraceContext();
  sub.traceType = TraceType.SUB_BEFORE;
  sub.groupName = 'G';
  sub.requestId = 'rid';
  const subBean = new TraceBean();
  subBean.topic = 'T';
  subBean.retryTimes = 0;
  subBean.keys = ''; // no keys
  sub.traceBeans = [subBean];
  const enc2 = EncodeTraceContext(sub);
  // Encoder always writes 8 fields (type..keys) → 7 `\x01` separators. The
  // Java "7 segments" quirk is a DECODE-side effect: String#split drops the
  // trailing empty keys segment, leaving 7.
  const segs = enc2!.transData.split('\u0001').length;
  check('keys-less SubBefore emits 8 fields (7 separators)', segs === 8);
  const dec2 = DecodeTraceDataString(enc2!.transData);
  check('keys-less SubBefore decodes without dying', dec2.length === 1);
  check('decoded keys-less SubBefore reads missing segment as ""',
    dec2.length === 1 && dec2[0].traceBeans[0].keys === '');
}

console.log('== consumer construct sanity ==');
{
  const consumer = new DefaultMQPushConsumer('MyGroup');
  consumer.subscribe('T', '*');
  consumer.setNamesrvAddr('127.0.0.1:9876');
  consumer.setConsumeThreadNums(4);
  check('subscribe registers expression', consumer.subscription.has('T'));
  check('statsManager null before start', consumer._statsManager == null);

  const mgr = new ConsumerStatsManager();
  const item = mgr.topicAndGroupConsumeOKTPS.getAndCreate('T@MyGroup');
  item.addValue(5, 1); item.sample();
  (item as any)._minute.unshift({ ts: Date.now() - 10000, value: 0, times: 0 });
  const cs = mgr.consumeStatus('MyGroup', 'T');
  check('manager consumeOKTPS = 0.5', Math.abs(cs.consumeOKTPS - 0.5) < 0.01);
}

console.log('== admin construct sanity ==');
{
  const admin = new DefaultMQAdminExt();
  check('admin default timeout 20000 (Java)', admin.timeoutMillis === 20000);
  check('string2Properties comments/whitespace/colon',
    JSON.stringify(string2Properties('a=1\n# c\nb = x y\nc:z\n')) === '{"a":"1","b":"x y","c":"z"}');
  // 307 statusTable pre-start → zero-filled ConsumeStatus per subscribed topic.
  const consumer = new DefaultMQPushConsumer('G2');
  consumer.subscribe('TT', '*');
  const info = (consumer as any).buildConsumerRunningInfo(false);
  check('307 statusTable has zero ConsumeStatus',
    JSON.stringify(info.statusTable.TT) === '{"pullRT":0,"pullTPS":0,"consumeRT":0,"consumeOKTPS":0,"consumeFailedTPS":0,"consumeFailedMsgs":0}');
  check('307 userConsumerInfo = {}', JSON.stringify(info.userConsumerInfo) === '{}');
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
