// live_trace_consume is the Node.js consume-side message-trace probe against a
// real RocketMQ 5.x cluster (broker needs traceTopicEnable=true).
//
//   node --experimental-strip-types examples/live_trace_consume.ts --ns 127.0.0.1:9876
//
// The Java client (ConsumeMessageTraceHookImpl + AsyncTraceDispatcher) emits
// SubBefore before every delivery and SubAfter after it. A RECONSUME_LATER
// delivery yields SubAfter(success=false, contextCode=4 == ConsumeReturnType.FAILED)
// and the %RETRY% redelivery yields SubBefore(retryTimes=1) +
// SubAfter(success=true, contextCode=0). This probe asserts the whole failure
// ladder off RMQ_SYS_TRACE_TOPIC, decoded with the client's own
// DecodeTraceDataString.
//
// Closing line `PASS=<n> FAIL=<n>`; the process exits non-zero on any FAIL.
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';
import { DecodeTraceDataString, TraceContext } from '../src/client/trace_context.ts';
import { Message } from '../src/common/message.ts';
import { MixAll } from '../src/common/mixAll.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = Date.now();
const TOPIC = `NodeTraceConsume_${STAMP}`;
const GROUP = `GID_Node_trace_${STAMP}`;
const TARGET = `fail-me-${STAMP}`;

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface Delivery { body: string; recon: number; }

function summarize(ctxs: TraceContext[]): string {
  return ctxs.map((r) => `(${r.isSuccess},${r.contextCode})`).join(' ');
}

async function main(): Promise<void> {
  const producer = new DefaultMQProducer('GID_Node_trace_prod');
  producer.setNamesrvAddr(NS);
  producer.start();
  await producer.createTopic(TOPIC, producer.createTopicKey, 2);
  await sleep(3000);

  // Trace reader first (FIRST_OFFSET over RMQ_SYS_TRACE_TOPIC).
  const traceRecords: TraceContext[] = [];
  const traceReader = new DefaultMQPushConsumer(`${GROUP}_reader`);
  traceReader.setNamesrvAddr(NS);
  traceReader.setConsumeFromWhere('CONSUME_FROM_FIRST_OFFSET');
  traceReader.subscribe(MixAll.TRACE_TOPIC, '*');
  traceReader.registerMessageListenerConcurrently((msgs: any[]) => {
    for (const m of msgs) {
      try {
        traceRecords.push(...DecodeTraceDataString((m.getBody() || Buffer.alloc(0)).toString('utf8')));
      } catch (e: any) {
        console.log(`WARN decode trace failed: ${e?.message ?? e}`);
      }
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await traceReader.start();
  await sleep(500);

  // Business consumer with consume-side trace enabled.
  const deliveries: Delivery[] = [];
  const consumer = new DefaultMQPushConsumer(GROUP);
  consumer.setNamesrvAddr(NS);
  consumer.subscribe(TOPIC, '*');
  consumer.setEnableTrace(true);
  consumer.registerMessageListenerConcurrently((msgs: any[]) => {
    let fail = false;
    for (const m of msgs) {
      const body = (m.getBody() || Buffer.alloc(0)).toString('utf8');
      deliveries.push({ body, recon: m.getReconsumeTimes() });
      if (body === TARGET && m.getReconsumeTimes() === 0) fail = true;
    }
    return fail ? ConsumeConcurrentlyStatus.RECONSUME_LATER : ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await consumer.start();

  // B5: consumer first, wait for the real assignment, then send.
  const assignDeadline = Date.now() + 60_000;
  while (Date.now() < assignDeadline) {
    const mine = (consumer.assigned || []).filter((q: any) => q.getTopic() === TOPIC);
    if (mine.length > 0) break;
    await sleep(500);
  }
  console.log(`assigned=${(consumer.assigned || []).filter((q: any) => q.getTopic() === TOPIC).length}`);

  const result = await producer.send(new Message(TOPIC, Buffer.from(TARGET), 'TagA', 'KeyFail'));
  const uniq = result.msgId;
  console.log(`sent uniq_key=${uniq} offset=${result.offsetMsgId}`);

  const ofUniq = (r: TraceContext) => r.traceBeans.length > 0 && r.traceBeans[0].msgId === uniq;
  const recordsOf = () => traceRecords.filter(ofUniq);

  async function waitFor(timeoutMs: number, cond: () => boolean): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (cond()) return true;
      await sleep(300);
    }
    return cond();
  }

  const first = await waitFor(25_000, () => deliveries.some((d) => d.body === TARGET));
  check('F1 the first delivery reached the listener', first, `deliveries=${deliveries.length}`);

  const failedAfter = await waitFor(25_000, () => recordsOf().some(
    (r) => r.traceType === 'SubAfter' && !r.isSuccess && r.contextCode === 4));
  const before1 = recordsOf().filter((r) => r.traceType === 'SubBefore');
  const after1 = recordsOf().filter((r) => r.traceType === 'SubAfter');
  check('F2 SubBefore(retryTimes=0) for the failed message',
    before1.some((r) => r.traceBeans[0].retryTimes === 0), `before=${before1.length}`);
  check('F3 SubAfter(success=false, contextCode=4 FAILED)', failedAfter, summarize(after1));

  const retried = await waitFor(60_000, () => deliveries.some((d) => d.recon >= 1));
  const finalAfter = await waitFor(25_000, () => recordsOf().some(
    (r) => r.traceType === 'SubAfter' && r.isSuccess && r.contextCode === 0));
  await consumer.shutdown();  // shutdown also flushes the dispatcher tail
  producer.shutdown();
  await sleep(1000);

  const before2 = recordsOf().filter((r) => r.traceType === 'SubBefore');
  const after2 = recordsOf().filter((r) => r.traceType === 'SubAfter');
  check('F4 the %RETRY% redelivery was consumed', retried,
    `deliveries=${deliveries.map((d) => `(recon=${d.recon})`).join(' ')}`);
  check('F5 SubBefore(retryTimes=1) for the same message id',
    before2.some((r) => r.traceBeans[0].retryTimes === 1),
    `retryTimes=${before2.map((r) => r.traceBeans[0].retryTimes).join(',')}`);
  check('F6 SubAfter(success=true, contextCode=0) for the retry', finalAfter, summarize(after2));

  const reqIds = new Set(before2.map((r) => r.requestId));
  check('F7 every SubAfter requestId was preceded by a SubBefore',
    after2.every((r) => reqIds.has(r.requestId)),
    `before=${before2.length} after=${after2.length}`);

  await traceReader.shutdown();

  console.log(`\n############ PASS=${passCount} FAIL=${failCount} ############`);
  process.exit(failCount === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error('FATAL', e);
  process.exit(2);
});
