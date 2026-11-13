// access_channel_smoke — Java ClientConfig#setAccessChannel parity.
//
//   node --experimental-strip-types test/access_channel_smoke.ts
//
// Java reference (hzhai666 5.x): ClientConfig.java:53 defaults LOCAL, :444
// exposes setAccessChannel; DefaultMQProducer.java:401 / DefaultMQPushConsumer
// .java:781 pass the facade's channel into the trace dispatcher start; the
// consume hook contexts (ConsumeMessageConcurrentlyService.java:410 et al.)
// carry the facade value too; AsyncTraceDispatcher.java:311-322 routes CLOUD
// records to "rmq_sys_TRACE_DATA_" + regionId instead of RMQ_SYS_TRACE_TOPIC,
// falling back to the dispatcher's own channel when a context carries none.
//
// LOCAL is the default and keeps every existing behavior: trace -> RMQ_SYS_TRACE_TOPIC.
import assert from 'node:assert';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { AsyncTraceDispatcher } from '../src/client/trace_dispatcher.ts';
import { FilterMessageHook } from '../src/client/hook.ts';
import { PopStatus } from '../src/client/pop_api.ts';
import { MessageExt } from '../src/common/message.ts';
import { processPopResult } from '../src/client/pop_consumer.ts';
import { TraceContext } from '../src/client/trace_context.ts';

let checks = 0;
let failed = 0;
function check(name: string, ok: boolean, detail?: string): void {
  checks++;
  if (ok) { console.log(`  ok    ${name}`); }
  else { failed++; console.log(`  FAIL  ${name}${detail ? ` — ${detail}` : ''}`); }
}

// ---------------------------------------------------------------------------
// 1. facade surface: producer + push consumer default LOCAL, accept CLOUD
// ---------------------------------------------------------------------------
function testFacadeSurface(): void {
  console.log('== facade accessChannel surface ==');
  const p = new DefaultMQProducer('GID_AC_P');
  check('producer defaults to LOCAL', p.getAccessChannel() === 'LOCAL');
  check('setAccessChannel is chainable', p.setAccessChannel('CLOUD') === p);
  check('producer round-trips CLOUD', p.getAccessChannel() === 'CLOUD');
  let threw = false;
  try { p.setAccessChannel('REGION' as any); } catch { threw = true; }
  check('unknown channel rejected', threw);
  check('rejected set keeps previous value', p.getAccessChannel() === 'CLOUD');

  const c = new DefaultMQPushConsumer('GID_AC_C');
  check('push consumer defaults to LOCAL', c.getAccessChannel() === 'LOCAL');
  c.setAccessChannel('CLOUD');
  check('push consumer round-trips CLOUD', c.getAccessChannel() === 'CLOUD');
  console.log('facade surface OK');
}

// ---------------------------------------------------------------------------
// 2. dispatcher start(nameSrv, accessChannel) remembers the channel; a blank
//    repeat start cannot silently reset CLOUD (Java :151-159)
// ---------------------------------------------------------------------------
async function testDispatcherStartChannel(): Promise<void> {
  console.log('== dispatcher start channel ==');
  const d = new AsyncTraceDispatcher('GID_AC_D1', 'PRODUCER');
  check('dispatcher defaults to LOCAL', d.accessChannel === 'LOCAL');
  await d.start('127.0.0.1:9876', 'CLOUD');
  check('start(...,CLOUD) switches the channel', d.accessChannel === 'CLOUD');
  await d.start('127.0.0.1:9876');
  check('repeat start without channel keeps CLOUD', d.accessChannel === 'CLOUD');
  await d.shutdown();

  const d2 = new AsyncTraceDispatcher('GID_AC_D2', 'PRODUCER');
  await d2.start('127.0.0.1:9876');
  check('start without channel stays LOCAL', d2.accessChannel === 'LOCAL');
  await d2.shutdown();
  console.log('dispatcher channel OK');
}

// ---------------------------------------------------------------------------
// 3. CLOUD routing: a context without its own channel falls back to the
//    dispatcher's CLOUD and lands on rmq_sys_TRACE_DATA_<regionId>;
//    LOCAL fallback lands on RMQ_SYS_TRACE_TOPIC; a context's own channel wins.
// ---------------------------------------------------------------------------
async function testCloudRoutingFallback(): Promise<void> {
  console.log('== CLOUD routing fallback ==');
  const sent: string[] = [];
  const makeDispatcher = (): AsyncTraceDispatcher => {
    const d = new AsyncTraceDispatcher('GID_AC_R', 'PRODUCER');
    (d as any)._producer = { send: async (msg: any) => { sent.push(msg.topic); } };
    return d;
  };

  const cloud = makeDispatcher();
  cloud.accessChannel = 'CLOUD';
  const ctx = new TraceContext();
  ctx.traceType = 'Pub' as any;
  ctx.regionId = 'RegionHangzhou';
  ctx.traceBeans = [{
    topic: 'T', msgId: 'm1', offsetMsgId: 'o1', tags: null, keys: 'k1',
    storeHost: '127.0.0.1:10911', clientHost: '127.0.0.1', timeStamp: 1,
    costTime: 1, status: 'SUCCESS', retryTimes: 0, bodyLength: 3, msgType: 'Normal',
  } as any];
  cloud.append(ctx);
  await cloud.flush();
  check('CLOUD fallback routes to region topic',
    sent[sent.length - 1] === 'rmq_sys_TRACE_DATA_RegionHangzhou',
    `got ${sent[sent.length - 1]}`);

  const local = makeDispatcher();
  local.accessChannel = 'LOCAL';
  const ctx2 = new TraceContext();
  ctx2.traceType = 'Pub' as any;
  ctx2.regionId = 'RegionHangzhou';
  ctx2.traceBeans = ctx.traceBeans;
  local.append(ctx2);
  await local.flush();
  check('LOCAL fallback routes to RMQ_SYS_TRACE_TOPIC',
    sent[sent.length - 1] === 'RMQ_SYS_TRACE_TOPIC',
    `got ${sent[sent.length - 1]}`);

  const override = makeDispatcher();
  override.accessChannel = 'LOCAL';
  const ctx3 = new TraceContext();
  ctx3.traceType = 'Pub' as any;
  ctx3.accessChannel = 'CLOUD';
  ctx3.regionId = 'RegionShanghai';
  ctx3.traceBeans = ctx.traceBeans;
  override.append(ctx3);
  await override.flush();
  check('context channel overrides dispatcher',
    sent[sent.length - 1] === 'rmq_sys_TRACE_DATA_RegionShanghai',
    `got ${sent[sent.length - 1]}`);
  console.log('CLOUD routing OK');
}

// ---------------------------------------------------------------------------
// 4. consume hook context carries the facade's channel (POP filter path,
//    Java ConsumeMessagePopConcurrentlyService.java:420)
// ---------------------------------------------------------------------------
function testConsumeContextCarriesFacadeChannel(): void {
  console.log('== consume context channel ==');
  const seen: string[] = [];
  const hook: FilterMessageHook = {
    hookName: 'ac-probe',
    filterMessage(ctx) { seen.push(ctx.accessChannel); },
  };
  const c = new DefaultMQPushConsumer('GID_AC_CTX');
  c.setAccessChannel('CLOUD');
  c.registerFilterMessageHook(hook);
  const msg = new MessageExt();
  msg.topic = 'T';
  msg.body = Buffer.from('body');
  const result: any = {
    popStatus: PopStatus.FOUND,
    msgFoundList: [msg],
    restNum: 0,
  };
  processPopResult(c, { brokerName: 'b', queueId: 0, topic: 'T' } as any,
    undefined, result);
  check('POP filter context carries CLOUD', seen.length === 1 && seen[0] === 'CLOUD',
    `got ${JSON.stringify(seen)}`);

  const c2 = new DefaultMQPushConsumer('GID_AC_CTX2');
  c2.registerFilterMessageHook(hook);
  const result2: any = { popStatus: PopStatus.FOUND, msgFoundList: [msg], restNum: 0 };
  processPopResult(c2, { brokerName: 'b', queueId: 0, topic: 'T' } as any,
    undefined, result2);
  check('POP filter context defaults to LOCAL', seen.length === 2 && seen[1] === 'LOCAL',
    `got ${JSON.stringify(seen)}`);
  console.log('consume context channel OK');
}

// ---------------------------------------------------------------------------
// 5. internal trace producer inherits the host's ACL hook (Java
//    getAndCreateTraceProducer :167-177 is `new DefaultMQProducer(rpcHook)`)
// ---------------------------------------------------------------------------
async function testInternalProducerSignature(): Promise<void> {
  console.log('== internal trace producer signature ==');
  const d = new AsyncTraceDispatcher('GID_AC_SIG', 'PRODUCER');
  const fakeHook: any = { hookName: 'fake-acl', doBeforeRequest() {} };
  d.rpcHook = fakeHook;
  d.setNamespaceV2('RMQ_INST_AC');
  const inner = await d.createInternalProducerForTest();
  assert.ok(inner != null);
  check('internal producer inherits host rpcHook', inner.rpcHook === fakeHook);
  check('internal producer inherits namespaceV2', inner.getNamespaceV2() === 'RMQ_INST_AC');
  check('internal producer group carries trace suffix',
    String(inner.producerGroup).endsWith('_INNER_TRACE_PRODUCER'),
    `got ${inner.producerGroup}`);
  console.log('internal producer signature OK');
}

async function main(): Promise<void> {
  console.log('== access channel smoke ==');
  testFacadeSurface();
  await testDispatcherStartChannel();
  await testCloudRoutingFallback();
  testConsumeContextCarriesFacadeChannel();
  await testInternalProducerSignature();
  console.log(`\n${checks} checks, ${failed} failures`);
  if (failed > 0) process.exit(1);
  console.log('PASS');
}

main().catch((e) => { console.error(e); process.exit(1); });
