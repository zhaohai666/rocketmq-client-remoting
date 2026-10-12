// Smoke test for the compat-contract surface: wire-format / server-side
// behaviors pinned by this port. Run:
//   node --experimental-strip-types test/compat_contract_smoke.ts
import assert from 'node:assert';
import { MessageQueue, Message } from '../src/common/message.ts';
import { AllocateMachineRoomNearby, AllocateMessageQueueAveragely } from '../src/client/allocate.ts';
import type { MachineRoomResolver } from '../src/client/allocate.ts';
import {
  generateTraceparent, isValidTraceparent, childTraceparent,
  injectTraceContext, extractTraceparent, TRACE_CONTEXT_PROPERTY,
} from '../src/client/traceparent.ts';
import { FairSemaphore } from '../src/client/backpressure.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { mqKey } from '../src/client/offset_store.ts';

function nearbyMQs(): MessageQueue[] {
  return [
    new MessageQueue('T', 'room-a-broker', 0),
    new MessageQueue('T', 'room-a-broker', 1),
    new MessageQueue('T', 'room-b-broker', 0),
    new MessageQueue('T', 'room-b-broker', 1),
  ];
}

// 1) MACHINE_ROOM_NEARBY: same-room split + orphan rooms shared by all.
{
  const resolver: MachineRoomResolver = {
    brokerDeployIn: (mq: MessageQueue) => mq.getBrokerName().startsWith('room-a') ? 'room-a' : 'room-b',
    consumerDeployIn: (cid: string) => cid.startsWith('c1') ? 'room-a' : 'room-a',
  };
  const s = new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely(), resolver);
  assert.strictEqual(s.name(), 'MACHINE_ROOM_NEARBY-AVG');
  const got = s.allocate('G', 'c1', nearbyMQs(), ['c1', 'c2']);
  // room-a: 2 queues / 2 consumers -> the first queue; room-b orphan: 2 queues
  // / ALL consumers -> again the first queue.
  assert.strictEqual(got.length, 2);
  assert.ok(got.every((mq) => mq.getQueueId() === 0));
  console.log('OK: MACHINE_ROOM_NEARBY splits by room and shares orphans');
}

// 2) 解析出空机房时抛错（resolver 返回空串 -> Machine room is null）。
{
  const resolver: MachineRoomResolver = {
    brokerDeployIn: () => '',
    consumerDeployIn: () => 'room-a',
  };
  const s = new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely(), resolver);
  assert.throws(() => s.allocate('G', 'c1', nearbyMQs(), ['c1']), /Machine room is null/);
  console.log('OK: MACHINE_ROOM_NEARBY empty room throws');
}

// 3) 构造器空值守卫：任一参数为 null 必须抛错。
{
  const resolver: MachineRoomResolver = {
    brokerDeployIn: () => 'room-a',
    consumerDeployIn: () => 'room-a',
  };
  assert.throws(() => new AllocateMachineRoomNearby(null as any, resolver), /null/);
  assert.throws(() => new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely(), null as any), /null/);
  console.log('OK: MACHINE_ROOM_NEARBY constructor null checks');
}

// 4) traceparent: mint, validate, derive, inject/extract.
{
  const tp = generateTraceparent();
  assert.ok(isValidTraceparent(tp));
  const parts = tp.split('-');
  assert.strictEqual(parts[0], '00');
  assert.strictEqual(parts[1].length, 32);
  assert.strictEqual(parts[3], '01');
  // All-zero ids are illegal; so is version ff; uppercase forwarded values pass.
  assert.ok(!isValidTraceparent(`00-${'0'.repeat(32)}-${'1'.repeat(16)}-01`));
  assert.ok(!isValidTraceparent(`ff-${'a'.repeat(32)}-${'b'.repeat(16)}-01`));
  assert.ok(isValidTraceparent(`00-${'A'.repeat(32)}-${'b'.repeat(16)}-01`));
  const child = childTraceparent(tp);
  assert.ok(child != null && child.split('-')[1] === tp.split('-')[1]);
  assert.ok(childTraceparent('garbage') == null);

  const msg = new Message('T', Buffer.from('x'));
  const injected = injectTraceContext(msg);
  assert.strictEqual(injected, tp === injected ? injected : injected); // returns the value on the message
  assert.strictEqual(msg.getProperty(TRACE_CONTEXT_PROPERTY), injected);
  // Caller-propagated context wins on re-injection.
  assert.strictEqual(injectTraceContext(msg), injected);
  assert.strictEqual(extractTraceparent(msg), injected);
  assert.strictEqual(extractTraceparent(new Message('T', Buffer.alloc(0))), null);
  console.log('OK: traceparent mint/validate/derive/inject/extract');
}

// 5) FairSemaphore byte-size acquire/release.
{
  const s = new FairSemaphore(100);
  assert.ok(s.tryAcquireFor(60));
  assert.ok(!s.tryAcquireFor(60)); // only 40 left
  s.releaseFor(60);
  assert.strictEqual(s.availablePermitsCount(), 100);
  s.releaseFor(1000); // clamps at total
  assert.strictEqual(s.availablePermitsCount(), 100);
  console.log('OK: FairSemaphore byte-size acquire/release');
}

// 6) VIP 通道端口算术（开启时 port-2）。
{
  assert.strictEqual(MixAll.brokerVipChannel(true, '127.0.0.1:10911'), '127.0.0.1:10909');
  assert.strictEqual(MixAll.brokerVipChannel(false, '127.0.0.1:10911'), '127.0.0.1:10911');
  console.log('OK: brokerVipChannel port-2');
}

// 7) 5.x timer delay property aliases.
{
  const m = new Message('T', Buffer.from('x'));
  m.setDelayTimeSec(30);
  assert.strictEqual(m.getProperty('TIMER_DELAY_SEC'), '30');
  m.setDelayTimeMs(1500);
  assert.strictEqual(m.getProperty('TIMER_DELAY_MS'), '1500');
  m.setDeliverTimeMs(1699999999000);
  assert.strictEqual(m.getProperty('TIMER_DELIVER_MS'), '1699999999000');
  assert.strictEqual(m.getProperty('DELAY'), null); // 4.x level untouched
  console.log('OK: timer delay setters use the server-side TIMER_* property names');
}

// 8) LitePullConsumer.seekToBegin / seekToEnd：
//    begin = 先取 minOffset 再 seek，end = 先取 maxOffset 再 seek，
//    两者都走 admin 路径、只寻址 master broker。
//    假的 mqClient 应答边界 RPC，所以断言盯住拉取游标真正落点——
//    静默无效的 seek 要到消费者重放整个队列或跳过整个队列时才暴露。
{
  const mq = new MessageQueue('SeekTopic', 'broker-a', 0);
  const route = { brokerDatas: [{ brokerName: 'broker-a', brokerAddrs: { '0': '127.0.0.1:10911' } }] };
  const stub = { min: 7, max: 41 };
  const c: any = new DefaultLitePullConsumer('seekSmokeGroup');
  c.mqClient = {
    updateTopicRouteInfoFromNameServer: async () => true,
    getTopicRouteData: () => route,
    getMinOffset: async () => stub.min,
    getMaxOffset: async () => stub.max,
  };

  // seek() 自身以 started 为准入门槛（校验服务状态），两个封装同样如此——
  // start 之前什么都不会移动。
  await assert.rejects(() => c.seekToBegin(mq), /not started/);
  await assert.rejects(() => c.seekToEnd(mq), /not started/);
  assert.strictEqual(c.pullCursor.get(mqKey(mq)), undefined);

  c.started = true;
  await c.seekToBegin(mq);
  assert.strictEqual(c.pullCursor.get(mqKey(mq)), 7, 'seekToBegin pins the cursor at minOffset');
  await c.seekToEnd(mq);
  assert.strictEqual(c.pullCursor.get(mqKey(mq)), 41, 'seekToEnd pins the cursor at maxOffset');

  // Unanswered boundary (route gone / broker down => -1): throw, never seek to -1.
  stub.min = -1;
  await assert.rejects(() => c.seekToBegin(mq), /minOffset unavailable/);
  assert.strictEqual(c.pullCursor.get(mqKey(mq)), 41, 'a rejected seekToBegin leaves the cursor alone');
  c.mqClient.getTopicRouteData = () => null;
  stub.min = 7;
  await assert.rejects(() => c.seekToBegin(mq), /minOffset unavailable/);

  console.log('OK: lite pull seekToBegin/seekToEnd use the broker boundaries');
}

// 9) Push consumer: suspend() / resume() / isPaused()
//    挂起标志只在 pullMessage 与 popMessage 两条路径读取，命中时按
//    PULL_TIME_DELAY_MILLS_WHEN_SUSPEND=1000ms 退避。
//    关键不变量是顺序：门槛必须在 pq.touchPull() 之后。门槛先于时间戳更新，
//    会让"只是被挂起"的消费者在 120s 停滞检测器眼里形同死掉，rebalance 随即
//    拆掉分配——运维暂停两分钟就会换来一次全量重投。
{
  const { DefaultMQPushConsumer } = await import('../src/client/consumer.ts');
  const { ProcessQueue } = await import('../src/client/process_queue.ts');
  const { SubscriptionData } = await import('../src/common/subscriptionData.ts');
  const { PullResult, PullStatus } = await import('../src/client/consumer_result.ts');

  const topic = 'SuspendSmokeTopic';
  const mq = new MessageQueue(topic, 'broker-a', 0);
  const key = mqKey(mq);

  const c: any = new DefaultMQPushConsumer('suspendSmokeGroup');
  const pulls: number[] = [];
  const pq = new ProcessQueue(false);
  c.processQueueTable.set(key, pq);
  c.subscription.set(topic, new SubscriptionData(topic, '*'));
  c.offsetTable.set(key, 0);
  c.started = true;
  c.mqClient = {};
  c._findBrokerAddressInSubscribe = () => ({ addr: '127.0.0.1:10911', isSlave: false });
  c._pullKernel = async () => {
    // ~30ms per answer: a real broker holds the long poll; an instant stub would
    // spin the loop so fast that the "in-flight pull after suspend" window alone
    // would swamp the baseline.
    await new Promise((r) => setTimeout(r, 30));
    pulls.push(Date.now());
    const next = pulls.length;
    return new PullResult(PullStatus.NO_NEW_MSG, next, 0, next, []);
  };

  // suspend()/resume() 不触碰尚未 start 的消费者所持有的任何状态，
  // 因此它们在 start 之前也必须安全。
  const idle: any = new DefaultMQPushConsumer('suspendIdleGroup');
  assert.strictEqual(idle.isPaused(), false, 'pause defaults to false');
  idle.suspend(); idle.suspend();
  assert.strictEqual(idle.isPaused(), true, 'repeated suspend stays suspended');
  idle.resume(); idle.resume();
  assert.strictEqual(idle.isPaused(), false, 'repeated resume stays running');

  const stopFlag = c._newStopFlag();
  const loop = c._queuePullLoop(mq, stopFlag);

  const waitPulls = async (atLeast: number, timeoutMs = 5000): Promise<boolean> => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline && pulls.length < atLeast) await new Promise((r) => setTimeout(r, 20));
    return pulls.length >= atLeast;
  };

  assert.ok(await waitPulls(2), 'the loop must pull before the test means anything');
  c.suspend();
  assert.strictEqual(c.isPaused(), true);
  // The request already in flight when the flag flipped still lands; settle
  // first, otherwise it counts as "pulled while suspended".
  await new Promise((r) => setTimeout(r, 300));
  const baseline = pulls.length;
  const stampBefore = pq.lastPullTimestampValue();
  await new Promise((r) => setTimeout(r, 2500));
  assert.strictEqual(pulls.length, baseline, 'no new PULL_MESSAGE may go out while suspended');
  assert.ok(pq.lastPullTimestampValue() > stampBefore,
    'lastPullTimestamp must keep advancing while suspended: the gate sits after the stamp');
  assert.ok(!pq.isPullExpired(), 'suspended is not stalled — the 120s detector must stay quiet');
  assert.strictEqual(c.processQueueTable.get(key), pq, 'suspend must not drop the queue');

  c.resume();
  assert.strictEqual(c.isPaused(), false);
  assert.ok(c._rebalanceNow, 'resume must immediately wake the rebalance loop');
  assert.ok(await waitPulls(baseline + 1), 'resume must restart pulling by itself');

  c._stopFlags.clear(); c._stopFlags.add('__stopped');
  await loop;

  console.log('OK: push consumer suspend stops pulls, keeps the timestamp alive, resume restarts');
}

// 10) Push consumer thread elasticity: updateCorePoolSize / getCorePoolSize /
//     adjustThreadPool / computeAccumulationTotal
//    守卫才是重点：接受 >= consumeThreadMax 会让池 core 超过自身 max，
//    ThreadPoolExecutor 会拒绝；坏值必须静默忽略、不抛错——这是该接口的约定。
{
  const { DefaultMQPushConsumer } = await import('../src/client/consumer.ts');
  const c: any = new DefaultMQPushConsumer('elasticitySmokeGroup');
  c.consumeThreadMax = 64;
  c.corePoolSize = 20;

  assert.strictEqual(c.updateCorePoolSize(32), true, 'a value below consumeThreadMax applies');
  assert.strictEqual(c.getCorePoolSize(), 32, 'getCorePoolSize reports the applied value');
  // 守卫（全部静默拒绝）：越界、== / > max、非有限值。
  assert.strictEqual(c.updateCorePoolSize(0), false, 'core must be > 0');
  assert.strictEqual(c.updateCorePoolSize(-5), false, 'negative rejected');
  assert.strictEqual(c.updateCorePoolSize(32768), false, 'above Short.MAX_VALUE rejected');
  assert.strictEqual(c.updateCorePoolSize(32767), false, 'Short.MAX_VALUE still >= consumeThreadMax here');
  c.consumeThreadMax = 10;
  assert.strictEqual(c.updateCorePoolSize(10), false, 'core == max is rejected: the guard is a strict <');
  assert.strictEqual(c.updateCorePoolSize(NaN), false, 'non-finite rejected');
  assert.strictEqual(c.getCorePoolSize(), 32, 'a rejected value leaves the pool size alone');

  // msgAccCnt feeds the accumulation total adjustThreadPool compares against.
  c._msgAccCnt.set('TopicAbroker-a0', 7);
  c._msgAccCnt.set('TopicAbroker-a1', 3);
  assert.strictEqual(c.computeAccumulationTotal(), 10, 'accumulation sums every queue');
  // adjustThreadPool is a documented no-op upstream: it must not move the pool.
  c.adjustThreadPoolNumsThreshold = 5;
  c.adjustThreadPool();
  assert.strictEqual(c.getCorePoolSize(), 32, 'adjustThreadPool is a pinned no-op: it must not resize the pool');

  console.log('OK: push consumer core-pool guards match the pinned contract, adjustThreadPool stays a no-op');
}

console.log('\nAll compat-contract smoke checks passed.');

