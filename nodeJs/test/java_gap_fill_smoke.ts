// Smoke test for the Java-gap-fill surface. Run:
//   node --experimental-strip-types test/java_gap_fill_smoke.ts
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

// 2) An empty machine room from the resolver throws (Java IllegalArgumentException).
{
  const resolver: MachineRoomResolver = {
    brokerDeployIn: () => '',
    consumerDeployIn: () => 'room-a',
  };
  const s = new AllocateMachineRoomNearby(new AllocateMessageQueueAveragely(), resolver);
  assert.throws(() => s.allocate('G', 'c1', nearbyMQs(), ['c1']), /Machine room is null/);
  console.log('OK: MACHINE_ROOM_NEARBY empty room throws');
}

// 3) Constructor null checks mirror Java's NPEs.
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

// 6) VIP channel port math (Java MixAll.brokerVIPChannel).
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
  console.log('OK: timer delay setters use Java property names');
}

console.log('\nAll java-gap-fill smoke checks passed.');
