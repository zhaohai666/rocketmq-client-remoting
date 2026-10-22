// -*- coding: utf-8 -*-
// The async-send backpressure gate, offline. Two layers are held to a contract here:
//
//   * the fair semaphore: a request that finds no permits free waits inside its own budget
//     and is served by the release that unblocks it; a budget that runs out ends the request
//     for good -- it leaves the line and no later release may hand it a permit it had given up
//     on. Only the head of the line acquires, and a request never stays in line after it has
//     been refused.
//   * the producer's use of it: both permits are held for the whole life of the request and
//     returned exactly once, a stage that cannot be served rolls back the stage it passed, and
//     the refusal carries the protocol wording and reaches a callback-style caller through its
//     callback instead of as a rejection nobody asked for.
//
// Run: node --experimental-strip-types test/backpressure_smoke.ts
import { MQClient } from '../src/client/mq_client.ts';
import { MQClientManager } from '../src/client/mq_client_manager.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { FairSemaphore, MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE } from '../src/client/backpressure.ts';
import { RemotingTooMuchRequestException } from '../src/remoting/exception.ts';
import { Message } from '../src/common/message.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { TopicRouteData, QueueData, BrokerData } from '../src/remoting/route.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = ''): void {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

// Polls for recorded state; the deadline only bounds the wait.
async function until(cond: () => boolean, timeoutMs = 3000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (cond()) return true;
    await new Promise((r) => setTimeout(r, 5));
  }
  return cond();
}

const tick = (): Promise<void> => new Promise((r) => setImmediate(r));

// ------------------------------------------------------------------ gate level

console.log('== constants and the single poll ==');
{
  check('MIN_ASYNC_SEND_NUM is the in-flight count floor', MIN_ASYNC_SEND_NUM === 10);
  check('MIN_ASYNC_SEND_SIZE is the in-flight byte floor', MIN_ASYNC_SEND_SIZE === 1024 * 1024);

  const s = new FairSemaphore(5);
  check('a fresh gate holds its whole budget free',
    s.totalPermitsCount() === 5 && s.availablePermitsCount() === 5);
  check('a fresh gate has an empty line', s.waitingCount() === 0);

  check('a request that fits is granted on the spot', await s.tryAcquire(3, 1000) === true);
  check('the grant is deducted', s.availablePermitsCount() === 2);
  check('a grant leaves no ticket in line', s.waitingCount() === 0);
  check('a request that does not fit is refused by the single poll',
    await s.tryAcquire(3, 0) === false);
  check('a refusal by the single poll queues nothing', s.waitingCount() === 0);
  check('a refusal consumes nothing', s.availablePermitsCount() === 2);

  // A non-positive budget is the same single poll -- it never waits, and it never queues.
  for (const budget of [0, -1, -60000]) {
    const k = new FairSemaphore(1);
    check(`budget ${budget}: a free gate still grants (poll once)`, await k.tryAcquire(1, budget) === true);
    check(`budget ${budget}: the grant queued nothing`, k.waitingCount() === 0);
    check(`budget ${budget}: an empty gate refuses at once`, await k.tryAcquire(1, budget) === false);
    check(`budget ${budget}: the refusal queued nothing`, k.waitingCount() === 0);
  }

  // Returning permits does not retroactively satisfy a request that was already refused: the
  // permit stays on the counter for the next poll.
  const p = new FairSemaphore(1);
  check('no retroactive grant: the only permit is taken', await p.tryAcquire(1, 0) === true);
  check('no retroactive grant: the next request is refused', await p.tryAcquire(1, 0) === false);
  p.release(1);
  check('no retroactive grant: the returned permit sits on the counter', p.availablePermitsCount() === 1);
  check('no retroactive grant: the returned permit serves the next poll', await p.tryAcquire(1, 0) === true);
}

console.log('== waiting for permits ==');
{
  const s = new FairSemaphore(2);
  check('waiter: the gate is drained', await s.tryAcquire(2, 0) === true);
  const waiter = s.tryAcquire(2, 5000);
  check('waiter: the request is in line, not refused on the spot', s.waitingCount() === 1);
  check('waiter: nothing is granted while the gate is empty', s.availablePermitsCount() === 0);
  s.release(2);
  check('waiter: a later release serves the request in line', await waiter === true);
  check('waiter: the served request leaves the line', s.waitingCount() === 0);
  check('waiter: the served request holds its permits', s.availablePermitsCount() === 0);

  // A budget that runs out ends the request for good: it removes itself and reports the
  // refusal, so nobody behind it waits on a request that has already given up.
  const g = new FairSemaphore(1);
  check('expiry: the gate is drained', await g.tryAcquire(1, 0) === true);
  const impossible = g.tryAcquire(9999, 30);
  check('expiry: an impossible request is queued, not refused by force', g.waitingCount() === 1);
  check('expiry: it is refused when its own budget runs out', await impossible === false);
  check('expiry: the refused request left the line', g.waitingCount() === 0);
  g.release(1);
  check('expiry: the next request reaches the head as soon as a permit returns',
    await g.tryAcquire(1, 0) === true);
  check('expiry: and takes it', g.availablePermitsCount() === 0);
}

console.log('== a wake-up taken after the deadline does not grant ==');
{
  // The gate's own clock is Date.now(); moving it forward without turning the event loop lets a
  // release arrive after the waiter's budget is gone but before its expiry timer runs, which is
  // exactly the window a grant must refuse.
  const realNow = Date.now;
  try {
    const s = new FairSemaphore(2);
    check('late grant: the gate is drained', await s.tryAcquire(2, 0) === true);
    const waiter = s.tryAcquire(2, 50);
    Date.now = () => realNow() + 100;
    s.release(2);                       // the release lands after the caller gave up
    Date.now = realNow;
    check('late grant: the permit is not handed to an expired request', await waiter === false);
    check('late grant: the expired request left the line', s.waitingCount() === 0);
    check('late grant: the permit it declined is still there', s.availablePermitsCount() === 2);
    check('late grant: a caller still inside its budget gets it', await s.tryAcquire(2, 0) === true);

    // Paired control: the same release one tick earlier, while the budget holds, grants.
    const c = new FairSemaphore(2);
    check('in-budget control: the gate is drained', await c.tryAcquire(2, 0) === true);
    const inTime = c.tryAcquire(2, 50);
    Date.now = () => realNow() + 10;
    c.release(2);
    Date.now = realNow;
    check('in-budget control: the same release serves a request still in budget', await inTime === true);
    check('in-budget control: and the grant is deducted', c.availablePermitsCount() === 0);
  } finally {
    Date.now = realNow;
  }
}

console.log('== only the head acquires ==');
{
  const s = new FairSemaphore(4);
  check('head: the gate is drained', await s.tryAcquire(4, 0) === true);
  let smallAnswered = false, smallGranted = true;
  const big = s.tryAcquire(4, 5000);
  const small = s.tryAcquire(1, 5000);
  void small.then((v) => { smallGranted = v; smallAnswered = true; });
  s.release(1);                        // enough for the request behind the head, not for the head
  await tick();
  check('head: a release that only fits a later request does not let it jump the line', !smallAnswered);
  check('head: both requests are still in line', s.waitingCount() === 2);
  s.release(3);                        // now the head can go
  check('head: the head is served when it fits', await big === true);
  check('head: the served head left the line', s.waitingCount() === 1);
  check('head: the head spending the permits did not free the request behind it', !smallAnswered);
  s.release(1);
  check('head: and the next in line is served in turn', await small === true);
  check('head: the line is empty again', s.waitingCount() === 0);
  check('head: the last grant is deducted too', smallGranted && s.availablePermitsCount() === 0);
}

console.log('== resizing the cap ==');
{
  // Resize moves the free counter by the delta: growth is usable at once, and shrinking under
  // an in-flight load gives a negative margin the gate refuses to grant over until that load
  // drains. Releasing more than was taken inflates the margin instead of clamping at the total.
  const s = new FairSemaphore(4);
  check('resize: all four in flight', await s.tryAcquire(4, 0) === true);
  s.setTotalPermits(6);
  check('resize: the total moved', s.totalPermitsCount() === 6);
  check('resize: growth frees the delta with the old requests still in flight',
    s.availablePermitsCount() === 2);
  check('resize: the delta is grantable at once', await s.tryAcquire(2, 0) === true);
  s.setTotalPermits(4);
  check('resize: shrinking under an in-flight load gives a negative margin',
    s.availablePermitsCount() === -2);
  check('resize: a negative margin grants nothing', await s.tryAcquire(1, 0) === false);
  s.release(4);
  check('resize: the first drain is eaten by the over-subscription', s.availablePermitsCount() === 2);
  s.release(2);
  check('resize: draining the whole load lands exactly on the new cap', s.availablePermitsCount() === 4);
  check('resize: the new cap is fully grantable', await s.tryAcquire(4, 0) === true);
  check('resize: nothing above the new cap is grantable', await s.tryAcquire(1, 0) === false);
  s.release(5);
  check('resize: releasing over what was taken inflates the margin', s.availablePermitsCount() === 5);
}

// ---------------------------------------------------------------- producer level

const GATE_TOPIC = 'GateSmokeTopic';
const GATE_BROKER = 'gateBroker';
const GATE_ADDR = '127.0.0.1:19411';
let harnessSeq = 0;

// A producer whose transport parks every async send until the test answers it: permits are held
// for a known, controllable lifetime, so the gate's ledger is readable without a cluster.
async function gateProducer(group: string): Promise<{
  producer: DefaultMQProducer; parked: Array<(err: Error) => void>;
}> {
  const producer = new DefaultMQProducer(group);
  producer.setInstanceName(`node-gate-${++harnessSeq}`);
  producer.setRetryTimesWhenSendAsyncFailed(0);
  const clientId = (producer as any)._buildClientId() as string;
  const parked: Array<(err: Error) => void> = [];
  const remoting: any = {
    rpcHooks: [],
    registerProcessor: (): void => { /* no reverse requests here */ },
    unregisterProcessor: (): void => { /* ditto */ },
    shutdown: (): void => { /* no sockets to close */ },
    invokeAsync: (_addr: string, _request: RemotingCommand, callback: any, _timeout: number): void => {
      parked.push((err: Error) => callback(null, err));
    },
    invokeOneway: (): Promise<void> => Promise.resolve(),
    invokeSync: async (_addr: string, request: RemotingCommand): Promise<RemotingCommand> => {
      const response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, '')!;
      response.opaque = request.opaque;
      if (request.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
        response.code = ResponseCode.TOPIC_NOT_EXIST;
        response.body = Buffer.alloc(0);
      }
      return response;
    },
  };
  const client = new MQClient(clientId, '127.0.0.1:19400', remoting);
  client.brokerAddrTable.set(GATE_BROKER, { [MixAll.MASTER_ID]: GATE_ADDR });
  const route = new TopicRouteData();
  route.brokerDatas = [new BrokerData(GATE_BROKER, GATE_BROKER, { [MixAll.MASTER_ID]: GATE_ADDR })];
  route.queueDatas = [new QueueData(GATE_BROKER, 1, 1, 6, 0)];
  client.topicPublishInfoTable.set(GATE_TOPIC, client.topicRouteData2TopicPublishInfo(GATE_TOPIC, route));
  MQClientManager.getInstance().getOrCreateMQClientInstance(clientId, () => client);
  await producer.start();
  return { producer, parked };
}

function gateMessage(bodyBytes: number): Message {
  return new Message(GATE_TOPIC, Buffer.alloc(bodyBytes, 0x61));
}

// Answer a parked send and let every continuation it wakes run to completion.
async function settle(parked: Array<(err: Error) => void>, i: number, text: string): Promise<void> {
  parked[i](new Error(text));
  await tick();
  await tick();
}

console.log('== the gate is only consulted in async mode ==');
{
  const { producer, parked } = await gateProducer('GID_GateOff');
  producer.setBackPressureForAsyncSendNum(MIN_ASYNC_SEND_NUM);
  producer.setBackPressureForAsyncSendSize(MIN_ASYNC_SEND_SIZE);
  const numCap = producer.getBackPressureForAsyncSendNum();
  const sizeCap = producer.getBackPressureForAsyncSendSize();
  check('knob floor: the in-flight count cap is the configured value', numCap === MIN_ASYNC_SEND_NUM);
  check('knob floor: the in-flight byte cap is the configured value', sizeCap === MIN_ASYNC_SEND_SIZE);

  producer.setEnableBackpressureForAsyncMode(false);
  let offError: Error | null = null;
  await producer.sendAsync(gateMessage(16), (_sr, err) => { offError = err; });
  check('gate off: the send reaches the transport', await until(() => parked.length === 1));
  check('gate off: the in-flight count gate is untouched',
    producer.getSemaphoreAsyncSendNumAvailablePermits() === numCap);
  check('gate off: the in-flight byte gate is untouched',
    producer.getSemaphoreAsyncSendSizeAvailablePermits() === sizeCap);
  await settle(parked, 0, 'broker answered');
  check('gate off: the transport answer reaches the caller',
    offError !== null && (offError as Error).message === 'broker answered');

  // A cap above the floor is kept as configured, on the knob and on the live gate.
  producer.setBackPressureForAsyncSendNum(100);
  producer.setBackPressureForAsyncSendSize(2 * MIN_ASYNC_SEND_SIZE);
  check('knob: a count cap above the floor is kept', producer.getBackPressureForAsyncSendNum() === 100);
  check('knob: the live count gate follows the cap',
    producer.getSemaphoreAsyncSendNumAvailablePermits() === 100);
  check('knob: a byte cap above the floor is kept',
    producer.getBackPressureForAsyncSendSize() === 2 * MIN_ASYNC_SEND_SIZE);
  check('knob: the live byte gate follows the cap',
    producer.getSemaphoreAsyncSendSizeAvailablePermits() === 2 * MIN_ASYNC_SEND_SIZE);

  // A cap below the floor is raised to it, for both the knob and the live gate.
  producer.setBackPressureForAsyncSendNum(1);
  producer.setBackPressureForAsyncSendSize(16);
  check('knob floor: a count cap under the floor is raised to it',
    producer.getBackPressureForAsyncSendNum() === MIN_ASYNC_SEND_NUM);
  check('knob floor: the live count gate was resized to the floor',
    producer.getSemaphoreAsyncSendNumAvailablePermits() === MIN_ASYNC_SEND_NUM);
  check('knob floor: a byte cap under the floor is raised to it',
    producer.getBackPressureForAsyncSendSize() === MIN_ASYNC_SEND_SIZE);
  check('knob floor: the live byte gate was resized to the floor',
    producer.getSemaphoreAsyncSendSizeAvailablePermits() === MIN_ASYNC_SEND_SIZE);
  await producer.shutdown();
}

console.log('== permits live and die with the request ==');
{
  const { producer, parked } = await gateProducer('GID_GateHeld');
  producer.setBackPressureForAsyncSendNum(MIN_ASYNC_SEND_NUM);
  producer.setBackPressureForAsyncSendSize(MIN_ASYNC_SEND_SIZE);
  producer.setEnableBackpressureForAsyncMode(true);
  const numCap = producer.getBackPressureForAsyncSendNum();
  const sizeCap = producer.getBackPressureForAsyncSendSize();

  let answered = 0, callbackError: Error | null = null;
  await producer.sendAsync(gateMessage(128), (_sr, err) => { answered++; callbackError = err; });
  check('held: the send reached the transport', await until(() => parked.length === 1));
  check('held: the in-flight count permit is held while the request is out',
    producer.getSemaphoreAsyncSendNumAvailablePermits() === numCap - 1);
  check('held: the in-flight bytes are held while the request is out',
    producer.getSemaphoreAsyncSendSizeAvailablePermits() === sizeCap - 128);
  await settle(parked, 0, 'broker answered');
  check('held: the caller was answered once', answered === 1);
  check('held: the count permit came back',
    producer.getSemaphoreAsyncSendNumAvailablePermits() === numCap);
  check('held: the bytes came back', producer.getSemaphoreAsyncSendSizeAvailablePermits() === sizeCap);
  // A second answer for the same request must not return the same permits twice.
  parked[0](new Error('late duplicate answer'));
  await tick();
  await tick();
  check('held: a duplicate transport answer is forwarded to the caller', answered === 2);
  check('held: the duplicate did not return the count permit twice',
    producer.getSemaphoreAsyncSendNumAvailablePermits() === numCap);
  check('held: the duplicate did not return the bytes twice',
    producer.getSemaphoreAsyncSendSizeAvailablePermits() === sizeCap);
  check('held: the transport error reached the caller',
    callbackError !== null && callbackError.message === 'late duplicate answer');
  await producer.shutdown();
}

console.log('== a full gate waits for a permit instead of refusing on the spot ==');
{
  const { producer, parked } = await gateProducer('GID_GateWaits');
  producer.setBackPressureForAsyncSendNum(MIN_ASYNC_SEND_NUM);
  producer.setBackPressureForAsyncSendSize(MIN_ASYNC_SEND_SIZE);
  producer.setEnableBackpressureForAsyncMode(true);
  const numCap = producer.getBackPressureForAsyncSendNum();
  const sizeCap = producer.getBackPressureForAsyncSendSize();

  const answeredBy: string[] = [];
  for (let i = 0; i < numCap; i++) {
    await producer.sendAsync(gateMessage(8), (_sr, err) => { answeredBy.push(err?.message ?? 'ok'); });
  }
  check('wait: every permit is now held by an in-flight send',
    await until(() => parked.length === numCap));
  check('wait: the count gate is empty', producer.getSemaphoreAsyncSendNumAvailablePermits() === 0);

  let waitedError: Error | null = null;
  const waiting = producer.sendAsync(gateMessage(8), (_sr, err) => { waitedError = err ?? new Error('ok'); }, 5000);
  await tick();
  check('wait: the eleventh request is not refused while it still has budget', waitedError === null);
  check('wait: and it has not reached the transport yet', parked.length === numCap);
  check('wait: it holds no permit yet either', producer.getSemaphoreAsyncSendNumAvailablePermits() === 0);

  await settle(parked, 0, 'broker answered');
  check('wait: the returned permit is granted to the request in line',
    await until(() => parked.length === numCap + 1));
  check('wait: the gate went straight back to empty (one permit returned, one taken)',
    producer.getSemaphoreAsyncSendNumAvailablePermits() === 0);
  await settle(parked, numCap, 'broker answered');
  check('wait: the served request finishes with the transport answer',
    await until(() => waitedError !== null));
  check('wait: the caller sees the transport error, not a gate refusal',
    waitedError !== null && waitedError.message === 'broker answered');
  await waiting;

  for (let i = 1; i < numCap; i++) await settle(parked, i, 'broker answered');
  check('wait: every held send was answered', answeredBy.length === numCap);
  check('wait: the count gate is balanced again once the in-flight traffic drained',
    producer.getSemaphoreAsyncSendNumAvailablePermits() === numCap);
  check('wait: the byte gate is balanced again too',
    producer.getSemaphoreAsyncSendSizeAvailablePermits() === sizeCap);

  // Paired control: run the gate out of budget and the same request is refused, with the
  // protocol wording, through the callback rather than as an unexpected rejection.
  for (let i = 0; i < numCap; i++) {
    await producer.sendAsync(gateMessage(8), () => { /* held again */ });
  }
  // The waiters above all left, so the parked total grows by another numCap: the first
  // numCap + 1 sends (the holders plus the request that had to wait) are answered already.
  check('refuse: the gate is drained again', await until(() => parked.length === 2 * numCap + 1),
    `parked=${parked.length} permits=${producer.getSemaphoreAsyncSendNumAvailablePermits()}`);
  let refused: Error | null = null;
  const resolved = await producer.sendAsync(gateMessage(8), (_sr, err) => { refused = err; }, 30)
    .then(() => true, () => false);
  check('refuse: a callback-style caller is not handed a rejected promise', resolved === true);
  check('refuse: the refusal is a too-much-request error',
    refused !== null && refused instanceof RemotingTooMuchRequestException);
  check('refuse: with the count-stage wording',
    refused !== null && refused.message === 'send message tryAcquire semaphoreAsyncNum timeout',
    `got ${refused === null ? 'nothing' : refused.message}`);
  check('refuse: the refusal consumed nothing on its way out',
    producer.getSemaphoreAsyncSendNumAvailablePermits() === 0);

  // Without a callback the caller owns the error through the returned promise.
  let thrown: Error | null = null;
  try {
    await producer.sendAsync(gateMessage(8), null as any, 30);
  } catch (e) {
    thrown = e as Error;
  }
  check('refuse: a callback-less caller still gets the rejection',
    thrown !== null && thrown instanceof RemotingTooMuchRequestException);
  check('refuse: and the byte gate never saw the refused request',
    producer.getSemaphoreAsyncSendSizeAvailablePermits() === sizeCap - 8 * numCap,
    `got ${producer.getSemaphoreAsyncSendSizeAvailablePermits()}`);
  await producer.shutdown();
}

console.log('== the byte stage rolls back the count stage ==');
{
  const { producer, parked } = await gateProducer('GID_GateSize');
  producer.setBackPressureForAsyncSendNum(MIN_ASYNC_SEND_NUM);
  producer.setBackPressureForAsyncSendSize(MIN_ASYNC_SEND_SIZE);
  producer.setEnableBackpressureForAsyncMode(true);
  const numCap = producer.getBackPressureForAsyncSendNum();
  const sizeCap = producer.getBackPressureForAsyncSendSize();

  await producer.sendAsync(gateMessage(sizeCap), () => { /* in flight */ }, 5000);
  check('byte stage: the first message holds the whole byte budget', await until(() => parked.length === 1));
  check('byte stage: the byte gate is empty', producer.getSemaphoreAsyncSendSizeAvailablePermits() === 0);
  check('byte stage: one count permit is held',
    producer.getSemaphoreAsyncSendNumAvailablePermits() === numCap - 1);

  let refused: Error | null = null;
  await producer.sendAsync(gateMessage(sizeCap), (_sr, err) => { refused = err; }, 30);
  check('byte stage: the second message is refused as too much request',
    refused !== null && refused instanceof RemotingTooMuchRequestException);
  check('byte stage: with the byte-stage wording',
    refused !== null && refused.message === 'send message tryAcquire semaphoreAsyncSize timeout',
    `got ${refused === null ? 'nothing' : refused.message}`);
  check('byte stage: the count permit it took on the way in was rolled back',
    producer.getSemaphoreAsyncSendNumAvailablePermits() === numCap - 1);
  check('byte stage: the byte gate was never over-drawn by the refusal',
    producer.getSemaphoreAsyncSendSizeAvailablePermits() === 0);
  check('byte stage: the refused message never reached the transport', parked.length === 1);

  await settle(parked, 0, 'broker answered');
  check('byte stage: draining the in-flight send restores the whole byte budget, although the send path compressed its body',
    producer.getSemaphoreAsyncSendSizeAvailablePermits() === sizeCap,
    `got ${producer.getSemaphoreAsyncSendSizeAvailablePermits()}`);
  check('byte stage: and the count budget',
    producer.getSemaphoreAsyncSendNumAvailablePermits() === numCap);
  await producer.shutdown();
}

console.log(`\n${fail === 0 ? 'ALL TESTS PASSED' : 'TESTS FAILED'} (${pass} checks, ${fail} failed)`);
process.exit(fail === 0 ? 0 : 1);
