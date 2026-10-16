// Coverage:
//   1. manager factoryTable: a clientId hit reuses, a miss creates fresh (the
//      created flag, the factory runs only once).
//   2. Refcounting: register/unregister stay balanced; **only reaching zero
//      really shuts down** (timers stop, the instance stops running).
//   3. Instances not cached by this manager (test doubles) keep the old
//      semantics on hand-back: shut down directly, do not blow up.
//   4. removeClientFactory: removes from the table without shutting down; a
//      mismatched identity does not wrongly delete the replacement instance.
//   5. MQClient.start() is idempotent: a second facade over a shared instance
//      neither re-creates the background timers nor re-attaches 39.
//   6. Facade processor multicast: several registrants for one code fan out (39
//      broadcasts) instead of overwriting each other; the last one removed
//      detaches the transport-layer slot.
//   7. Real facade reuse: two producers + one admin sharing an instanceName ->
//      one MQClient; shutting down a single facade does not kill the shared
//      instance; 39 reverse requests are dispatched by group to the right
//      producer and also broadcast to the other registrants.
//   8. Distinct clientIds are not shared: facades with different instanceNames
//      each get their own instance and timers, unaffected by each other.
//
// Fully offline: fake name server address (heartbeat/route beats have no broker
// and no publish info, nothing goes on the wire), and END_TRANSACTION oneway
// goes through a replaced capture function — no real cluster, no dead-port
// assertions.
import { MQClient } from '../src/client/mq_client.ts';
import { MQClientManager } from '../src/client/mq_client_manager.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { MessageExt } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { encodeMessageExt } from '../src/common/messageDecoder.ts';
import { MixAll } from '../src/common/mixAll.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, detail = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${detail}`); }
}
const mgr = MQClientManager.getInstance();
const FAKE_NS = '127.0.0.1:9876'; // never dialed: an empty table means the beats idle

async function tick(times = 2): Promise<void> {
  for (let i = 0; i < times; i++) await new Promise((r) => setTimeout(r, 0));
}

// ---------------------------------------------------------------- 1. factory table
console.log('== manager factoryTable ==');
{
  mgr.reset();
  let factoryCalls = 0;
  const r1 = mgr.getOrCreateMQClientInstance('cid_1A', () => {
    factoryCalls++;
    return new MQClient('cid_1A', FAKE_NS, null);
  });
  check('miss creates a new instance (created=true)', r1.created === true);
  const r2 = mgr.getOrCreateMQClientInstance('cid_1A', () => {
    factoryCalls++;
    return new MQClient('cid_1A', FAKE_NS, null);
  });
  check('hit reuses the cached instance (created=false)', r2.created === false && r2.instance === r1.instance);
  check('factory not run on a hit', factoryCalls === 1, `calls=${factoryCalls}`);
  const r3 = mgr.getOrCreateMQClientInstance('cid_1B', () => new MQClient('cid_1B', FAKE_NS, null));
  check('a distinct clientId is not reused (separate instance)', r3.instance !== r1.instance && r3.created === true);
  check('getClientFactory observation surface', mgr.getClientFactory('cid_1A') === r1.instance);
  let threw = '';
  try { mgr.getOrCreateMQClientInstance('', () => new MQClient('x', FAKE_NS, null)); }
  catch (e) { threw = (e as Error).message; }
  check('empty clientId reports directly', threw.includes('clientId is empty'), threw);
  r1.instance.shutdown(); r3.instance.shutdown();
  mgr.reset();
}

// ---------------------------------------------------------------- 2. refcount
console.log('== reference counting: only a zero count really shuts down ==');
{
  mgr.reset();
  const inst = new MQClient('cid_2', FAKE_NS, null);
  inst.start();
  check('instance is running', inst.isRunning() === true);
  mgr.registerClient('cid_2', inst);
  mgr.registerClient('cid_2', inst); // two facades on the same clientId
  check('refcount reaches 2', mgr.refcountOf('cid_2') === 2, `got ${mgr.refcountOf('cid_2')}`);
  const last1 = mgr.unregisterClient('cid_2', inst);
  check('count not yet zero: not shut down, instance stays alive', last1 === false && inst.isRunning() === true);
  check('table entry still present', mgr.getClientFactory('cid_2') === inst);
  const last2 = mgr.unregisterClient('cid_2', inst);
  check('count hits zero: real shutdown (timers stopped / connection closed)', last2 === true && inst.isRunning() === false);
  check('after hitting zero the entry is removed from the table', mgr.getClientFactory('cid_2') === null && mgr.refcountOf('cid_2') === 0);
}

// ------------------------------------------------------- 3. hand-back of an unlisted instance
console.log('== test doubles / instances not cached by this manager ==');
{
  mgr.reset();
  let shut = 0;
  const fake = { shutdown() { shut++; } } as any;
  check('clientId not in the table: closed directly per legacy semantics', mgr.unregisterClient('cid_unknown', fake) === true && shut === 1);
  let threw = false;
  try { mgr.unregisterClient('cid_unknown', {} as any); } catch (e) { threw = true; }
  check('a test double with no shutdown does not throw', threw === false);
  check('returning a null instance yields false', mgr.unregisterClient('cid_unknown', null) === false);
  // Do not harm someone else's entry when one exists: an identity mismatch shuts
  // down only the instance the caller holds.
  const inst = new MQClient('cid_3', FAKE_NS, null);
  inst.start();
  mgr.registerClient('cid_3', inst);
  const stranger = { shutdown() { shut++; } } as any;
  mgr.unregisterClient('cid_3', stranger);
  check('identity mismatch: table entry untouched, shared instance stays alive',
    mgr.refcountOf('cid_3') === 1 && inst.isRunning() === true && shut === 2);
  mgr.unregisterClient('cid_3', inst);
  mgr.reset();
}

// ------------------------------------------------------ 4. removeClientFactory
console.log('== removeClientFactory: removes the entry without shutting down ==');
{
  mgr.reset();
  const inst = new MQClient('cid_4', FAKE_NS, null);
  inst.start();
  mgr.registerClient('cid_4', inst);
  const other = { shutdown() { throw new Error('must NOT be shut down'); } } as any;
  mgr.removeClientFactory('cid_4', other); // identity mismatch: no wrong removal
  check('identity guard: table entry still present', mgr.getClientFactory('cid_4') === inst);
  mgr.removeClientFactory('cid_4', inst);
  check('after removal getOrCreate cannot poison a half-dead instance', mgr.getClientFactory('cid_4') === null);
  check('removeClientFactory does not shut down (instance still running)', inst.isRunning() === true);
  inst.shutdown();
  mgr.reset();
}

// ------------------------------------------------------- 5. start() idempotency
console.log('== MQClient.start() is idempotent (a second facade on a shared instance does not start background tasks again) ==');
{
  const inst = new MQClient('cid_5', FAKE_NS, null);
  inst.start();
  const hb = (inst as any)._heartbeatTimer;
  const rt = (inst as any)._routeTimer;
  const cnt39 = inst.facadeProcessorCount(RequestCode.CHECK_TRANSACTION_STATE);
  inst.start(); // a reusing facade calls start() again
  check('heartbeat timer started only once', (inst as any)._heartbeatTimer === hb && hb != null);
  check('route refresh timer started only once', (inst as any)._routeTimer === rt && rt != null);
  check('the 39 handler does not double on repeated start',
    inst.facadeProcessorCount(RequestCode.CHECK_TRANSACTION_STATE) === cnt39 && cnt39 === 1);
  inst.shutdown();
  check('shutdown clears the background timers',
    (inst as any)._heartbeatTimer === null && (inst as any)._routeTimer === null);
}

// ---------------------------------------------------------- 6. facade multicast
console.log('== instance-level reverse-request multicast (later registrants no longer overwrite earlier ones) ==');
{
  const inst = new MQClient('cid_6', FAKE_NS, null);
  inst.start();
  const rc = (inst as any).remotingClient;
  const hits: string[] = [];
  const h1 = () => { hits.push('h1'); return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, 'h1'); };
  const h2 = () => { hits.push('h2'); return null; };
  const code39 = RequestCode.CHECK_TRANSACTION_STATE;
  inst.registerFacadeProcessor(code39, h1);
  inst.registerFacadeProcessor(code39, h2);
  check('three registrants for the same code (builtin + two facades) fan out instead of overwriting',
    inst.facadeProcessorCount(code39) === 3, `got ${inst.facadeProcessorCount(code39)}`);
  const cmd39 = RemotingCommand.createRequestCommand(code39, null); // empty body: the built-in decoder still fails and answers
  const resp = inst.dispatchFacadeProcessor(cmd39, '127.0.0.1:10911');
  check('39 broadcast: both h1 and h2 are invoked', hits.includes('h1') && hits.includes('h2'), hits.join(','));
  check('the reply takes the first non-null (the builtin handler SYSTEM_ERROR)', resp != null && resp.code === ResponseCode.SYSTEM_ERROR);
  inst.unregisterFacadeProcessor(code39, h1);
  inst.unregisterFacadeProcessor(code39, h2);
  check('after removal it is back to builtin-only', inst.facadeProcessorCount(code39) === 1);
  // Attaching and detaching the transport-layer slot: the first registrant hooks
  // the dispatcher onto it, the last remover to leave takes it off (consumer-side
  // code 36).
  const code36 = RequestCode.NOTIFY_CONSUMER_IDS_CHANGED;
  const ha = () => null;
  const hb2 = () => null;
  inst.registerFacadeProcessor(code36, ha);
  check('the first registrant wires the dispatcher into the transport slot', rc.processors.has(code36));
  inst.registerFacadeProcessor(code36, hb2);
  inst.unregisterFacadeProcessor(code36, () => null); // removing an unregistered identity: a no-op
  check('identity-based removal only accepts the registered instance', inst.facadeProcessorCount(code36) === 2 && rc.processors.has(code36));
  inst.unregisterFacadeProcessor(code36, ha);
  check('the transport slot stays while other registrants remain', rc.processors.has(code36) && inst.facadeProcessorCount(code36) === 1);
  inst.unregisterFacadeProcessor(code36, hb2);
  check('the last remover unplugs the transport slot', !rc.processors.has(code36) && inst.facadeProcessorCount(code36) === 0);
  // Broadcast = full fan-out: later registrants produce an answer, earlier ones are
  // still invoked; the response handed back to the transport layer is the first
  // non-null one.
  const seen: string[] = [];
  const m1 = () => { seen.push('m1'); return null; };
  const m2 = () => { seen.push('m2'); return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, 'm2'); };
  inst.registerFacadeProcessor(code36, m1);
  inst.registerFacadeProcessor(code36, m2);
  const dresp = inst.dispatchFacadeProcessor(RemotingCommand.createRequestCommand(code36, null), '127.0.0.1:10911');
  check('broadcast: both registrants are woken (no short-circuit on first reply)', seen.includes('m1') && seen.includes('m2'), seen.join(','));
  check('the reply returned to the transport layer is the first non-null', dresp != null && dresp.remark === 'm2');
  inst.unregisterFacadeProcessor(code36, m1);
  inst.unregisterFacadeProcessor(code36, m2);
  check('an unregistered code dispatches null',
    inst.dispatchFacadeProcessor(RemotingCommand.createRequestCommand(9999, null), 'x') === null);
  inst.shutdown();
}

// ---------------------------------------------------------- 7. real facade reuse
console.log('== producer + producer + admin with the same instanceName -> one shared MQClient ==');
{
  mgr.reset();
  const sharedName = 'SMGR_shared';
  const expectedClientId = MixAll.clientIdFor(sharedName, null);
  const checkedFor: string[] = [];
  const listenerA = { checkLocalTransaction: (msg: MessageExt) => {
    checkedFor.push(String(msg.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP)));
    return 0; // COMMIT (LocalTransactionState.COMMIT_MESSAGE)
  } };
  const listenerB = { checkLocalTransaction: (msg: MessageExt) => {
    checkedFor.push('B:' + String(msg.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP)));
    return 2; // UNKNOW
  } };
  const pA = new DefaultMQProducer('GID_mgr_reuse_A');
  const pB = new DefaultMQProducer('GID_mgr_reuse_B');
  const admin = new DefaultMQAdminExt();
  pA.setInstanceName(sharedName); pB.setInstanceName(sharedName); admin.setInstanceName(sharedName);
  pA.setNamesrvAddr(FAKE_NS); pB.setNamesrvAddr(FAKE_NS); admin.setNamesrvAddr(FAKE_NS);
  (pA as any).transactionListener = listenerA;
  (pB as any).transactionListener = listenerB;
  await pA.start();
  await pB.start();
  admin.start();
  const shared = pA.client!;
  check('clientId follows the group-independent ip@instanceName dimension', shared.clientId === expectedClientId, shared.clientId);
  check('all three facades share one MQClient instance', pB.client === shared && admin.client === shared);
  check('reference count equals the facade count 3', mgr.refcountOf(shared.clientId) === 3, `got ${mgr.refcountOf(shared.clientId)}`);
  check('both producers are on the instance producerTable',
    shared.getRegisteredProducers().has('GID_mgr_reuse_A') && shared.getRegisteredProducers().has('GID_mgr_reuse_B'));
  // One RPC channel: the very same RemotingClient object
  check('only one RPC channel to the broker (same RemotingClient)',
    (pB.client as any).remotingClient === (admin.client as any).remotingClient);

  // Reverse request: END_TRANSACTION oneway uses a capture double (nothing goes out).
  const ended: RemotingCommand[] = [];
  (shared.remotingClient as any).invokeOneway = (_addr: string, req: RemotingCommand) => {
    ended.push(req); return Promise.resolve();
  };
  // An extra 39 registrant (broadcast observation): on a shared instance 39 fans
  // out to every registrant.
  const extra: number[] = [];
  const ex = () => { extra.push(1); return null; };
  shared.registerFacadeProcessor(RequestCode.CHECK_TRANSACTION_STATE, ex);

  const half = new MessageExt();
  half.setTopic('T_mgr');
  half.setBody(Buffer.from('x'));
  half.putProperty(MessageConst.PROPERTY_PRODUCER_GROUP, 'GID_mgr_reuse_A');
  half.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, 'UNIQ-A-1');
  const cmd = RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, null);
  cmd.body = encodeMessageExt(half, false);
  cmd.addExtField('tranStateTableOffset', '1');
  cmd.addExtField('commitLogOffset', '2');
  const resp = shared.dispatchFacadeProcessor(cmd, '127.0.0.1:10911');
  await tick(3);
  check('39 replies SUCCESS immediately (the check runs in setImmediate, not synchronously on the dispatch thread)',
    resp != null && resp.code === ResponseCode.SUCCESS);
  check('39 dispatches by group: only group A listener is woken',
    checkedFor.length === 1 && checkedFor[0] === 'GID_mgr_reuse_A', checkedFor.join(','));
  check('39 also broadcasts to other registrants (multicast, not overwrite)', extra.length === 1);
  check('group A COMMIT: the END_TRANSACTION oneway was sent',
    ended.length === 1 && ended[0].code === RequestCode.END_TRANSACTION);
  shared.unregisterFacadeProcessor(RequestCode.CHECK_TRANSACTION_STATE, ex);

  // Shutting down a single facade does not kill the shared instance
  pA.shutdown();
  check('A leaves: reference count 3->2, shared instance stays alive',
    mgr.refcountOf(shared.clientId) === 2 && shared.isRunning() === true);
  check('A is fully removed from producerTable', !shared.getRegisteredProducers().has('GID_mgr_reuse_A'));
  check('B still uses the same instance (unaffected by A leaving)', pB.client === shared && shared.isRunning() === true);
  pB.shutdown();
  check('B leaves: one admin reference remains, instance stays connected',
    mgr.refcountOf(shared.clientId) === 1 && shared.isRunning() === true);
  admin.shutdown();
  check('the last one leaves: count hits zero, instance really shuts down and is removed',
    shared.isRunning() === false && mgr.getClientFactory(shared.clientId) === null);
}

// ------------------------------------- 8. distinct clientIds are not shared
console.log('== independent clientIds each get their own set, unaffected by each other ==');
{
  mgr.reset();
  const pX = new DefaultMQProducer('GID_mgr_iso_X');
  const pY = new DefaultMQProducer('GID_mgr_iso_Y');
  pX.setInstanceName('SMGR_x'); pY.setInstanceName('SMGR_y');
  pX.setNamesrvAddr(FAKE_NS); pY.setNamesrvAddr(FAKE_NS);
  await pX.start();
  await pY.start();
  const xInst = pX.client!;
  const yInst = pY.client!;
  check('different instanceName -> different instances', xInst !== yInst && xInst.clientId !== yInst.clientId);
  check('the two clientIds are counted separately', mgr.refcountOf(xInst.clientId) === 1
    && mgr.refcountOf(yInst.clientId) === 1);
  pX.shutdown();
  check('X hitting zero closes only itself; the Y instance and timers are unaffected',
    xInst.isRunning() === false && yInst.isRunning() === true
    && mgr.getClientFactory(yInst.clientId) === yInst);
  pY.shutdown();
  check('Y also hits zero and is removed from the table', mgr.getClientFactory(yInst.clientId) === null);
  mgr.reset();
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
