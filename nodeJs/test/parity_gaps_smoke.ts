// Regression cases for the verified parity gaps in message identity, the
// orderly gate, offset handling, running info and the lite/pull surface.
//
// Run: node --experimental-strip-types test/parity_gaps_smoke.ts
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { createUniqID } from '../src/common/messageClientIdSetter.ts';
import { getHostIp, getPid } from '../src/common/utilAll.ts';
import { Message, MessageBatch, MessageExt, MessageQueue, isWaitStoreMsgOK } from '../src/common/message.ts';
import { MessageSysFlag } from '../src/common/sysflag.ts';
import { decodeInnerBatchMessages } from '../src/common/messageDecoder.ts';
import { MessageModel, ConsumeFromWhere } from '../src/remoting/heartbeat.ts';
import { MQClientException } from '../src/remoting/exception.ts';
import { FilterAPI } from '../src/common/subscriptionData.ts';
import {
  LocalFileOffsetStore, messageQueueKeyJson, mqKey, ReadOffsetMode,
} from '../src/client/offset_store.ts';
import { DefaultMQPushConsumer, encodeRunningInfoBody } from '../src/client/consumer.ts';
import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { ProcessQueue } from '../src/client/process_queue.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

async function checkThrows(needle: string, call: () => unknown, name: string,
  type?: new (...args: any[]) => Error): Promise<void> {
  let message = '';
  let typeOK = false;
  try {
    await call();
  } catch (e) {
    message = e instanceof Error ? e.message : String(e);
    typeOK = type == null || e instanceof type;
  }
  check(name, typeOK && message.includes(needle), `got: ${message}`);
}

const TOPIC = 'ParityTopic';
const BROKER = 'broker-a';
const ADDR = '127.0.0.1:10911';

function sleep(ms: number) { return new Promise((r) => setTimeout(r, ms)); }

// ---------------------------------------------------------------- B1
async function uniqKeyShape() {
  console.log('== B1 uniq id shape ==');
  const id = createUniqID();
  check('uniq id is 16 bytes (32 hex chars)', id.length === 32, `${id.length} chars: ${id}`);
  check('uniq id is uppercase hex', /^[0-9A-F]{32}$/.test(id), id);
  if (id.length !== 32) return;
  const raw = Buffer.from(id, 'hex');
  const ipParts = getHostIp().split('.').map(Number);
  check('first field is the 4-byte IPv4 address',
    raw.length === 16 && raw[0] === ipParts[0] && raw[1] === ipParts[1]
    && raw[2] === ipParts[2] && raw[3] === ipParts[3], raw.subarray(0, 4).toString('hex'));
  check('pid occupies bytes 4-5', raw.readUInt16BE(4) === (getPid() & 0xffff));
  check('class-loader hash bytes are zero',
    raw[6] === 0 && raw[7] === 0 && raw[8] === 0 && raw[9] === 0);
  const now = new Date();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1).getTime();
  const elapsed = raw.readUInt32BE(10);
  check('timestamp is millis since the start of the month',
    elapsed >= 0 && elapsed <= Date.now() - monthStart + 5000, String(elapsed));
  const first = raw.readUInt16BE(14);
  const second = Buffer.from(createUniqID(), 'hex').readUInt16BE(14);
  check('counter advances in the last two bytes', second === ((first + 1) & 0xffff),
    `${first} -> ${second}`);
}

// ---------------------------------------------------------------- B5
function innerBatchEnvelope(flags: number, body: Buffer): MessageExt {
  const env = new MessageExt(TOPIC, body);
  env.setQueueId(2);
  env.setQueueOffset(100);
  env.setStoreHost('10.0.0.5');
  env.storeHostPort = 10911;
  env.setBornHost('10.0.0.6');
  env.bornHostPort = 20911;
  env.setBornTimestamp(1111);
  env.setStoreTimestamp(2222);
  env.setCommitLogOffset(3333);
  env.setSysFlag(flags);
  env.setBrokerName(BROKER);
  return env;
}

async function innerBatchUnwrap() {
  console.log('== B5 inner-batch unwrap ==');
  const m1 = new Message(TOPIC, Buffer.from('one'), 'TagA', null, 11);
  m1.putProperty('K', 'v1');
  const m2 = new Message(TOPIC, Buffer.from('two'), 'TagB', null, 22);
  m2.putProperty('K', 'v2');
  const encoded = new MessageBatch([m1, m2]).encode();
  const flags = MessageSysFlag.INNER_BATCH_FLAG | MessageSysFlag.NEED_UNWRAP_FLAG;
  const env = innerBatchEnvelope(flags, encoded);
  const out = decodeInnerBatchMessages([env]);
  check('the envelope is replaced by one message per member', out.length === 2,
    String(out.length));
  check('members keep the pull topic and adjacent queue offsets',
    out[0].getTopic() === TOPIC && out[0].getQueueOffset() === 100
    && out[1].getQueueOffset() === 101, out.map((m) => m.getQueueOffset()).join(','));
  check('members keep the queue id and routing hosts',
    out[1].getQueueId() === 2 && out[1].getStoreHost() === '10.0.0.5'
    && out[1].getBornHost() === '10.0.0.6' && out[1].getStoreHostString() === '10.0.0.5:10911');
  check('members keep the envelope timestamps, sysflag and commitlog offset',
    out[0].getBornTimestamp() === 1111 && out[0].getStoreTimestamp() === 2222
    && out[0].getSysFlag() === flags && out[0].getCommitLogOffset() === 3333);
  check('members carry their own body, flag and properties',
    out[0].getBody().toString() === 'one' && out[1].getBody().toString() === 'two'
    && out[0].getFlag() === 11 && out[1].getFlag() === 22
    && out[0].getProperty('K') === 'v1' && out[1].getProperty('K') === 'v2');
  check('waitStoreMsgOK is carried over', out.every((m) => isWaitStoreMsgOK(m) === true));

  const undecodable = innerBatchEnvelope(flags, Buffer.from([0, 0, 0, 200, 1, 2]));
  const kept = decodeInnerBatchMessages([undecodable]);
  check('a failed decode keeps the envelope', kept.length === 1 && kept[0] === undecodable);

  const noUnwrap = innerBatchEnvelope(MessageSysFlag.INNER_BATCH_FLAG, encoded);
  const untouched = decodeInnerBatchMessages([noUnwrap]);
  check('an envelope without NEED_UNWRAP is left alone',
    untouched.length === 1 && untouched[0] === noUnwrap);
}

// ---------------------------------------------------------------- A1 / A2
async function orderlyGate() {
  console.log('== A1/A2 orderly pull gate ==');
  type Gate = { pulls: number, sleeps: number[], pq: ProcessQueue, c: DefaultMQPushConsumer };

  async function drive(model: string, ordered: boolean, locked: boolean,
    lockAgeMs: number): Promise<Gate> {
    const c = new DefaultMQPushConsumer('GID_gate');
    const anyC = c as any;
    c.orderly = ordered;
    c.setMessageModel(model);
    c.pullThresholdForQueue = 1 << 30;
    c.pullThresholdSizeForQueue = 1 << 30;
    c.consumeConcurrentlyMaxSpan = 1 << 30;
    c.subscription.set(TOPIC, FilterAPI.buildSubscriptionData(TOPIC, '*'));
    c.started = true;
    const mq = new MessageQueue(TOPIC, BROKER, 0);
    const pq = new ProcessQueue(ordered);
    pq.setLocked(locked);
    pq.setLastLockTimestamp(Date.now() - lockAgeMs);
    c.processQueueTable.set(mqKey(mq), pq);
    c.offsetTable.set(mqKey(mq), 0);
    c.mqClient = { updateTopicRouteInfoFromNameServer: async () => {} } as never;
    anyC._findBrokerAddressInSubscribe = () => ({ addr: ADDR, isSlave: false });
    const flag = anyC._newStopFlag();
    const gate: Gate = { pulls: 0, sleeps: [], pq, c };
    anyC._pullKernel = async () => {
      gate.pulls++;
      anyC._stopFlags.delete(flag);
      return { status: 'NO_NEW_MSG', nextBeginOffset: 0, minOffset: 0, maxOffset: 0, msgFoundList: [] };
    };
    anyC._sleepOrStop = async (ms: number) => {
      gate.sleeps.push(ms);
      anyC._stopFlags.delete(flag);
      return true;
    };
    await anyC._queuePullLoop(mq, flag);
    return gate;
  }

  const broadcasting = await drive(MessageModel.BROADCASTING, true, false, 0);
  check('BROADCASTING orderly pulls without a queue lock', broadcasting.pulls === 1,
    `pulls=${broadcasting.pulls}`);

  const clusteringLocked = await drive(MessageModel.CLUSTERING, true, true, 1000);
  check('CLUSTERING orderly pulls once the lock is granted', clusteringLocked.pulls === 1,
    `pulls=${clusteringLocked.pulls}`);

  const clusteringUnlocked = await drive(MessageModel.CLUSTERING, true, false, 0);
  check('CLUSTERING orderly waits without a lock',
    clusteringUnlocked.pulls === 0 && clusteringUnlocked.sleeps[0] === 200,
    `pulls=${clusteringUnlocked.pulls} sleeps=${clusteringUnlocked.sleeps.join(',')}`);

  const expired = await drive(MessageModel.CLUSTERING, true, true, 31_000);
  check('a lock grant older than 30s counts as unlocked',
    expired.pulls === 0 && expired.sleeps[0] === 200, `pulls=${expired.pulls}`);
  check('the stale lock is logged and counted',
    expired.pq.tryUnlockTimes === 1, String(expired.pq.tryUnlockTimes));
}

// ---------------------------------------------------------------- C1
async function initialOffsetReadFailure() {
  console.log('== C1 initial offset read failure ==');
  const mq = new MessageQueue(TOPIC, BROKER, 0);
  function make(result: number): DefaultMQPushConsumer {
    const c = new DefaultMQPushConsumer('GID_init');
    c.consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;
    (c as any).mqClient = {};
    (c as any).offsetStore = {
      readOffset: async () => result,
      updateOffset: () => {}, persistAll: async () => {}, persist: async () => {}, load: async () => {},
    };
    return c;
  }

  await checkThrows('Failed to query consume offset', () =>
    make(-2).computePullFromWhere(mq), 'a failed store read surfaces as an error', MQClientException);
  const none = await make(-1).computePullFromWhere(mq);
  check('no committed offset (-1) falls through to the configured start', none === 0, String(none));
}

// ---------------------------------------------------------------- C2 / C3
async function offsetFileFormat() {
  console.log('== C2/C3 local offset file ==');
  const base = fs.mkdtempSync(path.join(os.tmpdir(), 'rmq_parity_'));
  const mq = new MessageQueue('T', 'b', 0);
  const store = new LocalFileOffsetStore('cid', 'grp', base);
  store.updateOffset(mq, 9, false);
  await store.persistAll([mq]);
  const file = path.join(base, 'cid', 'grp', 'offsets.json');
  const text = fs.readFileSync(file, 'utf8');
  check('queue keys are inline JSON objects',
    text.includes('"offsetTable"') && text.includes('{"brokerName":"b","queueId":0,"topic":"T"}:9'), text);

  const quotedKey = JSON.stringify({ brokerName: 'b2', queueId: 1, topic: 'T2' });
  fs.writeFileSync(file, JSON.stringify({ offsetTable: { [quotedKey]: 4 } }));
  const quotedStore = new LocalFileOffsetStore('cid', 'grp', base);
  await quotedStore.load();
  check('a quoted-string key is still read',
    (await quotedStore.readOffset(new MessageQueue('T2', 'b2', 1), ReadOffsetMode.READ_FROM_STORE)) === 4);

  const foreign = new MessageQueue('Keep', 'b3', 2);
  fs.writeFileSync(file, `{"offsetTable":{${messageQueueKeyJson(foreign)}:77}}`);
  await store.persistAll([mq]);
  const merged = new LocalFileOffsetStore('cid', 'grp', base);
  await merged.load();
  check('persistAll keeps entries of queues it did not flush',
    (await merged.readOffset(foreign, ReadOffsetMode.READ_FROM_STORE)) === 77
    && (await merged.readOffset(mq, ReadOffsetMode.READ_FROM_STORE)) === 9);
  fs.rmSync(base, { recursive: true, force: true });
}

// ---------------------------------------------------------------- F2
async function pullEntryGuards() {
  console.log('== F2 pull entry guards ==');
  const c = new DefaultMQPullConsumer('GID_pull_guards');
  (c as any).started = true;
  (c as any).pullAPI = {};
  const mq = new MessageQueue(TOPIC, BROKER, 0);
  await checkThrows('offset < 0', () => c.pull(mq, '*', -1, 10),
    'a negative offset is refused', MQClientException);
  await checkThrows('maxNums <= 0', () => c.pull(mq, '*', 0, 0),
    'maxNums 0 is refused', MQClientException);
  await checkThrows('maxNums <= 0', () => c.pull(mq, '*', 0, -5),
    'a negative maxNums is refused', MQClientException);

  const modes: number[] = [];
  (c as any).offsetStore = {
    readOffset: async (_mq: MessageQueue, mode: number) => { modes.push(mode); return 3; },
    updateOffset: () => {}, persistAll: async () => {}, persist: async () => {}, load: async () => {},
  };
  const offset = await c.fetchConsumeOffset(mq, false);
  check('fetchConsumeOffset(mq, false) reads memory first then the store',
    offset === 3 && modes[0] === ReadOffsetMode.READ_FROM_MEMORY_THEN_STORE, String(modes[0]));
}

// ---------------------------------------------------------------- C4
async function offsetStoreSelection() {
  console.log('== C4 pull/lite offset store selection ==');
  const { LocalFileOffsetStore: Local, RemoteBrokerOffsetStore: Remote } =
    await import('../src/client/offset_store.ts');
  const manager = (await import('../src/client/mq_client_manager.ts')).MQClientManager.getInstance();
  const fake = {
    clientId: 'fake-client',
    registerConsumer: () => {}, start: () => {},
    setPersistConsumerOffsetInterval: () => {}, setUseHeartbeatV2: () => {},
    setDecodeReadBody: () => {}, setMqClientApiTimeout: () => {},
    setEnableConcurrentHeartbeat: () => {}, setConcurrentHeartbeatThreadPoolSize: () => {},
    setMaxPageSizeInGetMetadata: () => {}, remotingClient: {},
    updateTopicRouteInfoFromNameServer: async () => false,
    sendHeartbeatToAllBrokers: async () => 0,
    checkClientInBroker: async () => {},
    unregisterConsumer: () => {},
  };
  const origGet = manager.getOrCreateMQClientInstance;
  const origRegister = manager.registerClient;
  (manager as any).getOrCreateMQClientInstance = () => ({ instance: fake, created: false });
  (manager as any).registerClient = () => {};
  const kindOf = (c: any) => c.offsetStore instanceof Local ? 'local'
    : c.offsetStore instanceof Remote ? 'remote' : 'unknown';
  const startWith = async (consumer: any, model: string) => {
    consumer.setNamesrvAddr('127.0.0.1:1');
    consumer.setMessageModel(model);
    await consumer.start();
    return kindOf(consumer);
  };
  try {
    check('pull BROADCASTING uses the local file store',
      (await startWith(new DefaultMQPullConsumer('GID_store_b'), 'BROADCASTING')) === 'local');
    check('pull CLUSTERING uses the broker store',
      (await startWith(new DefaultMQPullConsumer('GID_store_c'), 'CLUSTERING')) === 'remote');
    check('lite BROADCASTING uses the local file store',
      (await startWith(new DefaultLitePullConsumer('GID_store_lb'), 'BROADCASTING')) === 'local');
    check('lite CLUSTERING uses the broker store',
      (await startWith(new DefaultLitePullConsumer('GID_store_lc'), 'CLUSTERING')) === 'remote');
  } finally {
    (manager as any).getOrCreateMQClientInstance = origGet;
    (manager as any).registerClient = origRegister;
  }
}

// ---------------------------------------------------------------- F1
async function liteStartState() {
  console.log('== F1 lite start state machine ==');
  for (const state of ['RUNNING', 'START_FAILED', 'SHUTDOWN_ALREADY']) {
    const c = new DefaultLitePullConsumer('GID_lite_state');
    (c as any)._serviceState = state;
    await checkThrows(`The PullConsumer service state not OK, maybe started once, ${state}`,
      () => c.start(), `start() from ${state} is refused`);
  }

  const valid = new DefaultLitePullConsumer('GID_lite_valid');
  valid.setNamesrvAddr('127.0.0.1:1');
  let threw = '';
  try { (valid as any)._checkConfig(); } catch (e) { threw = (e as Error).message; }
  check('checkConfig does not require a subscription', threw === '', threw);

  await checkThrows('consumerGroup can not equal', () =>
    (new DefaultLitePullConsumer() as any)._checkConfig(), 'the default group is refused');
  const timeout = new DefaultLitePullConsumer('GID_lite_timeout');
  timeout.setBrokerSuspendMaxTimeMillis(20000);
  timeout.setConsumerTimeoutMillisWhenSuspend(19999);
  await checkThrows('consumerTimeoutMillisWhenSuspend', () =>
    (timeout as any)._checkConfig(), 'a suspend timeout below the broker window is refused');

  const c = new DefaultLitePullConsumer('GID_lite_sub');
  const routes: string[] = [];
  let beats = 0;
  (c as any).started = true;
  (c as any).mqClient = {
    updateTopicRouteInfoFromNameServer: async (t: string) => { routes.push(t); return true; },
    sendHeartbeatToAllBrokers: async () => { beats++; return 1; },
  };
  c.subscribe('AddedWhileRunning', '*');
  await sleep(10);
  check('subscribing while RUNNING refreshes the route',
    routes.includes('AddedWhileRunning'), routes.join(','));
  check('subscribing while RUNNING sends a heartbeat', beats === 1, String(beats));
}

// ---------------------------------------------------------------- D4
async function runningInfoTable() {
  console.log('== D4 running-info mqTable ==');
  const c = new DefaultMQPushConsumer('GID_running');
  const mq = new MessageQueue(TOPIC, BROKER, 0);
  const key = mqKey(mq);
  const pq = new ProcessQueue(false);
  pq.setLocked(true);
  pq.setLastLockTimestamp(4444);
  pq.incTryUnlockTimes();
  pq.touchPull();
  const msg = new MessageExt(TOPIC, Buffer.alloc(1024 * 1024));
  msg.setQueueOffset(5);
  pq.putMessage([msg]);
  c.processQueueTable.set(key, pq);
  c.offsetTable.set(key, 7);

  const info = c.buildConsumerRunningInfo(false);
  const table = info.mqTable as Record<string, any>;
  const keys = Object.keys(table);
  check('mqTable is keyed by an inline JSON message queue',
    keys.length === 1 && keys[0] === '{"brokerName":"broker-a","queueId":0,"topic":"ParityTopic"}',
    keys.join(','));
  const row = table[keys[0]] || {};
  const expected = ['commitOffset', 'cachedMsgMinOffset', 'cachedMsgMaxOffset', 'cachedMsgCount',
    'cachedMsgSizeInMiB', 'transactionMsgMinOffset', 'transactionMsgMaxOffset',
    'transactionMsgCount', 'locked', 'tryUnlockTimes', 'lastLockTimestamp', 'droped',
    'lastPullTimestamp', 'lastConsumeTimestamp'];
  check('mqTable carries the full field set',
    expected.every((f) => Object.prototype.hasOwnProperty.call(row, f)),
    Object.keys(row).join(','));
  check('mqTable values come from the queue state',
    row.commitOffset === 7 && row.cachedMsgCount === 1 && Number.isInteger(row.cachedMsgSizeInMiB)
    && row.locked === true && row.tryUnlockTimes === 1 && row.lastLockTimestamp === 4444
    && row.droped === false && row.lastPullTimestamp > 0,
    JSON.stringify(row));

  const body = encodeRunningInfoBody({ properties: { a: 'b' }, mqTable: table });
  check('the 307 body keeps the inline key form',
    body.includes(`"mqTable":{${keys[0]}:`) && body.includes('"properties":{"a":"b"}'), body);
}

async function main() {
  await uniqKeyShape();
  await innerBatchUnwrap();
  await orderlyGate();
  await initialOffsetReadFailure();
  await offsetFileFormat();
  await pullEntryGuards();
  await offsetStoreSelection();
  await liteStartState();
  await runningInfoTable();
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail > 0 ? 1 : 0);
}

await main();
