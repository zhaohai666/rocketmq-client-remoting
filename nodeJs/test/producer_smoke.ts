// -*- coding: utf-8 -*-
// Producer smoke test (run: node --experimental-strip-types test/producer_smoke.ts).
//
// Verifies the ported client core + producer WITHOUT any network:
//   1. TopicRouteData -> TopicPublishInfo with a shared round-robin cursor (4 distinct queues).
//   2. SendMessageRequestHeader / V2 toExtFields() round-trips.
//   3. ACL signature determinism via AclRPCHook.
// It also imports every new client module so a successful run load-checks the whole layer.
import assert from 'node:assert';
import { Buffer } from 'node:buffer';

import { TopicRouteData, QueueData, BrokerData } from '../src/remoting/route.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import {
  SendMessageRequestHeader, SendMessageRequestHeaderV2,
} from '../src/remoting/headers.ts';
import { Message, MessageQueue } from '../src/common/message.ts';
import { signAcl, AclRPCHook } from '../src/remoting/acl.ts';

// Explicitly import the modules NOT pulled in transitively, so they are load-checked too.
import '../src/client/send_result.ts';
import '../src/client/consumer_result.ts';
import '../src/client/exception.ts';
import '../src/client/hook.ts';
import '../src/client/latency.ts';
import '../src/client/top_addressing.ts';
import '../src/client/backpressure.ts';
import '../src/client/produce_accumulator.ts';
import '../src/client/request_reply.ts';
import { MQClient } from '../src/client/mq_client.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';

function section(name: string): void {
  console.log(`--- ${name} ---`);
}

// ---------------------------------------------------------------------------
// 1. Route -> publish info -> round-robin distinct queues (shared cursor)
// ---------------------------------------------------------------------------
function testRoundRobin(): void {
  section('TopicPublishInfo round-robin');
  const trd = new TopicRouteData();
  const bd = new BrokerData('broker-a', 'broker-a', { 0: '127.0.0.1:10911' });
  trd.brokerDatas = [bd];
  // 4 writable queues, perm = READ(4) | WRITE(2) = 6
  trd.queueDatas = [new QueueData('broker-a', 4, 4, 6, 0)];

  const client = new MQClient('testClientId@instance', null, null);
  const info = client.topicRouteData2TopicPublishInfo('TestTopic', trd);
  assert.ok(info.ok(), 'publish info should have queues');
  assert.strictEqual(info.msgQueueList.length, 4, 'should assemble 4 write queues');

  const seen: number[] = [];
  for (let i = 0; i < 4; i++) {
    const mq = info.selectOneMessageQueue();
    assert.ok(mq != null, 'queue must not be null');
    seen.push(mq!.getQueueId());
  }
  // Shared cursor -> 0,1,2,3 across the 4 consecutive calls.
  assert.deepStrictEqual(seen, [0, 1, 2, 3], `round-robin should yield distinct queues, got ${JSON.stringify(seen)}`);

  // After a full cycle, the cursor wraps and the 5th call returns queue 0 again.
  const fifth = info.selectOneMessageQueue();
  assert.strictEqual(fifth!.getQueueId(), 0, 'cursor should wrap to 0');

  // lastBrokerName avoidance: when lastBrokerName === broker-a, we still get a queue
  // on broker-a (only one broker), but the API must not throw.
  const mq2 = info.selectOneMessageQueue('broker-a');
  assert.ok(mq2 != null);
  console.log('round-robin OK:', JSON.stringify(seen));
}

// ---------------------------------------------------------------------------
// 2. SendMessageRequestHeader toExtFields() round-trip
// ---------------------------------------------------------------------------
function testHeaderRoundTrip(): void {
  section('SendMessageRequestHeader round-trip');
  const h = new SendMessageRequestHeader();
  h.producerGroup = 'myGroup';
  h.topic = 'myTopic';
  h.defaultTopic = 'TBW102';
  h.defaultTopicQueueNums = 4;
  h.queueId = 2;
  h.sysFlag = 0;
  h.bornTimestamp = 123456789;
  h.flag = 0;
  h.properties = 'KEYS=a TAGS=b';
  h.reconsumeTimes = 0;
  h.unitMode = false;
  h.maxReconsumeTimes = 16;
  h.batch = false;
  h.brokerName = 'broker-a';

  const e1 = h.toExtFields();

  // V2 -> toV1 -> toExtFields must equal the V1 toExtFields.
  const v2 = SendMessageRequestHeaderV2.createV2(h);
  const e2 = v2.toExtFields();
  const back = v2.toV1().toExtFields();
  assert.deepStrictEqual(back, e1, 'V2 -> V1 round-trip should preserve fields');

  // RemotingCommand encode/decode of the custom header via extFields.
  const cmd = RemotingCommand.createRequestCommand(310, h);
  cmd.makeCustomHeaderToNet();
  const h2 = new SendMessageRequestHeader();
  h2.fromExtFields(cmd.extFields);
  assert.deepStrictEqual(h2.toExtFields(), e1, 'command extFields round-trip should preserve fields');

  // V2 short-key form carries a..n.
  assert.strictEqual(e2['b'], 'myTopic', 'V2 short key b should be the topic');
  assert.strictEqual(e2['a'], 'myGroup', 'V2 short key a should be the producer group');
  console.log('header round-trip OK (v1 fields', Object.keys(e1).length, ', v2 fields', Object.keys(e2).length, ')');
}

// ---------------------------------------------------------------------------
// 3. ACL signature determinism
// ---------------------------------------------------------------------------
function testAclDeterminism(): void {
  section('ACL sign determinism');
  const body = Buffer.from('hello-rocketmq', 'utf8');
  const extFieldsTemplate = () => ({
    a: 'myGroup', b: 'myTopic', f: '0', i: 'KEYS=a',
  } as Record<string, string>);

  function signOnce(): string {
    const cmd = RemotingCommand.createRequestCommand(310, null);
    cmd.extFields = extFieldsTemplate();
    cmd.body = body;
    const hook = new AclRPCHook('accessKey1', 'secretKey1');
    hook.doBeforeRequest('127.0.0.1:10911', cmd);
    return cmd.extFields['Signature'];
  }

  const s1 = signOnce();
  const s2 = signOnce();
  assert.ok(typeof s1 === 'string' && s1.length > 0, 'signature should be non-empty');
  assert.strictEqual(s1, s2, 'identical inputs must produce identical signatures');
  assert.strictEqual(s1, signAcl('accessKey1', 'secretKey1', extFieldsTemplate(), body), 'AclRPCHook must use signAcl');

  // Different secret -> different signature.
  const cmd3 = RemotingCommand.createRequestCommand(310, null);
  cmd3.extFields = extFieldsTemplate();
  cmd3.body = body;
  new AclRPCHook('accessKey1', 'secretKey2').doBeforeRequest('127.0.0.1:10911', cmd3);
  assert.notStrictEqual(s1, cmd3.extFields['Signature'], 'different secret must change signature');

  // AccessKey + SecurityToken must be present on the wire before signing.
  const cmd4 = RemotingCommand.createRequestCommand(310, null);
  cmd4.extFields = extFieldsTemplate();
  cmd4.body = body;
  new AclRPCHook('akX', 'skX', 'tokX').doBeforeRequest('127.0.0.1:10911', cmd4);
  assert.strictEqual(cmd4.extFields['AccessKey'], 'akX');
  assert.strictEqual(cmd4.extFields['SecurityToken'], 'tokX');
  console.log('acl determinism OK');
}

// ---------------------------------------------------------------------------
// 4. Producer boots without network (constructor + buildSendRequest wiring)
// ---------------------------------------------------------------------------
function testProducerInstantiation(): void {
  section('DefaultMQProducer instantiation');
  const p = new DefaultMQProducer('myTestGroup');
  assert.strictEqual(p.getProducerGroup(), 'myTestGroup');
  // Constructing must not require a name server / network.
  const mq = new MessageQueue('TestTopic', 'broker-a', 1);
  const msg = new Message('TestTopic', Buffer.from('payload', 'utf8'), 'tagA', 'keyA');
  // buildSendRequest needs a started client; verify it fails cleanly before start() with a clear error.
  assert.throws(() => p.buildSendRequest(msg, mq), /producer not started/);
  console.log('producer instantiation OK');
}

// ---------------------------------------------------------------------------
function main(): void {
  testRoundRobin();
  testHeaderRoundTrip();
  testAclDeterminism();
  testProducerInstantiation();
  console.log('\nPASS');
}

main();
