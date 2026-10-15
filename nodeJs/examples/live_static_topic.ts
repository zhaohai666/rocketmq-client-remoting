// live_static_topic verifies the static-topic (513 + endpoint rewrite) path
// end-to-end against a real RocketMQ 5.x cluster — the canonical static-topic
// scenario (S1-S5).
//
//   node --experimental-strip-types examples/live_static_topic.ts --ns 127.0.0.1:9876
//
// S1 createStaticTopic(513): single broker, totalQueues=8, physical 4 queues.
// S2 route read-back: topicQueueMappingByBroker non-empty.
// S3 topicRouteData2EndpointsForStaticTopic: 8 entries (0-3 → broker,
//    4-7 → __syslo__none__).
// S4 instance rewrite: getBrokerNameFromMessageQueue resolves the mock queue
//    name to the real broker; out-of-table ids and non-static topics pass
//    through untouched.
// S5 end-to-end: plain producer sends 4, PUSH consumer consumes 4.
//
// Prints PASS/FAIL per item; the last line is `PASS=<n> FAIL=<n>`.
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';
import { Message, MessageQueue } from '../src/common/message.ts';
import { TopicConfig } from '../src/common/topic_config.ts';
import {
  endpointsToKeyStrings,
  getMockBrokerName,
  LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST,
  LogicQueueMappingItem,
  METADATA_SCOPE_GLOBAL,
  TopicQueueMappingDetail,
  topicRouteData2EndpointsForStaticTopic,
} from '../src/remoting/static_topic.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Date.now()));
const TOPIC = argOf('--topic', `StaticLive_${STAMP}`);
const GROUP = `StaticPG_${STAMP}`;

const PHYSICAL_QUEUES = 4;
const TOTAL_QUEUES = 8;
const MOCK_NAME = getMockBrokerName(METADATA_SCOPE_GLOBAL);

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Single-broker TopicQueueMappingDetail (the 513 body, fastjson2 field names). */
function mappingDetail(bname: string): TopicQueueMappingDetail {
  const currIdMap: Record<string, number> = {};
  const hostedQueues: Record<string, LogicQueueMappingItem[]> = {};
  for (let gid = 0; gid < PHYSICAL_QUEUES; gid++) {
    currIdMap[String(gid)] = gid;
    hostedQueues[String(gid)] = [new LogicQueueMappingItem(
      0, gid, bname, 0, 0, -1, -1, -1,
    )];
  }
  return new TopicQueueMappingDetail(
    TOPIC, TOTAL_QUEUES, bname, 0, METADATA_SCOPE_GLOBAL, false,
    currIdMap, hostedQueues,
  );
}

async function main(): Promise<void> {
  console.log(`=== Node static topic live: ns=${NS} topic=${TOPIC} ===`);

  // ---------------- S1 createStaticTopic(513) ----------------
  console.log('\nS1 admin up, createStaticTopic(513)');
  const admin = new DefaultMQAdminExt();
  admin.setNamesrvAddr(NS);
  admin.start();
  let brokerName = '';
  let brokerAddr = '';
  try {
    const ci = await admin.examineBrokerClusterInfo();
    for (const name of Object.keys(ci.brokerAddrTable).sort()) {
      const ids = ci.brokerAddrTable[name];
      const master = ids[0];
      if (master) { brokerName = name; brokerAddr = master; break; }
    }
    check('S1 broker address resolved', brokerAddr !== '', `${brokerName}(${brokerAddr})`);
    if (brokerAddr === '') throw new Error('no master broker registered');

    await admin.createStaticTopic(brokerAddr, 'TBW102',
      new TopicConfig(TOPIC, PHYSICAL_QUEUES, PHYSICAL_QUEUES),
      mappingDetail(brokerName), true);
    check('S1 createStaticTopic(513) accepted', true, `totalQueues=${TOTAL_QUEUES}`);
  } catch (e) {
    check('S1 createStaticTopic(513)', false, (e as Error).message);
    console.log(`PASS=${passCount} FAIL=${failCount}`);
    process.exit(1);
  } finally {
    admin.shutdown();
  }

  // ---------------- S2/S3 route read-back + endpoints ----------------
  console.log('\nS2/S3 route read-back + endpoints table');
  let endpoints: Map<MessageQueue, string> | null = null;
  let lastDetail = 'no poll ran';
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    // Fresh admin RPC per poll. Do NOT poll
    // the client's cached route: a route fetched before the mapping
    // propagated would never refresh.
    const probe = new DefaultMQAdminExt();
    probe.setNamesrvAddr(NS);
    probe.start();
    try {
      const route = await probe.examineTopicRouteInfo(TOPIC);
      const n = route.topicQueueMappingByBroker != null
        ? Object.keys(route.topicQueueMappingByBroker).length : 0;
      lastDetail = `mappingEntries=${n} queueDatas=${route.queueDatas.length}`
        + ` brokerDatas=${route.brokerDatas.length}`;
      if (n > 0) {
        endpoints = topicRouteData2EndpointsForStaticTopic(TOPIC, route);
        break;
      }
    } catch (e) {
      lastDetail = `examine err: ${(e as Error).message}`;
    } finally {
      probe.shutdown();
    }
    await sleep(2000);
  }
  check('S2 route carries TopicQueueMappingByBroker',
    endpoints != null && endpoints.size > 0, lastDetail);
  if (endpoints == null || endpoints.size === 0) {
    console.log(`PASS=${passCount} FAIL=${failCount}`);
    process.exit(1);
  }

  // JS Map keys are MessageQueue REFERENCES (no value hashing) — look up via
  // the string view, same convention as the offline tests.
  const byKey = endpointsToKeyStrings(endpoints);
  check('S3 endpoint count = totalQueues', byKey.size === TOTAL_QUEUES,
    `got=${byKey.size} want=${TOTAL_QUEUES}`);
  let hostedOk = true;
  for (let gid = 0; gid < PHYSICAL_QUEUES; gid++) {
    if (byKey.get(`${TOPIC}@${MOCK_NAME}@${gid}`) !== brokerName) hostedOk = false;
  }
  check('S3 hosted ids 0-3 -> broker', hostedOk);
  let unhostedOk = true;
  for (let gid = PHYSICAL_QUEUES; gid < TOTAL_QUEUES; gid++) {
    if (byKey.get(`${TOPIC}@${MOCK_NAME}@${gid}`)
      !== LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST) {
      unhostedOk = false;
    }
  }
  check('S3 unhosted ids 4-7 -> __syslo__none__', unhostedOk);

  // ---------------- S4/S5 instance rewrite + end-to-end ----------------
  console.log('\nS4/S5 instance rewrite + end-to-end');
  const bodies: string[] = [];
  const consumer = new DefaultMQPushConsumer(GROUP);
  consumer.setNamesrvAddr(NS);
  // A new group must use FIRST_OFFSET: node's first fetch of the consumer id
  // list (heartbeat registration) may happen after sending, and LAST_OFFSET
  // would skip every message persisted before the send completes (C-family
  // knowledge: starting the consumer before sending is unreliable here, so the
  // start-offset semantics are the fallback).
  consumer.setConsumeFromWhere('CONSUME_FROM_FIRST_OFFSET');
  consumer.subscribe(TOPIC, '*');
  consumer.registerMessageListenerConcurrently((msgs) => {
    for (const m of msgs) {
      bodies.push((m.getBody() || Buffer.alloc(0)).toString('utf8'));
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  consumer.start();
  await sleep(4000);

  const inst = consumer.mqClient;
  let rewriteOk = inst != null;
  if (inst != null) {
    for (let gid = 0; gid < PHYSICAL_QUEUES; gid++) {
      if (inst.getBrokerNameFromMessageQueue(
        new MessageQueue(TOPIC, MOCK_NAME, gid)) !== brokerName) {
        rewriteOk = false;
        break;
      }
    }
  }
  check('S4 instance rewrites mock queues to the real broker', rewriteOk);

  // id TOTAL_QUEUES-1 IS in the table, and the protocol semantics return the
  // endpoint value, i.e. the sentinel itself; the passthrough case must use
  // an out-of-table id (the unit test uses 99).
  const unhostedRewrite = inst != null
    ? inst.getBrokerNameFromMessageQueue(new MessageQueue(TOPIC, MOCK_NAME, TOTAL_QUEUES - 1))
    : null;
  check('S4 unhosted id returns the sentinel value',
    unhostedRewrite === LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST,
    String(unhostedRewrite));
  const outsideRewrite = inst != null
    ? inst.getBrokerNameFromMessageQueue(new MessageQueue(TOPIC, MOCK_NAME, 99))
    : null;
  check('S4 out-of-table id passes the mock name through', outsideRewrite === MOCK_NAME, String(outsideRewrite));
  const plainRewrite = inst != null
    ? inst.getBrokerNameFromMessageQueue(new MessageQueue(`Plain_${STAMP}`, brokerName, 0))
    : null;
  check('S4 non-static topic passes through unchanged', plainRewrite === brokerName, String(plainRewrite));

  // Start the consumer before sending: a new group with
  // CONSUME_FROM_LAST_OFFSET skips messages from before startup.
  const prod = new DefaultMQProducer(`StaticProd_${STAMP}`);
  prod.setNamesrvAddr(NS);
  prod.start();
  let sent = 0;
  let lastErr = '';
  try {
    for (let i = 0; i < PHYSICAL_QUEUES; i++) {
      try {
        await prod.send(new Message(TOPIC, Buffer.from(`static-${i}`)));
        sent++;
      } catch (e) {
        lastErr = (e as Error).message;
      }
    }
  } finally {
    prod.shutdown();
  }
  check('S5 sent 4', sent === PHYSICAL_QUEUES, `sent=${sent} lastErr=${lastErr}`);

  let consumed = false;
  const consumeDeadline = Date.now() + 40_000;
  while (Date.now() < consumeDeadline) {
    if (bodies.length >= PHYSICAL_QUEUES) { consumed = true; break; }
    await sleep(1000);
  }
  check('S5 consumed all 4', consumed, `got=${bodies.length}`);
  await consumer.shutdown();

  console.log(`PASS=${passCount} FAIL=${failCount}`);
  if (failCount > 0) process.exit(1);
}

main().catch((e) => {
  console.error(`static_topic live fatal: ${(e as Error).message}`);
  process.exit(2);
});
