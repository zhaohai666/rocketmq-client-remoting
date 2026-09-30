// live_admin is the Node.js DefaultMQAdminExt end-to-end smoke: topic CRUD,
// cluster info, broker runtime/config (java.util.Properties TEXT body — the
// wire trap), subscription-group management, consumer/producer connections,
// consume stats, and offset queries.
//
//   node --experimental-strip-types examples/live_admin.ts --ns 127.0.0.1:9876
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { Message } from '../src/common/message.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = argOf('--topic', `NodeAdminLive_${STAMP}`);
const GROUP = argOf('--group', `GID_NodeAdminLive_${STAMP}`);

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main(): Promise<void> {
  // -- a producer + a consumer so connection/stats checks have something live --
  const producer = new DefaultMQProducer('GID_NodeLive_Producer');
  producer.setNamesrvAddr(NS);
  producer.start();
  // The topic MUST exist before the consumer starts: the consumer registers
  // its group at the broker via a heartbeat that can only travel once a route
  // is cached, and the topic needs to be at the nameserver for that.
  await producer.createTopic(TOPIC, producer.createTopicKey, 4);
  await sleep(3000);

  const consumer = new DefaultMQPushConsumer(GROUP);
  consumer.setNamesrvAddr(NS);
  consumer.subscribe(TOPIC, '*');
  consumer.registerMessageListenerConcurrently(
    (msgs) => { void msgs; return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; });
  await consumer.start();
  // The group registration at the broker (heartbeat) and the %RETRY% topic's
  // nameserver route both lag the start by seconds; wait for a real assignment
  // so the consumer-scoped admin checks have something live to inspect.
  const assignDeadline = Date.now() + 60_000;
  while (Date.now() < assignDeadline) {
    const mine = (consumer as any).assigned.filter((q: any) => q.getTopic() === TOPIC);
    if (mine.length > 0) break;
    await sleep(1000);
  }
  console.log(`consumer ${GROUP} online topic=${TOPIC}`);

  const admin = new DefaultMQAdminExt();
  admin.setNamesrvAddr(NS);
  admin.start();
  console.log(`admin started ns=${NS} clientId=${admin.client.clientId}`);

  // 1. create topic via the ADMIN path (idempotent on the existing topic —
  // broker answers SUCCESS "no changes"; still exercises TBW102 fan-out)
  try {
    await admin.createTopic('TBW102', TOPIC, 4, 0);
    check('createTopic', true);
  } catch (e) {
    check('createTopic', false, String((e as Error).message));
  }
  await sleep(3000);

  // 2. route
  try {
    const route = await admin.examineTopicRouteInfo(TOPIC);
    check('examineTopicRouteInfo', route.brokerDatas.length > 0 && route.queueDatas.length > 0);
  } catch (e) {
    check('examineTopicRouteInfo', false, String((e as Error).message));
  }

  // 3. all topic list
  try {
    const list = await admin.fetchAllTopicList();
    check('fetchAllTopicList', list.topicList.includes(TOPIC), `topics=${list.topicList.length}`);
  } catch (e) {
    check('fetchAllTopicList', false, String((e as Error).message));
  }

  // 4. cluster info
  let clusterName = '';
  try {
    const info = await admin.examineBrokerClusterInfo();
    const clusters = Object.keys(info.clusterAddrTable);
    clusterName = clusters[0] || '';
    check('examineBrokerClusterInfo', clusters.length > 0, `clusters=${clusters.join(',')}`);
  } catch (e) {
    check('examineBrokerClusterInfo', false, String((e as Error).message));
  }

  // 5. topic stats
  try {
    const stats = await admin.examineTopicStats(TOPIC);
    check('examineTopicStats', stats.offsetTable.size === 4, `queues=${stats.offsetTable.size}`);
  } catch (e) {
    check('examineTopicStats', false, String((e as Error).message));
  }

  // 6. subscription group: create + examine
  try {
    const cfg: Record<string, any> = {
      groupName: GROUP, consumeEnable: true, consumeFromMinEnable: true,
      consumeBroadcastEnable: true, consumeMessageOrderly: false,
      retryQueueNums: 1, retryMaxTimes: 16,
      groupRetryPolicy: { type: 'CUSTOMIZED' }, brokerId: 0,
      whichBrokerWhenConsumeSlowly: 1, notifyConsumerIdsChangedEnable: true,
      groupSysFlag: 0, consumeTimeoutMinute: 15, attributes: {},
    };
    const brokerAddr = (await admin.examineTopicRouteInfo(TOPIC)).brokerDatas[0].selectBrokerAddr();
    await admin.createAndUpdateSubscriptionGroupConfig(brokerAddr!, cfg);
    const got = await admin.examineSubscriptionGroupConfig(brokerAddr!, GROUP);
    check('subscription group create+examine', got != null, JSON.stringify(got || {}).slice(0, 80));
  } catch (e) {
    check('subscription group create+examine', false, String((e as Error).message));
  }

  // 7. broker config: GET_BROKER_CONFIG body is Properties TEXT, NOT JSON.
  try {
    const route = await admin.examineTopicRouteInfo(TOPIC);
    const brokerAddr = route.brokerDatas[0].selectBrokerAddr()!;
    const props = await admin.getBrokerConfig(brokerAddr);
    check('getBrokerConfig (Properties TEXT)', Object.keys(props).length > 10
      && props['brokerClusterName'] != null || props['brokerName'] != null,
      `keys=${Object.keys(props).length}`);
  } catch (e) {
    check('getBrokerConfig (Properties TEXT)', false, String((e as Error).message));
  }

  // 8. consumer connection (needs the consumer above to be online)
  try {
    const conn = await admin.examineConsumerConnectionInfo(GROUP);
    check('examineConsumerConnectionInfo', conn.connectionSet.length >= 1,
      `clients=${conn.connectionSet.length}`);
  } catch (e) {
    check('examineConsumerConnectionInfo', false, String((e as Error).message));
  }

  // 9. consumer list by group
  try {
    const route = await admin.examineTopicRouteInfo(TOPIC);
    const brokerAddr = route.brokerDatas[0].selectBrokerAddr()!;
    const ids = await admin.getConsumerListByGroup(GROUP, brokerAddr);
    check('getConsumerListByGroup', ids.length >= 1, `ids=${ids.length}`);
  } catch (e) {
    check('getConsumerListByGroup', false, String((e as Error).message));
  }

  // 10. consume stats (group must be online; empty → CONSUMER_NOT_ONLINE)
  try {
    await producer.send(new Message(TOPIC, Buffer.from('admin-probe'), 'TagA', 'ka'));
    await sleep(500);
    const stats = await admin.examineConsumeStats(GROUP);
    check('examineConsumeStats', stats.offsetTable.size >= 1, `entries=${stats.offsetTable.size}`);
  } catch (e) {
    check('examineConsumeStats', false, String((e as Error).message));
  }

  // 11. consume stats fan-out check for maxOffset/minOffset
  try {
    const route = await admin.examineTopicRouteInfo(TOPIC);
    const bd = route.brokerDatas[0];
    const addr = bd.selectBrokerAddr()!;
    const max = await admin.client!.getMaxOffset(addr, TOPIC, 0);
    check('maxOffset', max >= 0, `max=${max}`);
  } catch (e) {
    check('maxOffset', false, String((e as Error).message));
  }

  await admin.shutdown();
  await consumer.shutdown();
  producer.shutdown();

  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
