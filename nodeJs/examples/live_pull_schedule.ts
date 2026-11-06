// live_pull_schedule is the Node.js PullConsumerScheduleService end-to-end
// smoke: the facade expands one PullTask per assigned queue, the scheduled
// pull loop delivers every seed body through the callback, and the
// broker-side committed offset for every queue advances.
//
//   node --experimental-strip-types examples/live_pull_schedule.ts --ns 127.0.0.1:9876
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { PullConsumerScheduleService } from '../src/client/pull_schedule_service.ts';
import { Message, MessageQueue } from '../src/common/message.ts';
import { PullStatus } from '../src/client/consumer_result.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = argOf('--topic', `NodePullScheduleLive_${STAMP}`);
const GROUP = argOf('--group', `GID_NodePullScheduleLive_${STAMP}`);
const TOTAL = parseInt(argOf('--seed', '6'), 10);
const QUEUES = parseInt(argOf('--queues', '2'), 10);

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface RouteQueue {
  brokerName: string;
  queueId: number;
}

function queuesFromRoute(route: any): RouteQueue[] {
  const out: RouteQueue[] = [];
  const brokerDatas = route?.brokerDatas ?? [];
  // route.queueDatas is TOP-LEVEL: one entry per broker (readQueueNums = the
  // broker's queue count for the topic), NOT a per-queue list on brokerData.
  const queueDatas = route?.queueDatas ?? [];
  for (const bd of brokerDatas) {
    const brokerName = bd.brokerName;
    const masterId = Object.keys(bd.brokerAddrs ?? {}).filter((id) => Number(id) === 0)[0]
      ?? Object.keys(bd.brokerAddrs ?? {})[0];
    if (brokerName == null || masterId == null || bd.brokerAddrs?.[masterId] == null) continue;
    for (const qd of queueDatas.filter((q: any) => q.brokerName === brokerName)) {
      for (let qid = 0; qid < (qd.readQueueNums ?? 0); qid++) {
        out.push({ brokerName, queueId: qid });
      }
    }
  }
  return out;
}

async function main(): Promise<void> {
  // ---- S1: cluster probe + topic + producer ----
  const admin = new DefaultMQAdminExt(null);
  admin.setNamesrvAddr(NS);
  await admin.start();

  const producer = new DefaultMQProducer('GID_NodePullScheduleSend_' + STAMP);
  producer.setNamesrvAddr(NS);
  await producer.start();
  await producer.createTopic(TOPIC, producer.createTopicKey, QUEUES);
  await sleep(3000);

  let route: any = null;
  // The namesrv only learns a freshly created topic at the broker's next
  // registration heartbeat (~30s period): the probe window must outlast it.
  for (let i = 0; i < 80; i++) {
    try {
      route = await admin.examineTopicRouteInfo(TOPIC);
      if (queuesFromRoute(route).length > 0) break;
      if (i % 10 === 9) console.log(`  route probe ${i}: empty route ${JSON.stringify(route)}`);
    } catch (e) {
      if (i % 10 === 9) console.log(`  route probe ${i}: ${(e as Error).message}`);
    }
    await sleep(500);
  }
  const routeQueues = queuesFromRoute(route);
  if (routeQueues.length === 0) {
    check('S1 cluster probe', false, 'no route for the topic; broker not registered?');
    await producer.shutdown();
    await admin.shutdown();
    process.exit(1);
  }
  const brokerName = routeQueues[0].brokerName;
  check('S1 cluster probe', true, `broker=${brokerName} queues=${routeQueues.length}`);
  check('S1 topic created', true, `${TOPIC}`);

  for (let i = 0; i < TOTAL; i++) {
    await producer.send(new Message(TOPIC, Buffer.from(`schedule-${i}`)));
  }
  check('S1 producer delivered the seed batch', true, `${TOTAL} messages`);

  // ---- S2/S3: schedule service pulls through the callback ----
  const received: string[] = [];
  const seenQueues = new Set<string>();
  const svc = new PullConsumerScheduleService(GROUP);
  // The facade's default inner pull consumer carries no nameserver address:
  // point it at the cluster before start().
  svc.getPullConsumer().setNamesrvAddr(NS);
  // All seeds were sent BEFORE the service started; the default offset source is
  // broker-committed-then-queue-tail (LAST semantics), which would resolve to the
  // tail and pull nothing. Pin the first round to offset 0 (FIRST_OFFSET reverse leg).
  svc.registerPullTask(TOPIC, async (mq, pullResult, ctx) => {
    if (pullResult.status === PullStatus.FOUND) {
      seenQueues.add(`${mq.getBrokerName()}-q${mq.getQueueId()}`);
      for (const m of pullResult.msgFoundList) {
        received.push((m.getBody() || Buffer.alloc(0)).toString('utf8'));
      }
      // The pull model is caller-driven: the loop never commits, the callback
      // advances the broker-side cursor (this is what S4 asserts).
      console.log(`  commit q${mq.getQueueId()} -> ${pullResult.nextBeginOffset} (${received.length})`);
      ctx.pullConsumer.updateConsumeOffset(mq, pullResult.nextBeginOffset);
      await ctx.pullConsumer.persistConsumeOffset();
    }
    return undefined; // default resident interval
  }, { offset: 0 });
  await svc.start();
  check('S2 schedule service started', true, `group=${GROUP}`);

  let tasksOK = false;
  for (let i = 0; i < 100 && !tasksOK; i++) {
    tasksOK = svc.taskCount() >= QUEUES;
    if (!tasksOK) await sleep(200);
  }
  check('S2 one PullTask per assigned queue expanded', tasksOK,
    `tasks=${svc.taskCount()} (want >=${QUEUES})`);

  let delivered = false;
  for (let i = 0; i < 200 && !delivered; i++) {
    delivered = received.length >= TOTAL;
    if (!delivered) await sleep(200);
  }
  check('S3 pull loop delivered every seed body', delivered,
    `collected=${received.length} (want ${TOTAL})`);
  check('S3 pulls landed on the assigned queues', seenQueues.size >= 1,
    `queues=${seenQueues.size}`);

  // ---- S3b: final durable flush + local read-back (the callback persists as
  // it goes; this re-flush guards against any drain-time race) ----
  const pullConsumer = svc.getPullConsumer();
  await pullConsumer.persistConsumeOffset();
  for (const q of routeQueues) {
    const mq = new MessageQueue(TOPIC, q.brokerName, q.queueId);
    const local = await pullConsumer.fetchConsumeOffset(mq, true);
    console.log(`  read-back q${q.queueId}: local_from_store=${local}`);
  }

  // ---- S4: broker-side offsets advanced on every queue ----
  let offsetsOK = true;
  const detailParts: string[] = [];
  for (const q of routeQueues) {
    const mq = new MessageQueue(TOPIC, q.brokerName, q.queueId);
    try {
      const { offset, hasOffset } = await admin.examineConsumerOffset(GROUP, mq);
      offsetsOK = offsetsOK && hasOffset && offset >= 1;
      detailParts.push(`q${q.queueId}=${hasOffset ? offset : 'none'}`);
    } catch (e) {
      offsetsOK = false;
      detailParts.push(`q${q.queueId}=err(${(e as Error).message})`);
    }
  }
  check('S4 broker-side committed offset exists on every queue', offsetsOK,
    detailParts.join(' '));

  // ---- S5: clean shutdown ----
  await svc.shutdown();
  check('S5 shutdown clean', true, '');

  await producer.shutdown();
  try { await admin.deleteTopic(TOPIC, ''); } catch { /* best effort */ }
  await admin.shutdown();

  console.log(`\nPASS=${passCount} FAIL=${failCount}`);
  if (failCount > 0) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
