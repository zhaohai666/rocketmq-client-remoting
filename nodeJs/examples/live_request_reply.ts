// live_request_reply is the Node.js 326 Request-Reply end-to-end smoke against
// a real RocketMQ 5.5 cluster: a requester producer asks via request(), a
// responder consumer+producer replies via reply(), and the answer comes back
// as a broker-pushed PUSH_REPLY_MESSAGE_TO_CLIENT(326).
//
//   node --experimental-strip-types examples/live_request_reply.ts --ns 127.0.0.1:9876
//
// Java shape honoured here: the reply's TOPIC is `<cluster>_REPLY_TOPIC`
// (cluster from the request's CLUSTER property, stamped by the broker), the
// requester's clientId rides as REPLY_TO_CLIENT, and the broker finds the
// requestor's channel via ProducerManager.findChannel to push 326.
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { Message } from '../src/common/message.ts';
import { MessageAccessor } from '../src/common/message_accessor.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';
import { MixAll } from '../src/common/mixAll.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = argOf('--topic', `NodeRR_${STAMP}`);
const GROUP = argOf('--group', `GID_NodeRR_${STAMP}`);
const TOTAL = parseInt(argOf('--total', '10'), 10);
const CLUSTER = argOf('--cluster', 'DefaultCluster');
const REPLY_TOPIC = MixAll.getReplyTopic(CLUSTER);

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// waitRoute polls until `producer`'s client sees a route for `topic` (the
// fresh reply topic only reaches the nameserver on the broker's registration
// cycle, up to ~30s).
async function waitRoute(producer: DefaultMQProducer, topic: string, ms = 60_000): Promise<boolean> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    try {
      await (producer as any).client.updateTopicRouteInfoFromNameServer(topic, false);
      if ((producer as any).client.getTopicRouteData(topic) != null) return true;
    } catch { /* retry */ }
    await sleep(1000);
  }
  return false;
}

async function main(): Promise<void> {
  // ---- responders FIRST (rule #8): consumer + its reply producer ----
  const responder = new DefaultMQProducer('GID_NodeRR_Responder');
  responder.setNamesrvAddr(NS);
  responder.start();

  const requestor = new DefaultMQProducer('GID_NodeRR_Requestor');
  requestor.setNamesrvAddr(NS);
  requestor.start();

  // The topic create needs the broker's TBW102 route at the nameserver, which
  // lands on the broker's first registration — retry until then.
  let created = false;
  for (let attempt = 0; attempt < 30 && !created; attempt++) {
    try {
      await requestor.createTopic(TOPIC, requestor.createTopicKey, 4);
      created = true; // REPLY_TOPIC is broker-owned (system topic) — never client-creatable
    } catch (e) {
      if (attempt % 10 === 9) console.log(`  createTopic attempt ${attempt}: ${(e as Error).message}`);
      await sleep(1000);
    }
  }
  check('topics created (request + reply)', created);
  await sleep(3000);
  const responderRoute = await waitRoute(responder, REPLY_TOPIC);
  check('reply topic route visible to responder', responderRoute, REPLY_TOPIC);

  const consumer = new DefaultMQPushConsumer(GROUP);
  consumer.setNamesrvAddr(NS);
  consumer.subscribe(TOPIC, '*');
  consumer.setConsumeThreadNums(2);
  let repliesSent = 0;
  consumer.registerMessageListenerConcurrently((msgs) => {
    for (const m of msgs) {
      const body = (m.getBody() || Buffer.alloc(0)).toString('utf8');
      if (process.env['RR_DEBUG'] === '1') {
        console.log(`  [debug] consumed body=${body} cluster=${MessageAccessor.getProperty(m, MessageConst.PROPERTY_CLUSTER)} props=${JSON.stringify(m.getProperties())}`);
      }
      if (!body.startsWith('req-')) continue;
      const idx = body.slice(4);
      void responder.reply(m, Buffer.from(`resp-${idx}`)).then(() => {
        repliesSent++;
      }, (e) => {
        console.log(`  reply failed for ${body}: ${(e as Error).message}`);
      });
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await consumer.start();
  const assignDeadline = Date.now() + 60_000;
  while (Date.now() < assignDeadline && (consumer.assigned || []).filter((q) => q.getTopic() === TOPIC).length === 0) {
    await sleep(1000);
  }
  console.log(`responder up, assigned=${(consumer.assigned || []).filter((q) => q.getTopic() === TOPIC).length}`);

  // ---- request ----
  let answered = 0;
  let mismatched = 0;
  for (let i = 0; i < TOTAL; i++) {
    const req = new Message(TOPIC, Buffer.from(`req-${i}`), 'TagR', `k${i}`);
    try {
      const reply = await requestor.request(req, 15_000);
      const cid = MessageAccessor.getProperty(req, MessageConst.PROPERTY_CORRELATION_ID);
      const replyCid = MessageAccessor.getProperty(reply, MessageConst.PROPERTY_CORRELATION_ID);
      const body = (reply.getBody() || Buffer.alloc(0)).toString('utf8');
      if (body === `resp-${i}` && cid != null && cid === replyCid) answered++;
      else {
        mismatched++;
        console.log(`  mismatch i=${i} body=${body} cid=${cid}/${replyCid}` +
          ` topic=${reply.getTopic()} msgType=${MessageAccessor.getMessageType(reply)}` +
          ` props=${JSON.stringify(reply.getProperties ? reply.getProperties() : {})}`);
      }
    } catch (e) {
      mismatched++;
      console.log(`  request ${i} failed: ${(e as Error).message}`);
    }
  }
  check(`all ${TOTAL} request-reply round-trips exact`, answered === TOTAL && mismatched === 0,
    `answered=${answered} mismatched=${mismatched}`);
  // The responder's sent-counter lands a beat after the last 326; let it settle.
  await sleep(1000);
  check('326 arrived as broker push (replies sent by responder)', repliesSent >= TOTAL,
    `repliesSent=${repliesSent}`);

  await consumer.shutdown();
  requestor.shutdown();
  responder.shutdown();

  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
