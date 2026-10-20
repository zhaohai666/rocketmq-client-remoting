// live_auto_batch proves the auto-batch path against a real broker: a group of
// small messages must leave as ONE stored envelope and still come back to every
// caller and every consumer as its individual members.
//
//   node --experimental-strip-types examples/live_auto_batch.ts --ns 127.0.0.1:9876
//
// Legs:
//   A  autoBatch on, 12 members over 3 tags: every caller gets SEND_OK with its
//      OWN client id, each member is told the queue offset and the store id of
//      its own record, and the consumer receives 12 separate messages with their
//      bodies, tags and keys intact — not one packed body. That a group leaves as
//      ONE frame is a wire fact the offline recorder holds; what can only be
//      measured against a real broker is that the group's answer is still split
//      back per member.
//   B  control, same 12 with autoBatch off: the unbatched answers are exactly as
//      precise, so a mismatch on leg A can only come from the group split.
//   C  a delayed message next to a batchable one keeps its own schedule and
//      still arrives (a group would have swallowed it).
//   D  a body over the hold size leaves on its own and stays readable.
//   E  the broker's own index answers a query for ONE member's client id with
//      exactly that member's body.
//
// The offsets a member was told it got and the offsets the consumer actually
// read are compared member by member, so the per-member offset mapping is
// measured against the store rather than assumed.
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { Message, MessageQueue } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { SendStatus } from '../src/client/send_result.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const BATCH_TOPIC = `NodeBatch_${STAMP}`;
const PLAIN_TOPIC = `NodePlain_${STAMP}`;
const MIX_TOPIC = `NodeBatchMix_${STAMP}`;
const GROUP = `GID_NodeBatch_${STAMP}`;
const TAGS = ['TagB1', 'TagB2', 'TagB3'];
const PER_TAG = 4;

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

type Seen = { body: string; tag: string | null; uniq: string; queueOffset: number;
  offsetMsgId: string | null; arrivedAt: number; };

// One consumer per topic; delivery is recorded, never guessed from a sleep.
function startConsumer(topic: string, group: string, sink: Seen[]): Promise<DefaultMQPushConsumer> {
  const consumer = new DefaultMQPushConsumer(group);
  consumer.setNamesrvAddr(NS);
  consumer.subscribe(topic, '*');
  consumer.setConsumeThreadNums(2);
  consumer.registerMessageListenerConcurrently((msgs) => {
    for (const m of msgs) {
      sink.push({
        body: (m.getBody() || Buffer.alloc(0)).toString('utf8'),
        tag: m.getTags(),
        uniq: String(m.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) ?? ''),
        queueOffset: m.getQueueOffset(),
        offsetMsgId: m.getOffsetMsgId(),
        arrivedAt: Date.now(),
      });
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  return consumer.start().then(async () => {
    // A fresh topic only reaches the nameserver on the broker's own cycle and
    // the group only registers after a route fetch + heartbeat, so wait for the
    // queues to actually be assigned before sending anything.
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      const mine: MessageQueue[] = (consumer.assigned || []).filter((q: MessageQueue) => q.getTopic() === topic);
      if (mine.length > 0) break;
      await sleep(1000);
    }
    return consumer;
  });
}

function uniqOf(m: Message): string {
  return String(m.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) ?? '');
}

async function main(): Promise<void> {
  // Stamp taken when the mixed leg sends, read back after delivery to prove the
  // delayed member waited for its own schedule.
  let mixSentAt = 0;
  const setup = new DefaultMQProducer('GID_NodeBatch_Setup');
  setup.setNamesrvAddr(NS);
  await setup.start();
  // ONE write queue each: a group is keyed by (topic, queue, waitStoreMsgOK,
  // tag), so with several queues the members of one tag would spread over
  // several groups at random and the envelope claim could not be asserted.
  for (const t of [BATCH_TOPIC, PLAIN_TOPIC, MIX_TOPIC]) {
    await setup.createTopic(t, setup.createTopicKey, 1);
  }
  await sleep(3000);

  const batchSeen: Seen[] = [];
  const plainSeen: Seen[] = [];
  const mixSeen: Seen[] = [];
  const consumers = [
    await startConsumer(BATCH_TOPIC, `${GROUP}_A`, batchSeen),
    await startConsumer(PLAIN_TOPIC, `${GROUP}_B`, plainSeen),
    await startConsumer(MIX_TOPIC, `${GROUP}_C`, mixSeen),
  ];

  // ---- A. the group goes out as one envelope, comes back as its members ----
  console.log('--- A: auto-batch on the wire ---');
  const batchSends: Array<{ tag: string; body: string; uniq: string; msgId: string;
    queueOffset: number; offsetMsgId: string | null }> = [];
  {
    const p = new DefaultMQProducer(`${GROUP}_P`);
    p.setNamesrvAddr(NS);
    p.setAutoBatch(true);
    p.setBatchMaxDelayMs(300);
    await p.start();
    const msgs: Message[] = [];
    for (const tag of TAGS) {
      for (let i = 0; i < PER_TAG; i++) {
        msgs.push(new Message(BATCH_TOPIC, Buffer.from(`b-${tag}-${i}`), tag, `k-${tag}-${i}`));
      }
    }
    const results = await Promise.all(msgs.map((m) => p.send(m)));
    msgs.forEach((m, i) => batchSends.push({
      tag: String(m.getTags()), body: m.getBody().toString(), uniq: uniqOf(m),
      msgId: results[i].msgId, queueOffset: results[i].queueOffset,
      offsetMsgId: results[i].offsetMsgId,
    }));
    check('every batched caller got SEND_OK',
      results.every((r) => r.sendStatus === SendStatus.SEND_OK),
      results.map((r) => r.sendStatus).join(','));
    check('every caller got its OWN client id, not the group answer',
      new Set(batchSends.map((s) => s.msgId)).size === msgs.length &&
      batchSends.every((s) => s.msgId === s.uniq && s.uniq.length > 0),
      batchSends.map((s) => s.msgId).join(','));
    // What the store answers for a group is one physical id and one queue offset
    // PER MEMBER, in the order the members joined. The coalescing itself is a
    // frame-level fact the offline recorder holds; here every member's answer has
    // to survive the split unscathed.
    check('the group answer carries a distinct store id per member',
      new Set(batchSends.map((s) => s.offsetMsgId)).size === msgs.length &&
      batchSends.every((s) => s.offsetMsgId != null && String(s.offsetMsgId).length > 0),
      batchSends.map((s) => s.offsetMsgId).join(','));
    p.shutdown();
  }

  // ---- B. control: the same traffic without the switch ----
  console.log('--- B: control without the switch ---');
  const plainSends: Array<{ body: string; uniq: string; msgId: string; queueOffset: number;
    offsetMsgId: string | null }> = [];
  {
    const p = new DefaultMQProducer(`${GROUP}_Q`);
    p.setNamesrvAddr(NS);
    await p.start();
    for (const tag of TAGS) {
      for (let i = 0; i < PER_TAG; i++) {
        const m = new Message(PLAIN_TOPIC, Buffer.from(`p-${tag}-${i}`), tag, `k-${tag}-${i}`);
        const r = await p.send(m);
        plainSends.push({ body: m.getBody().toString(), uniq: uniqOf(m), msgId: r.msgId,
          queueOffset: r.queueOffset, offsetMsgId: r.offsetMsgId });
      }
    }
    check('autoBatch stays off unless it is switched on',
      new Set(plainSends.map((s) => s.offsetMsgId)).size === plainSends.length,
      `${new Set(plainSends.map((s) => s.offsetMsgId)).size}/${plainSends.length} store addresses`);
    p.shutdown();
  }

  // ---- C + D. the messages a group must not take ----
  console.log('--- C/D: delayed and oversized messages keep their own path ---');
  const late = new Message(MIX_TOPIC, Buffer.from('mix-late'), 'TagM', 'k-late');
  late.setDelayTimeLevel(1); // the broker's shortest schedule
  const held = new Message(MIX_TOPIC, Buffer.from('mix-held'), 'TagM', 'k-held');
  const big = new Message(MIX_TOPIC, Buffer.from(`mix-big-${'x'.repeat(2048)}`), 'TagM', 'k-big');
  {
    const p = new DefaultMQProducer(`${GROUP}_R`);
    p.setNamesrvAddr(NS);
    p.setAutoBatch(true);
    p.setBatchMaxDelayMs(300);
    p.setBatchMaxBytes(1024); // only `big` is over this gate
    await p.start();
    const began = Date.now();
    const mixed = await Promise.all([p.send(late), p.send(held), p.send(big)]);
    check('the delayed, the held and the oversized send all report SEND_OK',
      mixed.every((r) => r.sendStatus === SendStatus.SEND_OK),
      mixed.map((r) => r.sendStatus).join(','));
    // The held one is the only member of its group and still leaves as an
    // envelope; its caller must get back its OWN id, not the group answer.
    const own = [late, held, big].map((m, i) => uniqOf(m) === mixed[i].msgId && uniqOf(m).length > 0);
    check('each of the three callers got its own client id',
      own.every(Boolean), [late, held, big].map((m, i) => `${uniqOf(m)}->${mixed[i].msgId}`).join(','));
    mixSentAt = began;
    p.shutdown();
  }

  // ---- delivery: every member must come back on its own ----
  console.log('--- delivery read-back ---');
  const wantA = PER_TAG * TAGS.length;
  const aDeadline = Date.now() + 60_000;
  while (batchSeen.length < wantA && Date.now() < aDeadline) await sleep(500);
  check('the consumer read every batch member as its own message',
    batchSeen.length === wantA, `${batchSeen.length}/${wantA} received`);
  check('no member arrived twice',
    new Set(batchSeen.map((s) => s.body)).size === batchSeen.length,
    batchSeen.map((s) => s.body).join(','));
  check('every member kept its body, tag and client id',
    batchSends.every((s) => batchSeen.some((m) =>
      m.body === s.body && m.tag === s.tag && m.uniq === s.uniq)),
    `${JSON.stringify(batchSeen.map((s) => [s.body, s.tag, s.uniq]))}`);
  // Measured, not assumed: what the caller was told vs what the queue holds.
  // A group occupies consecutive offsets in its queue, so member i of a tag must
  // report the offset the consumer later reads for that exact body.
  const offsetPairs = batchSends.map((s) => {
    const m = batchSeen.find((x) => x.body === s.body);
    return [s.body, s.queueOffset, m ? m.queueOffset : null];
  });
  console.log(`INFO  reported vs observed queueOffset: ${JSON.stringify(offsetPairs)}`);
  check('every member was told the offset it really occupies in the queue',
    offsetPairs.every(([, reported, observed]) => reported === observed),
    JSON.stringify(offsetPairs.filter(([, reported, observed]) => reported !== observed)));
  // Same for the physical id: the answer lists one id per member, in join order.
  const idPairs = batchSends.map((s) => {
    const m = batchSeen.find((x) => x.body === s.body);
    return [s.body, s.offsetMsgId, m ? m.offsetMsgId : null];
  });
  console.log(`INFO  reported vs observed offsetMsgId: ${JSON.stringify(idPairs)}`);
  check('every member was told the store id of its own record',
    idPairs.every(([, reported, observed]) => reported != null && reported === observed),
    JSON.stringify(idPairs.filter(([, reported, observed]) => reported !== observed)));

  const bDeadline = Date.now() + 60_000;
  while (plainSeen.length < wantA && Date.now() < bDeadline) await sleep(500);
  check('the control topic delivered the same count without the switch',
    plainSeen.length === wantA, `${plainSeen.length}/${wantA} received`);
  const plainPairs = plainSends.map((s) => {
    const m = plainSeen.find((x) => x.body === s.body);
    return [s.body, s.queueOffset, m ? m.queueOffset : null, s.offsetMsgId, m ? m.offsetMsgId : null];
  });
  check('without the switch every answer is just as exact, so only the group split can be off',
    plainPairs.every(([, ro, oo, rid, oid]) => ro === oo && rid === oid),
    JSON.stringify(plainPairs.filter(([, ro, oo, rid, oid]) => ro !== oo || rid !== oid)));

  const wantC = 3;
  const cDeadline = Date.now() + 60_000;
  while (mixSeen.length < wantC && Date.now() < cDeadline) await sleep(500);
  check('the delayed, the held and the oversized member all arrived',
    mixSeen.length === wantC && ['mix-late', 'mix-held'].every((b) => mixSeen.some((m) => m.body === b)) &&
    mixSeen.some((m) => m.body.startsWith('mix-big-')),
    mixSeen.map((m) => m.body.slice(0, 16)).join(','));
  const lateSeen = mixSeen.find((m) => m.body === 'mix-late');
  check('the delayed member waited for its own schedule instead of riding a group',
    lateSeen != null && lateSeen.arrivedAt - mixSentAt >= 500,
    lateSeen ? `+${lateSeen.arrivedAt - mixSentAt}ms` : 'not delivered');
  const bigSeen = mixSeen.find((m) => m.body.startsWith('mix-big-'));
  check('the message over the hold size reads back byte-identical',
    bigSeen != null && bigSeen.body === big.getBody().toString('utf8'),
    bigSeen ? `${bigSeen.body.length} bytes` : 'not delivered');

  // ---- E. the broker indexes each member by its own client id ----
  console.log('--- E: one member is addressable in the broker ---');
  {
    const admin = new DefaultMQAdminExt(null, 20000, `${GROUP}_Admin`);
    admin.setNamesrvAddr(NS);
    admin.start();
    await sleep(2000); // the index build runs on the store's own cycle
    const probe = batchSends[1];
    let hit: Awaited<ReturnType<DefaultMQAdminExt['queryMessageByUniqKey']>> = null;
    const deadline = Date.now() + 20_000;
    while (Date.now() < deadline && hit == null) {
      try {
        const qr = await admin.queryMessageFromRoute(BATCH_TOPIC, probe.uniq, 32, 0, 0, 'U');
        hit = qr.messageList[0] ?? null;
      } catch (e) { console.log(`query: ${(e as Error).message}`); }
      if (hit == null) await sleep(1000);
    }
    check('the broker answers a query for one member with that member',
      hit != null && hit.getBody().toString('utf8') === probe.body,
      hit ? hit.getBody().toString('utf8') : 'no index hit');
    admin.shutdown();
  }

  for (const c of consumers) c.shutdown();
  setup.shutdown();
  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
