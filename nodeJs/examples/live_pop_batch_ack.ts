// live_pop_batch_ack proves BATCH_ACK_MESSAGE(200151) against a real RocketMQ 5.5.1 broker.
//
//   node --experimental-strip-types examples/live_pop_batch_ack.ts --ns 127.0.0.1:9876
//   bash scripts/run_node_live.sh pop_batch_ack
//
// Why it needs a live leg of its own: the POP push consumer acknowledges one message at a
// time through ACK_MESSAGE(200051) and never builds a batch, so no runtime path ever reaches
// the batch wire path. Grouping key, bit index, base64 BitSet and the "no custom header"
// request shape are only reachable through the exported primitive -- and a primitive nobody
// has pointed at a broker is a primitive that may not work.
//
// The batch body carries no per-message offsets, only a start offset plus a bit set, so a
// return code cannot answer "which messages did the broker actually clear". As in every POP
// leg the criterion is a timed window plus broker-side observable state:
//
//   L1 ONE REQUEST, WHOLE BATCH. Pop the batch, clear it with a single 200151, then keep
//      popping the business topic and the group's POP retry topic for 2.5x the invisibility
//      window. Wrong offsets or bits leave the checkpoints alive, and the revive service puts
//      those messages back inside that window.
//   L2 PAIRED CONTROL. Send the same batch with exactly one checkpoint missing. That message
//      must come back while the acked ones stay gone -- the assertion that fails if a bit set
//      is treated as "the whole batch". Its follow-up pins the retry direction: the revived
//      checkpoint carries marker "1" and the batch body that clears it names the BUSINESS
//      topic, because the broker re-derives the retry topic from marker + group. The window
//      waiting for the revival stops at the first sighting plus a short tail: a sighted
//      checkpoint left to expire is revived again before the ack lands, and no ack can take a
//      revival back, so a longer window would fail on timing instead of on the wire path.
//      The retry topic's route is probed AFTER the ack (route registration can lag a broker
//      heartbeat), and a POP of the business topic is never used as evidence about which
//      queue answered.
//   L3 NO OVER-MERGING. Two pop rounds of the same queue cleared by one request. Checkpoints
//      of different rounds must land in different entries (retry@queueId@ckOffset@popTime is
//      the merge key); collapsing them would acknowledge offsets nobody popped.
//
// The broker needs no special switch: 200151 is registered unconditionally, so the standard
// test cluster config already serves it. Each leg creates a single-queue topic up front and
// waits for the expected queue depth before popping -- a partially dispatched batch would make
// "one request cleared the batch" unprovable.
//
// The last line is '<pass> passed, <fail> failed'; the process exits non-zero on any failure.
import { DefaultMQProducer } from '../src/client/producer.ts';
import { PopStatus } from '../src/client/pop_api.ts';
import { Message } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { ResponseCode } from '../src/remoting/codes.ts';
import { PopMessageRequestHeader } from '../src/remoting/headers.ts';
import { findBrokerAddressInPublish } from '../src/remoting/route.ts';
import {
  buildPopRetryTopicV1, split, getCkQueueOffset, getPopTime, getInvisibleTime,
  getReviveQid, getRetry, getBrokerName, getQueueId, getQueueOffset,
} from '../src/remoting/extra_info.ts';
import { buildBatchAckMessageRequestBody } from '../src/remoting/pop_bodies.ts';
import type { BatchAck, BatchAckMessageRequestBody } from '../src/remoting/pop_bodies.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));

const INVISIBLE_MS = 10000;
const OBSERVE_SEC = (INVISIBLE_MS * 2.5) / 1000; // long silence window: 2.5x invisibility
const REVIVE_TAIL_SEC = 3;                       // extra watching after the first sighting
const DEPTH_WAIT_SEC = 30;
const ROUTE_WAIT_SEC = 30;

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ------------------------------------------------------------------ checkpoints

type Ck = {
  raw: string; body: string; ckOffset: number; queueOffset: number; popTime: number;
  invisible: number; retry: string; brokerName: string; queueId: number; reviveQid: number;
};

/** Parse the 8-segment POP_CK a popped message carries, or say what is wrong with it. */
function parseCk(raw: string, body: string): { ck?: Ck; problem?: string } {
  const segs = split(raw);
  if (segs.length !== 8) return { problem: `not 8 segments (${segs.length}): ${raw}` };
  return {
    ck: {
      raw, body,
      ckOffset: getCkQueueOffset(segs),
      popTime: getPopTime(segs),
      invisible: getInvisibleTime(segs),
      reviveQid: getReviveQid(segs),
      retry: getRetry(segs),
      brokerName: getBrokerName(segs),
      queueId: getQueueId(segs),
      queueOffset: getQueueOffset(segs),
    },
  };
}

function mergeKey(c: Ck): string {
  return `${c.retry}@${c.queueId}@${c.ckOffset}@${c.popTime}`;
}

function mergeKeyOf(ack: BatchAck): string {
  return `${ack.retry}@${ack.queueId}@${ack.startOffset}@${ack.popTime}`;
}

/** The consume-queue offsets one entry clears: start offset plus every set bit. */
function ackedOffsets(ack: BatchAck): number[] {
  const bytes = Buffer.from(ack.bitSet.encodeBase64(), 'base64');
  const out: number[] = [];
  for (let i = 0; i < bytes.length * 8; i++) {
    if (bytes[i >> 3] & (1 << (i & 7))) out.push(ack.startOffset + i);
  }
  return out.sort((a, b) => a - b);
}

function sameIntSet(got: number[], want: number[]): boolean {
  const g = [...got].sort((a, b) => a - b);
  const w = [...want].sort((a, b) => a - b);
  return g.length === w.length && g.every((v, i) => v === w[i]);
}

function sortedUnique(values: string[]): string[] {
  return Array.from(new Set(values)).sort();
}

// ------------------------------------------------------------------ the POP side

class Verifier {
  client: any;
  brokerName: string;
  addr: string;

  constructor(producer: DefaultMQProducer, brokerName: string, topic: string) {
    this.client = producer.client;
    this.brokerName = brokerName;
    const route = this.client.getTopicRouteData(topic);
    this.addr = findBrokerAddressInPublish(brokerName, route);
  }

  async pop(group: string, topic: string, queueId: number, maxNums: number,
    pollMs: number): Promise<any> {
    const header = new PopMessageRequestHeader();
    header.bname = this.brokerName;
    header.consumerGroup = group;
    header.topic = topic;
    header.queueId = queueId;
    header.maxMsgNums = maxNums;
    header.invisibleTime = INVISIBLE_MS;
    header.pollTime = pollMs;
    header.bornTime = Date.now();
    header.initMode = 0;
    return this.client.popMessage(this.brokerName, this.addr, header, '', pollMs + 5000);
  }

  /** Clear the checkpoints of one pop with a single 200151; reports the wire code. */
  async batchAck(topic: string, group: string, raws: string[]): Promise<{ code: number; error: string }> {
    const body: BatchAckMessageRequestBody = buildBatchAckMessageRequestBody(topic, group, raws);
    try {
      await this.client.batchAckMessage(this.addr, body, 5000);
      return { code: ResponseCode.SUCCESS, error: '' };
    } catch (e: any) {
      return {
        code: typeof e?.responseCode === 'number' ? e.responseCode : -1,
        error: String(e?.message || e),
      };
    }
  }
}

// ------------------------------------------------------------------ observation

/** Everything a run of pops saw: bodies, the topics they were reported under, and their CKs. */
class Eyes {
  sightings: { body: string; topic: string; ck: string }[] = [];

  topicsSeen(): string[] { return sortedUnique(this.sightings.map((s) => s.topic)); }

  bodies(): string[] { return this.sightings.map((s) => s.body).sort(); }

  checkpoints(): string[] { return this.sightings.map((s) => s.ck); }

  /** The raw CK strings of one body, which is how a re-popped message is recognised. */
  checkpointsFor(body: string): string[] {
    return this.sightings.filter((s) => s.body === body).map((s) => s.ck);
  }

  cameBack(): boolean { return this.sightings.length > 0; }
}

/** Pop both the business topic and the group's POP retry topic once, and record what came. */
async function popRound(v: Verifier, group: string, topic: string, eyes: Eyes): Promise<void> {
  for (const t of [topic, buildPopRetryTopicV1(topic, group)]) {
    let res: any;
    try {
      res = await v.pop(group, t, 0, 32, 0);
    } catch (e) {
      continue; // a retry topic the group has never revived into is simply absent
    }
    if (res.popStatus !== PopStatus.FOUND) continue;
    for (const m of res.msgFoundList) {
      eyes.sightings.push({
        body: (m.getBody() || Buffer.alloc(0)).toString('utf8'),
        topic: m.getTopic(),
        ck: m.getProperty(MessageConst.PROPERTY_POP_CK) || '',
      });
    }
  }
}

/** Watch for a fixed window: used where the expected answer is silence. */
async function watch(v: Verifier, group: string, topic: string, seconds: number,
  eyes: Eyes): Promise<void> {
  const until = Date.now() + seconds * 1000;
  while (Date.now() < until) {
    await popRound(v, group, topic, eyes);
    await sleep(1000);
  }
}

/** Watch until the first sighting, then keep watching a short tail so the whole revived
 *  batch is recorded -- stopping at the first answer keeps the follow-up ack inside the
 *  invisibility window that this very pop started. */
async function probe(v: Verifier, group: string, topic: string, eyes: Eyes): Promise<boolean> {
  const until = Date.now() + OBSERVE_SEC * 1000;
  while (Date.now() < until) {
    await popRound(v, group, topic, eyes);
    if (eyes.cameBack()) {
      const tail = Date.now() + REVIVE_TAIL_SEC * 1000;
      while (Date.now() < tail) {
        await popRound(v, group, topic, eyes);
        await sleep(1000);
      }
      return true;
    }
    await sleep(1000);
  }
  return false;
}

// ------------------------------------------------------------------ setup helpers

/** Create a single-queue topic and send one message per body, retrying the sends. */
async function makeTopic(producer: any, topic: string, bodies: string[]): Promise<string> {
  await producer.createTopic(topic, producer.createTopicKey, 1);
  await forceRoute(producer.client, topic);
  const brokerName = brokerNameOf(producer.client, topic);
  for (const body of bodies) {
    let sent = false;
    for (let attempt = 0; attempt < 5 && !sent; attempt++) {
      try {
        await producer.send(new Message(topic, Buffer.from(body), 'TagBa', `pk-${body}`));
        sent = true;
      } catch (e) {
        await sleep(2000); // the route can still be propagating right after createTopic
      }
    }
    if (!sent) throw new Error(`send failed for ${body}`);
  }
  return brokerName;
}

async function forceRoute(client: any, topic: string): Promise<boolean> {
  await client.updateTopicRouteInfoFromNameServer(topic);
  const route = client.getTopicRouteData(topic);
  return route != null && (route.queueDatas || []).length > 0;
}

function brokerNameOf(client: any, topic: string): string | null {
  const route = client.getTopicRouteData(topic);
  const brokerDatas = (route && route.brokerDatas) || [];
  return brokerDatas.length ? brokerDatas[0].brokerName : null;
}

/** Messages are dispatched to the consume queue asynchronously: wait until the queue really
 *  holds what was sent, or "one request cleared the batch" proves nothing. */
async function waitDepth(v: Verifier, topic: string, want: number): Promise<number> {
  const until = Date.now() + DEPTH_WAIT_SEC * 1000;
  let depth = 0;
  for (;;) {
    const min = await v.client.getMinOffset(v.addr, topic, 0);
    const max = await v.client.getMaxOffset(v.addr, topic, 0);
    depth = max - min;
    if (depth >= want) return depth;
    if (Date.now() > until) return depth;
    await sleep(1000);
  }
}

async function waitRoute(client: any, topic: string): Promise<boolean> {
  const until = Date.now() + ROUTE_WAIT_SEC * 1000;
  for (;;) {
    if (await forceRoute(client, topic)) return true;
    if (Date.now() > until) return false;
    await sleep(2000);
  }
}

// ------------------------------------------------------------------ L1

async function legAll(producer: any): Promise<void> {
  const topic = `NodeBaAll_${STAMP}`;
  const group = `GID_NodeBaAll_${STAMP}`;
  const bodies = [];
  for (let i = 0; i < 8; i++) bodies.push(`ba-all-${i}`);

  console.log(`=== L1 one request clears a whole batch (${topic}) ===`);
  const brokerName = await makeTopic(producer, topic, bodies);
  const v = new Verifier(producer, brokerName, topic);
  check('L1 sent 8 messages to a single-queue topic', brokerName != null, `${topic} @ ${v.addr}`);
  const depth = await waitDepth(v, topic, 8);
  check('L1 all 8 messages are on the queue before popping', depth >= 8, `depth=${depth}`);

  let res: any;
  try {
    res = await v.pop(group, topic, 0, 32, 0);
  } catch (e: any) {
    check('L1 POP_MESSAGE answered the whole batch', false, String(e?.message || e));
    return;
  }
  const popped = res.msgFoundList || [];
  check('L1 POP_MESSAGE answered the whole batch',
    res.popStatus === PopStatus.FOUND && popped.length === 8,
    `status=${res.popStatus} got=${popped.length}`);
  if (popped.length !== 8) return;

  const cks: Ck[] = [];
  const problems: string[] = [];
  for (const m of popped) {
    const r = parseCk(m.getProperty(MessageConst.PROPERTY_POP_CK) || '',
      (m.getBody() || Buffer.alloc(0)).toString('utf8'));
    if (r.problem) problems.push(r.problem); else cks.push(r.ck);
  }
  check('L1 every message carries a parsable 8-segment checkpoint', problems.length === 0,
    problems.join('; '));
  check('L1 checkpoint count', cks.length === 8, `${cks.length}`);

  const wantOffsets = cks.map((c) => c.queueOffset);
  check('L1 checkpoint fields the batch body depends on are uniform',
    cks.every((c) => c.brokerName === brokerName && c.queueId === 0
      && c.invisible === INVISIBLE_MS && c.retry === '0'
      && sortedUnique(cks.map((x) => String(x.ckOffset))).length === 1),
    `ckOffset=${cks[0]?.ckOffset} popTime=${cks[0]?.popTime} reviveQid=${cks[0]?.reviveQid}`);

  const body = buildBatchAckMessageRequestBody(topic, group, cks.map((c) => c.raw));
  const ckOffset = cks[0].ckOffset;
  check('L1 body acknowledges exactly the popped offsets, once each',
    body.acks.length === 1 && sameIntSet(ackedOffsets(body.acks[0]), wantOffsets),
    `acked=${JSON.stringify(body.acks.length ? ackedOffsets(body.acks[0]) : [])} `
    + `want=${JSON.stringify(wantOffsets)}`);
  check('L1 body has exactly one entry per merge key',
    body.acks.length === sortedUnique(cks.map(mergeKey)).length,
    `${body.acks.length} entries for ${sortedUnique(cks.map(mergeKey)).length} keys`);

  const ack = body.acks[0];
  const echoes: Record<string, boolean> = {
    bodyBroker: body.brokerName === brokerName, group: ack.consumerGroup === group,
    topic: ack.topic === topic, start: ack.startOffset === ckOffset, queue: ack.queueId === 0,
    marker: ack.retry === '0', popTime: ack.popTime === cks[0].popTime,
    invisible: ack.invisibleTime === INVISIBLE_MS,
    reviveQueueId: ack.reviveQueueId === cks[0].reviveQid,
  };
  check('L1 every entry echoes its checkpoint fields, names the group, the business topic '
    + 'and the first checkpoint broker name',
    Object.values(echoes).every(Boolean),
    `failed=${JSON.stringify(Object.keys(echoes).filter((k) => !echoes[k]))} `
    + `bodyBroker=${body.brokerName} ckBroker=${cks[0].brokerName} topicBroker=${brokerName} `
    + JSON.stringify(ack.toDict()));
  check('L1 bit set is the compact little-endian base64 of the whole batch',
    ack.bitSet.encodeBase64() === Buffer.from([0xff]).toString('base64'),
    `b=${ack.bitSet.encodeBase64()}`);

  const decoded = JSON.parse(body.encode().toString('utf8'));
  check('L1 body round-trips through the wire codec',
    decoded.brokerName === brokerName && decoded.acks.length === 1
    && decoded.acks[0].so === ckOffset && decoded.acks[0].t === topic
    && decoded.acks[0].c === group && decoded.acks[0].b === ack.bitSet.encodeBase64(),
    JSON.stringify(decoded.acks[0]));

  const code = await v.batchAck(topic, group, cks.map((c) => c.raw));
  check('L1 one BATCH_ACK_MESSAGE answered SUCCESS', code.code === ResponseCode.SUCCESS,
    `code=${code.code} ${code.error}`);

  const eyes = new Eyes();
  await watch(v, group, topic, OBSERVE_SEC, eyes);
  check('L1 nothing revived inside 2.5x the invisibility window (the batch really landed)',
    !eyes.cameBack(), eyes.sightings.map((s) => s.body).join(','));
}

// ------------------------------------------------------------------ L2

async function legPartial(producer: any): Promise<void> {
  const topic = `NodeBaPart_${STAMP}`;
  const group = `GID_NodeBaPart_${STAMP}`;
  const bodies = [];
  for (let i = 0; i < 6; i++) bodies.push(`ba-part-${i}`);

  console.log(`=== L2 one checkpoint left out of the batch (${topic}) ===`);
  const brokerName = await makeTopic(producer, topic, bodies);
  const v = new Verifier(producer, brokerName, topic);
  check('L2 sent 6 messages to a single-queue topic', brokerName != null, `${topic} @ ${v.addr}`);
  const depth = await waitDepth(v, topic, 6);
  check('L2 all 6 messages are on the queue before popping', depth >= 6, `depth=${depth}`);

  let res: any;
  try {
    res = await v.pop(group, topic, 0, 32, 0);
  } catch (e: any) {
    check('L2 POP_MESSAGE answered the batch', false, String(e?.message || e));
    return;
  }
  const popped = res.msgFoundList || [];
  check('L2 POP_MESSAGE answered the batch',
    res.popStatus === PopStatus.FOUND && popped.length === 6,
    `status=${res.popStatus} got=${popped.length}`);
  if (popped.length !== 6) return;

  const cks: Ck[] = [];
  const problems: string[] = [];
  for (const m of popped) {
    const r = parseCk(m.getProperty(MessageConst.PROPERTY_POP_CK) || '',
      (m.getBody() || Buffer.alloc(0)).toString('utf8'));
    if (r.problem) problems.push(r.problem); else cks.push(r.ck);
  }
  check('L2 checkpoints parsable', problems.length === 0, problems.join('; '));
  if (cks.length !== 6) return;
  cks.sort((a, b) => a.queueOffset - b.queueOffset);

  // the dropped message is the highest offset: the last bit of the batch
  const dropped = cks[cks.length - 1];
  const kept = cks.slice(0, cks.length - 1);
  const keptTopicBodies = kept.map((c) => c.body);
  const partial = buildBatchAckMessageRequestBody(topic, group, kept.map((c) => c.raw));
  check('L2 body acknowledges 5 of 6 offsets and not the dropped one',
    partial.acks.length === 1
    && sameIntSet(ackedOffsets(partial.acks[0]), kept.map((c) => c.queueOffset))
    && !ackedOffsets(partial.acks[0]).includes(dropped.queueOffset),
    `acked=${JSON.stringify(ackedOffsets(partial.acks[0]))} dropped=${dropped.queueOffset}`);

  const code = await v.batchAck(topic, group, kept.map((c) => c.raw));
  check('L2 partial BATCH_ACK_MESSAGE answered SUCCESS', code.code === ResponseCode.SUCCESS,
    `code=${code.code} ${code.error}`);

  const eyes = new Eyes();
  const broughtBack = await probe(v, group, topic, eyes);
  const sightedBodies = eyes.bodies();
  check('L2 the unacked message really came back', broughtBack,
    `bodies=${JSON.stringify(sightedBodies)} topics=${JSON.stringify(eyes.topicsSeen())}`);
  check('L2 the five acked messages stayed gone while the dropped one came back',
    sightedBodies.length === 1 && sightedBodies[0] === dropped.body
    && !sightedBodies.some((b) => keptTopicBodies.includes(b)),
    `sighted=${JSON.stringify(sightedBodies)} kept=${JSON.stringify(keptTopicBodies)}`);
  if (!broughtBack) return;

  const revived = [];
  const problems2: string[] = [];
  for (const s of eyes.sightings) {
    const r = parseCk(s.ck, s.body);
    if (r.problem) problems2.push(r.problem); else revived.push(r.ck);
  }
  check('L2 the revived checkpoints are parsable', problems2.length === 0, problems2.join('; '));
  if (revived.length === 0) return;
  check('L2 the revived checkpoints carry the retry marker (they live in the retry queue)',
    revived.every((c) => c.retry === '1'),
    `markers=${JSON.stringify(sortedUnique(revived.map((c) => c.retry)))}`);

  const retryRaws = revived.map((c) => c.raw);
  const retryBody = buildBatchAckMessageRequestBody(topic, group, retryRaws);
  check('L2 the retry body keeps the business topic and carries the marker on every entry',
    retryBody.acks.length >= 1
    && retryBody.acks.every((a) => a.topic === topic && a.retry === '1'
      && a.consumerGroup === group),
    JSON.stringify(retryBody.acks.map((a) => a.toDict())));

  const code2 = await v.batchAck(topic, group, retryRaws);
  check('L2 retry-topic BATCH_ACK_MESSAGE answered SUCCESS',
    code2.code === ResponseCode.SUCCESS, `code=${code2.code} ${code2.error}`);

  // durable broker state, read only after the ack: the POP retry topic now has a route
  const retryTopic = MixAll.RETRY_GROUP_TOPIC_PREFIX + group + '_' + topic;
  check('L2 the POP retry topic exists on the broker (the revive service really moved it there)',
    await waitRoute(v.client, retryTopic), retryTopic);

  const after = new Eyes();
  await watch(v, group, topic, OBSERVE_SEC, after);
  check('L2 the marker-driven retry ack silenced it (no further revival)',
    !after.cameBack(), after.sightings.map((s) => s.body).join(','));
}

// ------------------------------------------------------------------ L3

async function legRounds(producer: any): Promise<void> {
  const topic = `NodeBaRounds_${STAMP}`;
  const group = `GID_NodeBaRounds_${STAMP}`;
  const bodies = [];
  for (let i = 0; i < 4; i++) bodies.push(`ba-rounds-${i}`);

  console.log(`=== L3 two pop rounds of one queue, one batch request (${topic}) ===`);
  const brokerName = await makeTopic(producer, topic, bodies);
  const v = new Verifier(producer, brokerName, topic);
  check('L3 sent 4 messages to a single-queue topic', brokerName != null, `${topic} @ ${v.addr}`);
  const depth = await waitDepth(v, topic, 4);
  check('L3 all 4 messages are on the queue before popping', depth >= 4, `depth=${depth}`);

  const rounds: Ck[][] = [];
  try {
    for (let r = 0; r < 2; r++) {
      const res = await v.pop(group, topic, 0, 2, 0);
      const popped = res.msgFoundList || [];
      if (res.popStatus !== PopStatus.FOUND || popped.length !== 2) {
        check(`L3 POP round ${r} answers 2 messages`, false,
          `status=${res.popStatus} got=${popped.length}`);
        return;
      }
      const cks: Ck[] = [];
      const problems: string[] = [];
      for (const m of popped) {
        const parsed = parseCk(m.getProperty(MessageConst.PROPERTY_POP_CK) || '',
          (m.getBody() || Buffer.alloc(0)).toString('utf8'));
        if (parsed.problem) problems.push(parsed.problem); else cks.push(parsed.ck);
      }
      check(`L3 POP round ${r} answers 2 messages with parsable checkpoints`,
        problems.length === 0, problems.join('; '));
      if (problems.length) return;
      rounds.push(cks);
    }
  } catch (e: any) {
    check('L3 the two POP rounds answered 2 messages each', false, String(e?.message || e));
    return;
  }

  const flat = rounds.flat();
  const offsets = flat.map((c) => c.queueOffset);
  check('L3 the two rounds are disjoint batches of one queue',
    sortedUnique(offsets.map(String)).length === 4
    && rounds[0].every((c) => c.queueId === 0) && rounds[1].every((c) => c.queueId === 0),
    `offsets=${JSON.stringify(offsets)}`);

  const keys = sortedUnique(flat.map(mergeKey));
  check('L3 the two rounds produce different merge keys, so the body cannot collapse them',
    keys.length === 2, JSON.stringify(keys));

  const body = buildBatchAckMessageRequestBody(topic, group, flat.map((c) => c.raw));
  check('L3 one body covers all 4 offsets exactly once',
    body.acks.length === 2
    && sameIntSet(body.acks.flatMap(ackedOffsets), offsets),
    `entries=${body.acks.length} acked=${JSON.stringify(body.acks.map(ackedOffsets))}`);

  const code = await v.batchAck(topic, group, flat.map((c) => c.raw));
  check('L3 one BATCH_ACK_MESSAGE for two rounds answered SUCCESS',
    code.code === ResponseCode.SUCCESS, `code=${code.code} ${code.error}`);

  const eyes = new Eyes();
  await watch(v, group, topic, OBSERVE_SEC, eyes);
  check('L3 no message of either round revived inside 2.5x the window',
    !eyes.cameBack(), eyes.sightings.map((s) => s.body).join(','));
}

// ------------------------------------------------------------------ main

async function main(): Promise<void> {
  console.log(`namesrv=${NS} stamp=${STAMP} invisibleTime=${INVISIBLE_MS}ms `
    + `observe=${OBSERVE_SEC}s`);
  const producer = new DefaultMQProducer(`PG_NodeBa_${STAMP}`);
  producer.setNamesrvAddr(NS);
  producer.start();
  try {
    await legAll(producer);
    await legPartial(producer);
    await legRounds(producer);
  } finally {
    producer.shutdown();
  }
  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
