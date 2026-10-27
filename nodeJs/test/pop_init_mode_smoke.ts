// What a POP request puts on the wire for the broker's start position —
// offline, against an in-process fake broker.
//
//   node --experimental-strip-types test/pop_init_mode_smoke.ts
//
// A POP names no offset: for a (group, queue) the broker has never served, the
// initMode field of POP_MESSAGE(200050) IS the start position. MIN(0) replays
// the queue from its first message; MAX(1) starts at its tail. The consumer's
// ConsumeFromWhere is the only thing that decides which one the request
// carries, so a client that sends a constant replays the backlog for a
// tail-starting group (or skips it for a first-offset one) and nothing on the
// client side records that it happened.
//
// Every claim below is read off the request the fake broker DECODED FROM THE
// SOCKET, never off a client-side variable, and every positive claim is paired
// with the control that goes red if the mapping is inverted, hardcoded or
// gated on the wrong value.
import net from 'node:net';

import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { ConsumeFromWhere } from '../src/remoting/heartbeat.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { MESSAGE_REQUEST_MODE_POP } from '../src/remoting/pop_bodies.ts';
import { MixAll } from '../src/common/mixAll.ts';
import {
  POP_INIT_MODE_MAX, POP_INIT_MODE_MIN, consumeInitMode,
} from '../src/client/pop_consumer.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'PopInitModeTopic';
const BROKER = 'broker-a';

class FakePopBroker {
  server: net.Server;
  port = 0;
  queues = new Map<string, number>();
  cidList: string[] = [];
  // One entry per POP_MESSAGE frame the socket actually delivered.
  pops: Array<Record<string, string>> = [];
  modeSets: Array<Record<string, any>> = [];

  constructor() { this.server = net.createServer((s) => this.onConnection(s)); }

  listen(): Promise<number> {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => {
        this.port = (this.server.address() as net.AddressInfo).port;
        resolve(this.port);
      });
    });
  }

  get addr() { return `127.0.0.1:${this.port}`; }
  addTopic(topic: string, n: number) { this.queues.set(topic, n); }

  // popsFor returns the recorded requests of one topic.
  popsFor(topic: string): Array<Record<string, string>> {
    return this.pops.filter((p) => p['topic'] === topic);
  }
  firstInitMode(topic: string): string | undefined {
    const p = this.popsFor(topic)[0];
    return p ? p['initMode'] : undefined;
  }

  private routeBody(topic: string, n: number): string {
    return JSON.stringify({
      orderTopicConf: null,
      queueDatas: [{ brokerName: BROKER, readQueueNums: n, writeQueueNums: n, perm: 6, topicSysFlag: 0 }],
      brokerDatas: [{
        cluster: 'DefaultCluster', brokerName: BROKER,
        brokerAddrs: { '0': this.addr }, zoneName: null, enableActingMaster: false,
      }],
      filterServerTable: {},
    });
  }

  onConnection(sock: net.Socket) {
    let buf = Buffer.alloc(0);
    sock.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      for (;;) {
        if (buf.length < 4) return;
        const total = buf.readInt32BE(0);
        if (total <= 0 || total > 20 * 1024 * 1024) { sock.destroy(); return; }
        if (buf.length < 4 + total) return;
        const frame = buf.subarray(0, 4 + total);
        buf = buf.subarray(4 + total);
        let req: RemotingCommand;
        try { req = RemotingCommand.decode(frame); } catch { sock.destroy(); return; }
        const resp = this.respond(req);
        if (resp) sock.write(resp.encode());
      }
    });
    sock.on('error', () => { /* the client hangs up on teardown */ });
  }

  respond(req: RemotingCommand): RemotingCommand | null {
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS)!;
    resp.opaque = req.opaque;
    resp.serializeTypeCurrentRpc = req.serializeTypeCurrentRpc;
    const ext = req.extFields || {};

    if (req.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      const n = this.queues.get(ext['topic'] || '');
      if (n === undefined) { resp.code = ResponseCode.TOPIC_NOT_EXIST; return resp; }
      resp.body = Buffer.from(this.routeBody(ext['topic'], n), 'utf8');
      return resp;
    }
    if (req.code === RequestCode.HEART_BEAT) {
      // Record the beat's clientID so the group has a member and the
      // assignment is not empty, the way a live broker would.
      try {
        const hb = JSON.parse(req.body ? req.body.toString('utf8') : '{}');
        if (hb.clientID && !this.cidList.includes(String(hb.clientID))) {
          this.cidList.push(String(hb.clientID));
        }
      } catch { /* an unparseable beat simply leaves the group empty */ }
      return resp;
    }
    if (req.code === RequestCode.GET_CONSUMER_LIST_BY_GROUP) {
      resp.body = Buffer.from(JSON.stringify({ consumerIdList: this.cidList }), 'utf8');
      return resp;
    }
    if (req.code === RequestCode.SET_MESSAGE_REQUEST_MODE) {
      try { this.modeSets.push(JSON.parse(req.body ? req.body.toString('utf8') : '{}')); }
      catch { this.modeSets.push({}); }
      return resp;
    }
    if (req.code === RequestCode.POP_MESSAGE) {
      // The frame as the broker reads it: every header field is a string on the
      // wire, so 0 arrives as '0' and 1 as '1'.
      this.pops.push({ ...ext });
      // POLLING_FULL parks the round for as long as this client's own backoff,
      // so one run asks once per queue instead of spinning.
      resp.code = ResponseCode.POLLING_FULL;
      return resp;
    }
    return resp;
  }

  close() { this.server.close(); }
}

async function waitFor(cond: () => boolean, timeoutMs = 4000): Promise<boolean> {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    if (cond()) return true;
    await new Promise<void>((r) => setTimeout(r, 10));
  }
  return cond();
}

// popRun brings up a POP consumer with the given start policy and returns the
// broker it talked to; every POP request it received is on the wire record.
async function popRun(group: string, policy: string | null): Promise<FakePopBroker> {
  const broker = new FakePopBroker();
  await broker.listen();
  broker.addTopic(TOPIC, 1);
  broker.addTopic(MixAll.getRetryTopic(group), 1);
  const c = new DefaultMQPushConsumer(group);
  c.setNamesrvAddr(broker.addr);
  c.setPopMode(true);
  // The business topic and the group's retry topic are both popped, so the
  // policy has to reach the request for each.
  c.subscribe(TOPIC, '*');
  if (policy !== null) c.setConsumeFromWhere(policy);
  c.registerMessageListener({ consumeMessage: () => 0 });
  await c.start();
  const seen = await waitFor(() => broker.popsFor(TOPIC).length >= 1
    && broker.popsFor(MixAll.getRetryTopic(group)).length >= 1);
  await c.shutdown();
  if (!seen) throw new Error(`the POP consumer never asked for both queues (got ${broker.pops.length} pops)`);
  broker.close();
  return broker;
}

async function main() {
  // ---------------- 1. the default policy (CONSUME_FROM_LAST_OFFSET) -> MAX
  {
    const broker = await popRun('GID_pop_init_last', ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
    const reqs = broker.popsFor(TOPIC);
    check('the default consumer puts exactly one start position on each POP request',
      reqs.length >= 1 && reqs.every((p) => p['initMode'] === '1'),
      reqs.map((p) => String(p['initMode'])).join(','));
    check('and the same MAX(1) for the group\'s retry queue',
      broker.popsFor(MixAll.getRetryTopic('GID_pop_init_last'))
        .every((p) => p['initMode'] === '1'),
      String(broker.firstInitMode(MixAll.getRetryTopic('GID_pop_init_last'))));
    check('the field travels as the wire string, not an absent key',
      typeof reqs[0]['initMode'] === 'string' && 'initMode' in reqs[0],
      JSON.stringify(reqs[0]['initMode']));
    check('the request is a POP of the subscribed topic by this group',
      reqs[0]['topic'] === TOPIC && reqs[0]['consumerGroup'] === 'GID_pop_init_last',
      JSON.stringify(reqs[0]).slice(0, 160));
  }

  // --------------------- 2. never configured at all: the same tail position
  {
    const broker = await popRun('GID_pop_init_unset', null);
    check('control: a consumer that never set the policy still asks for MAX(1)',
      broker.popsFor(TOPIC).every((p) => p['initMode'] === '1'),
      String(broker.firstInitMode(TOPIC)));
  }

  // ------------------------ 3. CONSUME_FROM_FIRST_OFFSET -> MIN
  {
    const broker = await popRun('GID_pop_init_first', ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
    check('a first-offset consumer asks the broker to replay from MIN(0)',
      broker.popsFor(TOPIC).every((p) => p['initMode'] === '0')
      && broker.popsFor(MixAll.getRetryTopic('GID_pop_init_first')).every((p) => p['initMode'] === '0'),
      `${broker.firstInitMode(TOPIC)}/${broker.firstInitMode(MixAll.getRetryTopic('GID_pop_init_first'))}`);
  }

  // ------------- 4. every other policy is "not FIRST", so it means MAX too
  {
    const broker = await popRun('GID_pop_init_ts', ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
    check('a timestamp consumer is not a first-offset one, so it asks for MAX(1)',
      broker.popsFor(TOPIC).every((p) => p['initMode'] === '1'),
      String(broker.firstInitMode(TOPIC)));
  }

  // ------------------ 5. the mapping itself, as the request builder uses it
  {
    check('MIN is 0 and MAX is 1 on the wire',
      POP_INIT_MODE_MIN === 0 && POP_INIT_MODE_MAX === 1,
      `${POP_INIT_MODE_MIN}/${POP_INIT_MODE_MAX}`);
    check('FIRST alone maps to MIN',
      consumeInitMode(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET) === POP_INIT_MODE_MIN);
    check('LAST maps to MAX',
      consumeInitMode(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET) === POP_INIT_MODE_MAX);
    check('TIMESTAMP maps to MAX',
      consumeInitMode(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP) === POP_INIT_MODE_MAX);
    // The unset/unknown spellings must not fall through to MIN: an unread policy
    // is a tail start, not a replay of the backlog.
    check('control: an unset or unknown policy maps to MAX',
      consumeInitMode(null) === POP_INIT_MODE_MAX && consumeInitMode(undefined) === POP_INIT_MODE_MAX
      && consumeInitMode('CONSUME_FROM_NOW') === POP_INIT_MODE_MAX);
  }

  // ------------------ 6. the POP mode switch rode the same start-up
  {
    const broker = new FakePopBroker();
    await broker.listen();
    broker.addTopic(TOPIC, 1);
    broker.addTopic(MixAll.getRetryTopic('GID_pop_init_mode'), 1);
    const c = new DefaultMQPushConsumer('GID_pop_init_mode');
    c.setNamesrvAddr(broker.addr);
    c.setPopMode(true);
    c.subscribe(TOPIC, '*');
    c.registerMessageListener({ consumeMessage: () => 0 });
    await c.start();
    const asked = await waitFor(() => broker.modeSets.length >= 1);
    await c.shutdown();
    check('the consumer asked the broker to serve the business topic in POP mode',
      asked && broker.modeSets.some((b) => b['topic'] === TOPIC && b['mode'] === MESSAGE_REQUEST_MODE_POP
        && b['consumerGroup'] === 'GID_pop_init_mode'),
      JSON.stringify(broker.modeSets).slice(0, 200));
    check('and never for the group\'s own retry topic',
      !broker.modeSets.some((b) => String(b['topic'] || '').startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)),
      JSON.stringify(broker.modeSets).slice(0, 200));
    broker.close();
  }

  console.log(`\npop init mode: ${pass} checks, ${fail} failed`);
  if (fail > 0) process.exitCode = 1;
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
