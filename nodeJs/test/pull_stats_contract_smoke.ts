// The pull-path consume-stats contract — offline, against an in-process fake
// broker that serves scripted PULL_MESSAGE answers.
//
//   node --experimental-strip-types test/pull_stats_contract_smoke.ts
//
// What one push-consumer pull round books into PULL_RT / PULL_TPS:
//
//   (1) PULL_RT is booked ONLY by a round that answered FOUND. A NO_NEW_MSG,
//       NO_MATCHED_MSG or OFFSET_ILLEGAL round, a broker error code and a
//       client-side timeout book nothing at all: PULL_RT is meant to describe
//       the latency of rounds that actually carried work, so a round that
//       carried none has no business in that average.
//   (2) PULL_TPS is fed the size of the list AFTER the pre-delivery filter —
//       the second client-side tag pass, then the FilterMessageHook list —
//       has rewritten it, and only when that list is non-empty. Rows dropped
//       before delivery never reached the listener, so they are not pulled
//       throughput. A FOUND round whose rows are all filtered out therefore
//       books its RT but zero TPS.
//   (3) Control legs prove the bookings that DO happen are not over-eager
//       either: a kept row is booked once, a throwing hook drops nothing from
//       the books.
//
// The stats items are cumulative (value/times, no reset before the 10s
// sampler), so every assertion below is an exact count of what the rounds
// booked, not a rate read off a window. Each scenario keeps pulling AFTER its
// scripted first round (the idle spin re-pulls the same next offset), which is
// what makes leg 1 discriminating: if a non-FOUND round booked RT, the RT
// times would grow with every spin round before the check runs.
import net from 'node:net';

import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { MessageExt } from '../src/common/message.ts';
import { encodeMessageExt } from '../src/common/messageDecoder.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';
import { FilterMessageContext, FilterMessageHook } from '../src/client/hook.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { ConsumeFromWhere } from '../src/remoting/heartbeat.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const BROKER = 'pull-stats-broker';

// PullReply is one scripted PULL_MESSAGE answer: the response code, the
// nextBeginOffset it reports (default: echo the request so the cursor holds),
// optional encoded rows, and an optional defer that keeps the answer from
// arriving until after the client's request timeout has fired.
interface PullReply {
  code: number;
  next?: number;
  rows?: MessageExt[];
  delayMs?: number;
}

function row(topic: string, offset: number, tag: string | null): MessageExt {
  const m = new MessageExt(topic, Buffer.from('body-' + offset), tag);
  m.setQueueId(0);
  m.setQueueOffset(offset);
  m.setCommitLogOffset(offset);
  m.setBornTimestamp(Date.now());
  m.setStoreHost('127.0.0.1');
  m.setBornHost('127.0.0.1');
  return m;
}

class FakePullBroker {
  server: net.Server;
  port = 0;
  topic = '';
  plan: (offset: number) => PullReply = (offset) => ({ code: ResponseCode.PULL_NOT_FOUND, next: offset });
  // queueOffset of every PULL_MESSAGE that reached the socket, in order.
  pulls: number[] = [];

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

  private routeBody(topic: string): string {
    return JSON.stringify({
      orderTopicConf: null,
      queueDatas: [{ brokerName: BROKER, readQueueNums: 1, writeQueueNums: 1, perm: 6, topicSysFlag: 0 }],
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
        this.respond(req, sock);
      }
    });
    sock.on('error', () => { /* the client hangs up on teardown */ });
  }

  respond(req: RemotingCommand, sock: net.Socket): void {
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS)!;
    resp.opaque = req.opaque;
    resp.serializeTypeCurrentRpc = req.serializeTypeCurrentRpc;
    const ext = req.extFields || {};
    const write = (delayMs: number) => {
      if (delayMs > 0) setTimeout(() => { if (!sock.destroyed) sock.write(resp.encode()); }, delayMs);
      else sock.write(resp.encode());
    };

    if (req.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      if (ext['topic'] !== this.topic) { resp.code = ResponseCode.TOPIC_NOT_EXIST; write(0); return; }
      resp.body = Buffer.from(this.routeBody(this.topic), 'utf8');
      write(0); return;
    }
    if (req.code === RequestCode.HEART_BEAT) {
      try {
        const hb = JSON.parse(req.body ? req.body.toString('utf8') : '{}');
        // The group needs exactly this one member, or the assignment is empty
        // and no pull loop ever starts.
        if (hb.clientID && !this.cidList.includes(String(hb.clientID))) {
          this.cidList.push(String(hb.clientID));
        }
      } catch { /* an unparseable beat simply leaves the group empty */ }
      write(0); return;
    }
    if (req.code === RequestCode.GET_CONSUMER_LIST_BY_GROUP) {
      resp.body = Buffer.from(JSON.stringify({ consumerIdList: this.cidList }), 'utf8');
      write(0); return;
    }
    if (req.code === RequestCode.QUERY_CONSUMER_OFFSET) {
      // Nothing committed: the consume-from-where policy decides, and the
      // scenario pins it to FIRST so the first pull starts at offset 0.
      resp.code = ResponseCode.QUERY_NOT_FOUND;
      write(0); return;
    }
    if (req.code === RequestCode.PULL_MESSAGE) {
      const offset = parseInt(ext['queueOffset'] || '-1', 10);
      this.pulls.push(offset);
      const reply = this.plan(offset);
      resp.code = reply.code;
      resp.extFields['nextBeginOffset'] = String(reply.next ?? offset);
      resp.extFields['minOffset'] = '0';
      resp.extFields['maxOffset'] = '1000';
      resp.extFields['suggestWhichBrokerId'] = '0';
      if (reply.rows && reply.rows.length > 0) {
        resp.body = Buffer.concat(reply.rows.map((r) => encodeMessageExt(r)));
      }
      write(reply.delayMs ?? 0);
      return;
    }
    write(0);
  }

  cidList: string[] = [];
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

// Book is the exact state of the two pull stats items after the scenario ran.
interface Book {
  rtTimes: number; rtValue: number;      // 0 when the PULL_RT item was never created
  tpsTimes: number; tpsValue: number;    // 0 when the PULL_TPS item was never created
  rtSeen: boolean; tpsSeen: boolean;
}

function bookOf(c: DefaultMQPushConsumer, topic: string, group: string): Book {
  const stats = (c as any)._statsManager;
  const rt = stats.topicAndGroupPullRT.find(`${topic}@${group}`);
  const tps = stats.topicAndGroupPullTPS.find(`${topic}@${group}`);
  return {
    rtTimes: rt ? rt.times : 0, rtValue: rt ? rt.value : 0,
    tpsTimes: tps ? tps.times : 0, tpsValue: tps ? tps.value : 0,
    rtSeen: rt != null, tpsSeen: tps != null,
  };
}

// scenario drives one push consumer against one scripted broker and returns
// the books, the delivered rows and the observed pull offsets. The consumer is
// shut down before returning, so a mutated booking would already be visible in
// the cumulative items.
async function scenario(opts: {
  group: string;
  topic: string;
  expression?: string;
  plan: (offset: number) => PullReply;
  hooks?: FilterMessageHook[];
  pullTimeoutMillis?: number;
  minPulls: number;
}): Promise<{ broker: FakePullBroker; book: Book; delivered: MessageExt[]; pulls: number[] }> {
  const broker = new FakePullBroker();
  broker.topic = opts.topic;
  broker.plan = opts.plan;
  await broker.listen();

  const c = new DefaultMQPushConsumer(opts.group);
  c.setNamesrvAddr(broker.addr);
  c.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
  if (opts.pullTimeoutMillis !== undefined) c.setPullTimeoutMillis(opts.pullTimeoutMillis);
  c.subscribe(opts.topic, opts.expression ?? 'TagA');
  for (const hook of opts.hooks ?? []) c.registerFilterMessageHook(hook);
  const delivered: MessageExt[] = [];
  c.registerMessageListener({
    consumeMessage: (msgs: MessageExt[]) => {
      delivered.push(...msgs);
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    },
  });
  await c.start();
  const reached = await waitFor(() => broker.pulls.length >= opts.minPulls);
  if (!reached) throw new Error(`scenario ${opts.group}: the broker never saw ${opts.minPulls} pulls `
    + `(got ${broker.pulls.length})`);
  // Let any further round land before reading the books: leg 1 is proven by
  // the RT count NOT growing while idle rounds keep spinning.
  await new Promise<void>((r) => setTimeout(r, 150));
  const book = bookOf(c, opts.topic, opts.group);
  await c.shutdown();
  broker.close();
  return { broker, book, delivered, pulls: broker.pulls };
}

class DropFirstHook extends FilterMessageHook {
  filterMessage(ctx: FilterMessageContext): any[] | null {
    ctx.msgList = (ctx.msgList ?? []).slice(1);
    return ctx.msgList;
  }
}

class ThrowingHook extends FilterMessageHook {
  filterMessage(_ctx: FilterMessageContext): any[] | null {
    throw new Error('this hook always blows up');
  }
}

async function main() {
  // ---- leg 1a: NO_NEW_MSG rounds book nothing, however many of them run
  {
    const s = await scenario({
      group: 'GID_pullstats_none', topic: 'PullStatsNoNewMsg',
      plan: (offset) => ({ code: ResponseCode.PULL_NOT_FOUND, next: offset }),
      minPulls: 6,
    });
    check('a NO_NEW_MSG round repeated on the wire at least six times',
      s.pulls.length >= 6, `pulls=${s.pulls.length}`);
    check('NO_NEW_MSG books no PULL_RT at all',
      !s.book.rtSeen && s.book.rtTimes === 0, `rt=${s.book.rtTimes}/${s.book.rtSeen}`);
    check('NO_NEW_MSG books no PULL_TPS at all',
      !s.book.tpsSeen && s.book.tpsValue === 0, `tps=${s.book.tpsValue}/${s.book.tpsSeen}`);
  }

  // ---- leg 1b: NO_MATCHED_MSG rounds book nothing
  {
    const s = await scenario({
      group: 'GID_pullstats_nomatch', topic: 'PullStatsNoMatchedMsg',
      plan: (offset) => ({ code: ResponseCode.PULL_RETRY_IMMEDIATELY, next: offset }),
      minPulls: 6,
    });
    check('a NO_MATCHED_MSG round repeated on the wire at least six times',
      s.pulls.length >= 6, `pulls=${s.pulls.length}`);
    check('NO_MATCHED_MSG books no PULL_RT at all',
      !s.book.rtSeen, `rt=${s.book.rtTimes}`);
    check('NO_MATCHED_MSG books no PULL_TPS at all',
      !s.book.tpsSeen, `tps=${s.book.tpsValue}`);
  }

  // ---- leg 1c: the OFFSET_ILLEGAL round fixes the cursor but books nothing
  // (the rebuilt queue only comes back on the next rebalance tick, so this leg
  // judges the single illegal round, not a spin of them)
  {
    const s = await scenario({
      group: 'GID_pullstats_illegal', topic: 'PullStatsOffsetIllegal',
      plan: (offset) => ({ code: ResponseCode.PULL_OFFSET_MOVED, next: 100 }),
      minPulls: 1,
    });
    check('the pull answered OFFSET_ILLEGAL', s.pulls[0] === 0, `pulls=${s.pulls.join(',')}`);
    check('the OFFSET_ILLEGAL round books no PULL_RT',
      !s.book.rtSeen, `rt=${s.book.rtTimes}`);
    check('and no PULL_TPS', !s.book.tpsSeen, `tps=${s.book.tpsValue}`);
  }

  // ---- leg 1d: a round that throws (broker error code) books nothing
  {
    const s = await scenario({
      group: 'GID_pullstats_error', topic: 'PullStatsError',
      plan: () => ({ code: ResponseCode.SYSTEM_ERROR }),
      minPulls: 2,
    });
    check('the failing pull was actually issued twice (the 1000 ms generic error backoff apart)',
      s.pulls.length >= 2, `pulls=${s.pulls.length}`);
    check('a pull that threw a broker error books no PULL_RT',
      !s.book.rtSeen, `rt=${s.book.rtTimes}`);
    check('and no PULL_TPS', !s.book.tpsSeen, `tps=${s.book.tpsValue}`);
  }

  // ---- leg 1e: a round that times out on the client books nothing
  {
    const s = await scenario({
      group: 'GID_pullstats_timeout', topic: 'PullStatsTimeout',
      pullTimeoutMillis: 300,
      // The answer exists but arrives after the request timeout has fired.
      plan: (offset) => ({ code: ResponseCode.PULL_NOT_FOUND, next: offset, delayMs: 1500 }),
      minPulls: 3,
    });
    check('the consumer re-issued the pull after timing out',
      s.pulls.length >= 3, `pulls=${s.pulls.length}`);
    check('a pull that timed out books no PULL_RT',
      !s.book.rtSeen, `rt=${s.book.rtTimes}`);
    check('and no PULL_TPS', !s.book.tpsSeen, `tps=${s.book.tpsValue}`);
  }

  // ---- leg 2a: FOUND whose rows are ALL dropped by the tag pass: RT yes, TPS zero
  {
    const s = await scenario({
      group: 'GID_pullstats_allout', topic: 'PullStatsAllFiltered',
      plan: (offset) => offset === 0 ? {
        code: ResponseCode.SUCCESS, next: 10,
        rows: [row('PullStatsAllFiltered', 1, 'TagB'),
          row('PullStatsAllFiltered', 2, 'TagC'),
          row('PullStatsAllFiltered', 3, null)],
      } : { code: ResponseCode.PULL_NOT_FOUND, next: offset },
      minPulls: 6,
    });
    check('the FOUND round then the idle spin both reached the broker',
      s.pulls[0] === 0 && s.pulls.filter((o) => o === 10).length >= 4,
      `pulls=${s.pulls.join(',')}`);
    check('a fully filtered FOUND round books exactly its one PULL_RT',
      s.book.rtTimes === 1, `rt times=${s.book.rtTimes}`);
    check('nothing of it reaches the listener',
      s.delivered.length === 0, `delivered=${s.delivered.length}`);
    check('and the fully filtered FOUND round books zero pulled messages',
      s.book.tpsValue === 0 && s.book.tpsTimes === 0,
      `tps=${s.book.tpsValue}/${s.book.tpsTimes}`);
  }

  // ---- leg 2b: FOUND with M in and N kept: TPS is N, not M (tag pass)
  {
    const s = await scenario({
      group: 'GID_pullstats_tagkeep', topic: 'PullStatsTagKeep',
      plan: (offset) => offset === 0 ? {
        code: ResponseCode.SUCCESS, next: 10,
        rows: [row('PullStatsTagKeep', 1, 'TagA'), row('PullStatsTagKeep', 2, 'TagB'),
          row('PullStatsTagKeep', 3, 'TagA'), row('PullStatsTagKeep', 4, 'TagC')],
      } : { code: ResponseCode.PULL_NOT_FOUND, next: offset },
      minPulls: 6,
    });
    check('the FOUND round answered four rows and the spin kept pulling',
      s.pulls[0] === 0 && s.pulls.length >= 5, `pulls=${s.pulls.join(',')}`);
    check('the FOUND round books exactly its one PULL_RT',
      s.book.rtTimes === 1, `rt times=${s.book.rtTimes}`);
    check('PULL_TPS gets the kept count 2, not the fetched count 4',
      s.book.tpsValue === 2 && s.book.tpsTimes === 1,
      `tps=${s.book.tpsValue}/${s.book.tpsTimes}`);
    check('the listener saw the same two rows that were booked',
      s.delivered.length === 2, `delivered=${s.delivered.length}`);
  }

  // ---- leg 2c: the hook rewrite is booked too, and a throwing hook loses nothing
  {
    const s = await scenario({
      group: 'GID_pullstats_hookkeep', topic: 'PullStatsHookKeep',
      hooks: [new DropFirstHook()],
      plan: (offset) => offset === 0 ? {
        code: ResponseCode.SUCCESS, next: 10,
        rows: [row('PullStatsHookKeep', 1, 'TagA'), row('PullStatsHookKeep', 2, 'TagA'),
          row('PullStatsHookKeep', 3, 'TagA'), row('PullStatsHookKeep', 4, 'TagB')],
      } : { code: ResponseCode.PULL_NOT_FOUND, next: offset },
      minPulls: 6,
    });
    check('tag pass 3, hook drops 1 of the kept 3: two rows delivered',
      s.delivered.length === 2, `delivered=${s.delivered.length}`);
    check('PULL_TPS is the post-hook count 2 — not 4, not 3',
      s.book.tpsValue === 2 && s.book.tpsTimes === 1,
      `tps=${s.book.tpsValue}/${s.book.tpsTimes}`);
    check('the hook round still books exactly its one PULL_RT',
      s.book.rtTimes === 1, `rt times=${s.book.rtTimes}`);
  }
  {
    const s = await scenario({
      group: 'GID_pullstats_hookthrows', topic: 'PullStatsHookThrows',
      hooks: [new ThrowingHook()],
      plan: (offset) => offset === 0 ? {
        code: ResponseCode.SUCCESS, next: 10,
        rows: [row('PullStatsHookThrows', 1, 'TagA'), row('PullStatsHookThrows', 2, 'TagA'),
          row('PullStatsHookThrows', 3, 'TagA')],
      } : { code: ResponseCode.PULL_NOT_FOUND, next: offset },
      minPulls: 5,
    });
    check('control: a throwing hook keeps the whole FOUND batch',
      s.delivered.length === 3, `delivered=${s.delivered.length}`);
    check('control: and the round books its three kept rows and one RT',
      s.book.tpsValue === 3 && s.book.rtTimes === 1,
      `tps=${s.book.tpsValue} rt=${s.book.rtTimes}`);
  }

  console.log(`\npull stats contract: ${pass} checks, ${fail} failed`);
  if (fail > 0) process.exitCode = 1;
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
