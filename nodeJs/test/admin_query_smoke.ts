// DefaultMQAdminExt.queryConsumeTimeSpan (303) — offline wire test.
//
// Behaviour baseline (the broker's QUERY_CONSUME_TIME_SPAN reply shape as this
// admin exercises it, plus the error contract it must keep for callers):
//   * one QUERY_CONSUME_TIME_SPAN(303) per broker of the topic's route, header
//     keys `topic` + `group` (NOT consumerGroup), body decoded as
//     QueryConsumeTimeSpanBody and its consumeTimeSpanSet concatenated;
//   * the sweep walks EVERY broker in the route, master only (selectBrokerAddr),
//     and a broker that answers with an empty body contributes nothing;
//   * unlike examineTopicStats, a failing broker is NOT swallowed: the admin call
//     rejects, so the caller sees the error.
//
// Run: node --experimental-strip-types test/admin_query_smoke.ts
import net from 'node:net';

import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { MQBrokerException, MQClientException } from '../src/remoting/exception.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'AdminSpanTopic';
const GROUP = 'GID_admin_span';

class FakeCluster {
  server: net.Server;
  port = 0;
  queueNums = new Map<string, number>();
  brokerNames: string[] = ['spanBroker'];
  // 303 replies handed out in order; an entry without `body` answers with an
  // empty body, `code` overrides SUCCESS.
  script: { body?: string; code?: number }[] = [];
  // Every 303 request header seen, as `topic=<t>;group=<g>`.
  requests: string[] = [];
  // 303 responses already consumed; the tail of the script repeats the last entry.
  private served = 0;

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
  setQueues(topic: string, n: number) { this.queueNums.set(topic, n); }

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
      const topic = String(ext['topic'] || '');
      const n = this.queueNums.get(topic);
      if (n == null) { resp.code = ResponseCode.TOPIC_NOT_EXIST; return resp; }
      const addrs: Record<string, string> = {};
      for (let i = 0; i < this.brokerNames.length; i++) addrs['0'] = this.addr;
      resp.body = Buffer.from(JSON.stringify({
        orderTopicConf: null,
        queueDatas: [{
          brokerName: this.brokerNames[0], readQueueNums: n, writeQueueNums: n,
          perm: 6, topicSysFlag: 0,
        }],
        brokerDatas: this.brokerNames.map((bn) => ({
          cluster: 'DefaultCluster', brokerName: bn, brokerAddrs: { '0': this.addr },
          zoneName: null, enableActingMaster: false,
        })),
        filterServerTable: {},
      }), 'utf8');
      return resp;
    }

    if (req.code === RequestCode.QUERY_CONSUME_TIME_SPAN) {
      this.requests.push(`topic=${ext['topic']};group=${ext['group']}`);
      const step = this.script[Math.min(this.served, this.script.length - 1)] || {};
      this.served++;
      if (step.code != null) resp.code = step.code;
      if (step.body != null) resp.body = Buffer.from(step.body, 'utf8');
      return resp;
    }

    return resp; // heartbeat / unregistration / everything else: SUCCESS
  }

  close() { this.server.close(); }
}

function span(brokerName: string, queueId: number, minTs: number, maxTs: number): string {
  return JSON.stringify({
    consumeTimeSpanSet: [{
      messageQueue: { topic: TOPIC, brokerName, queueId },
      consumeTimeStamp: maxTs - minTs, minTimestamp: minTs, maxTimestamp: maxTs,
    }],
  });
}

async function started(cluster: FakeCluster): Promise<DefaultMQAdminExt> {
  const admin = new DefaultMQAdminExt();
  admin.setNamesrvAddr(cluster.addr);
  admin.setInstanceName(`ADMIN_span_${process.pid}_${Math.floor(Math.random() * 1e6)}`);
  admin.start();
  return admin;
}

async function main() {
  // ------------------------------------------------ 1. one request per broker
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.setQueues(TOPIC, 2);
    cluster.brokerNames = ['spanBrokerA', 'spanBrokerB'];
    cluster.script = [{ body: span('spanBrokerA', 0, 1000, 1500) },
      { body: span('spanBrokerB', 0, 2000, 2600) }];
    const admin = await started(cluster);
    const spans = await admin.queryConsumeTimeSpan(TOPIC, GROUP);
    check('one 303 per broker of the route', cluster.requests.length === 2,
      String(cluster.requests.length));
    check('header carries topic + group',
      cluster.requests.every((r) => r === `topic=${TOPIC};group=${GROUP}`), cluster.requests[0]);
    check('both brokers\' sets are concatenated, in route order', spans.length === 2 &&
      spans[0]['maxTimestamp'] === 1500 && spans[1]['maxTimestamp'] === 2600,
      JSON.stringify(spans.map((s) => s['maxTimestamp'])));
    check('the QueueTimeSpan fields are passed through untouched',
      spans[0]['messageQueue']['brokerName'] === 'spanBrokerA' &&
      spans[0]['messageQueue']['queueId'] === 0 &&
      spans[0]['consumeTimeStamp'] === 500, JSON.stringify(spans[0]));
    admin.shutdown();
    cluster.close();
  }

  // ------------------------------------------ 2. empty body contributes nothing
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.setQueues(TOPIC, 2);
    cluster.brokerNames = ['spanBrokerA', 'spanBrokerB'];
    cluster.script = [{}, { body: span('spanBrokerB', 0, 2000, 2600) }];
    const admin = await started(cluster);
    const spans = await admin.queryConsumeTimeSpan(TOPIC, GROUP);
    check('a broker with an empty body is skipped, the other still counts',
      spans.length === 1 && spans[0]['maxTimestamp'] === 2600, JSON.stringify(spans));
    admin.shutdown();
    cluster.close();
  }

  // ----------------------------------------- 3. a failing broker is not swallowed
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.setQueues(TOPIC, 1);
    cluster.script = [{ code: ResponseCode.SYSTEM_ERROR }];
    const admin = await started(cluster);
    let code = 0;
    try {
      await admin.queryConsumeTimeSpan(TOPIC, GROUP);
    } catch (e) {
      code = e instanceof MQBrokerException ? (e as MQBrokerException).responseCode : -1;
    }
    check('a broker error propagates instead of returning a short list',
      code === ResponseCode.SYSTEM_ERROR, String(code));
    admin.shutdown();
    cluster.close();
  }

  // ------------------------------------------------- 4. unrouted topic reports
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const admin = await started(cluster);
    let msg = '';
    try {
      await admin.queryConsumeTimeSpan('NoRouteTopic', GROUP);
    } catch (e) {
      msg = e instanceof MQClientException ? (e as Error).message : `wrong type: ${(e as Error).message}`;
    }
    check('a topic without route fails before any 303 is sent',
      msg.includes('route') && cluster.requests.length === 0, msg);
    admin.shutdown();
    cluster.close();
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
