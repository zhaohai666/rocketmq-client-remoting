// DefaultMQPullConsumer.fetchMessageQueuesInBalance — offline wire test.
//
// Baseline (Java 5.5.1, read line by line; the same judgements as
// python/tests/test_pull_consumer.py, go/client/pull_consumer_test.go,
// csharp PullBalanceViewTests, rust and cpp's test_pull_consumer_balance_view):
//   * MQPullConsumer:187 → DefaultMQPullConsumerImpl:120-135 filters
//     rebalanceImpl.getProcessQueueTable() by topic, then
//     parseSubscribeMessageQueues:153-161 strips the namespace.
//     example/simple/PullConsumer.java:62 is the official caller: it pulls ONLY
//     its own share, so "returns every queue" is the duplicate-consumption bug.
//   * This port runs no pull-side rebalance thread, so the view is computed on
//     demand with RebalanceImpl#rebalanceByTopic's formula; the tests therefore
//     pin the FORMULA, the "cannot compute" fallback and the isRunning() guard.
//   ⚠ The fallback is deliberately NOT "all queues": a group of N instances must
//     never all read the same messages because one query went unanswered.
//
// Run: node --experimental-strip-types test/pull_balance_view_smoke.ts
import net from 'node:net';

import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import { MessageQueue } from '../src/common/message.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'PullBalanceViewTopic';
const BROKER = 'b1';

// One fake endpoint that plays both nameserver (route) and broker (38 / 11).
// The balance view only needs "a route + a consumer list", so who answers is
// not part of the judgement.
class FakeCluster {
  server: net.Server;
  port = 0;
  routes = new Map<string, string>();
  cidList: string[] = [];
  pulls: string[] = [];

  constructor() {
    this.server = net.createServer((sock) => this.onConnection(sock));
  }

  listen(): Promise<number> {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => {
        this.port = (this.server.address() as net.AddressInfo).port;
        resolve(this.port);
      });
    });
  }

  get addr() { return `127.0.0.1:${this.port}`; }

  addRoute(topic: string, readQueues: number, masterAddr: string) {
    const queueDatas = [];
    for (let i = 0; i < 1; i++) {
      queueDatas.push({
        brokerName: BROKER, readQueueNums: readQueues, writeQueueNums: readQueues,
        perm: 6, topicSysFlag: 0,
      });
    }
    this.routes.set(topic, JSON.stringify({
      orderTopicConf: null,
      queueDatas,
      brokerDatas: [{
        cluster: 'DefaultCluster', brokerName: BROKER,
        brokerAddrs: { '0': masterAddr }, zoneName: null, enableActingMaster: false,
      }],
      filterServerTable: {},
    }));
  }

  setCidList(ids: string[]) { this.cidList = ids; }

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
        try {
          req = RemotingCommand.decode(frame);
        } catch {
          sock.destroy();
          return;
        }
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
    if (req.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      const topic = req.extFields['topic'] || '';
      const body = this.routes.get(topic);
      if (!body) {
        resp.code = ResponseCode.TOPIC_NOT_EXIST;
        return resp;
      }
      resp.body = Buffer.from(body, 'utf8');
      return resp;
    }
    if (req.code === RequestCode.GET_CONSUMER_LIST_BY_GROUP) {
      // An empty list is how a real broker answers a group it has never heard
      // of — it is not an error, and the view must not read it as "I own all".
      resp.body = Buffer.from(JSON.stringify({ consumerIdList: this.cidList }), 'utf8');
      return resp;
    }
    if (req.code === RequestCode.PULL_MESSAGE || req.code === RequestCode.LITE_PULL_MESSAGE) {
      this.pulls.push(`${req.extFields['topic']}@${req.extFields['queueId']}`);
      resp.code = ResponseCode.PULL_NOT_FOUND;
      const offset = req.extFields['queueOffset'] || '0';
      resp.addExtField('nextBeginOffset', offset);
      resp.addExtField('minOffset', '0');
      resp.addExtField('maxOffset', '0');
      resp.addExtField('suggestWhichBrokerId', '0');
      return resp;
    }
    return resp;
  }

  close() { this.server.close(); }
}

function queueIds(mqs: MessageQueue[]): number[] {
  return mqs.map((m) => m.getQueueId());
}

async function started(group: string, cluster: FakeCluster, topic: string,
  model = 'CLUSTERING'): Promise<DefaultMQPullConsumer> {
  const c = new DefaultMQPullConsumer(group);
  c.setNamesrvAddr(cluster.addr);
  c.setMessageModel(model);
  c.subscribe(topic, '*');
  await c.start();
  return c;
}

async function main() {
  // ------------------------------------------------------------ 1. sole instance
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addRoute(TOPIC, 3, cluster.addr);
    const c = await started('GID_pull_bal_sole', cluster, TOPIC);
    cluster.setCidList([c.clientID]);
    const view = await c.fetchMessageQueuesInBalance(TOPIC);
    check('sole instance takes every queue, in order',
      queueIds(view).join(',') === '0,1,2', queueIds(view).join(','));
    check('queue identity is kept',
      view.every((m) => m.getTopic() === TOPIC && m.getBrokerName() === BROKER));
    await c.shutdown();
    cluster.close();
  }

  // ------------------------------------------------ 2. only my own share, twice
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addRoute(TOPIC, 4, cluster.addr);
    const a = await started('GID_pull_bal_split', cluster, TOPIC);
    const b = new DefaultMQPullConsumer('GID_pull_bal_split');
    b.setNamesrvAddr(cluster.addr);
    b.subscribe(TOPIC, '*');
    await b.start();
    cluster.setCidList([a.clientID, b.clientID].sort());
    const mine = await a.fetchMessageQueuesInBalance(TOPIC);
    const theirs = await b.fetchMessageQueuesInBalance(TOPIC);
    check('two instances split 4 queues 2+2', mine.length === 2 && theirs.length === 2,
      `${queueIds(mine)} / ${queueIds(theirs)}`);
    const all = [...mine, ...theirs].map((m) => m.getQueueId());
    check('the two shares do not overlap', new Set(all).size === all.length, all.join(','));
    check('the two shares cover every queue',
      [...new Set(all)].sort((x, y) => x - y).join(',') === '0,1,2,3', all.join(','));
    await a.shutdown();
    await b.shutdown();
    cluster.close();
  }

  // ----------------------------- 3. cannot compute → keep the current assignment
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addRoute(TOPIC, 3, cluster.addr);
    const c = await started('GID_pull_bal_keep', cluster, TOPIC);
    cluster.setCidList([]);
    const before = await c.fetchMessageQueuesInBalance(TOPIC);
    check('an unknown group with nothing pulled gives an empty view, NOT all 3',
      before.length === 0, queueIds(before).join(','));

    const all = await c.fetchSubscribeMessageQueues(TOPIC);
    check('subscribe view is the route\'s read queues', all.length === 3, queueIds(all).join(','));
    const sorted = [...all].sort((x, y) => x.compareTo(y));
    await c.pull(sorted[2], '*', 0, 32);
    check('the pull reached the broker', cluster.pulls.length === 1, cluster.pulls.join(','));

    const view = await c.fetchMessageQueuesInBalance(TOPIC);
    check('fallback keeps exactly the queue already pulled',
      view.length === 1 && view[0].getQueueId() === 2, queueIds(view).join(','));

    const other = await c.fetchMessageQueuesInBalance('NoSuchPullBalanceTopic');
    check('an unrouted topic keeps ITS OWN (empty) assignment, not this topic\'s',
      other.length === 0, queueIds(other).join(','));
    await c.shutdown();
    cluster.close();
  }

  // ---------------------------------------------------- 4. BROADCASTING ignores it
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addRoute(TOPIC, 3, cluster.addr);
    const c = await started('GID_pull_bal_bcast', cluster, TOPIC, 'BROADCASTING');
    cluster.setCidList(['someone-else@other']);
    const view = await c.fetchMessageQueuesInBalance(TOPIC);
    check('BROADCASTING takes everything without consulting the cid list',
      queueIds(view).join(',') === '0,1,2', queueIds(view).join(','));
    await c.shutdown();
    cluster.close();
  }

  // ------------------------------------------------------ 5. isRunning() guard
  {
    const c = new DefaultMQPullConsumer('GID_pull_bal_guard');
    let threw = '';
    try {
      await c.fetchMessageQueuesInBalance(TOPIC);
    } catch (e) {
      threw = (e as Error).message;
    }
    // An empty slice here reads as "no queue is mine" and the caller stops pulling.
    check('an unstarted consumer reports instead of returning []',
      threw.includes('not started'), threw);
  }

  // -------------------------------------------- 6. fetchPublish vs fetchSubscribe
  // Java :137 vs :142 are two DIFFERENT views; conflating them silently changes
  // the queue set whenever readQueueNums != writeQueueNums.
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.addRoute(TOPIC, 3, cluster.addr);
    const c = await started('GID_pull_bal_views', cluster, TOPIC);
    const sub = await c.fetchSubscribeMessageQueues(TOPIC);
    const pub = await c.fetchPublishMessageQueues(TOPIC);
    check('subscribe view = readQueueNums', sub.length === 3, queueIds(sub).join(','));
    check('publish view = writeQueueNums of the master-ful broker', pub.length === 3,
      queueIds(pub).join(','));
    await c.shutdown();
    cluster.close();
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
