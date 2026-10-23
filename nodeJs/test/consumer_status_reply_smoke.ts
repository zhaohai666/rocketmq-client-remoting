// What GET_CONSUMER_STATUS_FROM_CLIENT(221) answers — offline wire test.
//
// The asking side decodes the reply into a GetConsumerStatusBody OBJECT and
// reads its `messageQueueTable`, whose keys are rendered MessageQueue objects.
// Anything else — a bare list of entries, or a per-entry {mq, offset} shape —
// decodes into an empty table, so the admin query silently reports nothing even
// though the consumer replied.
//
// Also pinned here: the table follows the topic of the request, a group this
// consumer does not own never gets this consumer's offsets reported, and the
// instance still replies with an empty table when nobody on it owns the group.
//
// The tails for the other broker-initiated codes differ on purpose: 307 and 309
// answer an error naming the group nobody here hosts, and 220 — a oneway push —
// answers nothing.
//
// Run: node --experimental-strip-types test/consumer_status_reply_smoke.ts
import net from 'node:net';

import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { MessageQueue } from '../src/common/message.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { GetConsumerStatusBody, messageQueueKey } from '../src/remoting/bodies.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'ConsumerStatusTopic';
const OTHER_TOPIC = 'NeverSubscribedTopic';
const BROKER = 'b1';
const GROUP = 'GID_consumer_status';

class FakeCluster {
  server: net.Server;
  port = 0;
  queues = new Map<string, number>();

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

  private routeBody(topic: string, n: number): string {
    return JSON.stringify({
      orderTopicConf: null,
      queueDatas: [{
        brokerName: BROKER, readQueueNums: n, writeQueueNums: n, perm: 6, topicSysFlag: 0,
      }],
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
    sock.on('error', () => { /* client hangs up on teardown */ });
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
    if (req.code === RequestCode.QUERY_CONSUMER_OFFSET) {
      // Nothing committed yet: the consumer starts from the policy, and the
      // table this test reads is the one the test writes by hand.
      resp.code = ResponseCode.QUERY_NOT_FOUND;
      return resp;
    }
    if (req.code === RequestCode.GET_MAX_OFFSET || req.code === RequestCode.GET_MIN_OFFSET) {
      resp.extFields['offset'] = '0';
      return resp;
    }
    if (req.code === RequestCode.PULL_MESSAGE) {
      resp.code = ResponseCode.PULL_NOT_FOUND;
      resp.extFields['nextBeginOffset'] = ext['queueOffset'] || '0';
      resp.extFields['minOffset'] = '0';
      resp.extFields['maxOffset'] = '0';
      resp.extFields['suggestWhichBrokerId'] = '0';
      return resp;
    }
    return resp;
  }

  close() { this.server.close(); }
}

async function main() {
  const cluster = new FakeCluster();
  await cluster.listen();
  cluster.addTopic(TOPIC, 1);
  cluster.addTopic(OTHER_TOPIC, 1);
  cluster.addTopic(MixAll.getRetryTopic(GROUP), 1);

  const consumer = new DefaultMQPushConsumer(GROUP);
  consumer.setNamesrvAddr(cluster.addr);
  consumer.setInstanceName('consumerStatusTest');
  consumer.setPullBatchSize(1);
  consumer.subscribe(TOPIC, '*');
  consumer.registerMessageListener({ consumeMessage: () => 0 });
  await consumer.start();
  check('the consumer is up', consumer.isStarted());

  // Two queues of the same topic plus one queue of a topic nobody subscribes to:
  // the reply must carry exactly the first two.
  const mq0 = new MessageQueue(TOPIC, BROKER, 0);
  const mq1 = new MessageQueue(TOPIC, BROKER, 1);
  const mqOther = new MessageQueue(OTHER_TOPIC, BROKER, 0);
  consumer.offsetStore!.updateOffset(mq0, 42, false);
  consumer.offsetStore!.updateOffset(mq1, 7, false);
  consumer.offsetStore!.updateOffset(mqOther, 99, false);

  const ask = (topic?: string, group = GROUP) => {
    const cmd = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT)!;
    cmd.extFields = { topic: topic ?? '', group };
    return consumer.mqClient!.dispatchFacadeProcessor(cmd, cluster.addr);
  };

  const reply = ask();
  check('221 is answered', reply !== null);
  check('221 answers SUCCESS', reply !== null && reply.code === ResponseCode.SUCCESS,
    `code=${reply ? reply.code : 'null'}`);

  if (reply && reply.body) {
    const decoded = GetConsumerStatusBody.decode(reply.body);
    check('the reply decodes as a GetConsumerStatusBody', decoded !== null);
    const table = decoded.messageQueueTable;
    // No topic in the request means no filter: the whole offset table is
    // reported, including queues of topics this consumer never subscribed to.
    check(`an unfiltered query reports every offset (got ${table.size})`, table.size === 3);
    const byKey = new Map<string, number>();
    for (const [mq, off] of table) byKey.set(messageQueueKey(mq), off);
    check(`q0 carries its consumed offset (got ${byKey.get(messageQueueKey(mq0))})`,
      byKey.get(messageQueueKey(mq0)) === 42);
    check(`q1 carries its consumed offset (got ${byKey.get(messageQueueKey(mq1))})`,
      byKey.get(messageQueueKey(mq1)) === 7);
    check(`a queue of another topic rides along (got ${byKey.get(messageQueueKey(mqOther))})`,
      byKey.get(messageQueueKey(mqOther)) === 99);
    // The key text is the rendered MessageQueue object: the asking side maps it
    // back to a queue, so a plain "topic@broker@id" string decodes into nothing.
    check('the keys are rendered message queue objects',
      byKey.has('{"brokerName":"b1","queueId":0,"topic":"ConsumerStatusTopic"}'));
    check('the deprecated consumerTable field is still encoded',
      Object.keys(decoded.consumerTable).length === 0);
    // Control for the shape claim above: a bare list of entries decodes into an
    // empty table, which is what the asking side would report.
    const asList = Buffer.from(JSON.stringify(
      [{ topic: TOPIC, brokerName: BROKER, queueId: 0, offset: 42 }]), 'utf8');
    check('a bare list reply decodes into nothing (the shape is what the asking side reads)',
      GetConsumerStatusBody.decode(asList).messageQueueTable.size === 0);
  }

  const scoped = ask(OTHER_TOPIC);
  if (scoped && scoped.body) {
    const decoded = GetConsumerStatusBody.decode(scoped.body);
    check(`a topic-scoped query answers only that topic (got ${decoded.messageQueueTable.size})`,
      decoded.messageQueueTable.size === 1);
    const [mq, offset] = [...decoded.messageQueueTable.entries()][0];
    check('the scoped entry is the other topic at its own offset',
      mq.getTopic() === OTHER_TOPIC && offset === 99);
  } else {
    check('a topic-scoped query is answered', false);
  }

  // A group this consumer does not own is not answered from this consumer's
  // offsets: a sibling consumer sharing the instance holds that group's table,
  // and replying here would shadow it. When nobody on the instance owns it the
  // dispatch still answers SUCCESS with an empty table, because the asking side
  // decodes the body — a silent drop reads as an unreachable client instead of
  // "this group consumed nothing".
  const foreign = ask(undefined, 'GID_somebody_else');
  check('an unowned group is still answered', foreign !== null);
  if (foreign && foreign.body) {
    check(`an unowned group answers SUCCESS (got ${foreign.code})`,
      foreign.code === ResponseCode.SUCCESS);
    const empty = GetConsumerStatusBody.decode(foreign.body);
    check(`an unowned group reports none of this consumer's offsets (got ${empty.messageQueueTable.size})`,
      empty.messageQueueTable.size === 0);
  } else {
    check('an unowned group reply carries a body', false);
  }

  // 307 and 309 need the opposite tail: the initiating admin reports whichever
  // client it probed, so a group nobody here hosts has to come back as an error
  // naming that group rather than as a request that never got an answer.
  const askWith = (code: number, ext: Record<string, string>) => {
    const cmd = RemotingCommand.createRequestCommand(code)!;
    cmd.extFields = ext;
    return consumer.mqClient!.dispatchFacadeProcessor(cmd, cluster.addr);
  };

  const hostedInfo = askWith(RequestCode.GET_CONSUMER_RUNNING_INFO, { consumerGroup: GROUP });
  check(`307 for the hosted group answers SUCCESS (got ${hostedInfo ? hostedInfo.code : 'null'})`,
    hostedInfo !== null && hostedInfo.code === ResponseCode.SUCCESS);

  const wantRemark = 'The Consumer Group <GID_somebody_else> not exist in this consumer';
  const missingInfo = askWith(RequestCode.GET_CONSUMER_RUNNING_INFO,
    { consumerGroup: 'GID_somebody_else' });
  check(`307 for an unhosted group answers the fixed error (got ${missingInfo ? missingInfo.code : 'null'})`,
    missingInfo !== null && missingInfo.code === ResponseCode.SYSTEM_ERROR);
  check('the 307 error names the group that was asked about',
    missingInfo !== null && missingInfo.remark === wantRemark,
    `remark=${missingInfo ? missingInfo.remark : 'null'}`);

  const missingDirect = askWith(RequestCode.CONSUME_MESSAGE_DIRECTLY,
    { consumerGroup: 'GID_somebody_else' });
  check(`309 for an unhosted group answers the fixed error (got ${missingDirect ? missingDirect.code : 'null'})`,
    missingDirect !== null && missingDirect.code === ResponseCode.SYSTEM_ERROR);
  check('the 309 error names the group that was asked about',
    missingDirect !== null && missingDirect.remark === wantRemark,
    `remark=${missingDirect ? missingDirect.remark : 'null'}`);

  // 220 is a oneway push: the consumer applies it and sends nothing back.
  const reset = askWith(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, { group: 'GID_somebody_else' });
  check('a reset-offset push sends no reply', reset === null,
    `got code=${reset ? reset.code : 'null'}`);

  consumer.shutdown();
  cluster.close();
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
