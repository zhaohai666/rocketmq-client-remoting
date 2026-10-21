// The seven Concurrent admin fan-outs — live two-broker fake cluster.
//
// Two socket nodes: node A doubles as the name server (route / cluster info)
// and broker-a; node B is broker-b. Every fan-out faces a route that lists
// BOTH masters, so the parallel sweep is genuinely two concurrent dials, and
// the brokers can be made to answer differently:
//   * examineTopicStatsConcurrent / examineConsumeStatsConcurrent merge the
//     offset tables and SUM the tps; an empty stats table falls to the
//     connection probe (CONSUMER_NOT_ONLINE vs BROADCAST_CONSUMPTION);
//   * deleteTopicInBrokerConcurrent collects every settled outcome;
//   * resetOffsetNewConcurrent falls back to the old per-queue walk on a
//     broker whose consumer is offline, treats SYSTEM_ERROR as a no-op
//     success, and only succeeds when EVERY broker did;
//   * queryTopicsByConsumerConcurrent unions the topic lists on the group's
//     retry route; queryConsumeTimeSpanConcurrent concatenates the span sets
//     and answers an empty list (not a failure) on a missing route;
//   * messageTrackDetailConcurrent reaches the same verdicts as the sequential
//     sweep.
//
// Run: node --experimental-strip-types test/admin_concurrent_smoke.ts
import net from 'node:net';

import { DefaultMQAdminExt, MessageTrack, TrackType } from '../src/client/admin.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { MessageExt } from '../src/common/message.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { ConsumeType } from '../src/remoting/heartbeat.ts';
import {
  AdminToolResult, AdminToolsResultCodeEnum,
} from '../src/remoting/admin_body.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const BROKER_A = 'BrokerA';
const BROKER_B = 'BrokerB';

// The inline-JSON map-key form the broker's fastjson codec produces for
// MessageQueue-keyed tables.
function MQK(broker: string, queueId: number, topic: string): string {
  return JSON.stringify({ brokerName: broker, queueId, topic });
}

function okBody(req: RemotingCommand, body: any): RemotingCommand {
  const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null)!;
  resp.opaque = req.opaque;
  resp.serializeTypeCurrentRpc = req.serializeTypeCurrentRpc;
  resp.body = Buffer.from(JSON.stringify(body), 'utf8');
  return resp;
}

function failResp(req: RemotingCommand, code: number, remark: string): RemotingCommand {
  const resp = RemotingCommand.createResponseCommand(code, remark)!;
  resp.opaque = req.opaque;
  resp.serializeTypeCurrentRpc = req.serializeTypeCurrentRpc;
  return resp;
}

class FakeNode {
  server: net.Server;
  port = 0;
  handlers = new Map<number, (req: RemotingCommand) => RemotingCommand | null>();
  requests: { code: number; ext: Record<string, string> }[] = [];
  routes = new Set<string>();

  constructor() { this.server = net.createServer((s) => this.onConnection(s)); }

  listen(): Promise<void> {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => {
        this.port = (this.server.address() as net.AddressInfo).port;
        resolve();
      });
    });
  }

  get addr() { return `127.0.0.1:${this.port}`; }

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
    sock.on('error', () => { /* teardown hang-up */ });
  }

  respond(req: RemotingCommand): RemotingCommand {
    this.requests.push({ code: req.code, ext: Object.assign({}, req.extFields || {}) });
    const h = this.handlers.get(req.code);
    if (h != null) {
      const r = h(req);
      if (r != null) return r;
    }
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null)!;
    resp.opaque = req.opaque;
    resp.serializeTypeCurrentRpc = req.serializeTypeCurrentRpc;
    return resp;
  }

  hits(code: number) { return this.requests.filter((r) => r.code === code); }
  close() { this.server.close(); }
}

// Node A doubles as the name server: it serves the route of every registered
// topic pointing at BOTH nodes, plus the cluster info the consumed-check needs.
function serveNameServerRole(a: FakeNode, b: FakeNode) {
  a.handlers.set(RequestCode.GET_ROUTEINFO_BY_TOPIC, (req) => {
    const topic = String(req.extFields['topic'] || '');
    if (!a.routes.has(topic)) return failResp(req, ResponseCode.TOPIC_NOT_EXIST, 'route not found');
    return okBody(req, {
      orderTopicConf: null,
      queueDatas: [
        { brokerName: BROKER_A, readQueueNums: 8, writeQueueNums: 8, perm: 6, topicSysFlag: 0 },
        { brokerName: BROKER_B, readQueueNums: 8, writeQueueNums: 8, perm: 6, topicSysFlag: 0 },
      ],
      brokerDatas: [
        { cluster: 'DefaultCluster', brokerName: BROKER_A, brokerAddrs: { '0': a.addr } },
        { cluster: 'DefaultCluster', brokerName: BROKER_B, brokerAddrs: { '0': b.addr } },
      ],
      filterServerTable: {},
    });
  });
  a.handlers.set(RequestCode.GET_BROKER_CLUSTER_INFO, (req) => okBody(req, {
    brokerAddrTable: {
      [BROKER_A]: { cluster: 'DefaultCluster', brokerName: BROKER_A, brokerAddrs: { '0': a.addr } },
      [BROKER_B]: { cluster: 'DefaultCluster', brokerName: BROKER_B, brokerAddrs: { '0': b.addr } },
    },
    clusterAddrTable: { DefaultCluster: [BROKER_A, BROKER_B] },
  }));
}

async function started(a: FakeNode): Promise<DefaultMQAdminExt> {
  const admin = new DefaultMQAdminExt();
  admin.setNamesrvAddr(a.addr);
  admin.setInstanceName(`ADMIN_cc_${process.pid}_${Math.floor(Math.random() * 1e6)}`);
  admin.start();
  return admin;
}

function trackedMessage(topic: string, port: number): MessageExt {
  const msg = new MessageExt(topic, Buffer.from('hello'), 'TagA');
  msg.setQueueId(0);
  msg.setQueueOffset(4);
  msg.setStoreHost('127.0.0.1');
  msg.storeHostPort = port;
  return msg;
}

function connectionBody(consumeType: string, subscriptionTable: Record<string, any> = {},
  messageModel = 'CLUSTERING'): any {
  return {
    connectionSet: [{ clientId: 'cid_1', clientAddr: '127.0.0.1:9999', language: 'JAVA', version: 500 }],
    subscriptionTable,
    consumeType,
    messageModel,
    consumeFromWhere: 'CONSUME_FROM_LAST_OFFSET',
  };
}

async function main() {
  // ------------------------------------------------ 1. examineTopicStatsConcurrent
  {
    const a = new FakeNode(), b = new FakeNode();
    await a.listen(); await b.listen();
    serveNameServerRole(a, b);
    a.routes.add('TpStats');
    a.handlers.set(RequestCode.GET_TOPIC_STATS_INFO, (req) => okBody(req, {
      offsetTable: { [MQK(BROKER_A, 0, 'TpStats')]: { minOffset: 0, maxOffset: 10, lastUpdateTimestamp: 0 } },
      topicPutTps: 1.5,
    }));
    b.handlers.set(RequestCode.GET_TOPIC_STATS_INFO, (req) => okBody(req, {
      offsetTable: { [MQK(BROKER_B, 0, 'TpStats')]: { minOffset: 0, maxOffset: 20, lastUpdateTimestamp: 0 } },
      topicPutTps: 2.5,
    }));
    const admin = await started(a);
    const r: AdminToolResult<any> = await admin.examineTopicStatsConcurrent('TpStats');
    check('topic stats sweep succeeds with both brokers merged',
      r.success && r.code === AdminToolsResultCodeEnum.SUCCESS && r.data.offsetTable.size === 2,
      JSON.stringify({ success: r.success, code: r.code, size: r.data?.offsetTable?.size }));
    const keys = Array.from(r.data.offsetTable.keys()).map((q) => `${q.getBrokerName()}#${q.getQueueId()}`).sort();
    check('the merged table holds one queue per broker',
      JSON.stringify(keys) === JSON.stringify(['BrokerA#0', 'BrokerB#0']), JSON.stringify(keys));
    check('topicPutTps is SUMMED across brokers (1.5 + 2.5)',
      r.data.topicPutTps === 4, String(r.data.topicPutTps));
    check('the route query went to the name-server node',
      a.hits(RequestCode.GET_ROUTEINFO_BY_TOPIC).length === 1 &&
      b.hits(RequestCode.GET_ROUTEINFO_BY_TOPIC).length === 0);
    admin.shutdown(); a.close(); b.close();
  }

  // ---------------------------------------------- 2. examineConsumeStatsConcurrent
  {
    const a = new FakeNode(), b = new FakeNode();
    await a.listen(); await b.listen();
    serveNameServerRole(a, b);
    a.routes.add(MixAll.getRetryTopic('GID_cc'));
    const statsHandler = (broker: string, tps: number) => (req: RemotingCommand) => okBody(req, {
      offsetTable: { [MQK(broker, 0, 'CcTopic')]: { brokerOffset: 10, consumerOffset: 5, lastTimestamp: 0, pullOffset: 0 } },
      consumeTps: tps,
    });
    a.handlers.set(RequestCode.GET_CONSUME_STATS, statsHandler(BROKER_A, 1.5));
    b.handlers.set(RequestCode.GET_CONSUME_STATS, statsHandler(BROKER_B, 2.5));
    const admin = await started(a);
    const r = await admin.examineConsumeStatsConcurrent('GID_cc', 'CcTopic');
    check('consume stats sweep merges the offset tables of both brokers',
      r.success && r.code === AdminToolsResultCodeEnum.SUCCESS && r.data.offsetTable.size === 2,
      JSON.stringify({ success: r.success, code: r.code }));
    check('consumeTps is SUMMED across brokers (1.5 + 2.5)',
      r.data.consumeTps === 4, String(r.data.consumeTps));
    const routeTopics = a.hits(RequestCode.GET_ROUTEINFO_BY_TOPIC).map((h) => h.ext['topic']);
    check('route discovery tries the retry topic FIRST and stops there',
      routeTopics[0] === MixAll.getRetryTopic('GID_cc') && !routeTopics.includes('CcTopic'),
      JSON.stringify(routeTopics));
    admin.shutdown(); a.close(); b.close();
  }
  {
    // Empty stats: the connection probe decides the verdict.
    const a = new FakeNode(), b = new FakeNode();
    await a.listen(); await b.listen();
    serveNameServerRole(a, b);
    a.routes.add(MixAll.getRetryTopic('GID_off'));
    a.handlers.set(RequestCode.GET_CONSUME_STATS, (req) => okBody(req, { offsetTable: {}, consumeTps: 0 }));
    b.handlers.set(RequestCode.GET_CONSUME_STATS, (req) => okBody(req, { offsetTable: {}, consumeTps: 0 }));
    a.handlers.set(RequestCode.GET_CONSUMER_CONNECTION_LIST, (req) => okBody(req, {
      connectionSet: [], subscriptionTable: {},
      consumeType: ConsumeType.CONSUME_PASSIVELY, messageModel: 'CLUSTERING',
      consumeFromWhere: 'CONSUME_FROM_LAST_OFFSET',
    }));
    b.handlers.set(RequestCode.GET_CONSUMER_CONNECTION_LIST, (req) => okBody(req, {
      connectionSet: [], subscriptionTable: {},
      consumeType: ConsumeType.CONSUME_PASSIVELY, messageModel: 'CLUSTERING',
      consumeFromWhere: 'CONSUME_FROM_LAST_OFFSET',
    }));
    const admin = await started(a);
    const r = await admin.examineConsumeStatsConcurrent('GID_off', null);
    check('an empty merged table with no connections is CONSUMER_NOT_ONLINE',
      !r.success && r.code === AdminToolsResultCodeEnum.CONSUMER_NOT_ONLINE &&
      (r.errorMsg ?? '').includes('not consume any message'),
      JSON.stringify({ code: r.code, errorMsg: r.errorMsg }));
    admin.shutdown(); a.close(); b.close();
  }
  {
    // Empty stats but the consumer IS online in clustering mode: the probe
    // succeeds and the sweep falls through to success with an empty table.
    const a = new FakeNode(), b = new FakeNode();
    await a.listen(); await b.listen();
    serveNameServerRole(a, b);
    a.routes.add(MixAll.getRetryTopic('GID_live'));
    a.handlers.set(RequestCode.GET_CONSUME_STATS, (req) => okBody(req, { offsetTable: {}, consumeTps: 0 }));
    b.handlers.set(RequestCode.GET_CONSUME_STATS, (req) => okBody(req, { offsetTable: {}, consumeTps: 0 }));
    a.handlers.set(RequestCode.GET_CONSUMER_CONNECTION_LIST, (req) =>
      okBody(req, connectionBody(ConsumeType.CONSUME_PASSIVELY)));
    b.handlers.set(RequestCode.GET_CONSUMER_CONNECTION_LIST, (req) =>
      okBody(req, connectionBody(ConsumeType.CONSUME_PASSIVELY)));
    const admin = await started(a);
    const r = await admin.examineConsumeStatsConcurrent('GID_live', null);
    check('an empty merged table with a live clustering consumer still succeeds',
      r.success && r.code === AdminToolsResultCodeEnum.SUCCESS &&
      r.data.offsetTable.size === 0 && r.data.consumeTps === 0,
      JSON.stringify({ code: r.code, errorMsg: r.errorMsg }));
    admin.shutdown(); a.close(); b.close();
  }
  {
    // Broadcast mode consumer: a different empty-table verdict.
    const a = new FakeNode(), b = new FakeNode();
    await a.listen(); await b.listen();
    serveNameServerRole(a, b);
    a.routes.add(MixAll.getRetryTopic('GID_bcast'));
    a.handlers.set(RequestCode.GET_CONSUME_STATS, (req) => okBody(req, { offsetTable: {}, consumeTps: 0 }));
    b.handlers.set(RequestCode.GET_CONSUME_STATS, (req) => okBody(req, { offsetTable: {}, consumeTps: 0 }));
    a.handlers.set(RequestCode.GET_CONSUMER_CONNECTION_LIST, (req) =>
      okBody(req, connectionBody(ConsumeType.CONSUME_PASSIVELY, {}, 'BROADCASTING')));
    b.handlers.set(RequestCode.GET_CONSUMER_CONNECTION_LIST, (req) =>
      okBody(req, connectionBody(ConsumeType.CONSUME_PASSIVELY, {}, 'BROADCASTING')));
    const admin = await started(a);
    const r = await admin.examineConsumeStatsConcurrent('GID_bcast', null);
    check('an empty merged table under broadcast mode is BROADCAST_CONSUMPTION',
      !r.success && r.code === AdminToolsResultCodeEnum.BROADCAST_CONSUMPTION &&
      (r.errorMsg ?? '').includes('broadcast mode'),
      JSON.stringify({ code: r.code, errorMsg: r.errorMsg }));
    admin.shutdown(); a.close(); b.close();
  }

  // --------------------------------------------- 3. deleteTopicInBrokerConcurrent
  {
    const a = new FakeNode(), b = new FakeNode();
    await a.listen(); await b.listen();
    serveNameServerRole(a, b);
    b.handlers.set(RequestCode.DELETE_TOPIC_IN_BROKER, (req) =>
      failResp(req, ResponseCode.SYSTEM_ERROR, 'delete refused'));
    const admin = await started(a);
    const r = await admin.deleteTopicInBrokerConcurrent(new Set([a.addr, b.addr]), 'DelTopic');
    check('delete fan-out reports every settled outcome, code success',
      r.success && r.code === AdminToolsResultCodeEnum.SUCCESS &&
      JSON.stringify(r.data.successList) === JSON.stringify([a.addr]) &&
      JSON.stringify(r.data.failureList) === JSON.stringify([b.addr]),
      JSON.stringify(r.data));
    check('both brokers received the topic header',
      a.hits(RequestCode.DELETE_TOPIC_IN_BROKER)[0].ext['topic'] === 'DelTopic' &&
      b.hits(RequestCode.DELETE_TOPIC_IN_BROKER)[0].ext['topic'] === 'DelTopic');
    admin.shutdown(); a.close(); b.close();
  }

  // ------------------------------------------------ 4. resetOffsetNewConcurrent
  {
    // a) every broker resets in one round.
    const a = new FakeNode(), b = new FakeNode();
    await a.listen(); await b.listen();
    serveNameServerRole(a, b);
    a.routes.add('RstTopic');
    a.handlers.set(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, (req) => okBody(req, {
      offsetTable: { [MQK(BROKER_A, 0, 'RstTopic')]: 5 },
    }));
    b.handlers.set(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, (req) => okBody(req, {
      offsetTable: { [MQK(BROKER_B, 0, 'RstTopic')]: 6 },
    }));
    const admin = await started(a);
    const r = await admin.resetOffsetNewConcurrent('GID_rst', 'RstTopic', 1234567);
    check('a clean sweep succeeds with both broker addresses',
      r.success && JSON.stringify(r.data.successList) === JSON.stringify([a.addr, b.addr]) &&
      r.data.failureList.length === 0, JSON.stringify(r.data));
    check('the reset request carries the group/topic/timestamp/force header',
      a.hits(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET)[0].ext['consumerGroup'] === 'GID_rst' &&
      a.hits(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET)[0].ext['topic'] === 'RstTopic' &&
      a.hits(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET)[0].ext['timestamp'] === '1234567' &&
      a.hits(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET)[0].ext['isForce'] === 'true',
      JSON.stringify(a.hits(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET)[0].ext));
    admin.shutdown(); a.close(); b.close();
  }
  {
    // b) one broker's consumer is offline: the OLD per-queue walk runs there.
    const a = new FakeNode(), b = new FakeNode();
    await a.listen(); await b.listen();
    serveNameServerRole(a, b);
    a.routes.add('RstTopic');
    a.handlers.set(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, (req) => okBody(req, {
      offsetTable: { [MQK(BROKER_A, 0, 'RstTopic')]: 5 },
    }));
    b.handlers.set(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, (req) =>
      failResp(req, ResponseCode.CONSUMER_NOT_ONLINE, 'consumer not online'));
    // Old walk on broker b: no consumed queues -> every read queue of the route.
    b.handlers.set(RequestCode.GET_CONSUME_STATS, (req) => okBody(req, { offsetTable: {}, consumeTps: 0 }));
    b.handlers.set(RequestCode.GET_TOPIC_STATS_INFO, (req) => okBody(req, {
      offsetTable: { [MQK(BROKER_B, 0, 'RstTopic')]: { minOffset: 0, maxOffset: 9, lastUpdateTimestamp: 0 } },
      topicPutTps: 0,
    }));
    b.handlers.set(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, (req) => {
      const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null)!;
      resp.opaque = req.opaque;
      resp.serializeTypeCurrentRpc = req.serializeTypeCurrentRpc;
      resp.addExtField('offset', '7');
      return resp;
    });
    const admin = await started(a);
    const r = await admin.resetOffsetNewConcurrent('GID_rst', 'RstTopic', 1234567);
    check('the offline broker falls back to the old walk and still succeeds',
      r.success && JSON.stringify(r.data.successList) === JSON.stringify([a.addr, b.addr]),
      JSON.stringify(r.data));
    check('the fallback walked the read queues of broker b (search + update per queue)',
      b.hits(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP).length === 8 &&
      b.hits(RequestCode.UPDATE_CONSUMER_OFFSET).length === 8,
      `search=${b.hits(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP).length} `
      + `update=${b.hits(RequestCode.UPDATE_CONSUMER_OFFSET).length}`);
    admin.shutdown(); a.close(); b.close();
  }
  {
    // c) SYSTEM_ERROR: the group never existed there — a no-op success.
    const a = new FakeNode(), b = new FakeNode();
    await a.listen(); await b.listen();
    serveNameServerRole(a, b);
    a.routes.add('RstTopic');
    a.handlers.set(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, (req) => okBody(req, {
      offsetTable: { [MQK(BROKER_A, 0, 'RstTopic')]: 5 },
    }));
    b.handlers.set(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, (req) =>
      failResp(req, ResponseCode.SYSTEM_ERROR, 'group not exist, never online'));
    const admin = await started(a);
    const r = await admin.resetOffsetNewConcurrent('GID_rst', 'RstTopic', 1234567);
    check('SYSTEM_ERROR counts as a success and runs NO fallback',
      r.success && JSON.stringify(r.data.successList) === JSON.stringify([a.addr, b.addr]) &&
      b.hits(RequestCode.GET_CONSUME_STATS).length === 0,
      JSON.stringify(r.data));
    admin.shutdown(); a.close(); b.close();
  }
  {
    // d) a genuine broker failure: overall failure with the operator result.
    const a = new FakeNode(), b = new FakeNode();
    await a.listen(); await b.listen();
    serveNameServerRole(a, b);
    a.routes.add('RstTopic');
    a.handlers.set(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, (req) => okBody(req, {
      offsetTable: { [MQK(BROKER_A, 0, 'RstTopic')]: 5 },
    }));
    b.handlers.set(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, (req) =>
      failResp(req, ResponseCode.SUBSCRIPTION_NOT_EXIST, 'subscription not exist'));
    const admin = await started(a);
    const r = await admin.resetOffsetNewConcurrent('GID_rst', 'RstTopic', 1234567);
    check('a hard broker failure is an overall operator failure',
      !r.success && r.code === AdminToolsResultCodeEnum.MQ_BROKER_ERROR &&
      (r.errorMsg ?? '') === 'operator failure' &&
      JSON.stringify(r.data.successList) === JSON.stringify([a.addr]) &&
      JSON.stringify(r.data.failureList) === JSON.stringify([b.addr]),
      JSON.stringify({ code: r.code, errorMsg: r.errorMsg, data: r.data }));
    admin.shutdown(); a.close(); b.close();
  }

  // -------------------------------------------- 5. queryTopicsByConsumerConcurrent
  {
    const a = new FakeNode(), b = new FakeNode();
    await a.listen(); await b.listen();
    serveNameServerRole(a, b);
    a.routes.add(MixAll.getRetryTopic('GID_qt'));
    a.handlers.set(RequestCode.QUERY_TOPICS_BY_CONSUMER, (req) =>
      okBody(req, { topicList: ['T1', 'T2'] }));
    b.handlers.set(RequestCode.QUERY_TOPICS_BY_CONSUMER, (req) =>
      okBody(req, { topicList: ['T2', 'T3'] }));
    const admin = await started(a);
    const r = await admin.queryTopicsByConsumerConcurrent('GID_qt');
    check('topic lists are unioned across brokers',
      r.success && JSON.stringify(r.data.topicList) === JSON.stringify(['T1', 'T2', 'T3']),
      JSON.stringify(r.data?.topicList));

    a.routes.clear();
    const missing = await admin.queryTopicsByConsumerConcurrent('GID_qt');
    check('a missing retry route is a route-not-found failure',
      !missing.success && missing.code === AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST &&
      (missing.errorMsg ?? '').includes('Not exist route info'),
      JSON.stringify({ code: missing.code, errorMsg: missing.errorMsg }));
    admin.shutdown(); a.close(); b.close();
  }

  // ------------------------------------------- 6. queryConsumeTimeSpanConcurrent
  {
    const a = new FakeNode(), b = new FakeNode();
    await a.listen(); await b.listen();
    serveNameServerRole(a, b);
    a.routes.add('SpanTopic');
    a.handlers.set(RequestCode.QUERY_CONSUME_TIME_SPAN, (req) => okBody(req, {
      consumeTimeSpanSet: [{ timeSpan: 100, msgQueue: JSON.parse(MQK(BROKER_A, 0, 'SpanTopic')) }],
    }));
    b.handlers.set(RequestCode.QUERY_CONSUME_TIME_SPAN, (req) => okBody(req, {
      consumeTimeSpanSet: [{ timeSpan: 200, msgQueue: JSON.parse(MQK(BROKER_B, 0, 'SpanTopic')) }],
    }));
    const admin = await started(a);
    const r = await admin.queryConsumeTimeSpanConcurrent('SpanTopic', 'GID_span');
    check('consume time spans are concatenated across brokers',
      r.success && r.data.length === 2 && r.data[0].timeSpan === 100 && r.data[1].timeSpan === 200,
      JSON.stringify(r.data));

    a.routes.clear();
    const missing = await admin.queryConsumeTimeSpanConcurrent('SpanTopic', 'GID_span');
    check('a missing route is a route-not-found failure (the empty branch is for a degenerate route)',
      !missing.success && missing.code === AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST &&
      (missing.errorMsg ?? '').includes('Not exist route info'),
      JSON.stringify({ code: missing.code, errorMsg: missing.errorMsg, data: missing.data }));
    admin.shutdown(); a.close(); b.close();
  }

  // ------------------------------------------------ 7. messageTrackDetailConcurrent
  {
    const a = new FakeNode(), b = new FakeNode();
    await a.listen(); await b.listen();
    serveNameServerRole(a, b);
    const TOPIC = 'TrackTopic';
    a.routes.add(TOPIC);
    for (const g of ['G_track_pull', 'G_track_offline', 'G_track_done']) {
      a.routes.add(MixAll.getRetryTopic(g));
    }
    a.handlers.set(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, (req) =>
      okBody(req, { groupList: ['G_track_pull', 'G_track_offline', 'G_track_done'] }));
    const sub = {
      [TOPIC]: { topic: TOPIC, subString: 'TagA', tagsSet: ['TagA'], expressionType: 'TAG' },
    };
    for (const node of [a, b]) {
      node.handlers.set(RequestCode.GET_CONSUMER_CONNECTION_LIST, (req) => {
        const group = String(req.extFields['consumerGroup'] || '');
        if (group === 'G_track_pull') {
          return okBody(req, connectionBody(ConsumeType.CONSUME_ACTIVELY));
        }
        if (group === 'G_track_offline') {
          return okBody(req, {
            connectionSet: [], subscriptionTable: {},
            consumeType: ConsumeType.CONSUME_PASSIVELY, messageModel: 'CLUSTERING',
            consumeFromWhere: 'CONSUME_FROM_LAST_OFFSET',
          });
        }
        return okBody(req, connectionBody(ConsumeType.CONSUME_PASSIVELY, sub));
      });
    }
    a.handlers.set(RequestCode.GET_CONSUME_STATS, (req) => okBody(req, {
      offsetTable: { [MQK(BROKER_A, 0, TOPIC)]: { brokerOffset: 10, consumerOffset: 5, lastTimestamp: 0, pullOffset: 0 } },
      consumeTps: 0,
    }));
    b.handlers.set(RequestCode.GET_CONSUME_STATS, (req) => okBody(req, { offsetTable: {}, consumeTps: 0 }));

    const admin = await started(a);
    const msg = trackedMessage(TOPIC, a.port);
    const concurrent = await admin.messageTrackDetailConcurrent(msg);
    const byGroupC = new Map<string, MessageTrack>();
    for (const t of concurrent) byGroupC.set(t.consumerGroup, t);
    check('the concurrent track covers every consuming group',
      concurrent.length === 3 && byGroupC.size === 3, JSON.stringify(concurrent.map((t) => t.consumerGroup)));
    check('active consumer is PULL',
      byGroupC.get('G_track_pull')?.trackType === TrackType.PULL,
      byGroupC.get('G_track_pull')?.trackType ?? 'missing');
    check('group without connections is NOT_ONLINE with the code in the description',
      byGroupC.get('G_track_offline')?.trackType === TrackType.NOT_ONLINE &&
      (byGroupC.get('G_track_offline')?.exceptionDesc ?? '').includes('206'),
      JSON.stringify(byGroupC.get('G_track_offline')));
    check('a consumer past the offset on the message master queue is CONSUMED',
      byGroupC.get('G_track_done')?.trackType === TrackType.CONSUMED,
      byGroupC.get('G_track_done')?.trackType ?? 'missing');

    const sequential = await admin.messageTrackDetail(msg);
    const same = sequential.length === concurrent.length &&
      sequential.every((t) => {
        const c = byGroupC.get(t.consumerGroup);
        return c != null && c.trackType === t.trackType &&
          (c.exceptionDesc ?? null) === (t.exceptionDesc ?? null);
      });
    check('the concurrent sweep reaches the SAME verdicts as the sequential one', same,
      JSON.stringify({ sequential: sequential.map((t) => [t.consumerGroup, t.trackType]),
        concurrent: concurrent.map((t) => [t.consumerGroup, t.trackType]) }));
    admin.shutdown(); a.close(); b.close();
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
