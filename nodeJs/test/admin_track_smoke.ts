// DefaultMQAdminExt message tracking + consume-queue queries — offline wire test.
//
// Covers three admin surfaces:
//   * QUERY_CONSUME_QUEUE(321): header keys topic/queueId/index/count/consumerGroup,
//     body decoded into QueryConsumeQueueResponseBody; an empty body yields the
//     default (empty) object;
//   * UPDATE_AND_GET_GROUP_FORBIDDEN(353): group/topic always sent, `readable`
//     omitted when unset (absent and "false" are different operations for the
//     broker) and rendered "true"/"false" when set; the reply body decodes into
//     GroupForbidden and an empty reply body is an error;
//   * messageTrackDetail: who consumes the topic, and per group the verdict
//     PULL (active consumer), NOT_ONLINE (no connection), CONSUMED /
//     CONSUMED_BUT_FILTERED (passive consumer that did or did not receive the
//     message's tag), NOT_CONSUME_YET — with the group list sorted.
//
// Run: node --experimental-strip-types test/admin_track_smoke.ts
import net from 'node:net';

import { DefaultMQAdminExt, MessageTrack, TrackType } from '../src/client/admin.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { MessageExt } from '../src/common/message.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { ConsumeType } from '../src/remoting/heartbeat.ts';
import { MQClientException } from '../src/remoting/exception.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'TrackTopic';
const BROKER = 'TrackBroker';

// The consumed message: queue 0 of the fake master, queue offset 4, tag TagA.
function trackedMessage(port: number): MessageExt {
  const msg = new MessageExt(TOPIC, Buffer.from('hello'), 'TagA');
  msg.setQueueId(0);
  msg.setQueueOffset(4);
  msg.setStoreHost('127.0.0.1');
  msg.storeHostPort = port;
  return msg;
}

function routeBody(addr: string): string {
  return JSON.stringify({
    orderTopicConf: null,
    queueDatas: [{
      brokerName: BROKER, readQueueNums: 8, writeQueueNums: 8,
      perm: 6, topicSysFlag: 0,
    }],
    brokerDatas: [{
      cluster: 'DefaultCluster', brokerName: BROKER, brokerAddrs: { '0': addr },
      zoneName: null, enableActingMaster: false,
    }],
    filterServerTable: {},
  });
}

function clusterInfoBody(addr: string): string {
  return JSON.stringify({
    brokerAddrTable: {
      [BROKER]: { cluster: 'DefaultCluster', brokerName: BROKER, brokerAddrs: { '0': addr } },
    },
    clusterAddrTable: { DefaultCluster: [BROKER] },
  });
}

// Consume-stats offset key format the decoder expects.
const OFFSET_KEY = `{"brokerName":"${BROKER}","queueId":0,"topic":"${TOPIC}"}`;

function consumeStatsBody(consumerOffset: number): string {
  return JSON.stringify({
    offsetTable: {
      [OFFSET_KEY]: {
        brokerOffset: 10, consumerOffset, lastTimestamp: 0, pullOffset: 0,
      },
    },
    consumeTps: 1.5,
  });
}

class FakeCluster {
  server: net.Server;
  port = 0;
  // Topics with a route; anything else answers TOPIC_NOT_EXIST.
  routes = new Set<string>([TOPIC]);
  // 300 reply: every group that consumes the topic.
  groups: string[] = [];
  // Per-group 203 reply: consumeType + subscriptionTable, or null for an empty
  // connection set (the group is not online).
  connections: Record<string, { consumeType: string; subscriptionTable?: Record<string, any> } | null> = {};
  // Per-group 208 reply: consumerOffset for the tracked queue.
  consumerOffsets: Record<string, number> = {};
  // 321 / 353 reply bodies (empty string = empty body).
  queueBody = '';
  forbiddenBody = '';
  // Request records.
  routeRequests: string[] = [];
  queryQueueExt: Record<string, string> | null = null;
  forbiddenExt: Record<string, string>[] = [];

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

  respond(req: RemotingCommand): RemotingCommand {
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS)!;
    resp.opaque = req.opaque;
    resp.serializeTypeCurrentRpc = req.serializeTypeCurrentRpc;
    const ext = req.extFields || {};

    if (req.code === RequestCode.GET_ROUTEINFO_BY_TOPIC) {
      const topic = String(ext['topic'] || '');
      this.routeRequests.push(topic);
      if (!this.routes.has(topic)) {
        resp.code = ResponseCode.TOPIC_NOT_EXIST;
        resp.remark = 'route not found';
        return resp;
      }
      resp.body = Buffer.from(routeBody(this.addr), 'utf8');
      return resp;
    }

    if (req.code === RequestCode.GET_BROKER_CLUSTER_INFO) {
      resp.body = Buffer.from(clusterInfoBody(this.addr), 'utf8');
      return resp;
    }

    if (req.code === RequestCode.QUERY_TOPIC_CONSUME_BY_WHO) {
      resp.body = Buffer.from(JSON.stringify({ groupList: this.groups }), 'utf8');
      return resp;
    }

    if (req.code === RequestCode.GET_CONSUMER_CONNECTION_LIST) {
      const group = String(ext['consumerGroup'] || '');
      const conn = this.connections[group];
      if (conn == null) {
        resp.body = Buffer.from(JSON.stringify({ connectionSet: [] }), 'utf8');
        return resp;
      }
      resp.body = Buffer.from(JSON.stringify({
        connectionSet: [{
          clientId: 'cid_1', clientAddr: '127.0.0.1:9999',
          language: 'JAVA', version: 500,
        }],
        subscriptionTable: conn.subscriptionTable || {},
        consumeType: conn.consumeType,
        messageModel: 'CLUSTERING',
        consumeFromWhere: 'CONSUME_FROM_LAST_OFFSET',
      }), 'utf8');
      return resp;
    }

    if (req.code === RequestCode.GET_CONSUME_STATS) {
      const group = String(ext['consumerGroup'] || '');
      const offset = this.consumerOffsets[group];
      if (offset == null) {
        resp.body = Buffer.from(JSON.stringify({ offsetTable: {}, consumeTps: 0 }), 'utf8');
        return resp;
      }
      resp.body = Buffer.from(consumeStatsBody(offset), 'utf8');
      return resp;
    }

    if (req.code === RequestCode.QUERY_CONSUME_QUEUE) {
      this.queryQueueExt = Object.assign({}, ext);
      if (this.queueBody !== '') resp.body = Buffer.from(this.queueBody, 'utf8');
      return resp;
    }

    if (req.code === RequestCode.UPDATE_AND_GET_GROUP_FORBIDDEN) {
      this.forbiddenExt.push(Object.assign({}, ext));
      if (this.forbiddenBody !== '') resp.body = Buffer.from(this.forbiddenBody, 'utf8');
      return resp;
    }

    return resp; // heartbeat / unregistration / everything else: SUCCESS
  }

  close() { this.server.close(); }
}

async function started(cluster: FakeCluster): Promise<DefaultMQAdminExt> {
  const admin = new DefaultMQAdminExt();
  admin.setNamesrvAddr(cluster.addr);
  admin.setInstanceName(`ADMIN_track_${process.pid}_${Math.floor(Math.random() * 1e6)}`);
  admin.start();
  return admin;
}

function forbiddenEntry(topic: string, group: string, readable: boolean): string {
  const entry: Record<string, any> = { topic, group };
  if (readable != null) entry['readable'] = readable;
  return JSON.stringify(entry);
}

async function main() {
  // ------------------------------------ 1. queryConsumeQueue header + decode
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.queueBody = JSON.stringify({
      subscriptionData: { topic: TOPIC, subString: '*' },
      filterData: 'sql: color = "red"',
      queueData: [{ physicOffset: 300, physicSize: 44, tagsCode: 9, eval: false }],
      maxQueueIndex: 9,
      minQueueIndex: 3,
    });
    const admin = await started(cluster);
    const body = await admin.queryConsumeQueue(cluster.addr, TOPIC, 1, 5, 8, 'GID_track');
    check('321 header carries topic/queueId/index/count/consumerGroup',
      cluster.queryQueueExt != null &&
      JSON.stringify(cluster.queryQueueExt) === JSON.stringify({
        topic: TOPIC, queueId: '1', index: '5', count: '8', consumerGroup: 'GID_track',
      }), JSON.stringify(cluster.queryQueueExt));
    check('321 body decodes into QueryConsumeQueueResponseBody',
      body.maxQueueIndex === 9 && body.minQueueIndex === 3 &&
      body.filterData === 'sql: color = "red"' &&
      Array.isArray(body.queueData) && body.queueData!.length === 1 &&
      body.queueData![0].physicOffset === 300 && body.queueData![0].tagsCode === 9,
      JSON.stringify(body));
    admin.shutdown();
    cluster.close();
  }

  // -------------------------------- 2. queryConsumeQueue with an empty body
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const admin = await started(cluster);
    const body = await admin.queryConsumeQueue(cluster.addr, TOPIC, 0, 0, 32, '');
    check('321 empty body answers the default (empty) response object',
      body.queueData === null && body.subscriptionData === null &&
      body.maxQueueIndex === 0 && body.minQueueIndex === 0, JSON.stringify(body));
    admin.shutdown();
    cluster.close();
  }

  // ------------------------- 3. updateAndGetGroupReadForbidden: unset readable
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.forbiddenBody = forbiddenEntry(TOPIC, 'GID_forbid', true);
    const admin = await started(cluster);
    const entry = await admin.updateAndGetGroupReadForbidden(cluster.addr, 'GID_forbid', TOPIC);
    check('353 without readable sends exactly group/topic',
      cluster.forbiddenExt.length === 1 &&
      JSON.stringify(cluster.forbiddenExt[0]) === JSON.stringify({ group: 'GID_forbid', topic: TOPIC }),
      JSON.stringify(cluster.forbiddenExt));
    check('353 reply decodes into GroupForbidden',
      entry.topic === TOPIC && entry.group === 'GID_forbid' && entry.readable === true,
      JSON.stringify(entry));
    admin.shutdown();
    cluster.close();
  }

  // ------------------------------------------ 4. readable true/false on the wire
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.forbiddenBody = forbiddenEntry(TOPIC, 'GID_forbid', false);
    const admin = await started(cluster);
    const entry = await admin.updateAndGetGroupReadForbidden(cluster.addr, 'GID_forbid', TOPIC, false);
    check('353 readable=false is rendered "false"',
      cluster.forbiddenExt.length === 1 && cluster.forbiddenExt[0]['readable'] === 'false',
      JSON.stringify(cluster.forbiddenExt));
    check('353 readable=false decodes', entry.readable === false, JSON.stringify(entry));

    cluster.forbiddenBody = forbiddenEntry(TOPIC, 'GID_forbid', true);
    const flipped = await admin.updateAndGetGroupReadForbidden(cluster.addr, 'GID_forbid', TOPIC, true);
    check('353 readable=true is rendered "true"',
      cluster.forbiddenExt.length === 2 && cluster.forbiddenExt[1]['readable'] === 'true',
      JSON.stringify(cluster.forbiddenExt));
    check('353 readable=true decodes', flipped.readable === true, JSON.stringify(flipped));
    admin.shutdown();
    cluster.close();
  }

  // ----------------------------------- 5. forbidden error + validation contract
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    const admin = await started(cluster);
    let msg = '';
    try {
      await admin.updateAndGetGroupReadForbidden(cluster.addr, 'GID_forbid', TOPIC);
    } catch (e) {
      msg = e instanceof MQClientException ? (e as Error).message : `wrong type: ${(e as Error).message}`;
    }
    check('353 empty reply body is an error',
      msg.includes('empty response body') && cluster.forbiddenExt.length === 1, msg);

    let sentNothing = false;
    cluster.forbiddenExt.length = 0;
    try {
      await admin.updateAndGetGroupReadForbidden(cluster.addr, '', TOPIC);
    } catch {
      sentNothing = true;
    }
    check('353 rejects a missing group before sending anything',
      sentNothing && cluster.forbiddenExt.length === 0,
      `sent=${cluster.forbiddenExt.length} thrown=${sentNothing}`);
    admin.shutdown();
    cluster.close();
  }

  // ------------------------------------------- 6. messageTrackDetail full sweep
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    // Served unsorted; the admin must sort.
    cluster.groups = ['G_track_pull', 'G_track_filtered', 'G_track_offline', 'G_track_consumed'];
    cluster.routes.add(MixAll.getRetryTopic('G_track_pull'));
    cluster.routes.add(MixAll.getRetryTopic('G_track_offline'));
    cluster.routes.add(MixAll.getRetryTopic('G_track_consumed'));
    cluster.routes.add(MixAll.getRetryTopic('G_track_filtered'));
    cluster.connections['G_track_pull'] = { consumeType: ConsumeType.CONSUME_ACTIVELY };
    cluster.connections['G_track_offline'] = null;
    cluster.connections['G_track_consumed'] = {
      consumeType: ConsumeType.CONSUME_PASSIVELY,
      subscriptionTable: {
        [TOPIC]: { topic: TOPIC, subString: 'TagA', tagsSet: ['TagA'], expressionType: 'TAG' },
      },
    };
    cluster.connections['G_track_filtered'] = {
      consumeType: ConsumeType.CONSUME_PASSIVELY,
      subscriptionTable: {
        [TOPIC]: { topic: TOPIC, subString: 'TagB', tagsSet: ['TagB'], expressionType: 'TAG' },
      },
    };
    cluster.consumerOffsets['G_track_consumed'] = 5;
    cluster.consumerOffsets['G_track_filtered'] = 5;

    const admin = await started(cluster);
    const tracks = await admin.messageTrackDetail(trackedMessage(cluster.port));

    const byGroup = new Map<string, MessageTrack>();
    for (const t of tracks) byGroup.set(t.consumerGroup, t);
    check('one track per consuming group, sorted',
      tracks.length === 4 &&
      tracks.map((t) => t.consumerGroup).join(',') ===
        'G_track_consumed,G_track_filtered,G_track_offline,G_track_pull',
      JSON.stringify(tracks.map((t) => t.consumerGroup)));
    check('the active consumer is PULL',
      byGroup.get('G_track_pull')?.trackType === TrackType.PULL,
      byGroup.get('G_track_pull')?.trackType ?? 'missing');
    check('a group without connections is NOT_ONLINE with the code in the description',
      byGroup.get('G_track_offline')?.trackType === TrackType.NOT_ONLINE &&
      (byGroup.get('G_track_offline')?.exceptionDesc ?? '').includes('206'),
      JSON.stringify(byGroup.get('G_track_offline')));
    check('a passive consumer past the offset on the message\'s master queue is CONSUMED',
      byGroup.get('G_track_consumed')?.trackType === TrackType.CONSUMED &&
      byGroup.get('G_track_consumed')?.exceptionDesc === null,
      JSON.stringify(byGroup.get('G_track_consumed')));
    check('a narrower subscription than the message tag is CONSUMED_BUT_FILTERED',
      byGroup.get('G_track_filtered')?.trackType === TrackType.CONSUMED_BUT_FILTERED,
      byGroup.get('G_track_filtered')?.trackType ?? 'missing');
    check('the tracking sweep starts at the topic route, then the retry routes',
      cluster.routeRequests[0] === TOPIC &&
      cluster.routeRequests.slice(1).every((t) => t.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)),
      JSON.stringify(cluster.routeRequests));
    admin.shutdown();
    cluster.close();
  }

  // -------------------------------------- 7. not consumed: offset not past yet
  {
    const cluster = new FakeCluster();
    await cluster.listen();
    cluster.groups = ['G_track_lagging'];
    cluster.routes.add(MixAll.getRetryTopic('G_track_lagging'));
    cluster.connections['G_track_lagging'] = {
      consumeType: ConsumeType.CONSUME_PASSIVELY,
      subscriptionTable: {
        [TOPIC]: { topic: TOPIC, subString: '*', tagsSet: ['*'], expressionType: 'TAG' },
      },
    };
    // consumerOffset equals the message offset: not consumed yet.
    cluster.consumerOffsets['G_track_lagging'] = 4;

    const admin = await started(cluster);
    const tracks = await admin.messageTrackDetail(trackedMessage(cluster.port));
    check('a consumer still behind the message is NOT_CONSUME_YET',
      tracks.length === 1 && tracks[0].trackType === TrackType.NOT_CONSUME_YET &&
      tracks[0].consumerGroup === 'G_track_lagging',
      JSON.stringify(tracks));
    admin.shutdown();
    cluster.close();
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
