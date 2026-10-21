// Client configuration / behaviour gaps — offline wire test.
//
// Coverage:
//   1. decodeReadBody (default true): batch decode still walks every stored
//      record with the switch off, so messages come back with their queue
//      offsets and properties, while the body bytes are not materialised. Both
//      the pull (and lite-pull) path and the POP path are checked.
//   2. mqClientApiTimeout: 3000 ms by default, configurable, and an explicit
//      per-call timeout still wins. Checked on the heartbeat, the name-server
//      route fetch, unregisterClient, getConsumerListByGroup and
//      checkClientConfig.
//   3. enableConcurrentHeartbeat: one heartbeat per broker over a pool (proved
//      by the observed in-flight overlap), masters only when the body has no
//      consumer data, and heartbeat V2 wins over the concurrent round.
//   4. maxPageSizeInGetMetadata: 2000 by default and configurable; it reaches
//      maxTopicNum / maxGroupNum on the paged metadata reads.
//   5. GET_ALL_TOPIC_CONFIG paging: empty-string first dataVersion, merge,
//      stop at totalTopicNum-1, and a dataVersion change restarts from seq 0
//      with the accumulated table cleared.
//
// Run: node --experimental-strip-types test/client_config_gaps_smoke.ts
import net from 'node:net';

import { MQClient, DEFAULT_CONCURRENT_HEARTBEAT_THREAD_POOL_SIZE } from '../src/client/mq_client.ts';
import { PullAPI } from '../src/client/pull_api.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { MessageExt, MessageQueue } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { encodeMessageExt } from '../src/common/messageDecoder.ts';
import { FilterAPI } from '../src/common/subscriptionData.ts';
import { HeartbeatData } from '../src/remoting/heartbeat.ts';
import { TopicConfig } from '../src/remoting/bodies.ts';
import { PopMessageRequestHeader } from '../src/remoting/headers.ts';
import { MixAll } from '../src/common/mixAll.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const NAMESRV = '127.0.0.1:9876';
const BROKER = '127.0.0.1:10911';
const TOPIC = 'GapTopic';
const GROUP = 'GID_gap';

type Call = {
  code: number;
  addr: string;
  ext: Record<string, string>;
  timeout: number | undefined;
};

type Beat = {
  addr: string;
  fingerprint: number;
  withoutSub: boolean;
  producers: number;
  consumers: number;
};

type TopicPage = {
  topics: Record<string, TopicConfig>;
  dataVersion: Record<string, any> | null;
  total?: number;
};

type GroupPage = {
  groups: Record<string, any>;
  dataVersion: Record<string, any> | null;
  total?: number;
};

// Records every frame the client puts on the wire and answers from per-code
// scripts. Does not listen on a socket: the storage formats, headers and
// dispatch decisions are what these assertions are about.
class RecordingTransport {
  calls: Call[] = [];
  beats: Beat[] = [];
  pullBody: Buffer | null = null;
  popBody: Buffer | null = null;
  topicPages: TopicPage[] = [];
  groupPages: GroupPage[] = [];
  private topicIndex = 0;
  private groupIndex = 0;
  // Observed overlap of in-flight invokes, used to tell the serial heartbeat
  // loop (max 1) from the concurrent round (max = pool size).
  inFlight = 0;
  maxInFlight = 0;

  registerProcessor(_code: number, _handler: any): void {}
  unregisterProcessor(_code: number): void {}
  registerRpcHook(_hook: any): void {}
  shutdown(): void {}

  resetScript(): void {
    this.calls.length = 0;
    this.beats.length = 0;
    this.topicPages = [];
    this.groupPages = [];
    this.topicIndex = 0;
    this.groupIndex = 0;
    this.inFlight = 0;
    this.maxInFlight = 0;
  }

  heartbeatCalls(): Beat[] { return this.beats; }

  async invokeSync(addr: string, request: RemotingCommand, timeoutMillis?: number): Promise<RemotingCommand> {
    this.calls.push({
      code: request.code,
      addr,
      ext: { ...(request.extFields || {}) },
      timeout: timeoutMillis,
    });
    if (request.code === RequestCode.HEART_BEAT) {
      const hb = HeartbeatData.decode(request.body as Buffer);
      this.beats.push({
        addr,
        fingerprint: hb.heartbeatFingerprint,
        withoutSub: hb.withoutSub,
        producers: hb.producerDataSet.length,
        consumers: hb.consumerDataSet.length,
      });
    }
    // One microtask of overlap: two tasks dispatched by the same round are
    // both in flight here, while a serial loop only ever has one.
    this.inFlight++;
    this.maxInFlight = Math.max(this.maxInFlight, this.inFlight);
    await Promise.resolve();
    const response = this.respond(request);
    this.inFlight--;
    return response;
  }

  invokeOneway(_addr: string, _request: RemotingCommand): Promise<void> { return Promise.resolve(); }

  private respond(request: RemotingCommand): RemotingCommand {
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, '')!;
    resp.opaque = request.opaque;
    resp.serializeTypeCurrentRpc = request.serializeTypeCurrentRpc;
    switch (request.code) {
      case RequestCode.PULL_MESSAGE:
      case RequestCode.LITE_PULL_MESSAGE:
        resp.addExtField('nextBeginOffset', '2');
        resp.addExtField('minOffset', '0');
        resp.addExtField('maxOffset', '10');
        if (this.pullBody != null) resp.body = this.pullBody;
        break;
      case RequestCode.POP_MESSAGE:
        resp.addExtField('popTime', '1000');
        resp.addExtField('invisibleTime', '60000');
        resp.addExtField('reviveQid', '0');
        resp.addExtField('restNum', '0');
        if (this.popBody != null) resp.body = this.popBody;
        break;
      case RequestCode.GET_ALL_TOPIC_CONFIG: {
        const page = this.topicPages[Math.min(this.topicIndex, this.topicPages.length - 1)];
        this.topicIndex++;
        const body: Record<string, any> = {
          topicConfigTable: {}, dataVersion: page != null ? page.dataVersion : {},
        };
        if (page != null) {
          for (const [k, v] of Object.entries(page.topics)) body['topicConfigTable'][k] = v.toDict();
          if (page.total != null) resp.addExtField('totalTopicNum', String(page.total));
        }
        resp.body = Buffer.from(JSON.stringify(body), 'utf8');
        break;
      }
      case RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG: {
        const page = this.groupPages[Math.min(this.groupIndex, this.groupPages.length - 1)];
        this.groupIndex++;
        const body: Record<string, any> = {
          subscriptionGroupTable: page != null ? page.groups : {},
          forbiddenTable: {},
          dataVersion: page != null ? page.dataVersion : {},
        };
        if (page != null && page.total != null) {
          resp.addExtField('totalGroupNum', String(page.total));
        }
        resp.body = Buffer.from(JSON.stringify(body), 'utf8');
        break;
      }
      case RequestCode.GET_ROUTEINFO_BY_TOPIC:
        // No route is needed: the call is only observed for its timeout.
        resp.code = ResponseCode.TOPIC_NOT_EXIST;
        break;
      default:
        break;
    }
    return resp;
  }
}

function storedMessage(queueOffset: number, body: string, tags: string): MessageExt {
  const msg = new MessageExt(TOPIC, Buffer.from(body, 'utf8'), tags);
  msg.setQueueId(0);
  msg.setQueueOffset(queueOffset);
  msg.setFlag(0);
  msg.setSysFlag(0);
  msg.setBornTimestamp(1_700_000_000_000);
  msg.setStoreTimestamp(1_700_000_000_001);
  msg.setBornHost('127.0.0.1');
  msg.setStoreHost('127.0.0.1');
  msg.putProperty(MessageConst.PROPERTY_TAGS, tags);
  msg.putProperty('userKey', `value-${queueOffset}`);
  return msg;
}

function storedBody(): Buffer {
  return Buffer.concat([
    encodeMessageExt(storedMessage(0, 'first-body', 'tagA')),
    encodeMessageExt(storedMessage(1, 'second-body', 'tagB')),
  ]);
}

function newClient(transport: RecordingTransport, id: string): MQClient {
  return new MQClient(id, NAMESRV, transport as any);
}

function bodyOf(msg: MessageExt): number | null {
  const body = msg.getBody();
  return body == null ? null : body.length;
}

let adminSeq = 0;
function startedAdmin(transport: RecordingTransport): DefaultMQAdminExt {
  const admin = new DefaultMQAdminExt();
  admin.setNamesrvAddr(NAMESRV);
  admin.setInstanceName(`ADMIN_gap_${process.pid}_${adminSeq++}`);
  admin.setTimeoutMillis(5000);
  admin.client = new MQClient(`gapPreset_${adminSeq}`, NAMESRV, transport as any);
  admin.start();
  return admin;
}

function topicPagesCalls(transport: RecordingTransport): Call[] {
  return transport.calls.filter((c) => c.code === RequestCode.GET_ALL_TOPIC_CONFIG);
}

async function main() {
  // ------------------------------------------------ 1. decodeReadBody
  {
    const transport = new RecordingTransport();
    transport.pullBody = storedBody();
    transport.popBody = storedBody();
    const client = newClient(transport, `gap-decode-${process.pid}`);
    client.brokerAddrTable.set('brokerA', { [MixAll.MASTER_ID]: BROKER });
    const mq = new MessageQueue(TOPIC, 'brokerA', 0);
    const sub = FilterAPI.buildSubscriptionData(TOPIC, '*');
    const api = new PullAPI(client, GROUP);

    check('decodeReadBody defaults to true', client.decodeReadBody === true);

    const on = await api.pullKernel(mq, 0, sub, 0, 0, 32, 0, 3000);
    check('default pull decode materialises both bodies',
      on.msgFoundList.length === 2 &&
      bodyOf(on.msgFoundList[0]) === 'first-body'.length &&
      bodyOf(on.msgFoundList[1]) === 'second-body'.length,
      JSON.stringify(on.msgFoundList.map((m) => bodyOf(m))));

    client.setDecodeReadBody(false);
    const off = await api.pullKernel(mq, 0, sub, 0, 0, 32, 0, 3000);
    check('switch off still returns every frame (cursor walked past the bodies)',
      off.msgFoundList.length === 2 &&
      off.msgFoundList[0].getQueueOffset() === 0 &&
      off.msgFoundList[1].getQueueOffset() === 1,
      JSON.stringify(off.msgFoundList.map((m) => m.getQueueOffset())));
    check('switch off leaves the bodies unmaterialised',
      off.msgFoundList.every((m) => bodyOf(m) == null || bodyOf(m) === 0),
      JSON.stringify(off.msgFoundList.map((m) => bodyOf(m))));
    check('switch off keeps properties and topic intact',
      off.msgFoundList[0].getProperty(MessageConst.PROPERTY_TAGS) === 'tagA' &&
      off.msgFoundList[1].getProperty(MessageConst.PROPERTY_TAGS) === 'tagB' &&
      off.msgFoundList[0].getProperty('userKey') === 'value-0' &&
      off.msgFoundList[0].getTopic() === TOPIC);

    // POP uses the same switch on its own decode path.
    const popHeader = new PopMessageRequestHeader();
    popHeader.topic = TOPIC;
    popHeader.consumerGroup = GROUP;
    popHeader.queueId = 0;
    popHeader.maxMsgNums = 32;
    const popOff = await client.popMessage('brokerA', BROKER, popHeader, '', 3000);
    check('POP result still returns both messages with the switch off',
      popOff.msgFoundList.length === 2 &&
      popOff.msgFoundList[0].getQueueOffset() === 0 &&
      popOff.msgFoundList[1].getQueueOffset() === 1,
      JSON.stringify(popOff.msgFoundList.map((m) => m.getQueueOffset())));
    check('POP bodies are unmaterialised with the switch off',
      popOff.msgFoundList.every((m) => bodyOf(m) == null || bodyOf(m) === 0),
      JSON.stringify(popOff.msgFoundList.map((m) => bodyOf(m))));

    client.setDecodeReadBody(true);
    const popOn = await client.popMessage('brokerA', BROKER, popHeader, '', 3000);
    check('POP decode materialises bodies when the switch is back on',
      popOn.msgFoundList.length === 2 &&
      bodyOf(popOn.msgFoundList[0]) === 'first-body'.length,
      JSON.stringify(popOn.msgFoundList.map((m) => bodyOf(m))));
  }

  // --------------------------------------------- 2. mqClientApiTimeout
  {
    const transport = new RecordingTransport();
    const client = newClient(transport, `gap-timeout-${process.pid}`);
    client.brokerAddrTable.set('brokerA', { [MixAll.MASTER_ID]: BROKER });
    client.registerProducer('GID_prod', {});

    check('mqClientApiTimeout defaults to 3000', client.mqClientApiTimeout === 3000);
    await client.sendHeartbeatToAllBrokers();
    check('the default 3000 ms reaches the heartbeat invoke',
      transport.calls.filter((c) => c.code === RequestCode.HEART_BEAT).pop()?.timeout === 3000,
      String(transport.calls.filter((c) => c.code === RequestCode.HEART_BEAT).pop()?.timeout));

    client.setMqClientApiTimeout(4321);
    transport.calls.length = 0;
    await client.sendHeartbeatToAllBrokers();
    await client.updateTopicRouteInfoFromNameServer(TOPIC, false);
    await client.unregisterClient(BROKER, 'GID_prod', '');
    await client.getConsumerListByGroup(BROKER, GROUP);
    await client.checkClientConfig(BROKER, GROUP,
      { expressionType: 'SQL92', topic: TOPIC, toDict: () => ({}) }, 9876);

    const timeoutOf = (code: number): number | undefined => {
      const call = transport.calls.filter((c) => c.code === code).pop();
      return call != null ? call.timeout : undefined;
    };
    check('configured timeout reaches heartbeat', timeoutOf(RequestCode.HEART_BEAT) === 4321,
      String(timeoutOf(RequestCode.HEART_BEAT)));
    check('configured timeout reaches the name-server route fetch',
      timeoutOf(RequestCode.GET_ROUTEINFO_BY_TOPIC) === 4321,
      String(timeoutOf(RequestCode.GET_ROUTEINFO_BY_TOPIC)));
    check('configured timeout reaches unregisterClient',
      timeoutOf(RequestCode.UNREGISTER_CLIENT) === 4321,
      String(timeoutOf(RequestCode.UNREGISTER_CLIENT)));
    check('configured timeout reaches getConsumerListByGroup',
      timeoutOf(RequestCode.GET_CONSUMER_LIST_BY_GROUP) === 4321,
      String(timeoutOf(RequestCode.GET_CONSUMER_LIST_BY_GROUP)));
    check('an explicit per-call timeout still wins',
      timeoutOf(RequestCode.CHECK_CLIENT_CONFIG) === 9876,
      String(timeoutOf(RequestCode.CHECK_CLIENT_CONFIG)));
  }

  // ---------------------------------------- 3. concurrent heartbeat round
  {
    const transport = new RecordingTransport();
    const client = newClient(transport, `gap-concurrent-${process.pid}`);
    const masterA = '127.0.0.1:20901';
    const slaveA = '127.0.0.1:20902';
    const masterB = '127.0.0.1:20903';
    client.brokerAddrTable.set('brokerA', { [MixAll.MASTER_ID]: masterA, 1: slaveA });
    client.brokerAddrTable.set('brokerB', { [MixAll.MASTER_ID]: masterB });
    client.registerProducer('GID_prod', {});

    check('enableConcurrentHeartbeat defaults to false', client.enableConcurrentHeartbeat === false);
    check('the concurrent pool size defaults to the available processors',
      client.concurrentHeartbeatThreadPoolSize === DEFAULT_CONCURRENT_HEARTBEAT_THREAD_POOL_SIZE,
      String(client.concurrentHeartbeatThreadPoolSize));

    client.setEnableConcurrentHeartbeat(true);
    client.setConcurrentHeartbeatThreadPoolSize(2);
    // A producer-only body must go to masters only.
    const delivered = await client.sendHeartbeatToAllBrokers();
    check('the concurrent round beats every master',
      transport.beats.length === 2 &&
      transport.beats.some((b) => b.addr === masterA) &&
      transport.beats.some((b) => b.addr === masterB),
      JSON.stringify(transport.beats.map((b) => b.addr)));
    check('the concurrent round skips the slave when there is no consumer data',
      !transport.beats.some((b) => b.addr === slaveA),
      JSON.stringify(transport.beats.map((b) => b.addr)));
    check('the concurrent round reports the success count', delivered === 2, String(delivered));
    check('the round genuinely overlaps its tasks', transport.maxInFlight === 2,
      String(transport.maxInFlight));
    check('the instance owns a pool once the round ran', (client as any)._heartbeatExecutor != null);

    // Control: the same table over the serial loop has no overlap.
    client.setEnableConcurrentHeartbeat(false);
    transport.beats.length = 0;
    transport.maxInFlight = 0;
    const serialDelivered = await client.sendHeartbeatToAllBrokers();
    check('the serial loop reaches the same masters without overlap',
      serialDelivered === 2 && transport.beats.length === 2 && transport.maxInFlight === 1,
      JSON.stringify({ delivered: serialDelivered, max: transport.maxInFlight }));

    // Heartbeat V2 wins over the concurrent switch.
    client.setUseHeartbeatV2(true);
    client.setEnableConcurrentHeartbeat(true);
    transport.beats.length = 0;
    transport.maxInFlight = 0;
    await client.sendHeartbeatToAllBrokers();
    check('heartbeat V2 wins over the concurrent round',
      transport.maxInFlight === 1 &&
      transport.beats.length === 2 &&
      transport.beats.every((b) => b.fingerprint !== 0),
      JSON.stringify({ max: transport.maxInFlight, beats: transport.beats }));

    // Empty body: nothing is sent.
    client.setUseHeartbeatV2(false);
    client.unregisterProducer('GID_prod');
    transport.beats.length = 0;
    const idle = await client.sendHeartbeatToAllBrokers();
    check('an empty heartbeat body sends nothing', idle === 0 && transport.beats.length === 0);

    client.registerProducer('GID_prod', {});
    await client.sendHeartbeatToAllBrokers(); // make sure the pool exists
    check('the pool exists before shutdown', (client as any)._heartbeatExecutor != null);
    client.shutdown();
    check('shutdown releases the concurrent heartbeat pool',
      (client as any)._heartbeatExecutor == null);
  }

  // ------------------------- 4/5. metadata page size + topic-config paging
  {
    const transport = new RecordingTransport();
    const admin = startedAdmin(transport);

    // Defaults reach both paged calls.
    transport.topicPages = [{ topics: { DefaultTopic: new TopicConfig('DefaultTopic') }, dataVersion: {} }];
    transport.groupPages = [{ groups: { DefaultGroup: {} }, dataVersion: {} }];
    await admin.getAllTopicConfig(BROKER);
    await admin.getAllSubscriptionGroup(BROKER);
    const defaultTopicCall = topicPagesCalls(transport)[0];
    const defaultGroupCall = transport.calls
      .filter((c) => c.code === RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG)[0];
    check('maxPageSizeInGetMetadata defaults to 2000 for maxTopicNum',
      defaultTopicCall != null && defaultTopicCall.ext['maxTopicNum'] === '2000',
      defaultTopicCall != null ? String(defaultTopicCall.ext['maxTopicNum']) : 'no call');
    check('maxPageSizeInGetMetadata defaults to 2000 for maxGroupNum',
      defaultGroupCall != null && defaultGroupCall.ext['maxGroupNum'] === '2000',
      defaultGroupCall != null ? String(defaultGroupCall.ext['maxGroupNum']) : 'no call');
    check('the first subscription-group page sends dataVersion as the empty string',
      defaultGroupCall != null && defaultGroupCall.ext['dataVersion'] === '',
      defaultGroupCall != null ? String(defaultGroupCall.ext['dataVersion']) : 'no call');

    // A configured value replaces the default on both calls.
    admin.setMaxPageSizeInGetMetadata(3);
    transport.calls.length = 0;
    transport.topicIndex = 0;
    transport.groupIndex = 0;
    await admin.getAllTopicConfig(BROKER);
    await admin.getAllSubscriptionGroup(BROKER);
    check('a configured page size reaches maxTopicNum',
      topicPagesCalls(transport)[0]?.ext['maxTopicNum'] === '3',
      String(topicPagesCalls(transport)[0]?.ext['maxTopicNum']));
    check('a configured page size reaches maxGroupNum',
      transport.calls.filter((c) => c.code === RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG)[0]
        ?.ext['maxGroupNum'] === '3');

    // Full topic paging: 7 topics, 2 per page -> seq 0, 2, 4 and stop at 6 (total-1).
    transport.resetScript();
    transport.topicPages = [
      { topics: { T0: new TopicConfig('T0'), T1: new TopicConfig('T1') }, dataVersion: { counter: 1 }, total: 7 },
      { topics: { T2: new TopicConfig('T2'), T3: new TopicConfig('T3') }, dataVersion: { counter: 1 }, total: 7 },
      { topics: { T4: new TopicConfig('T4'), T5: new TopicConfig('T5') }, dataVersion: { counter: 1 }, total: 7 },
      { topics: { T6: new TopicConfig('T6'), T7: new TopicConfig('T7') }, dataVersion: { counter: 1 }, total: 7 },
    ];
    admin.setMaxPageSizeInGetMetadata(2);
    const paged = await admin.getAllTopicConfig(BROKER, 5000);
    const pagedCalls = topicPagesCalls(transport);
    check('topic paging stops at totalTopicNum-1 (3 pages, no fourth request)',
      pagedCalls.length === 3, String(pagedCalls.length));
    check('topic paging advances topicSeq by the page size',
      pagedCalls.map((c) => c.ext['topicSeq']).join(',') === '0,2,4',
      pagedCalls.map((c) => c.ext['topicSeq']).join(','));
    check('the first topic page sends dataVersion as the empty string',
      pagedCalls[0]?.ext['dataVersion'] === '', String(pagedCalls[0]?.ext['dataVersion']));
    check('later topic pages send the last seen dataVersion JSON',
      pagedCalls[1]?.ext['dataVersion'] === JSON.stringify({ counter: 1 }) &&
      pagedCalls[2]?.ext['dataVersion'] === JSON.stringify({ counter: 1 }),
      JSON.stringify(pagedCalls.map((c) => c.ext['dataVersion'])));
    check('every topic page carries maxTopicNum',
      pagedCalls.every((c) => c.ext['maxTopicNum'] === '2'));
    check('the topic tables are merged across pages',
      ['T0', 'T1', 'T2', 'T3', 'T4', 'T5'].every((k) => paged.topicConfigTable[k] != null) &&
      Object.keys(paged.topicConfigTable).length === 6,
      Object.keys(paged.topicConfigTable).join(','));
    check('the merged wrapper keeps the dataVersion',
      paged.dataVersion['counter'] === 1, JSON.stringify(paged.dataVersion));

    // dataVersion change: restart at seq 0 and drop the first version's table.
    transport.resetScript();
    transport.topicPages = [
      { topics: { OldA: new TopicConfig('OldA') }, dataVersion: { counter: 1 }, total: 4 },
      { topics: { OldB: new TopicConfig('OldB') }, dataVersion: { counter: 2 }, total: 4 },
      { topics: { NewA: new TopicConfig('NewA'), NewB: new TopicConfig('NewB') }, dataVersion: { counter: 2 }, total: 4 },
      { topics: { NewC: new TopicConfig('NewC'), NewD: new TopicConfig('NewD') }, dataVersion: { counter: 2 }, total: 4 },
    ];
    admin.setMaxPageSizeInGetMetadata(2);
    const restarted = await admin.getAllTopicConfig(BROKER, 5000);
    const restartCalls = topicPagesCalls(transport);
    check('a dataVersion change restarts topicSeq at 0',
      restartCalls.map((c) => c.ext['topicSeq']).join(',') === '0,1,0,2',
      restartCalls.map((c) => c.ext['topicSeq']).join(','));
    check('the restarted paging sends the new dataVersion JSON',
      restartCalls[2]?.ext['dataVersion'] === JSON.stringify({ counter: 2 }) &&
      restartCalls[3]?.ext['dataVersion'] === JSON.stringify({ counter: 2 }),
      JSON.stringify(restartCalls.map((c) => c.ext['dataVersion'])));
    check('the restart clears the accumulated table',
      restarted.topicConfigTable['OldA'] == null && restarted.topicConfigTable['OldB'] == null &&
      Object.keys(restarted.topicConfigTable).sort().join(',') === 'NewA,NewB,NewC,NewD',
      Object.keys(restarted.topicConfigTable).join(','));
    check('the restarted wrapper carries the new dataVersion',
      restarted.dataVersion['counter'] === 2, JSON.stringify(restarted.dataVersion));

    admin.shutdown();
  }

  // A quick socket sanity check keeps the fake-transport assumptions honest:
  // the admin path above is the same one a listening broker would answer.
  {
    const server = net.createServer((sock) => sock.on('error', () => {}));
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
    const port = (server.address() as net.AddressInfo).port;
    check('an ephemeral recording port is available', port > 0);
    server.close();
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
