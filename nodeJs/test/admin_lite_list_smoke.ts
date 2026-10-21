// Lite-topic admin (200074..200079) and the list-form topic/group pushes
// (18 / 225) — offline wire test (no sockets).
//
// Pinned contracts:
//   * Every lite RPC rides the fast channel (port - 2) through ONE shared
//     invoke leaf; a code-only answer decodes as null (never as an empty
//     object) and a non-SUCCESS answer raises the broker exception carrying
//     the address as the CALLER passed it.
//   * Nullable lite header fields are OMITTED when unset (an empty string is
//     a different request for the broker), while the primitive maxCount/topK
//     ride along ALWAYS — including 0.
//   * The list forms send the whole config set as one JSON body and carry an
//     EMPTY header — no per-field validation of the entries — and a
//     non-SUCCESS answer raises the CLIENT exception, not the broker one.
//
// Run: node --experimental-strip-types test/admin_lite_list_smoke.ts
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { MQClient } from '../src/client/mq_client.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { MQBrokerException, MQClientException } from '../src/remoting/exception.ts';
import { TopicConfig, TopicFilterType } from '../src/remoting/bodies.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const NS = '127.0.0.1:9876';
const BROKER = '127.0.0.1:10911';
const BROKER_VIP = '127.0.0.1:10909';

interface Call {
  code: number; addr: string; ext: Record<string, string>;
  body: Buffer | null; timeout: number;
}

class RecordingClient {
  calls: Call[] = [];
  responder: ((req: RemotingCommand) => RemotingCommand) | null = null;
  reset() { this.calls = []; this.responder = null; }
  callFor(code: number): Call | undefined {
    for (let i = this.calls.length - 1; i >= 0; i--) if (this.calls[i].code === code) return this.calls[i];
    return undefined;
  }
  respond(req: RemotingCommand): RemotingCommand {
    if (this.responder != null) return this.responder(req);
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null)!;
    resp.opaque = req.opaque;
    return resp;
  }
  invokeSync(addr: string, request: RemotingCommand, timeout?: number): Promise<RemotingCommand> {
    this.calls.push({
      code: request.code, addr,
      ext: Object.assign({}, request.extFields || {}),
      body: request.body != null ? Buffer.from(request.body) : null,
      timeout: timeout as number,
    });
    return Promise.resolve(this.respond(request));
  }
  invokeOneway(addr: string, request: RemotingCommand): Promise<void> {
    this.calls.push({ code: request.code, addr, ext: Object.assign({}, request.extFields || {}), body: null, timeout: 0 });
    return Promise.resolve();
  }
  invokeAsync(addr: string, request: RemotingCommand,
    cb: (r: RemotingCommand | null, e: Error | null) => void, _timeout?: number): void {
    this.calls.push({ code: request.code, addr, ext: Object.assign({}, request.extFields || {}), body: null, timeout: 0 });
    cb(this.respond(request), null);
  }
}

let clientSeq = 0;
function newAdmin(rc: RecordingClient): DefaultMQAdminExt {
  const admin = new DefaultMQAdminExt();
  admin.client = new MQClient(`127.0.0.1@lite_${process.pid}_${clientSeq++}`, NS, rc as any);
  (admin as any)._started = true;
  admin.setVipChannelEnabled(true);
  return admin;
}

function failure(req: RemotingCommand, code: number, remark: string): RemotingCommand {
  const resp = RemotingCommand.createResponseCommand(code, remark)!;
  resp.opaque = req.opaque;
  return resp;
}

async function main() {
  // --------------------------------------------------- 1. GET_BROKER_LITE_INFO
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc);
    rc.responder = (req) => {
      const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null)!;
      resp.opaque = req.opaque;
      resp.body = Buffer.from(JSON.stringify({
        storeType: 'rocksdb', maxLmqNum: 1000000, currentLmqNum: 7,
        liteSubscriptionCount: 12, orderInfoCount: 3, cqTableSize: 4096,
        offsetTableSize: 128, eventMapSize: 64,
      }), 'utf8');
      return resp;
    };
    const info = await admin.getBrokerLiteInfo(BROKER);
    const call = rc.callFor(RequestCode.GET_BROKER_LITE_INFO)!;
    check('200074 rides the fast channel with NO header fields at all',
      call.addr === BROKER_VIP && Object.keys(call.ext).length === 0 && call.body === null,
      `${call.addr} ${JSON.stringify(call.ext)}`);
    check('200074 body decodes into the broker lite summary',
      info != null && info.storeType === 'rocksdb' && info.maxLmqNum === 1000000 &&
      info.currentLmqNum === 7 && info.liteSubscriptionCount === 12 && info.cqTableSize === 4096,
      JSON.stringify(info));
    rc.reset();
    const bare = await admin.getBrokerLiteInfo(BROKER);
    check('200074 code-only answer decodes as null, not an empty object',
      bare === null, JSON.stringify(bare));
  }

  // ---------------------------------------------------- 2. GET_PARENT_TOPIC_INFO
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc);
    rc.responder = (req) => {
      const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null)!;
      resp.opaque = req.opaque;
      resp.body = Buffer.from(JSON.stringify({
        topic: 'ParentA', ttl: 4, groups: ['G1', 'G2'], lmqNum: 5, liteTopicCount: 2,
      }), 'utf8');
      return resp;
    };
    const info = await admin.getParentTopicInfo(BROKER, 'ParentA');
    const call = rc.callFor(RequestCode.GET_PARENT_TOPIC_INFO)!;
    check('200075 header is exactly the topic, on the fast channel',
      call.addr === BROKER_VIP &&
      JSON.stringify(call.ext) === JSON.stringify({ topic: 'ParentA' }), JSON.stringify(call.ext));
    check('200075 body decodes into the parent topic summary',
      info != null && info.topic === 'ParentA' && info.ttl === 4 &&
      JSON.stringify(info.groups) === JSON.stringify(['G1', 'G2']) && info.liteTopicCount === 2,
      JSON.stringify(info));
  }

  // ----------------------------------------------------- 3. GET_LITE_TOPIC_INFO
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc);
    await admin.getLiteTopicInfo(BROKER, 'ParentA', '%RETRY%G1_lmq_1');
    const call = rc.callFor(RequestCode.GET_LITE_TOPIC_INFO)!;
    check('200076 header carries parentTopic and liteTopic when set',
      JSON.stringify(call.ext) === JSON.stringify({ parentTopic: 'ParentA', liteTopic: '%RETRY%G1_lmq_1' }),
      JSON.stringify(call.ext));
    rc.reset();
    await admin.getLiteTopicInfo(BROKER, 'ParentA', null);
    const minimal = rc.callFor(RequestCode.GET_LITE_TOPIC_INFO)!;
    check('200076 an unset liteTopic is OMITTED, not sent empty',
      JSON.stringify(minimal.ext) === JSON.stringify({ parentTopic: 'ParentA' }),
      JSON.stringify(minimal.ext));
  }

  // ---------------------------------------------------- 4. GET_LITE_CLIENT_INFO
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc);
    await admin.getLiteClientInfo(BROKER, 'ParentA', 'GID_lite', 'cid_9');
    const call = rc.callFor(RequestCode.GET_LITE_CLIENT_INFO)!;
    check('200077 full header carries parentTopic/group/clientId/maxCount',
      JSON.stringify(call.ext) === JSON.stringify({
        parentTopic: 'ParentA', group: 'GID_lite', clientId: 'cid_9', maxCount: '1000',
      }), JSON.stringify(call.ext));
    rc.reset();
    await admin.getLiteClientInfo(BROKER, 'ParentA', 'GID_lite', null);
    const minimal = rc.callFor(RequestCode.GET_LITE_CLIENT_INFO)!;
    check('200077 unset clientId is omitted but maxCount is ALWAYS sent',
      JSON.stringify(minimal.ext) === JSON.stringify({
        parentTopic: 'ParentA', group: 'GID_lite', maxCount: '1000',
      }), JSON.stringify(minimal.ext));
  }

  // ----------------------------------------------------- 5. GET_LITE_GROUP_INFO
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc);
    rc.responder = (req) => {
      const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null)!;
      resp.opaque = req.opaque;
      resp.body = Buffer.from(JSON.stringify({
        group: 'GID_lite', parentTopic: 'ParentA',
        lagCountTopK: [{ liteTopic: '%RETRY%G1_lmq_1', lagCount: 9, consumerOffset: 3 }],
      }), 'utf8');
      return resp;
    };
    const info = await admin.getLiteGroupInfo(BROKER, 'GID_lite', null, 10);
    const call = rc.callFor(RequestCode.GET_LITE_GROUP_INFO)!;
    check('200078 header carries group and topK; unset liteTopic omitted',
      JSON.stringify(call.ext) === JSON.stringify({ group: 'GID_lite', topK: '10' }),
      JSON.stringify(call.ext));
    check('200078 body decodes the lag rows',
      info != null && info.group === 'GID_lite' && info.parentTopic === 'ParentA' &&
      Array.isArray(info.lagCountTopK) && info.lagCountTopK![0].lagCount === 9,
      JSON.stringify(info));
    rc.reset();
    await admin.getLiteGroupInfo(BROKER, 'GID_lite', '%RETRY%G1_lmq_1', 0);
    const zero = rc.callFor(RequestCode.GET_LITE_GROUP_INFO)!;
    check('200078 topK=0 (primitive) is STILL sent, liteTopic present when set',
      JSON.stringify(zero.ext) === JSON.stringify({
        group: 'GID_lite', liteTopic: '%RETRY%G1_lmq_1', topK: '0',
      }), JSON.stringify(zero.ext));
  }

  // ------------------------------------------------------ 6. TRIGGER_LITE_DISPATCH
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc);
    await admin.triggerLiteDispatch(BROKER, 'GID_lite', 'cid_9');
    const call = rc.callFor(RequestCode.TRIGGER_LITE_DISPATCH)!;
    check('200079 header is group + clientId on the fast channel',
      call.addr === BROKER_VIP &&
      JSON.stringify(call.ext) === JSON.stringify({ group: 'GID_lite', clientId: 'cid_9' }),
      `${call.addr} ${JSON.stringify(call.ext)}`);
    rc.reset();
    await admin.triggerLiteDispatch(BROKER, 'GID_lite', null);
    const minimal = rc.callFor(RequestCode.TRIGGER_LITE_DISPATCH)!;
    check('200079 unset clientId is omitted', JSON.stringify(minimal.ext) === JSON.stringify({ group: 'GID_lite' }),
      JSON.stringify(minimal.ext));

    rc.responder = (req) => failure(req, ResponseCode.SYSTEM_ERROR, 'dispatch busy');
    let msg = '';
    let brokerAddr = '';
    try {
      await admin.triggerLiteDispatch(BROKER, 'GID_lite', null);
    } catch (e) {
      if (e instanceof MQBrokerException) {
        msg = (e as Error).message;
        brokerAddr = String((e as any).brokerAddr);
      } else {
        msg = `wrong type: ${(e as Error).message}`;
      }
    }
    check('200079 non-SUCCESS raises the broker exception at the CALLER address',
      msg.includes('dispatch busy') && brokerAddr === BROKER, `${msg} @ ${brokerAddr}`);
  }

  // ------------------------------------------------------- 7. list-form pushes
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc);
    const cfg = new TopicConfig('ListTopicA', 8, 8, 6, TopicFilterType.SINGLE_TAG, 0, false);
    // Deliberately odd entry: the list form carries NO per-field validation.
    const odd = new TopicConfig('', 1, 1, 0, TopicFilterType.SINGLE_TAG, 0, true);
    await admin.createAndUpdateTopicConfigList(BROKER, [cfg, odd]);
    const call = rc.callFor(RequestCode.UPDATE_AND_CREATE_TOPIC_LIST)!;
    check('18 rides the fast channel with an EMPTY header',
      call.addr === BROKER_VIP && Object.keys(call.ext).length === 0, `${call.addr} ${JSON.stringify(call.ext)}`);
    const parsed = JSON.parse(call.body!.toString('utf8'));
    check('18 body is exactly the topicConfigList wrapper',
      Object.keys(parsed).length === 1 && Array.isArray(parsed['topicConfigList']) &&
      parsed['topicConfigList'].length === 2, call.body!.toString('utf8'));
    const first = parsed['topicConfigList'][0];
    check('18 entries carry the full fastjson getter shape',
      JSON.stringify(Object.keys(first)) === JSON.stringify([
        'topicName', 'readQueueNums', 'writeQueueNums', 'perm', 'topicFilterType',
        'topicSysFlag', 'order', 'attributes',
      ]) && first['topicName'] === 'ListTopicA' && first['topicFilterType'] === 'SINGLE_TAG',
      JSON.stringify(Object.keys(first)));
    check('18 sends entries WITHOUT validating them',
      parsed['topicConfigList'][1]['topicName'] === '' && parsed['topicConfigList'][1]['order'] === true,
      JSON.stringify(parsed['topicConfigList'][1]));

    rc.responder = (req) => failure(req, ResponseCode.SYSTEM_ERROR, 'store busy');
    let msg = '';
    let isBrokerEx = false;
    try {
      await admin.createAndUpdateTopicConfigList(BROKER, [cfg]);
    } catch (e) {
      msg = (e as Error).message;
      isBrokerEx = e instanceof MQBrokerException;
    }
    check('18 non-SUCCESS raises the CLIENT exception, not the broker one',
      msg.includes('store busy') && !isBrokerEx, `${msg} brokerEx=${isBrokerEx}`);
  }
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc);
    const groups = [
      { groupName: 'GID_a', consumeEnable: true, retryMaxTimes: 16 },
      { groupName: 'GID_b', consumeEnable: false },
    ];
    await admin.createAndUpdateSubscriptionGroupConfigList(BROKER, groups);
    const call = rc.callFor(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST)!;
    check('225 rides the fast channel with no header and the groupConfigList body',
      call.addr === BROKER_VIP && Object.keys(call.ext).length === 0 &&
      JSON.stringify(JSON.parse(call.body!.toString('utf8'))) ===
        JSON.stringify({ groupConfigList: groups }),
      `${call.addr} ${call.body?.toString('utf8')}`);
    rc.responder = (req) => failure(req, ResponseCode.SYSTEM_ERROR, 'store busy');
    let isClientEx = false;
    try {
      await admin.createAndUpdateSubscriptionGroupConfigList(BROKER, groups);
    } catch (e) {
      isClientEx = e instanceof MQClientException;
    }
    check('225 non-SUCCESS raises the CLIENT exception, not the broker one', isClientEx);
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
