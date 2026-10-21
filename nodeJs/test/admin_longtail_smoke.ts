// Admin long-tail RPCs — offline wire test (no sockets).
//
// Pins, per request family, the exact dial address (including which families
// ignore the VIP switch), header extFields (key names, omitted-vs-empty
// nullable strings, always-sent primitives), body bytes, and the timeout each
// family dials with:
//   * broker container add/remove (ADD_BROKER / REMOVE_BROKER): verbatim
//     address, fixed 20000 ms, no body;
//   * ordered-topic config store: broadcast PUT to every name server, the
//     ';'-joined read-modify-write merge, the LAST failing PUT answer rethrown;
//   * controller reads/writes: leader resolution first, raw address, the
//     fixed 3000 ms budget on the leader calls, the properties-text config
//     bodies, the always-sent invokeTime/isCleanLivingBroker primitives;
//   * cold-data flow control / commitlog read-ahead / pop-record export /
//     rocksdb progress+export: their per-request VIP, body and remark rules.
//
// Run: node --experimental-strip-types test/admin_longtail_smoke.ts
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { MQClient } from '../src/client/mq_client.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { MQBrokerException, MQClientException } from '../src/remoting/exception.ts';
import {
  CheckRocksdbCqWriteStatus, GetMetaDataResponseHeader,
} from '../src/remoting/admin_body.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const NS1 = '127.0.0.1:9876';
const NS2 = '127.0.0.1:9877';
const BROKER = '127.0.0.1:10911';
const BROKER_VIP = '127.0.0.1:10909';

interface Call {
  code: number; addr: string; ext: Record<string, string>;
  body: Buffer | null; timeout: number;
}

// Records every dial: address, extFields, body bytes and the timeout handed
// over. Per-code responders can override the SUCCESS default.
class RecordingClient {
  calls: Call[] = [];
  responders = new Map<number, (req: RemotingCommand, n: number) => RemotingCommand>();
  seen = new Map<number, number>();
  reset() { this.calls = []; this.seen.clear(); }
  nthFor(code: number, n: number): Call | undefined {
    const hits = this.calls.filter((c) => c.code === code);
    return hits[n];
  }
  callFor(code: number): Call | undefined { return this.nthFor(code, 0); }
  respond(req: RemotingCommand): RemotingCommand {
    const n = (this.seen.get(req.code) ?? 0);
    this.seen.set(req.code, n + 1);
    const r = this.responders.get(req.code);
    if (r != null) return r(req, n);
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS)!;
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
function newAdmin(rc: RecordingClient, namesrv = NS1, opts?: { vip?: boolean; timeout?: number }):
  DefaultMQAdminExt {
  const admin = new DefaultMQAdminExt(null, opts?.timeout);
  admin.client = new MQClient(`127.0.0.1@longtail_${process.pid}_${clientSeq++}`, namesrv, rc as any);
  (admin as any)._started = true;
  if (opts?.vip) admin.setVipChannelEnabled(true);
  return admin;
}

function failure(req: RemotingCommand, code: number, remark: string): RemotingCommand {
  const resp = RemotingCommand.createResponseCommand(code)!;
  resp.opaque = req.opaque;
  resp.remark = remark;
  return resp;
}

function successWith(req: RemotingCommand, apply: (r: RemotingCommand) => void): RemotingCommand {
  const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS)!;
  resp.opaque = req.opaque;
  apply(resp);
  return resp;
}

async function main() {
  // ------------------------------------------------ 1. broker container add/remove
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc, NS1, { vip: true, timeout: 5555 });
    await admin.addBrokerToContainer(BROKER, '/rm/broker.conf');
    const add = rc.callFor(RequestCode.ADD_BROKER)!;
    check('ADD_BROKER(902) dials the container address verbatim, never VIP',
      add.addr === BROKER, add.addr);
    check('ADD_BROKER(902) header is exactly configPath, no body',
      JSON.stringify(add.ext) === JSON.stringify({ configPath: '/rm/broker.conf' }) && add.body === null,
      JSON.stringify(add.ext));
    check('ADD_BROKER(902) timeout is fixed 20000 despite the admin-wide 5555',
      add.timeout === 20000, String(add.timeout));
    await admin.removeBrokerFromContainer(BROKER, 'RaftCluster', 'broker-a', 3);
    const rem = rc.callFor(RequestCode.REMOVE_BROKER)!;
    check('REMOVE_BROKER(903) header is brokerName/brokerClusterName/brokerId',
      rem.code === RequestCode.REMOVE_BROKER &&
      JSON.stringify(rem.ext) === JSON.stringify({
        brokerName: 'broker-a', brokerClusterName: 'RaftCluster', brokerId: '3',
      }), JSON.stringify(rem.ext));
    check('REMOVE_BROKER(903) dials verbatim at the fixed 20000 ms',
      rem.addr === BROKER && rem.timeout === 20000, `${rem.addr} ${rem.timeout}`);
  }

  // ---------------------------------------------------- 2. ordered-topic config
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc, `${NS1};${NS2}`);
    await admin.createOrUpdateOrderConf('TopicOrder', 'b1:8', true);
    const puts = rc.calls.filter((c) => c.code === RequestCode.PUT_KV_CONFIG);
    check('isCluster PUTs the value verbatim to EVERY name server',
      puts.length === 2 && puts.every((p) => p.addr === NS1 || p.addr === NS2) &&
      JSON.stringify(puts[0].ext) === JSON.stringify({
        namespace: 'ORDER_TOPIC_CONFIG', key: 'TopicOrder', value: 'b1:8',
      }), JSON.stringify(rc.calls.map((c) => [c.code, c.addr])));
  }
  {
    // Merge: existing segments keep their order, the new key is appended.
    const rc = new RecordingClient();
    rc.responders.set(RequestCode.GET_KV_CONFIG, (req) =>
      successWith(req, (r) => r.addExtField('value', 'b1:8;b2:4')));
    const admin = newAdmin(rc, `${NS1};${NS2}`);
    await admin.createOrUpdateOrderConf('TopicOrder', 'b3:2', false);
    const get = rc.callFor(RequestCode.GET_KV_CONFIG)!;
    check('merge reads the stored value from one name server (header, no body)',
      get != null && get.ext['namespace'] === 'ORDER_TOPIC_CONFIG' &&
      get.ext['key'] === 'TopicOrder' && get.body === null, JSON.stringify(get?.ext));
    const put = rc.callFor(RequestCode.PUT_KV_CONFIG)!;
    check('a new key is appended to the stored order conf',
      put.ext['value'] === 'b1:8;b2:4;b3:2', put.ext['value']);
    check('the merged PUT is broadcast to both name servers',
      rc.calls.filter((c) => c.code === RequestCode.PUT_KV_CONFIG).length === 2);
  }
  {
    // Replace: same key keeps its position with the new value.
    const rc = new RecordingClient();
    rc.responders.set(RequestCode.GET_KV_CONFIG, (req) =>
      successWith(req, (r) => r.addExtField('value', 'b1:8;b2:4')));
    const admin = newAdmin(rc, NS1);
    await admin.createOrUpdateOrderConf('TopicOrder', 'b2:9', false);
    const put = rc.callFor(RequestCode.PUT_KV_CONFIG)!;
    check('a replaced key keeps its position with the new value',
      put.ext['value'] === 'b1:8;b2:9', put.ext['value']);
  }
  {
    // A failed read is ignored: the merge starts from the new value alone.
    const rc = new RecordingClient();
    rc.responders.set(RequestCode.GET_KV_CONFIG, (req) => failure(req, ResponseCode.SYSTEM_ERROR, 'no kv'));
    const admin = newAdmin(rc, NS1);
    await admin.createOrUpdateOrderConf('TopicOrder', 'b7:1', false);
    const put = rc.callFor(RequestCode.PUT_KV_CONFIG)!;
    check('a failing read does not block the store', put.ext['value'] === 'b7:1', put.ext['value']);
  }
  {
    // The LAST failing broadcast answer is the one rethrown.
    const rc = new RecordingClient();
    rc.responders.set(RequestCode.PUT_KV_CONFIG, (req, n) =>
      failure(req, ResponseCode.SYSTEM_ERROR, n === 0 ? 'first-failure' : 'last-failure'));
    const admin = newAdmin(rc, `${NS1};${NS2}`);
    let msg = '';
    try {
      await admin.createOrUpdateOrderConf('TopicOrder', 'b1:8', true);
    } catch (e) {
      msg = e instanceof MQBrokerException ? (e as Error).message : `wrong type: ${(e as Error).message}`;
    }
    check('the LAST failing PUT answer is rethrown',
      msg.includes('last-failure') && !msg.includes('first-failure'), msg);
  }

  // -------------------------------------------------------------- 3. controller
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc, NS1);
    rc.responders.set(RequestCode.CONTROLLER_GET_METADATA_INFO, (req) =>
      successWith(req, (r) => {
        r.addExtField('group', 'G1');
        r.addExtField('controllerLeaderId', 'c2');
        r.addExtField('controllerLeaderAddress', '127.0.0.1:19877');
        r.addExtField('isLeader', 'true');
        r.addExtField('peers', 'c1@127.0.0.1:19876;c2@127.0.0.1:19877');
      }));
    const meta = await admin.getControllerMetaData('127.0.0.1:19876');
    const call = rc.callFor(RequestCode.CONTROLLER_GET_METADATA_INFO)!;
    check('1005 dials the controller verbatim with no header and the 3000 ms budget',
      call.addr === '127.0.0.1:19876' && Object.keys(call.ext).length === 0 &&
      call.body === null && call.timeout === 3000, JSON.stringify(call));
    check('1005 extFields decode into the metadata header',
      meta instanceof GetMetaDataResponseHeader &&
      meta.group === 'G1' && meta.controllerLeaderId === 'c2' &&
      meta.controllerLeaderAddress === '127.0.0.1:19877' && meta.isLeader && meta.peers?.includes('c1@'),
      JSON.stringify(meta));

    rc.reset();
    rc.responders.set(RequestCode.CONTROLLER_GET_SYNC_STATE_DATA, (req) =>
      successWith(req, (r) => {
        r.body = Buffer.from(JSON.stringify({
          replicasInfoTable: {
            'broker-a': {
              masterBrokerId: '0', masterAddress: '127.0.0.1:10911', masterEpoch: 3,
              syncStateSetEpoch: 5,
              inSyncReplicas: [{ brokerName: 'broker-a', brokerId: 0, brokerAddress: '127.0.0.1:10911', alive: true }],
              notInSyncReplicas: [],
            },
          },
        }), 'utf8');
      }));
    const replicas = await admin.getInSyncStateData('127.0.0.1:19876', ['broker-a', 'broker-b']);
    const metaCall = rc.nthFor(RequestCode.CONTROLLER_GET_METADATA_INFO, 0)!;
    const syncCall = rc.nthFor(RequestCode.CONTROLLER_GET_SYNC_STATE_DATA, 0)!;
    check('1006 is two-phase: metadata first, then the LEADER address',
      metaCall.addr === '127.0.0.1:19876' && syncCall.addr === '127.0.0.1:19877',
      `${metaCall.addr} -> ${syncCall.addr}`);
    check('1006 body is the raw broker-name JSON array, no header',
      syncCall.ext == null || Object.keys(syncCall.ext).length === 0,
      JSON.stringify(syncCall.ext));
    check('1006 body bytes are exactly the JSON array of broker names',
      syncCall.body != null && syncCall.body.toString('utf8') === JSON.stringify(['broker-a', 'broker-b']),
      syncCall.body?.toString('utf8') ?? 'null');
    check('1006 answer decodes the replicas table',
      replicas.replicasInfoTable['broker-a'] != null &&
      replicas.replicasInfoTable['broker-a'].masterEpoch === 3 &&
      replicas.replicasInfoTable['broker-a'].inSyncReplicas?.[0]?.alive === true,
      JSON.stringify(replicas.replicasInfoTable));
  }
  {
    // Clean: nullable strings omitted when unset, primitives ALWAYS sent.
    const rc = new RecordingClient();
    const admin = newAdmin(rc, NS1);
    rc.responders.set(RequestCode.CONTROLLER_GET_METADATA_INFO, (req) =>
      successWith(req, (r) => r.addExtField('controllerLeaderAddress', '127.0.0.1:19877')));
    rc.responders.set(RequestCode.CLEAN_BROKER_DATA, () =>
      RemotingCommand.createResponseCommand(ResponseCode.SUCCESS)!);
    await admin.cleanControllerBrokerData('127.0.0.1:19876', null, 'broker-a', null, false);
    const minimal = rc.nthFor(RequestCode.CLEAN_BROKER_DATA, 0)!;
    check('1011 minimal header omits the nullable strings',
      minimal.addr === '127.0.0.1:19877' && minimal.ext['brokerName'] === 'broker-a' &&
      minimal.ext['clusterName'] === undefined && minimal.ext['brokerControllerIdsToClean'] === undefined,
      JSON.stringify(minimal.ext));
    check('1011 isCleanLivingBroker=false and invokeTime are ALWAYS sent',
      minimal.ext['isCleanLivingBroker'] === 'false' &&
      /^\d+$/.test(minimal.ext['invokeTime'] ?? ''), JSON.stringify(minimal.ext));
    check('1011 clean call rides the 3000 ms budget', minimal.timeout === 3000, String(minimal.timeout));

    rc.reset();
    await admin.cleanControllerBrokerData('127.0.0.1:19876', 'RaftCluster', 'broker-a', 'c1;c2', true);
    const full = rc.nthFor(RequestCode.CLEAN_BROKER_DATA, 0)!;
    check('1011 full header keeps field order and renders the primitives',
      JSON.stringify(Object.keys(full.ext)) ===
        JSON.stringify(['clusterName', 'brokerName', 'brokerControllerIdsToClean', 'isCleanLivingBroker', 'invokeTime']) &&
      full.ext['isCleanLivingBroker'] === 'true', JSON.stringify(full.ext));
  }
  {
    // GET_CONTROLLER_CONFIG: explicit list, properties TEXT body, admin budget.
    const rc = new RecordingClient();
    const admin = newAdmin(rc, NS1, { timeout: 5555 });
    rc.responders.set(RequestCode.GET_CONTROLLER_CONFIG, (req) =>
      successWith(req, (r) => { r.body = Buffer.from('listenPort=9878\ndlcPath=/rm/controller\n', 'utf8'); }));
    const cfg = await admin.getControllerConfig(['127.0.0.1:19876']);
    const call = rc.callFor(RequestCode.GET_CONTROLLER_CONFIG)!;
    check('1010 parses the properties-text body per controller',
      cfg != null && cfg['127.0.0.1:19876']?.['listenPort'] === '9878' &&
      cfg['127.0.0.1:19876']?.['dlcPath'] === '/rm/controller', JSON.stringify(cfg));
    check('1010 keeps the admin-wide timeout (5555), not the leader budget',
      call.timeout === 5555, String(call.timeout));

    rc.reset();
    // No explicit list: the name servers take the controller role.
    await admin.getControllerConfig(null);
    check('1010 without a list sweeps the name servers',
      rc.callFor(RequestCode.GET_CONTROLLER_CONFIG)?.addr === NS1, rc.calls.map((c) => c.addr).join(','));
  }
  {
    // No addresses at all: null, nothing dialled.
    const rc = new RecordingClient();
    const admin = newAdmin(rc, '');
    const cfg = await admin.getControllerConfig(null);
    check('1010 with no reachable controller address returns null and dials nothing',
      cfg === null && rc.calls.length === 0, JSON.stringify(cfg));
  }
  {
    // UPDATE_CONTROLLER_CONFIG: empty props / empty list = silent no-op.
    const rc = new RecordingClient();
    const admin = newAdmin(rc, NS1);
    await admin.updateControllerConfig({}, ['127.0.0.1:19876']);
    await admin.updateControllerConfig({ a: 'b' }, []);
    check('1009 empty properties or an empty controller list send nothing',
      rc.calls.length === 0, JSON.stringify(rc.calls));

    rc.reset();
    rc.responders.set(RequestCode.UPDATE_CONTROLLER_CONFIG, (req, n) =>
      failure(req, ResponseCode.SYSTEM_ERROR, n === 0 ? 'first-failure' : 'last-failure'));
    let msg = '';
    let code = 0;
    try {
      await admin.updateControllerConfig({ a: 'b' }, ['127.0.0.1:19876', '127.0.0.1:19877']);
    } catch (e) {
      msg = (e as Error).message;
      code = (e as any).responseCode ?? -1;
    }
    const bodies = rc.calls.filter((c) => c.code === RequestCode.UPDATE_CONTROLLER_CONFIG);
    check('1009 body is the properties TEXT and both controllers get it',
      bodies.length === 2 && bodies[0].body?.toString('utf8') === 'a=b\n', JSON.stringify(bodies));
    check('1009 keeps sweeping after a failure and rethrows the LAST one',
      msg.includes('last-failure') && code === ResponseCode.SYSTEM_ERROR, `${code} ${msg}`);
  }

  // --------------------------------------------- 4. cold-data flow control
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc, NS1, { vip: true });
    await admin.updateColdDataFlowCtrGroupConfig(BROKER, { consumedFlowCtlThreshold: '100' });
    const up = rc.callFor(RequestCode.UPDATE_COLD_DATA_FLOW_CTR_CONFIG)!;
    check('2001 rides the fast channel with a properties-text body',
      up.addr === BROKER_VIP && up.body?.toString('utf8') === 'consumedFlowCtlThreshold=100\n' &&
      Object.keys(up.ext).length === 0, `${up.addr} ${up.body?.toString('utf8')}`);
    rc.reset();
    await admin.updateColdDataFlowCtrGroupConfig(BROKER, {});
    check('2001 with no properties sends nothing', rc.calls.length === 0, JSON.stringify(rc.calls));

    rc.reset();
    await admin.removeColdDataFlowCtrGroupConfig(BROKER, 'GID_cold');
    const rm = rc.callFor(RequestCode.REMOVE_COLD_DATA_FLOW_CTR_CONFIG)!;
    check('2002 body is the raw group name, NOT JSON, on the fast channel',
      rm.addr === BROKER_VIP && rm.body?.toString('utf8') === 'GID_cold',
      `${rm.addr} ${rm.body?.toString('utf8')}`);
    rc.reset();
    await admin.removeColdDataFlowCtrGroupConfig(BROKER, '');
    check('2002 with an empty group sends nothing', rc.calls.length === 0, JSON.stringify(rc.calls));

    rc.reset();
    rc.responders.set(RequestCode.GET_COLD_DATA_FLOW_CTR_INFO, (req) =>
      successWith(req, (r) => { r.body = Buffer.from('coldReadThreshold=100', 'utf8'); }));
    const info = await admin.getColdDataFlowCtrInfo(BROKER);
    const get = rc.callFor(RequestCode.GET_COLD_DATA_FLOW_CTR_INFO)!;
    check('2003 reads the raw body text at the LISTENING port even with VIP on',
      info === 'coldReadThreshold=100' && get.addr === BROKER, `${info} @ ${get.addr}`);
    rc.reset();
    rc.responders.set(RequestCode.GET_COLD_DATA_FLOW_CTR_INFO, (req) =>
      RemotingCommand.createResponseCommand(ResponseCode.SUCCESS)!);
    const none = await admin.getColdDataFlowCtrInfo(BROKER);
    check('2003 with an empty body answers null', none === null, String(none));
  }

  // ------------------------------------------------- 5. commitlog read-ahead
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc, NS1, { vip: true });
    rc.responders.set(RequestCode.SET_COMMITLOG_READ_MODE, (req) =>
      successWith(req, (r) => { r.remark = 'changed to NORMAL'; }));
    const remark = await admin.setCommitLogReadAheadMode(BROKER, 'NORMAL');
    const call = rc.callFor(RequestCode.SET_COMMITLOG_READ_MODE)!;
    check('2004 header is the single READ_AHEAD_MODE key at the listening port',
      call.addr === BROKER &&
      JSON.stringify(call.ext) === JSON.stringify({ READ_AHEAD_MODE: 'NORMAL' }) && call.body === null,
      `${call.addr} ${JSON.stringify(call.ext)}`);
    check('2004 result is the response remark, not a body',
      remark === 'changed to NORMAL', String(remark));
    rc.reset();
    rc.responders.set(RequestCode.SET_COMMITLOG_READ_MODE, (req) =>
      RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null)!);
    const silent = await admin.setCommitLogReadAheadMode(BROKER, 'NORMAL');
    check('2004 without a remark answers null', silent === null, String(silent));
  }

  // ------------------------------------------------------ 6. pop record export
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc, NS1, { vip: true });
    await admin.exportPopRecords(BROKER, 12345);
    const call = rc.callFor(RequestCode.POP_ROLLBACK)!;
    check('200056 is a bare request at the listening port with no header',
      call.addr === BROKER && Object.keys(call.ext).length === 0 && call.body === null,
      `${call.addr} ${JSON.stringify(call.ext)}`);
    check('200056 forwards the CALLER timeout verbatim (12345)',
      call.timeout === 12345, String(call.timeout));
    rc.responders.set(RequestCode.POP_ROLLBACK, (req) => failure(req, ResponseCode.SYSTEM_ERROR, 'busy'));
    let thrown = '';
    try {
      await admin.exportPopRecords(BROKER, 12345);
    } catch (e) {
      thrown = e instanceof MQBrokerException ? (e as Error).message : `wrong type: ${(e as Error).message}`;
    }
    check('200056 non-SUCCESS raises the broker exception', thrown.includes('busy'), thrown);
  }

  // ------------------------------------------------ 7. rocksdb progress + export
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc, NS1, { vip: true });
    rc.responders.set(RequestCode.CHECK_ROCKSDB_CQ_WRITE_PROGRESS, (req) =>
      successWith(req, (r) => {
        r.body = Buffer.from(JSON.stringify({ checkResult: 'lag=3', checkStatus: 1 }), 'utf8');
      }));
    const result = await admin.checkRocksdbCqWriteProgress(BROKER, 'RdbTopic', 1234567);
    const call = rc.callFor(RequestCode.CHECK_ROCKSDB_CQ_WRITE_PROGRESS)!;
    check('354 header carries topic and checkStoreTime at the listening port',
      call.addr === BROKER &&
      JSON.stringify(call.ext) === JSON.stringify({ topic: 'RdbTopic', checkStoreTime: '1234567' }),
      `${call.addr} ${JSON.stringify(call.ext)}`);
    check('354 answer decodes into the progress result',
      result.checkResult === 'lag=3' && result.checkStatus === CheckRocksdbCqWriteStatus.CHECK_NOT_OK,
      JSON.stringify(result));
    rc.reset();
    await admin.checkRocksdbCqWriteProgress(BROKER, 'RdbTopic', 0);
    const zero = rc.callFor(RequestCode.CHECK_ROCKSDB_CQ_WRITE_PROGRESS)!;
    check('354 checkStoreTime=0 (primitive) is STILL sent',
      zero.ext['checkStoreTime'] === '0', JSON.stringify(zero.ext));
    rc.responders.set(RequestCode.CHECK_ROCKSDB_CQ_WRITE_PROGRESS, (req) =>
      failure(req, ResponseCode.SYSTEM_ERROR, 'not rocksdb'));
    let thrown = '';
    try {
      await admin.checkRocksdbCqWriteProgress(BROKER, 'RdbTopic', 1);
    } catch (e) {
      thrown = e instanceof MQBrokerException ? (e as Error).message : `wrong type: ${(e as Error).message}`;
    }
    check('354 non-SUCCESS raises the broker exception', thrown.includes('not rocksdb'), thrown);

    rc.reset();
    await admin.exportRocksDBConfigToJson(BROKER, ['Topics', 'SubscriptionGroups', 'ConsumerOffsets']);
    const exp = rc.callFor(RequestCode.EXPORT_ROCKSDB_CONFIG_TO_JSON)!;
    check('355 header is the lowercase name list, each closed with the separator',
      exp.addr === BROKER && exp.ext['configType'] === 'topics;subscriptiongroups;consumeroffsets;',
      `${exp.addr} ${JSON.stringify(exp.ext)}`);
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
