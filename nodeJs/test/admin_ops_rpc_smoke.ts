// #47 mqadmin ops RPC group — offline wire test (no sockets).
//
// Pins, per request family, the exact dial address (including which families
// translate to the VIP port), header extFields, the JSON body decoding of
// every response DTO (numeric-key maps, nested HA structures, the defaulted
// epoch endOffset), and the fixed 3000 ms budget of the controller-routed
// families:
//   * queryCorrectionOffset(308): topic/compareGroup/filterGroups header,
//     VIP two-state, numeric-key correctionOffsets map;
//   * the three unit-topic reads (311/312/313): null header to ONE name
//     server, TopicList body, %RETRY% filtered client-side when
//     containRetry is false — the raw answer untouched on the wire;
//   * getAllProducerInfo(328): empty header, VIP two-state, producer table;
//   * getBrokerHAStatus(907): plain address, nested runtime decode;
//   * resetMasterFlushOffset(908): the primitive long, 3000 ms budget;
//   * electMaster(1002): 1005 leader resolution first, ofAdminTrigger shape
//     (designateElect=true, brokerId or -1, invokeTime), response extFields
//     header + BrokerMemberGroup body;
//   * getBrokerEpochCache(1007): epoch list with the Long.MAX_VALUE default;
//   * switchTimerEngine(5001): the single E_T ext field.
//
// Run: node --experimental-strip-types test/admin_ops_rpc_smoke.ts
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { MQClient } from '../src/client/mq_client.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { MQBrokerException, MQClientException } from '../src/remoting/exception.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const NS = '127.0.0.1:9876';
const BROKER = '127.0.0.1:10911';
const BROKER_VIP = '127.0.0.1:10909';
const CONTROLLER = '127.0.0.1:12011';
const LEADER = '127.0.0.1:12012';

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
  callFor(code: number): Call | undefined {
    return this.calls.filter((c) => c.code === code)[0];
  }
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
function newAdmin(rc: RecordingClient, namesrv = NS, opts?: { vip?: boolean }):
  DefaultMQAdminExt {
  const admin = new DefaultMQAdminExt(null);
  admin.client = new MQClient(`127.0.0.1@ops_${process.pid}_${clientSeq++}`, namesrv, rc as any);
  (admin as any)._started = true;
  if (opts?.vip) admin.setVipChannelEnabled(true);
  return admin;
}

function successWith(req: RemotingCommand, apply: (r: RemotingCommand) => void): RemotingCommand {
  const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS)!;
  resp.opaque = req.opaque;
  apply(resp);
  return resp;
}

function failure(req: RemotingCommand, code = ResponseCode.SYSTEM_ERROR, remark = 'boom'): RemotingCommand {
  const resp = RemotingCommand.createResponseCommand(code)!;
  resp.opaque = req.opaque;
  resp.remark = remark;
  return resp;
}

function okBody(body: object, ext?: Record<string, string>): (req: RemotingCommand) => RemotingCommand {
  return (req) => successWith(req, (r) => {
    r.body = Buffer.from(JSON.stringify(body), 'utf8');
    if (ext != null) r.extFields = Object.assign({}, ext);
  });
}

async function main() {
  // ------------------------------------------------ 1. QUERY_CORRECTION_OFFSET(308)
  {
    const rc = new RecordingClient();
    rc.responders.set(RequestCode.QUERY_CORRECTION_OFFSET,
      okBody({ correctionOffsets: { '0': 110, '3': 407 } }));
    const admin = newAdmin(rc);
    const out = await admin.queryCorrectionOffset(BROKER, 'OpsTopic', 'GID_Ops',
      new Set(['GID_other', 'GID_x']));
    const call = rc.callFor(RequestCode.QUERY_CORRECTION_OFFSET)!;
    check('308 dials the broker address with the three header fields',
      call.code === RequestCode.QUERY_CORRECTION_OFFSET && call.addr === BROKER &&
      call.ext['topic'] === 'OpsTopic' && call.ext['compareGroup'] === 'GID_Ops',
      JSON.stringify(call));
    check('308 filterGroups is comma-joined and the body is empty',
      new Set(call.ext['filterGroups'].split(',')).size === 2 &&
      call.ext['filterGroups'].includes('GID_other') &&
      call.ext['filterGroups'].includes('GID_x') && call.body === null,
      JSON.stringify(call.ext));
    check('308 decodes the numeric-key correctionOffsets map',
      out[0] === 110 && out[3] === 407, JSON.stringify(out));
    check('308 timeout is the admin default (20000)', call.timeout === 20000, String(call.timeout));

    rc.reset();
    const adminVip = newAdmin(rc, NS, { vip: true });
    await adminVip.queryCorrectionOffset(BROKER, 'OpsTopic', 'GID_Ops');
    check('308 with the VIP switch on dials port - 2',
      rc.callFor(RequestCode.QUERY_CORRECTION_OFFSET)!.addr === BROKER_VIP,
      rc.callFor(RequestCode.QUERY_CORRECTION_OFFSET)!.addr);

    rc.reset();
    const rc2 = new RecordingClient();
    rc2.responders.set(RequestCode.QUERY_CORRECTION_OFFSET,
      okBody({ correctionOffsets: {} }));
    await newAdmin(rc2).queryCorrectionOffset(BROKER, 'OpsTopic', 'GID_Ops');
    const noFilter = rc2.callFor(RequestCode.QUERY_CORRECTION_OFFSET)!;
    check('308 without filter groups omits the field entirely',
      noFilter.ext['topic'] === 'OpsTopic' && noFilter.ext['compareGroup'] === 'GID_Ops' &&
      noFilter.ext['filterGroups'] === undefined, JSON.stringify(noFilter.ext));
  }

  // ------------------------------------------------ 2. unit-topic family (311/312/313)
  {
    const family: Array<[number, (a: DefaultMQAdminExt) => Promise<any>]> = [
      [RequestCode.GET_UNIT_TOPIC_LIST, (a) => a.getUnitTopicList(true)],
      [RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, (a) => a.getHasUnitSubTopicList(true)],
      [RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST, (a) => a.getHasUnitSubUnUnitTopicList(true)],
    ];
    for (const [code, invoke] of family) {
      const rc = new RecordingClient();
      rc.responders.set(code, okBody({ topicList: ['TopicA', '%RETRY%G'] }));
      const admin = newAdmin(rc);
      const out = await invoke(admin);
      const call = rc.callFor(code)!;
      check(`${code} hits ONE name server with a null header`,
        call.addr === NS && Object.keys(call.ext).length === 0 && call.body === null,
        JSON.stringify(call));
      check(`${code} decodes the TopicList body`,
        out.topicList.length === 2 && out.topicList[0] === 'TopicA' &&
        out.topicList[1] === '%RETRY%G' && out.brokerAddr === null,
        JSON.stringify(out.topicList));
    }

    const rc2 = new RecordingClient();
    rc2.responders.set(RequestCode.GET_UNIT_TOPIC_LIST,
      okBody({ topicList: ['TopicA', '%RETRY%G', '%RETRY%G_lite'] }));
    const admin2 = newAdmin(rc2);
    const filtered = await admin2.getUnitTopicList(false);
    check('311 with containRetry=false strips %RETRY% topics client-side',
      filtered.topicList.length === 1 && filtered.topicList[0] === 'TopicA',
      JSON.stringify(filtered.topicList));
    check('311 the raw answer is untouched on the wire (filter after decode)',
      Object.keys(rc2.callFor(RequestCode.GET_UNIT_TOPIC_LIST)!.ext).length === 0, '');

    const rc3 = new RecordingClient();
    rc3.responders.set(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, (req) => failure(req));
    let thrown = '';
    try {
      await newAdmin(rc3).getHasUnitSubTopicList();
    } catch (e) {
      thrown = e instanceof MQClientException ? (e as Error).message : `wrong type: ${(e as Error).message}`;
    }
    check('312 non-SUCCESS raises the client exception', thrown.includes('boom'), thrown);
  }

  // ------------------------------------------------ 3. GET_ALL_PRODUCER_INFO(328)
  {
    const rc = new RecordingClient();
    rc.responders.set(RequestCode.GET_ALL_PRODUCER_INFO, okBody({
      data: { G: [{ clientId: 'cid-1', remoteIP: '10.0.0.1', language: 'JAVA',
        version: 500, lastUpdateTimestamp: 1700000000000 }] },
    }));
    const admin = newAdmin(rc);
    const table = await admin.getAllProducerInfo(BROKER);
    const call = rc.callFor(RequestCode.GET_ALL_PRODUCER_INFO)!;
    check('328 dials the broker with an EMPTY header',
      call.addr === BROKER && Object.keys(call.ext).length === 0, JSON.stringify(call));
    check('328 decodes the producer table',
      table.data['G'] != null && table.data['G'][0].clientId === 'cid-1' &&
      table.data['G'][0].remoteIP === '10.0.0.1' && table.data['G'][0].version === 500 &&
      table.data['G'][0].lastUpdateTimestamp === 1700000000000,
      JSON.stringify(table.data));

    rc.reset();
    const adminVip = newAdmin(rc, NS, { vip: true });
    await adminVip.getAllProducerInfo(BROKER);
    check('328 with the VIP switch on dials port - 2',
      rc.callFor(RequestCode.GET_ALL_PRODUCER_INFO)!.addr === BROKER_VIP,
      rc.callFor(RequestCode.GET_ALL_PRODUCER_INFO)!.addr);
  }

  // ------------------------------------------------ 4. GET_BROKER_HA_STATUS(907)
  {
    const rc = new RecordingClient();
    rc.responders.set(RequestCode.GET_BROKER_HA_STATUS, okBody({
      master: true,
      masterCommitLogMaxOffset: 9001,
      inSyncSlaveNums: 2,
      haConnectionInfo: [{ addr: '10.0.0.2:10912', slaveAckOffset: 8900, diff: 101,
        inSync: true, transferredByteInSecond: 2048, transferFromWhere: 8700 }],
      haClientRuntimeInfo: { masterAddr: '10.0.0.1:10912', transferredByteInSecond: 12,
        maxOffset: 9001, lastReadTimestamp: 111, lastWriteTimestamp: 222,
        masterFlushOffset: 8800, activated: true },
    }));
    const admin = newAdmin(rc);
    const info = await admin.getBrokerHAStatus(BROKER);
    const call = rc.callFor(RequestCode.GET_BROKER_HA_STATUS)!;
    check('907 dials the PLAIN broker address with no header',
      call.addr === BROKER && Object.keys(call.ext).length === 0, JSON.stringify(call));
    check('907 decodes the nested HA runtime info',
      info.master === true && info.masterCommitLogMaxOffset === 9001 &&
      info.inSyncSlaveNums === 2, JSON.stringify(info));
    const conn = info.haConnectionInfo[0];
    check('907 decodes each HA connection entry',
      conn.addr === '10.0.0.2:10912' && conn.slaveAckOffset === 8900 && conn.diff === 101 &&
      conn.inSync === true && conn.transferredByteInSecond === 2048 &&
      conn.transferFromWhere === 8700, JSON.stringify(conn));
    const hc = info.haClientRuntimeInfo;
    check('907 decodes the HA client runtime (both activated spellings accepted)',
      hc.masterAddr === '10.0.0.1:10912' && hc.maxOffset === 9001 &&
      hc.masterFlushOffset === 8800 && hc.isActivated === true, JSON.stringify(hc));

    const rc2 = new RecordingClient();
    rc2.responders.set(RequestCode.GET_BROKER_HA_STATUS, (req) => failure(req));
    let thrown = '';
    try {
      await newAdmin(rc2).getBrokerHAStatus(BROKER);
    } catch (e) {
      thrown = e instanceof MQBrokerException ? (e as Error).message : `wrong type: ${(e as Error).message}`;
    }
    check('907 non-SUCCESS raises the broker exception', thrown.includes('boom'), thrown);
  }

  // ------------------------------------------------ 5. RESET_MASTER_FLUSH_OFFSET(908)
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc);
    await admin.resetMasterFlushOffset(BROKER, 12345);
    const call = rc.callFor(RequestCode.RESET_MASTER_FLUSH_OFFSET)!;
    check('908 sends the primitive long and nothing else, at the 3000 ms budget',
      call.code === RequestCode.RESET_MASTER_FLUSH_OFFSET && call.addr === BROKER &&
      JSON.stringify(call.ext) === JSON.stringify({ masterFlushOffset: '12345' }) &&
      call.body === null && call.timeout === 3000, JSON.stringify(call));
  }

  // ------------------------------------------------ 6. CONTROLLER_ELECT_MASTER(1002)
  {
    const rc = new RecordingClient();
    rc.responders.set(RequestCode.CONTROLLER_GET_METADATA_INFO, (req) =>
      successWith(req, (r) => { r.extFields = { controllerLeaderAddress: LEADER }; }));
    rc.responders.set(RequestCode.CONTROLLER_ELECT_MASTER, (req) =>
      successWith(req, (r) => {
        r.body = Buffer.from(JSON.stringify({
          cluster: 'DefaultCluster', brokerName: 'broker-a',
          brokerAddrs: { '0': '10.0.0.1:10911', '1': '10.0.0.2:10911' },
        }), 'utf8');
        r.extFields = { masterBrokerId: '1', masterAddress: '10.0.0.2:10911',
          masterEpoch: '3', syncStateSetEpoch: '7' };
      }));
    const admin = newAdmin(rc);
    const [header, group] = await admin.electMaster(CONTROLLER, 'DefaultCluster', 'broker-a', 1);
    const meta = rc.callFor(RequestCode.CONTROLLER_GET_METADATA_INFO)!;
    const elect = rc.callFor(RequestCode.CONTROLLER_ELECT_MASTER)!;
    check('1002 is two-phase: 1005 names the leader, 1002 goes to the leader',
      meta.addr === CONTROLLER && elect.addr === LEADER, `${meta.addr} -> ${elect.addr}`);
    check('1002 carries the ofAdminTrigger header shape',
      elect.ext['clusterName'] === 'DefaultCluster' && elect.ext['brokerName'] === 'broker-a' &&
      elect.ext['brokerId'] === '1' && elect.ext['designateElect'] === 'true' &&
      parseInt(elect.ext['invokeTime'], 10) > 0, JSON.stringify(elect.ext));
    check('1002 elect leg is pinned to the 3000 ms budget',
      elect.timeout === 3000, String(elect.timeout));
    check('1002 parses the response extFields header',
      header.masterBrokerId === 1 && header.masterAddress === '10.0.0.2:10911' &&
      header.masterEpoch === 3 && header.syncStateSetEpoch === 7,
      JSON.stringify(header));
    check('1002 decodes the numeric-key BrokerMemberGroup body',
      group.cluster === 'DefaultCluster' && group.brokerName === 'broker-a' &&
      group.brokerAddrs[0] === '10.0.0.1:10911' && group.brokerAddrs[1] === '10.0.0.2:10911',
      JSON.stringify(group.brokerAddrs));

    rc.reset();
    const rc2 = new RecordingClient();
    rc2.responders.set(RequestCode.CONTROLLER_GET_METADATA_INFO, (req) =>
      successWith(req, (r) => { r.extFields = { controllerLeaderAddress: LEADER }; }));
    await newAdmin(rc2).electMaster(CONTROLLER, 'DefaultCluster', 'broker-a');
    check('1002 without a designated id sends brokerId = -1',
      rc2.callFor(RequestCode.CONTROLLER_ELECT_MASTER)!.ext['brokerId'] === '-1',
      JSON.stringify(rc2.callFor(RequestCode.CONTROLLER_ELECT_MASTER)!.ext));

    const rc3 = new RecordingClient();
    rc3.responders.set(RequestCode.CONTROLLER_GET_METADATA_INFO, (req) =>
      successWith(req, (r) => { r.extFields = { controllerLeaderAddress: LEADER }; }));
    rc3.responders.set(RequestCode.CONTROLLER_ELECT_MASTER, (req) => failure(req));
    let thrown = '';
    try {
      await newAdmin(rc3).electMaster(CONTROLLER, 'DefaultCluster', 'broker-a');
    } catch (e) {
      thrown = e instanceof MQBrokerException ? (e as Error).message : `wrong type: ${(e as Error).message}`;
    }
    check('1002 non-SUCCESS raises the broker exception', thrown.includes('boom'), thrown);
  }

  // ------------------------------------------------ 7. GET_BROKER_EPOCH_CACHE(1007)
  {
    const rc = new RecordingClient();
    rc.responders.set(RequestCode.GET_BROKER_EPOCH_CACHE, okBody({
      clusterName: 'DefaultCluster', brokerName: 'broker-a', brokerId: 1,
      epochList: [{ epoch: 1, startOffset: 0, endOffset: 500 }, { epoch: 2, startOffset: 500 }],
      maxOffset: 900,
    }));
    const admin = newAdmin(rc);
    const cache = await admin.getBrokerEpochCache(BROKER);
    const call = rc.callFor(RequestCode.GET_BROKER_EPOCH_CACHE)!;
    check('1007 dials the plain broker address with no header, 3000 ms budget',
      call.addr === BROKER && Object.keys(call.ext).length === 0 && call.timeout === 3000,
      JSON.stringify(call));
    check('1007 decodes the epoch cache',
      cache.clusterName === 'DefaultCluster' && cache.brokerName === 'broker-a' &&
      cache.brokerId === 1 && cache.maxOffset === 900, JSON.stringify(cache));
    check('1007 a missing endOffset defaults to Long.MAX_VALUE',
      cache.epochList[0].endOffset === 500 &&
      cache.epochList[1].endOffset === 9223372036854775807,
      JSON.stringify(cache.epochList));
  }

  // ------------------------------------------------ 8. SWITCH_TIMER_ENGINE(5001)
  {
    const rc = new RecordingClient();
    const admin = newAdmin(rc);
    await admin.switchTimerEngine(BROKER, 'rocksdb', 4500);
    const call = rc.callFor(RequestCode.SWITCH_TIMER_ENGINE)!;
    check('5001 sends the single E_T ext field, timeout forwarded verbatim',
      call.code === RequestCode.SWITCH_TIMER_ENGINE && call.addr === BROKER &&
      JSON.stringify(call.ext) === JSON.stringify({ E_T: 'rocksdb' }) &&
      call.body === null && call.timeout === 4500, JSON.stringify(call));
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
