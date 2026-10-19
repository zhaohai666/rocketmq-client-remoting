// VIP-channel address-rewrite contract — offline wire test (no sockets).
//
// Contract being pinned (classic remoting, broker-side VIP listener):
//   * When the VIP switch is ON, a broker-facing request is dialled at the
//     route address's `port - 2`; when OFF (the default) the address is used
//     verbatim.
//   * The rewrite is a BROKER-only contract. Every name-server request MUST
//     target the plain name-server address; the VIP port never exists there, so
//     no name-server address may reach the broker translation.
//
// The switch is threaded exactly like the existing heartbeat-V2 knob: the
// producer's send switch and the admin switch drive the shared instance, and
// each request applies the translation at its own invoke boundary. Nothing here
// opens a connection; a recording client captures the address each request was
// dialled at, so the assertions are deterministic and offline.
//
// Run: node --experimental-strip-types test/vip_channel_smoke.ts
import { MQClient } from '../src/client/mq_client.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { MessageQueue } from '../src/common/message.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const BROKER = '127.0.0.1:10911';
const BROKER_VIP = '127.0.0.1:10909';
const NAMESRV = '127.0.0.1:9876';

// A remoting client that records every dial address per request code and answers
// SUCCESS. Response bodies/extFields are the bare minimum each exercised method
// parses; the test asserts only the resolved address.
class RecordingClient {
  calls: { code: number; addr: string }[] = [];
  tlsEnable: boolean | null = null;
  tlsOptions: any = null;
  socksProxyConfig: any = null;
  reset() { this.calls = []; }
  addrFor(code: number): string | undefined {
    for (let i = this.calls.length - 1; i >= 0; i--) if (this.calls[i].code === code) return this.calls[i].addr;
    return undefined;
  }
  respond(req: RemotingCommand): RemotingCommand {
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS)!;
    resp.opaque = req.opaque;
    switch (req.code) {
      case RequestCode.QUERY_CONSUMER_OFFSET: resp.addExtField('offset', '7'); break;
      case RequestCode.GET_MAX_OFFSET:
      case RequestCode.GET_MIN_OFFSET:
      case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP: resp.addExtField('offset', '5'); break;
      case RequestCode.GET_EARLIEST_MSG_STORETIME: resp.addExtField('timestamp', '123'); break;
      case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
        resp.body = Buffer.from(JSON.stringify({ consumerIdList: [] }), 'utf8'); break;
      case RequestCode.GET_BROKER_CONFIG:
        // The broker answers its own configuration as properties TEXT, not JSON.
        resp.body = Buffer.from('brokerName=broker-a\n', 'utf8'); break;
      case RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG:
        resp.body = Buffer.from(JSON.stringify({
          subscriptionGroupTable: {}, forbiddenTable: {}, dataVersion: {},
        }), 'utf8'); break;
      case RequestCode.LOCK_BATCH_MQ:
        resp.body = Buffer.from(JSON.stringify({ lockOKMQSet: [] }), 'utf8'); break;
      default: break;
    }
    return resp;
  }
  invokeSync(addr: string, request: RemotingCommand, _timeout?: number): Promise<RemotingCommand> {
    this.calls.push({ code: request.code, addr });
    return Promise.resolve(this.respond(request));
  }
  invokeOneway(addr: string, request: RemotingCommand): Promise<void> {
    this.calls.push({ code: request.code, addr });
    return Promise.resolve();
  }
  invokeAsync(addr: string, request: RemotingCommand,
    cb: (r: RemotingCommand | null, e: Error | null) => void, _timeout?: number): void {
    this.calls.push({ code: request.code, addr });
    cb(this.respond(request), null);
  }
}

function newClient(rc: RecordingClient): MQClient {
  return new MQClient(`127.0.0.1@vip_${Math.random().toString(16).slice(2)}`, NAMESRV, rc as any);
}

async function main() {
  const mq = new MessageQueue('VipTopic', 'broker-a', 0);

  // ------------------------------------------------ family A: shifted when ON
  {
    const rc = new RecordingClient();
    const client = newClient(rc);
    client.brokerAddrTable.set('broker-a', { 0: BROKER });
    client.setVipChannelEnabled(true);
    rc.reset();
    await client.queryConsumerOffset(BROKER, 'G', mq);
    check('QUERY_CONSUMER_OFFSET shifted when on', rc.addrFor(RequestCode.QUERY_CONSUMER_OFFSET) === BROKER_VIP,
      String(rc.addrFor(RequestCode.QUERY_CONSUMER_OFFSET)));
    await client.updateConsumerOffset(BROKER, 'G', mq, 9);
    check('UPDATE_CONSUMER_OFFSET shifted when on', rc.addrFor(RequestCode.UPDATE_CONSUMER_OFFSET) === BROKER_VIP);
    await client.getMaxOffset(BROKER, 'VipTopic', 0);
    check('GET_MAX_OFFSET shifted when on', rc.addrFor(RequestCode.GET_MAX_OFFSET) === BROKER_VIP);
    await client.getMinOffset(BROKER, 'VipTopic', 0);
    check('GET_MIN_OFFSET shifted when on', rc.addrFor(RequestCode.GET_MIN_OFFSET) === BROKER_VIP);
    await client.searchOffsetByTimestamp(BROKER, 'VipTopic', 0, 1);
    check('SEARCH_OFFSET_BY_TIMESTAMP shifted when on', rc.addrFor(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP) === BROKER_VIP);
    await client.getEarliestMsgStoretime(BROKER, 'VipTopic', 0);
    check('GET_EARLIEST_MSG_STORETIME shifted when on', rc.addrFor(RequestCode.GET_EARLIEST_MSG_STORETIME) === BROKER_VIP);
    await client.getConsumerListByGroup(BROKER, 'G');
    check('GET_CONSUMER_LIST_BY_GROUP shifted when on', rc.addrFor(RequestCode.GET_CONSUMER_LIST_BY_GROUP) === BROKER_VIP);
    await client.checkClientConfig(BROKER, 'G', { expressionType: 'SQL92', topic: 'VipTopic' }, 3000);
    check('CHECK_CLIENT_CONFIG shifted when on', rc.addrFor(RequestCode.CHECK_CLIENT_CONFIG) === BROKER_VIP);

    // LOCK / UNLOCK issued on the consumer facade share the same instance switch.
    const consumer = new DefaultMQPushConsumer('GID_vip');
    (consumer as any).mqClient = client;
    (consumer as any).clientID = 'cid';
    await (consumer as any)._lockBatchMQ([mq]);
    check('LOCK_BATCH_MQ shifted when on', rc.addrFor(RequestCode.LOCK_BATCH_MQ) === BROKER_VIP);
    await (consumer as any)._unlockBatchMQ([mq]);
    check('UNLOCK_BATCH_MQ shifted when on', rc.addrFor(RequestCode.UNLOCK_BATCH_MQ) === BROKER_VIP);

    // ------------------------------------------------ family A: unchanged when OFF
    client.setVipChannelEnabled(false);
    rc.reset();
    await client.queryConsumerOffset(BROKER, 'G', mq);
    check('QUERY_CONSUMER_OFFSET unchanged when off', rc.addrFor(RequestCode.QUERY_CONSUMER_OFFSET) === BROKER);
    await client.updateConsumerOffset(BROKER, 'G', mq, 9);
    check('UPDATE_CONSUMER_OFFSET unchanged when off', rc.addrFor(RequestCode.UPDATE_CONSUMER_OFFSET) === BROKER);
    await client.getConsumerListByGroup(BROKER, 'G');
    check('GET_CONSUMER_LIST_BY_GROUP unchanged when off', rc.addrFor(RequestCode.GET_CONSUMER_LIST_BY_GROUP) === BROKER);
    await (consumer as any)._lockBatchMQ([mq]);
    check('LOCK_BATCH_MQ unchanged when off', rc.addrFor(RequestCode.LOCK_BATCH_MQ) === BROKER);
    await (consumer as any)._unlockBatchMQ([mq]);
    check('UNLOCK_BATCH_MQ unchanged when off', rc.addrFor(RequestCode.UNLOCK_BATCH_MQ) === BROKER);
  }

  // ------------------------------- family B: heartbeat / pull-ish / pop / ack /
  //                       send / oneway / unregister NEVER shifted, even when ON
  {
    const rc = new RecordingClient();
    const client = newClient(rc);
    client.brokerAddrTable.set('broker-a', { 0: BROKER });
    client.setVipChannelEnabled(true); // switch ON: these must STILL ignore it
    rc.reset();
    await client.sendHeartbeat(BROKER);
    check('HEART_BEAT never shifted', rc.addrFor(RequestCode.HEART_BEAT) === BROKER);
    await client.unregisterClient(BROKER, 'P', 'G');
    check('UNREGISTER_CLIENT never shifted', rc.addrFor(RequestCode.UNREGISTER_CLIENT) === BROKER);

    // The send invoke boundary takes a caller-resolved address and does NOT
    // re-translate it — this is the same path the recall request uses, and the
    // oneway path the END_TRANSACTION request uses.
    const sendReq = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, null);
    try { await client.sendMessage(BROKER, sendReq, mq, 3000, null); } catch { /* parse of a bare reply */ }
    check('SEND (recall boundary) never shifted by the instance switch',
      rc.addrFor(RequestCode.SEND_MESSAGE_V2) === BROKER);
    const onewayReq = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, null);
    await client.sendMessageOneway(BROKER, onewayReq);
    check('oneway (end-transaction boundary) never shifted',
      rc.addrFor(RequestCode.END_TRANSACTION) === BROKER);

    const popReq = { topic: 'VipTopic', order: false };
    try { await client.popMessage('broker-a', BROKER, popReq as any, '', 3000); } catch { /* empty pop reply */ }
    check('POP_MESSAGE never shifted', rc.addrFor(RequestCode.POP_MESSAGE) === BROKER);
    await client.ackMessage(BROKER, {} as any, 3000);
    check('ACK_MESSAGE never shifted', rc.addrFor(RequestCode.ACK_MESSAGE) === BROKER);
    await client.changeInvisibleTime(BROKER, {} as any, 3000);
    check('CHANGE_MESSAGE_INVISIBLETIME never shifted',
      rc.addrFor(RequestCode.CHANGE_MESSAGE_INVISIBLETIME) === BROKER);
  }

  // -------------------------------- admin: broker request shifted, name-server
  //                       request NEVER shifted (the three former bugs included)
  {
    const rc = new RecordingClient();
    const admin = new DefaultMQAdminExt();
    admin.client = newClient(rc);
    (admin as any)._started = true;
    admin.setVipChannelEnabled(true);

    rc.reset();
    await admin.getAllSubscriptionGroup(BROKER);
    check('GET_ALL_SUBSCRIPTIONGROUP shifted when on',
      rc.addrFor(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG) === BROKER_VIP);

    // Reading a broker's own configuration is the one broker RPC that never moves: the fast
    // channel serves the other admin RPCs, not this one.
    await admin.getBrokerConfig(BROKER);
    check('GET_BROKER_CONFIG keeps the listening port even when the switch is on',
      rc.addrFor(RequestCode.GET_BROKER_CONFIG) === BROKER);

    // The offset family is delegated to the shared client instance, which applies the
    // translation itself -- so the admin switch has to drive that instance, not only its own RPCs.
    const mq = new MessageQueue('VipTopic', 'broker-a', 0);
    await admin.updateConsumeOffset(BROKER, 'GID_vip', mq, 7);
    check('delegated UPDATE_CONSUMER_OFFSET shifted when on',
      rc.addrFor(RequestCode.UPDATE_CONSUMER_OFFSET) === BROKER_VIP);
    await admin.getConsumerListByGroup('GID_vip', BROKER);
    check('delegated GET_CONSUMER_LIST_BY_GROUP shifted when on',
      rc.addrFor(RequestCode.GET_CONSUMER_LIST_BY_GROUP) === BROKER_VIP);

    // DELETE_TOPIC_IN_NAMESRV / WIPE_WRITE_PERM / ADD_WRITE_PERM target the
    // nameserver; the VIP port must never be applied there.
    await admin.deleteTopicInNameServer([NAMESRV], 'VipTopic');
    check('deleteTopicInNameServer hits the plain nameserver (not VIP)',
      rc.addrFor(RequestCode.DELETE_TOPIC_IN_NAMESRV) === NAMESRV);
    await admin.wipeWritePermOfBroker(NAMESRV, 'broker-a');
    check('wipeWritePermOfBroker hits the plain nameserver (not VIP)',
      rc.addrFor(RequestCode.WIPE_WRITE_PERM_OF_BROKER) === NAMESRV);
    await admin.addWritePermOfBroker(NAMESRV, 'broker-a');
    check('addWritePermOfBroker hits the plain nameserver (not VIP)',
      rc.addrFor(RequestCode.ADD_WRITE_PERM_OF_BROKER) === NAMESRV);

    // Generic nameserver requests (broadcast to the configured list).
    await admin.putKVConfig('ns', 'k', 'v');
    check('PUT_KV_CONFIG hits the plain nameserver (not VIP)',
      rc.addrFor(RequestCode.PUT_KV_CONFIG) === NAMESRV);
    await admin.getKVConfig('ns', 'k');
    check('GET_KV_CONFIG hits the plain nameserver (not VIP)',
      rc.addrFor(RequestCode.GET_KV_CONFIG) === NAMESRV);

    // With the switch off the broker request is the plain address again.
    admin.setVipChannelEnabled(false);
    rc.reset();
    await admin.getAllSubscriptionGroup(BROKER);
    check('GET_ALL_SUBSCRIPTIONGROUP unchanged when off',
      rc.addrFor(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG) === BROKER);
    await admin.updateConsumeOffset(BROKER, 'GID_vip', mq, 8);
    check('delegated UPDATE_CONSUMER_OFFSET back on the listening port',
      rc.addrFor(RequestCode.UPDATE_CONSUMER_OFFSET) === BROKER);
    await admin.getConsumerListByGroup('GID_vip', BROKER);
    check('delegated GET_CONSUMER_LIST_BY_GROUP back on the listening port',
      rc.addrFor(RequestCode.GET_CONSUMER_LIST_BY_GROUP) === BROKER);
    await admin.getBrokerConfig(BROKER);
    check('GET_BROKER_CONFIG unchanged when off',
      rc.addrFor(RequestCode.GET_BROKER_CONFIG) === BROKER);
  }

  // ------------------------------------------- the helper is not fed NS addrs:
  //                    route fetch on the shared instance stays on the NS port
  {
    const rc = new RecordingClient();
    const client = newClient(rc);
    client.setVipChannelEnabled(true);
    rc.reset();
    // updateTopicRouteInfoFromNameServer is served by the name server; it must
    // dial NAMESRV, never the VIP-shifted NS address. (The reply carries no
    // route body, so it simply reports the topic not found.)
    await client.updateTopicRouteInfoFromNameServer('VipTopic', false).catch(() => {});
    const routeAddr = rc.addrFor(RequestCode.GET_ROUTEINFO_BY_TOPIC);
    check('GET_ROUTEINFO_BY_TOPIC hits the plain nameserver (not VIP)', routeAddr === NAMESRV,
      String(routeAddr));
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
