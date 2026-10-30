// -*-
// Channel-active re-registration — offline contract.
//
//   node --experimental-strip-types test/channel_active_reregistration_smoke.ts
//
// Two layers, measured separately:
//   * the transport fires a registered listener exactly once per NEW
//     connection (a reused connection fires nothing), and a listener that
//     throws neither breaks the invoke nor the other listeners — it is logged;
//   * the instance re-registers every registered consumer group with a broker
//     when a connection to that broker comes up: an unknown address, an empty
//     consumer table or the switch off all suppress it, and a beat is followed
//     by a rebalance wake on every consumer.
import net from 'node:net';
import { Buffer } from 'node:buffer';

import { RemotingClient } from '../src/remoting/client.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { HeartbeatData } from '../src/remoting/heartbeat.ts';
import { SubscriptionData } from '../src/common/subscriptionData.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { MQClient } from '../src/client/mq_client.ts';
import { setLevel } from '../src/logging.ts';

setLevel('DEBUG');

let pass = 0, fail = 0;
const realLog = console.log.bind(console);
const realError = console.error.bind(console);
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; realLog(`  PASS ${name}`); }
  else { fail++; realError(`  FAIL ${name} ${extra}`); }
}

// The client logger writes through console.log / console.error; capture those
// so the listener-failure line can be asserted verbatim.
let captured: string[] = [];
console.log = (...a: any[]) => { captured.push(a.map((x) => String(x)).join(' ')); };
console.error = (...a: any[]) => { captured.push(a.map((x) => String(x)).join(' ')); };
function resetLogs() { captured = []; }
function linesWith(substr: string): string[] { return captured.filter((l) => l.includes(substr)); }

function settle(ms: number): Promise<void> {
  return new Promise((resolve) => { setTimeout(resolve, ms); });
}
async function waitFor(pred: () => boolean, timeoutMs: number, pollMs = 20): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (pred()) return true;
    await settle(pollMs);
  }
  return pred();
}

function routeRequest(): RemotingCommand {
  return new RemotingCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC);
}
function responseFrame(code: number, opaque: number): Buffer {
  const r = RemotingCommand.createResponseCommand(code, 'car');
  r.opaque = opaque;
  return r.encode();
}

// Answers every request with a matching SUCCESS response and counts connections
// that end, so a test can prove a connection was actually torn down.
class EchoPeer {
  server: net.Server;
  port = 0;
  closeCount = 0;
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
  private onConnection(sock: net.Socket) {
    sock.on('close', () => { this.closeCount++; });
    sock.on('error', () => { /* client went away */ });
    let buf = Buffer.alloc(0);
    sock.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      for (;;) {
        if (buf.length < 4) return;
        const total = buf.readInt32BE(0);
        if (total <= 0 || total > 16 * 1024 * 1024) { sock.destroy(); return; }
        if (buf.length < 4 + total) return;
        const frame = buf.subarray(0, 4 + total);
        buf = buf.subarray(4 + total);
        let req: RemotingCommand;
        try { req = RemotingCommand.decode(frame); } catch { sock.destroy(); return; }
        sock.write(responseFrame(ResponseCode.SUCCESS, req.opaque));
      }
    });
  }
  stop() { this.server.close(); }
}

async function testTransportListener(): Promise<void> {
  realLog('== transport: one event per new connection ==');
  const peer = new EchoPeer();
  await peer.listen();
  const rc = new RemotingClient({ invokeTimeoutMillis: 1500 });
  const events: string[] = [];
  rc.registerChannelListener((addr) => { events.push(addr); });
  try {
    const first = await rc.invokeSync(peer.addr, routeRequest());
    check('first invoke succeeds', first.code === ResponseCode.SUCCESS, `code=${first.code}`);
    check('a new connection fires the listener once', events.length === 1, `events=${events.length}`);
    check('the listener is told which address came up', events[0] === peer.addr, `got=${events[0]}`);

    const again = await rc.invokeSync(peer.addr, routeRequest());
    check('reusing the connection fires nothing', again.code === ResponseCode.SUCCESS && events.length === 1,
      `events=${events.length}`);

    rc.closeChannel(peer.addr);
    await waitFor(() => peer.closeCount >= 1, 1500);
    check('closeChannel tears the peer connection down', peer.closeCount >= 1, `close=${peer.closeCount}`);

    const reopened = await rc.invokeSync(peer.addr, routeRequest());
    check('the next invoke opens a new connection', reopened.code === ResponseCode.SUCCESS && events.length === 2,
      `events=${events.length}`);
    check('the new event carries the same address', events[1] === peer.addr, `got=${events[1]}`);
  } finally {
    rc.shutdown();
    peer.stop();
  }
}

async function testThrowingListenerIsContained(): Promise<void> {
  realLog('== transport: a throwing listener neither breaks the invoke nor hides ==');
  const peer = new EchoPeer();
  await peer.listen();
  const rc = new RemotingClient({ invokeTimeoutMillis: 1500 });
  rc.registerChannelListener(() => { throw new Error('listener blew up'); });
  let events = 0;
  rc.registerChannelListener(() => { events++; });
  resetLogs();
  try {
    const response = await rc.invokeSync(peer.addr, routeRequest());
    check('the invoke still answers', response.code === ResponseCode.SUCCESS, `code=${response.code}`);
    check('the listener after the throwing one still runs', events === 1, `events=${events}`);
    const warns = linesWith('channel listener');
    check('the failure is logged as a WARN', warns.some((l) => l.includes('WARN')), warns.join('|'));
    check('the WARN names the address', warns.some((l) => l.includes(peer.addr)), warns.join('|'));
    check('the WARN carries the listener error', warns.some((l) => l.includes('listener blew up')), warns.join('|'));
  } finally {
    rc.shutdown();
    peer.stop();
  }
}

// Transport-shaped double: records heartbeats, records the channel listener the
// instance registers, and answers SUCCESS to everything else.
class ChannelTransport {
  listener: ((addr: string) => void) | null = null;
  beats: Array<{ addr: string; groups: string[] }> = [];
  registerProcessor(_code: number, _handler: any): void {}
  unregisterProcessor(_code: number): void {}
  registerRpcHook(_hook: any): void {}
  registerChannelListener(listener: (addr: string) => void): void { this.listener = listener; }
  shutdown(): void {}
  invokeOneway(): Promise<void> { return Promise.resolve(); }
  async invokeSync(addr: string, request: RemotingCommand): Promise<RemotingCommand> {
    if (request.code === RequestCode.HEART_BEAT) {
      const hb = HeartbeatData.decode(request.body as Buffer);
      this.beats.push({ addr, groups: hb.consumerDataSet.map((c) => c.groupName).sort() });
    }
    const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, '');
    resp.opaque = request.opaque;
    return resp;
  }
}

async function testInstanceReregistration(): Promise<void> {
  realLog('== instance: a broker connection re-registers every group ==');
  const transport = new ChannelTransport();
  const client = new MQClient(`channel-active@${process.pid}`, '127.0.0.1:1', transport as any);
  const brokerAddr = '10.0.0.1:10911';
  client.brokerAddrTable.set('b1', { [MixAll.MASTER_ID]: brokerAddr });
  let rebalances = 0;
  client.start();
  try {
    check('the switch defaults to on', client.getEnableHeartbeatChannelEventListener() === true);
    check('start registers a channel listener on the transport', transport.listener != null);

    client.registerConsumer('CG', {
      subscriptionDataSet: [new SubscriptionData('Tt', 'tagA||tagB')],
      rebalanceImmediately: () => { rebalances++; },
    } as any);

    client.setEnableHeartbeatChannelEventListener(false);
    transport.listener!(brokerAddr);
    await settle(50);
    check('switch off: no beat is sent', transport.beats.length === 0, `beats=${transport.beats.length}`);
    check('switch off: no rebalance is woken', rebalances === 0, `rebalances=${rebalances}`);
    client.setEnableHeartbeatChannelEventListener(true);

    transport.listener!(brokerAddr);
    await waitFor(() => transport.beats.length >= 1 && rebalances >= 1, 1000);
    check('a known broker gets exactly one re-registration beat', transport.beats.length === 1,
      `beats=${transport.beats.length}`);
    check('the beat goes to the address that came up', transport.beats[0]?.addr === brokerAddr,
      `addr=${transport.beats[0]?.addr}`);
    check('the beat carries every registered group', JSON.stringify(transport.beats[0]?.groups) === '["CG"]',
      `groups=${JSON.stringify(transport.beats[0]?.groups)}`);
    check('the re-registration wakes rebalance', rebalances === 1, `rebalances=${rebalances}`);

    transport.listener!('127.0.0.1:1');
    await settle(50);
    check('an address that is not a known broker re-registers nothing',
      transport.beats.length === 1 && rebalances === 1,
      `beats=${transport.beats.length} rebalances=${rebalances}`);

    client.setEnableHeartbeatChannelEventListener(false);
    transport.listener!(brokerAddr);
    await settle(50);
    check('the switch off suppresses a later event',
      transport.beats.length === 1 && rebalances === 1,
      `beats=${transport.beats.length} rebalances=${rebalances}`);
  } finally {
    client.shutdown();
  }
}

async function main(): Promise<void> {
  await testTransportListener();
  await testThrowingListenerIsContained();
  await testInstanceReregistration();
  realLog(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { realError(e); process.exit(1); });
