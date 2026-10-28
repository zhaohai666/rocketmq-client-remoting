// -*- coding: utf-8 -*-
// Remoting transport contract smoke — offline, in-process fake peers.
//
//   node --experimental-strip-types test/transport_contract_smoke.ts
//
// Each case drives the real RemotingClient against a net.createServer fake peer
// (no broker) and asserts what this client does plus what the client logged. The
// legs covered:
//   T1 an undecodable frame logs ERROR and closes that connection only
//   T2 an unmatched response logs a WARN and keeps the connection
//   T3 isChannelWritable is true when there is no connection; false only when a
//      live socket's write buffer is saturated
//   T4 an idle connection is swept away by the unref()'d timer; a busy one survives
//   T5 one WARN per expired pending entry (per-call timer)
//   T6 a sync timeout closes the socket when the knob is on, and not when off
import net from 'node:net';
import { Buffer } from 'node:buffer';

import { RemotingClient } from '../src/remoting/client.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { setLevel } from '../src/logging.ts';

// Emit DEBUG and above so WARN/ERROR lines are always produced regardless of env.
setLevel('DEBUG');

// ---------------- harness ----------------
let pass = 0, fail = 0;
const realLog = console.log.bind(console);
const realError = console.error.bind(console);
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; realLog(`  PASS ${name}`); }
  else { fail++; realError(`  FAIL ${name} ${extra}`); }
}

// The client logger funnels every line through console.log (DEBUG/INFO/WARN) or
// console.error (ERROR). Redirect both into a buffer so tests assert on the exact
// line, while the harness itself prints via realLog/realError.
let captured: string[] = [];
console.log = (...a: any[]) => { captured.push(a.map((x) => String(x)).join(' ')); };
console.error = (...a: any[]) => { captured.push(a.map((x) => String(x)).join(' ')); };
function resetLogs() { captured = []; }
function linesWith(substr: string): string[] { return captured.filter((l) => l.includes(substr)); }

function settle(ms: number): Promise<void> {
  return new Promise((resolve) => { const t = setTimeout(resolve, ms); t.unref?.(); });
}
async function waitFor(pred: () => boolean, timeoutMs: number, pollMs = 50): Promise<boolean> {
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

function responseFrame(code: number, opaque: number, remark = 'tc'): Buffer {
  const r = RemotingCommand.createResponseCommand(code, remark)!;
  r.opaque = opaque;
  return r.encode();
}

// A frame whose total-length prefix is legal (so it survives the read-loop gate)
// but whose header length is bogus: RemotingCommand.decode throws on it.
function badHeaderFrame(): Buffer {
  const b = Buffer.alloc(12);
  b.writeInt32BE(8, 0);          // totalLen=8, within 1..MAX_FRAME_LENGTH
  b.writeInt32BE(0x00FFFFFF, 4); // header length field = 0xFFFFFF -> "bad header length"
  return b;
}
// A frame whose total-length prefix itself is illegal (0 is not a valid frame size).
function illegalLengthFrame(): Buffer {
  const b = Buffer.alloc(4);
  b.writeInt32BE(0, 0);
  return b;
}

// ---------------- fake peers ----------------

// Answers every request with a matching SUCCESS response. Counts the connections it
// sees close, so tests can prove a connection survived.
class EchoPeer {
  server: net.Server;
  port = 0;
  requests = 0;
  closeCount = 0;
  constructor() { this.server = net.createServer((s) => this.onConnection(s)); }
  listen(): Promise<void> {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => { this.port = (this.server.address() as net.AddressInfo).port; resolve(); });
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
        this.requests++;
        sock.write(responseFrame(ResponseCode.SUCCESS, req.opaque));
      }
    });
  }
  stop() { this.server.close(); }
}

// After the first complete request, replies with a deliberately broken frame
// (undecodable header, or an illegal length prefix). It never closes the socket
// itself, so any close the peer observes is the client tearing it down.
class MisbehavingPeer {
  server: net.Server;
  port = 0;
  mode: 'badHeader' | 'illegalLength';
  sawRequest = false;
  closeCount = 0;
  constructor(mode: 'badHeader' | 'illegalLength') { this.mode = mode; this.server = net.createServer((s) => this.onConnection(s)); }
  listen(): Promise<void> {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => { this.port = (this.server.address() as net.AddressInfo).port; resolve(); });
    });
  }
  get addr() { return `127.0.0.1:${this.port}`; }
  private onConnection(sock: net.Socket) {
    sock.on('close', () => { this.closeCount++; });
    sock.on('error', () => { /* client went away */ });
    let buf = Buffer.alloc(0);
    sock.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      if (this.sawRequest) return;
      if (buf.length < 4) return;
      const total = buf.readInt32BE(0);
      if (total <= 0 || total > 16 * 1024 * 1024) return;
      if (buf.length < 4 + total) return; // wait for the whole request so a future is pending
      this.sawRequest = true;
      sock.write(this.mode === 'badHeader' ? badHeaderFrame() : illegalLengthFrame());
    });
  }
  stop() { this.server.close(); }
}

// Reads and discards everything, sends nothing back. Used both for connections that
// must go idle (no inbound traffic, so activity only on write) and for ordinary
// drained connections.
class SilentPeer {
  server: net.Server;
  port = 0;
  closeCount = 0;
  constructor() { this.server = net.createServer((s) => this.onConnection(s)); }
  listen(): Promise<void> {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => { this.port = (this.server.address() as net.AddressInfo).port; resolve(); });
    });
  }
  get addr() { return `127.0.0.1:${this.port}`; }
  private onConnection(sock: net.Socket) {
    sock.on('close', () => { this.closeCount++; });
    sock.on('error', () => { /* client went away */ });
    sock.on('data', () => { /* drain input, reply nothing */ });
  }
  stop() { this.server.close(); }
}

// Accepts but never reads. Used to build write-buffer backpressure on the client.
class SinkPeer {
  server: net.Server;
  port = 0;
  sockets: net.Socket[] = [];
  constructor() { this.server = net.createServer((s) => this.onConnection(s)); }
  listen(): Promise<void> {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => { this.port = (this.server.address() as net.AddressInfo).port; resolve(); });
    });
  }
  get addr() { return `127.0.0.1:${this.port}`; }
  private onConnection(sock: net.Socket) {
    sock.on('error', () => { /* never read */ });
    sock.on('close', () => { /* torn down */ });
    this.sockets.push(sock);
  }
  stop() { this.server.close(); }
}

// -------------------------------------------------------------- T1
async function testDecodeFailureCloses(): Promise<void> {
  realLog('== T1: undecodable frame -> ERROR + close that connection only ==');

  // (a) header that fails RemotingCommand.decode
  const echo = new EchoPeer(); await echo.listen();
  const bad = new MisbehavingPeer('badHeader'); await bad.listen();
  const rc = new RemotingClient({ invokeTimeoutMillis: 1500 });

  const okA1 = await rc.invokeSync(echo.addr, routeRequest()); // healthy connection A
  resetLogs();
  let errName = ''; let err: any = null;
  try { await rc.invokeSync(bad.addr, routeRequest()); } catch (e) { err = e; errName = (e as Error).name; }
  await waitFor(() => bad.closeCount > 0, 1500);
  const errLines = linesWith('ERROR');
  check('undecodable header: the pending request fails fast', err != null && errName === 'RemotingSendRequestException', `name=${errName}`);
  check('undecodable header: an ERROR line is logged', errLines.length >= 1, errLines.join('|'));
  check('undecodable header: the ERROR names the peer address', errLines.some((l) => l.includes(bad.addr)), errLines.join('|'));
  check('undecodable header: the ERROR names the decode failure', errLines.some((l) => l.includes('decode')), errLines.join('|'));
  check('undecodable header: the connection was closed', bad.closeCount >= 1, `closeCount=${bad.closeCount}`);
  const okA2 = await rc.invokeSync(echo.addr, routeRequest()); // A must be intact
  check('undecodable header: a separate healthy connection still works', okA2.code === ResponseCode.SUCCESS, `code=${okA2.code}`);
  check('undecodable header: the healthy peer saw no close', echo.closeCount === 0, `echoClose=${echo.closeCount}`);
  check('undecodable header: first healthy round-trip succeeded', okA1.code === ResponseCode.SUCCESS);
  rc.shutdown(); echo.stop(); bad.stop();

  // (b) illegal length prefix
  const echo2 = new EchoPeer(); await echo2.listen();
  const bad2 = new MisbehavingPeer('illegalLength'); await bad2.listen();
  const rc2 = new RemotingClient({ invokeTimeoutMillis: 1500 });
  await rc2.invokeSync(echo2.addr, routeRequest());
  resetLogs();
  let err2: any = null;
  try { await rc2.invokeSync(bad2.addr, routeRequest()); } catch (e) { err2 = e; }
  await waitFor(() => bad2.closeCount > 0, 1500);
  const errLines2 = linesWith('ERROR');
  check('illegal length prefix: the pending request fails', err2 != null);
  check('illegal length prefix: an ERROR names the peer address', errLines2.some((l) => l.includes(bad2.addr)), errLines2.join('|'));
  check('illegal length prefix: the connection was closed', bad2.closeCount >= 1, `closeCount=${bad2.closeCount}`);
  const ok2 = await rc2.invokeSync(echo2.addr, routeRequest());
  check('illegal length prefix: the other connection is untouched', ok2.code === ResponseCode.SUCCESS && echo2.closeCount === 0);
  rc2.shutdown(); echo2.stop(); bad2.stop();
}

// -------------------------------------------------------------- T2
async function testUnmatchedResponseWarns(): Promise<void> {
  realLog('== T2: unmatched response -> WARN, connection kept ==');
  // A peer that, on the first request, answers with BOTH an orphan response (an
  // opaque that was never issued) and the correct matching response.
  class OrphanPeer {
    server = net.createServer((s) => this.onConnection(s));
    port = 0;
    requests = 0;
    closeCount = 0;
    sentOrphan = false;
    ORPHAN_OPAQUE = 987654;
    listen(): Promise<void> { return new Promise((r) => { this.server.listen(0, '127.0.0.1', () => { this.port = (this.server.address() as net.AddressInfo).port; r(); }); }); }
    get addr() { return `127.0.0.1:${this.port}`; }
    private onConnection(sock: net.Socket) {
      sock.on('close', () => { this.closeCount++; });
      sock.on('error', () => {});
      let buf = Buffer.alloc(0);
      sock.on('data', (chunk) => {
        buf = Buffer.concat([buf, chunk]);
        for (;;) {
          if (buf.length < 4) return;
          const total = buf.readInt32BE(0);
          if (total <= 0 || total > 16 * 1024 * 1024) { sock.destroy(); return; }
          if (buf.length < 4 + total) return;
          const frame = buf.subarray(0, 4 + total); buf = buf.subarray(4 + total);
          let req: RemotingCommand;
          try { req = RemotingCommand.decode(frame); } catch { sock.destroy(); return; }
          this.requests++;
          if (!this.sentOrphan) { this.sentOrphan = true; sock.write(responseFrame(ResponseCode.SUCCESS, this.ORPHAN_OPAQUE)); }
          sock.write(responseFrame(ResponseCode.SUCCESS, req.opaque));
        }
      });
    }
    stop() { this.server.close(); }
  }

  const peer = new OrphanPeer(); await peer.listen();
  const rc = new RemotingClient({ invokeTimeoutMillis: 1500 });
  resetLogs();
  const resp = await rc.invokeSync(peer.addr, routeRequest()); // still completes via the matching reply
  await settle(150);
  const warn = linesWith('not matched any request');
  check('unmatched response: the matched reply still completes the request', resp.code === ResponseCode.SUCCESS, `code=${resp.code}`);
  check('unmatched response: exactly one WARN line', warn.length === 1, warn.join('|'));
  check('unmatched response: the WARN names the opaque', warn.length === 1 && warn[0].includes(String(peer.ORPHAN_OPAQUE)), warn.join('|'));
  check('unmatched response: the WARN names the peer address', warn.length === 1 && warn[0].includes(peer.addr), warn.join('|'));
  check('unmatched response: the connection stays alive (peer saw no close)', peer.closeCount === 0, `closeCount=${peer.closeCount}`);
  const resp2 = await rc.invokeSync(peer.addr, routeRequest());
  check('unmatched response: a follow-up request reuses the live connection', resp2.code === ResponseCode.SUCCESS && peer.requests === 2, `requests=${peer.requests}`);
  rc.shutdown(); peer.stop();
}

// -------------------------------------------------------------- T3
async function testChannelWritable(): Promise<void> {
  realLog('== T3: isChannelWritable ==');
  const rc = new RemotingClient({ invokeTimeoutMillis: 1500 });

  // no connection at all -> writable
  check('writable: an unknown address reports true', rc.isChannelWritable('127.0.0.1:59998') === true);

  // live + drained -> writable
  const drained = new SilentPeer(); await drained.listen();
  await rc.invokeOneway(drained.addr, routeRequest());
  check('writable: a live drained connection reports true', rc.isChannelWritable(drained.addr) === true);

  // live + saturated -> not writable.
  // How the saturated case is forced (honestly): the peer accepts but never reads;
  // we grab the client's pooled socket, cork it (so nothing flushes to the kernel)
  // and push 64 KiB chunks. The outbound writable buffer climbs past the writable
  // high-water mark until socket.write() reports backpressure, and stays there while
  // corked.
  const sink = new SinkPeer(); await sink.listen();
  await rc.invokeOneway(sink.addr, routeRequest()); // small frame: establishes the pooled socket
  const sock: net.Socket = (rc as any).conns.get(sink.addr);
  sock.cork();
  let tripped = false; let iterations = 0;
  for (; iterations < 16 && !tripped; iterations++) tripped = (sock.write(Buffer.alloc(64 * 1024)) === false);
  check('writable: the saturated buffer was actually forced', tripped, `iters=${iterations} len=${sock.writableLength} hwm=${sock.writableHighWaterMark}`);
  check('writable: a live saturated connection reports false', rc.isChannelWritable(sink.addr) === false);
  sock.uncork();

  // destroyed connection -> writable again (absent is not "cannot write")
  rc.closeChannel(sink.addr);
  await settle(50);
  check('writable: after close the address reports true again', rc.isChannelWritable(sink.addr) === true);

  rc.shutdown(); drained.stop(); sink.stop();
}

// -------------------------------------------------------------- T4
async function testIdleSweep(): Promise<void> {
  realLog('== T4: idle connection swept away, busy connection survives ==');
  const idlePeer = new SilentPeer(); await idlePeer.listen();
  const busyPeer = new SilentPeer(); await busyPeer.listen();

  const rc = new RemotingClient({ invokeTimeoutMillis: 1500, maxIdleTimeMillis: 600 });

  await rc.invokeOneway(idlePeer.addr, routeRequest());  // one write, then it goes quiet
  await rc.invokeOneway(busyPeer.addr, routeRequest());  // initial, then kept busy

  const beat = setInterval(() => {
    rc.invokeOneway(busyPeer.addr, routeRequest()).catch(() => { /* ignore */ });
  }, 200); // cadence well under the 600 ms idle limit

  resetLogs();
  const swept = await waitFor(() => idlePeer.closeCount > 0, 5000);

  // busy connection must still be present at the moment the idle one is reaped;
  // stop beating first is not needed since we assert immediately.
  const busyAlive = busyPeer.closeCount === 0 && (rc as any).conns.has(busyPeer.addr);
  clearInterval(beat);

  const idleWarn = linesWith('closing idle connection');
  check('idle sweep: the idle connection was closed', swept, `closeCount=${idlePeer.closeCount}`);
  check('idle sweep: the idle connection is gone from the table', !(rc as any).conns.has(idlePeer.addr));
  check('idle sweep: the close logs one WARN naming the address', idleWarn.length >= 1 && idleWarn.some((l) => l.includes(idlePeer.addr)), idleWarn.join('|'));
  check('idle sweep: the busy connection survived', busyAlive, `busyClose=${busyPeer.closeCount}`);

  // The sweep timer is unref()'d: prove the configured limit's non-positive fallback
  // and that a client with no configured limit never starts a sweep before it connects.
  const rcNoConn = new RemotingClient({ maxIdleTimeMillis: 0 }); // non-positive -> default, no disable
  check('idle knob: a non-positive limit keeps the default (120s) semantics',
    (rcNoConn as any)._idleLimitMillis() === 120000, `${(rcNoConn as any)._idleLimitMillis()}`);
  check('idle knob: a client that never connected installs no sweep timer', (rcNoConn as any).idleSweepTimer === null);
  rcNoConn.shutdown();

  rc.shutdown(); idlePeer.stop(); busyPeer.stop();
}

// -------------------------------------------------------------- T5
async function testExpiryWarnPerEntry(): Promise<void> {
  realLog('== T5: one WARN per expired pending entry ==');
  const peer = new SilentPeer(); await peer.listen();
  const rc = new RemotingClient({ invokeTimeoutMillis: 400, closeSocketOnSyncTimeout: false });

  // async: two concurrent requests, neither answered -> exactly two expiry WARNs
  resetLogs();
  const asyncErrs: any[] = [];
  await Promise.all([0, 1].map(() => new Promise<void>((resolve) => {
    rc.invokeAsync(peer.addr, routeRequest(), (_cmd, err) => { asyncErrs.push(err); resolve(); });
  })));
  await settle(150);
  const expired = linesWith('request expired');
  check('async expiry: one WARN per expired entry', expired.length === 2, expired.join('|'));
  check('async expiry: both callbacks got the timeout error',
    asyncErrs.length === 2 && asyncErrs.every((e) => (e as Error).name === 'RemotingTimeoutException'), asyncErrs.map((e) => (e as Error).name).join('|'));
  check('async expiry: the WARN names the peer address', expired.every((l) => l.includes(peer.addr)), expired.join('|'));

  // sync: a single timed-out request logs one expiry WARN naming addr + timeout
  resetLogs();
  let syncErr: any = null;
  try { await rc.invokeSync(peer.addr, routeRequest()); } catch (e) { syncErr = e; }
  await settle(150);
  const syncExpired = linesWith('invokeSync request expired');
  check('sync expiry: exactly one WARN per expired entry', syncExpired.length === 1, syncExpired.join('|'));
  check('sync expiry: the WARN names the peer address and timeout', syncExpired.length === 1 && syncExpired[0].includes(peer.addr) && syncExpired[0].includes('400'), syncExpired.join('|'));
  check('sync expiry: the caller still gets the timeout error', syncErr && syncErr.name === 'RemotingTimeoutException', `name=${syncErr && syncErr.name}`);

  rc.shutdown(); peer.stop();
}

// -------------------------------------------------------------- T6
async function testSyncTimeoutClosesSocket(): Promise<void> {
  realLog('== T6: sync timeout closes the socket, gated by the knob ==');

  // knob ON (default): the connection is torn down and a close WARN is logged
  const peerOn = new SilentPeer(); await peerOn.listen();
  const rcOn = new RemotingClient({ invokeTimeoutMillis: 500, closeSocketOnSyncTimeout: true });
  resetLogs();
  let errOn: any = null;
  try { await rcOn.invokeSync(peerOn.addr, routeRequest()); } catch (e) { errOn = e; }
  await waitFor(() => peerOn.closeCount > 0, 1500);
  const closeWarn = linesWith('invokeSync timed out, closing connection');
  check('knob on: sync timeout still rejects with the timeout error', errOn && errOn.name === 'RemotingTimeoutException', `name=${errOn && errOn.name}`);
  check('knob on: the connection was closed', peerOn.closeCount >= 1, `closeCount=${peerOn.closeCount}`);
  check('knob on: no usable connection remains in the table', !(rcOn as any).conns.has(peerOn.addr));
  check('knob on: the close logs a WARN naming the address', closeWarn.length === 1 && closeWarn[0].includes(peerOn.addr), closeWarn.join('|'));
  rcOn.shutdown(); peerOn.stop();

  // knob OFF: same timeout, same rejection, but the connection stays open
  const peerOff = new SilentPeer(); await peerOff.listen();
  const rcOff = new RemotingClient({ invokeTimeoutMillis: 500, closeSocketOnSyncTimeout: false });
  resetLogs();
  let errOff: any = null;
  try { await rcOff.invokeSync(peerOff.addr, routeRequest()); } catch (e) { errOff = e; }
  await settle(300);
  check('knob off: sync timeout still rejects with the timeout error', errOff && errOff.name === 'RemotingTimeoutException', `name=${errOff && errOff.name}`);
  check('knob off: the connection stays open (peer saw no close)', peerOff.closeCount === 0, `closeCount=${peerOff.closeCount}`);
  check('knob off: the connection is still pooled', (rcOff as any).conns.has(peerOff.addr));
  check('knob off: no close WARN is emitted', linesWith('invokeSync timed out, closing connection').length === 0);
  rcOff.shutdown(); peerOff.stop();
}

// -------------------------------------------------------------- T6 floors
// The close decision has two floors on the time still available when the wait
// began. This client schedules the deadline as the whole configured budget -- it
// does not subtract the connect and write cost -- so the budget IS the timeout and
// the second floor is out of reach for any positive value: a positive timeout
// always closes. A zero budget is the one shape that keeps the socket, and a
// request with no time at all was never going to be answered anyway.
async function testSyncTimeoutCloseFloors(): Promise<void> {
  realLog('== T6 floors: the keep-branch is reachable only at a zero budget ==');

  // Establish the connection first, so "kept" and "closed" are statements about a
  // socket that really exists.
  const keepPeer = new SilentPeer(); await keepPeer.listen();
  const rcKeep = new RemotingClient({ invokeTimeoutMillis: 1500, closeSocketOnSyncTimeout: true });
  await rcKeep.invokeOneway(keepPeer.addr, routeRequest());
  resetLogs();
  let keepErr: any = null;
  try { await rcKeep.invokeSync(keepPeer.addr, routeRequest(), 0); } catch (e) { keepErr = e; }
  await settle(200);
  check('zero budget: the caller still gets the timeout error',
    keepErr && keepErr.name === 'RemotingTimeoutException', `name=${keepErr && keepErr.name}`);
  check('zero budget: the socket is at both floors, so it is kept', (rcKeep as any).conns.has(keepPeer.addr));
  check('zero budget: the peer saw no close', keepPeer.closeCount === 0, `closeCount=${keepPeer.closeCount}`);
  check('zero budget: no close WARN is emitted', linesWith('closing connection').length === 0, linesWith('closing connection').join('|'));
  rcKeep.shutdown(); keepPeer.stop();

  const closePeer = new SilentPeer(); await closePeer.listen();
  const rcClose = new RemotingClient({ invokeTimeoutMillis: 1500, closeSocketOnSyncTimeout: true });
  await rcClose.invokeOneway(closePeer.addr, routeRequest());
  resetLogs();
  let closeErr: any = null;
  try { await rcClose.invokeSync(closePeer.addr, routeRequest(), 101); } catch (e) { closeErr = e; }
  await waitFor(() => closePeer.closeCount > 0, 1500);
  check('one millisecond past the floor: the caller still gets the timeout error',
    closeErr && closeErr.name === 'RemotingTimeoutException', `name=${closeErr && closeErr.name}`);
  check('one millisecond past the floor: the connection is closed', closePeer.closeCount >= 1, `closeCount=${closePeer.closeCount}`);
  check('one millisecond past the floor: the close is logged', linesWith('closing connection').length === 1, linesWith('closing connection').join('|'));
  rcClose.shutdown(); closePeer.stop();
}

(async () => {
  await testDecodeFailureCloses();  await testUnmatchedResponseWarns();
  await testChannelWritable();
  await testIdleSweep();
  await testExpiryWarnPerEntry();
  await testSyncTimeoutClosesSocket();
  await testSyncTimeoutCloseFloors();

  realLog(`\n${fail === 0 ? `ALL GREEN (${pass} checks)` : `${fail} failure(s), ${pass} passed`}`);
  process.exit(fail === 0 ? 0 : 1);
})();
