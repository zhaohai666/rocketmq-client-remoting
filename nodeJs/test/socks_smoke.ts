// -*- coding: utf-8 -*-
// SOCKS proxy support smoke — offline.
//
//   node --experimental-strip-types test/socks_smoke.ts
//
// Java baseline: ClientConfig#socksProxyConfig (SocksProxyConfig: addr /
// username / password) + NettyRemotingClient's Socks5ProxyHandler prepended
// BEFORE the SSL handler, i.e. wire order TCP connect → SOCKS CONNECT → TLS.
// The node port hand-rolls the handshake on node:net (zero deps): SOCKS5
// (RFC 1928 no-auth + RFC 1929 user/pass) and SOCKS4/4a.
//
// Structure:
//   1. Pure handshake logic (byte shapes, reply parsing, config parsing) — no network.
//   2. Live loopback: a minimal fake SOCKS5/SOCKS4 server + a fake remoting
//      target; RemotingClient goes through the proxy and completes one real
//      request/response roundtrip. Includes the user/pass subnegotiation and
//      the rejection paths (bad password, no acceptable method, CONNECT refused,
//      dead proxy port). Failure-class assertions get a 30s+ budget (loopback
//      quirk: a just-closed port can take ~2s to refuse on Windows).
//
// NOTE: TopAddressing's HTTP route fetch uses the global fetch() and never
// enters RemotingClient, so it is intentionally NOT proxied — same as Java.
import net from 'node:net';
import { Buffer } from 'node:buffer';

import { RemotingClient } from '../src/remoting/client.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import {
  buildSocks5Greeting, buildSocks5UserPassAuth, buildSocks5Connect,
  parseSocks5MethodSelection, parseSocks5UserPassReply, parseSocks5ConnectReply,
  buildSocks4Connect, parseSocks4ConnectReply, parseProxyAddr, parseSocksProxyConfig,
  proxyVersion, SOCKS5_METHOD_NO_ACCEPTABLE, SOCKS5_METHOD_USER_PASS,
} from '../src/remoting/socks_proxy.ts';
import type { SocksProxyConfig } from '../src/remoting/socks_proxy.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

function bytes(...xs: number[]): Buffer { return Buffer.from(xs); }

function eqBuf(a: Buffer | null | undefined, b: Buffer): boolean {
  return a != null && a.equals(b);
}

// ---------------- 1. pure handshake logic ----------------

function testPure(): void {
  console.log('== pure handshake logic ==');
  const [h, p] = parseProxyAddr('127.0.0.1:1080');
  check('parseProxyAddr host:port', h === '127.0.0.1' && p === 1080);
  const [h6, p6] = parseProxyAddr('[::1]:1080');
  check('parseProxyAddr [v6]:port', h6 === '::1' && p6 === 1080);
  let threw = '';
  try { parseProxyAddr('no-port-here'); } catch (e) { threw = (e as Error).message; }
  check('parseProxyAddr rejects missing port', threw.includes('host:port'), threw);
  threw = '';
  try { parseProxyAddr('h:0'); } catch (e) { threw = (e as Error).message; }
  check('parseProxyAddr rejects port 0', threw.includes('invalid port'), threw);

  check('proxyVersion default 5', proxyVersion({ addr: 'x:1' }) === 5);
  check('proxyVersion SOCKS4', proxyVersion({ addr: 'x:1', version: 'SOCKS4' }) === 4);
  threw = '';
  try { proxyVersion({ addr: 'x:1', version: 7 }); } catch (e) { threw = (e as Error).message; }
  check('proxyVersion rejects unknown version', threw.includes('unsupported version'), threw);

  check('greeting without creds offers no-auth only', eqBuf(buildSocks5Greeting(false), bytes(5, 1, 0)));
  check('greeting with creds offers no-auth + user/pass', eqBuf(buildSocks5Greeting(true), bytes(5, 2, 0, 2)));
  check('method selection parse', parseSocks5MethodSelection(bytes(5, SOCKS5_METHOD_USER_PASS)) === SOCKS5_METHOD_USER_PASS);
  check('method selection NO_ACCEPTABLE parse', parseSocks5MethodSelection(bytes(5, SOCKS5_METHOD_NO_ACCEPTABLE)) === SOCKS5_METHOD_NO_ACCEPTABLE);
  threw = '';
  try { parseSocks5MethodSelection(bytes(4, 0)); } catch (e) { threw = (e as Error).message; }
  check('method selection bad version throws', threw.includes('malformed'), threw);

  const auth = buildSocks5UserPassAuth('u', 'p');
  check('RFC1929 auth request shape', eqBuf(auth, bytes(1, 1, 0x75, 1, 0x70)));
  threw = '';
  try { buildSocks5UserPassAuth('x'.repeat(256), 'p'); } catch (e) { threw = (e as Error).message; }
  check('RFC1929 username >255 throws', threw.includes('255'), threw);
  parseSocks5UserPassReply(bytes(1, 0)); // must not throw
  threw = '';
  try { parseSocks5UserPassReply(bytes(1, 1)); } catch (e) { threw = (e as Error).message; }
  check('RFC1929 failure status throws', threw.includes('authentication failed'), threw);

  const c4 = buildSocks5Connect('127.0.0.1', 9876);
  check('CONNECT IPv4 shape', eqBuf(c4, bytes(5, 1, 0, 1, 127, 0, 0, 1, 9876 >> 8, 9876 & 0xff)));
  const cDom = buildSocks5Connect('broker.example', 10911);
  check('CONNECT domain shape (ATYP 3)',
    cDom[3] === 3 && cDom[4] === 14 && cDom.subarray(5, 19).toString() === 'broker.example'
    && cDom[cDom.length - 2] === (10911 >> 8) && cDom[cDom.length - 1] === (10911 & 0xff));
  const c6 = buildSocks5Connect('::1', 80);
  check('CONNECT IPv6 shape (ATYP 4, 16 bytes)', c6[3] === 4 && c6.length === 22 && c6[c6.length - 1] === 80);
  threw = '';
  try { buildSocks5Connect('h', 0); } catch (e) { threw = (e as Error).message; }
  check('CONNECT rejects port 0', threw.includes('invalid target port'), threw);

  const ok = parseSocks5ConnectReply(bytes(5, 0, 0, 1, 10, 1, 2, 3, 0x27, 0x10));
  check('CONNECT reply success parse', ok.boundAddr === '10.1.2.3' && ok.boundPort === 10000);
  threw = '';
  try { parseSocks5ConnectReply(bytes(5, 5, 0, 1, 0, 0, 0, 0, 0, 0)); } catch (e) { threw = (e as Error).message; }
  check('CONNECT reply refused maps 0x05', threw.includes('connection refused'), threw);
  const okDom = parseSocks5ConnectReply(Buffer.concat([bytes(5, 0, 0, 3, 4), Buffer.from('host'), bytes(0x1f, 0x90)]));
  check('CONNECT reply domain parse', okDom.boundAddr === 'host' && okDom.boundPort === 8080);

  const s4 = buildSocks4Connect('127.0.0.1', 10911, 'uid');
  check('SOCKS4 request shape (IP target)',
    eqBuf(s4, Buffer.concat([bytes(4, 1, (10911 >> 8) & 0xff, 10911 & 0xff, 127, 0, 0, 1), Buffer.from('uid'), bytes(0)])));
  const s4a = buildSocks4Connect('broker.example', 10911);
  check('SOCKS4a sentinel + domain (0.0.0.1)',
    s4a.subarray(4, 8).equals(bytes(0, 0, 0, 1)) && s4a.subarray(9, 9 + 14).toString() === 'broker.example');
  parseSocks4ConnectReply(bytes(0, 0x5a, 0, 0, 0, 0, 0, 0)); // must not throw
  threw = '';
  try { parseSocks4ConnectReply(bytes(0, 0x5b, 0, 0, 0, 0, 0, 0)); } catch (e) { threw = (e as Error).message; }
  check('SOCKS4 CD=91 throws', threw.includes('rejected or failed'), threw);

  const cfg = parseSocksProxyConfig('{"0.0.0.0/0": {"addr": "1.2.3.4:1080", "username": "u"}}');
  check('parseSocksProxyConfig Java map shape', cfg != null && cfg.addr === '1.2.3.4:1080' && cfg.username === 'u');
  const bare = parseSocksProxyConfig('{"addr": "5.6.7.8:1080"}');
  check('parseSocksProxyConfig bare object shape', bare != null && bare.addr === '5.6.7.8:1080');
  check('parseSocksProxyConfig empty/{} -> null', parseSocksProxyConfig('{}') === null && parseSocksProxyConfig('') === null && parseSocksProxyConfig(null) === null);
  threw = '';
  try { parseSocksProxyConfig('not json'); } catch (e) { threw = (e as Error).message; }
  check('parseSocksProxyConfig bad JSON throws', threw.includes('not valid JSON'), threw);
}

// ---------------- 2. live loopback ----------------

// Fake remoting target: answers any request with a SUCCESS frame carrying a
// JSON body (same shape as the FakeCluster helpers in the other smokes).
class FakeTarget {
  server: net.Server;
  port = 0;
  requests: number[] = [];
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
        this.requests.push(req.code);
        const resp = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, 'socks-smoke')!;
        resp.opaque = req.opaque;
        resp.serializeTypeCurrentRpc = req.serializeTypeCurrentRpc;
        resp.body = Buffer.from(JSON.stringify({ route: 'socks-route-ok' }), 'utf8');
        sock.write(resp.encode());
      }
    });
    sock.on('error', () => { /* tunnel teardown */ });
  }
  stop() { this.server.close(); }
}

// Minimal fake SOCKS5 server: greeting → (optional RFC1929) → CONNECT → pipe.
class FakeSocks5 {
  server: net.Server;
  port = 0;
  requireAuth = false;
  validUser = 'socksu';
  validPass = 'socksp';
  connectReplyCode = 0x00;
  connectAttempts = 0;
  authAttempts: { user: string; pass: string }[] = [];
  refusedAuthFailures = 0;
  private targetAddr: string;

  constructor(targetAddr: string) {
    this.targetAddr = targetAddr;
    this.server = net.createServer((s) => this.onConnection(s));
  }
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
    let buf = Buffer.alloc(0);
    let stage: 'greeting' | 'auth' | 'connect' = 'greeting';
    let nmethods = 0;
    let ulen = 0;
    let user = '';
    let tunnel: net.Socket | null = null;
    sock.on('error', () => { /* ignore */ });
    sock.on('data', (c) => {
      if (tunnel != null) { tunnel.write(c); return; }
      buf = Buffer.concat([buf, c]);
      for (;;) {
        if (stage === 'greeting') {
          if (buf.length < 2) return;
          nmethods = buf[1];
          if (buf.length < 2 + nmethods) return;
          const methods = [...buf.subarray(2, 2 + nmethods)];
          buf = buf.subarray(2 + nmethods);
          const chosen = this.requireAuth
            ? (methods.includes(SOCKS5_METHOD_USER_PASS) ? SOCKS5_METHOD_USER_PASS : SOCKS5_METHOD_NO_ACCEPTABLE)
            : (methods.includes(0x00) ? 0x00 : SOCKS5_METHOD_NO_ACCEPTABLE);
          sock.write(bytes(5, chosen));
          if (chosen === SOCKS5_METHOD_NO_ACCEPTABLE) { sock.destroy(); return; }
          stage = chosen === SOCKS5_METHOD_USER_PASS ? 'auth' : 'connect';
          continue;
        }
        if (stage === 'auth') {
          if (buf.length < 2) return;
          ulen = buf[1];
          if (buf.length < 2 + ulen + 1) return;
          const plen = buf[2 + ulen];
          if (buf.length < 3 + ulen + plen) return;
          user = buf.subarray(2, 2 + ulen).toString('utf8');
          const pass = buf.subarray(3 + ulen, 3 + ulen + plen).toString('utf8');
          buf = buf.subarray(3 + ulen + plen);
          this.authAttempts.push({ user, pass });
          const ok = user === this.validUser && pass === this.validPass;
          if (!ok) this.refusedAuthFailures++;
          sock.write(bytes(1, ok ? 0x00 : 0x01));
          if (!ok) { sock.destroy(); return; }
          stage = 'connect';
          continue;
        }
        // connect
        if (buf.length < 4) return;
        const atyp = buf[3];
        let host = '';
        let consumed = 0;
        if (atyp === 1) {
          if (buf.length < 10) return;
          host = [...buf.subarray(4, 8)].join('.');
          consumed = 10;
        } else if (atyp === 3) {
          const len = buf[4];
          if (buf.length < 5 + len + 2) return;
          host = buf.subarray(5, 5 + len).toString('utf8');
          consumed = 7 + len;
        } else if (atyp === 4) {
          if (buf.length < 22) return;
          host = '::1';
          consumed = 22;
        } else { sock.destroy(); return; }
        const port = (buf[consumed - 2] << 8) | buf[consumed - 1];
        buf = buf.subarray(consumed);
        this.connectAttempts++;
        if (this.connectReplyCode !== 0x00) {
          sock.write(bytes(5, this.connectReplyCode, 0, 1, 0, 0, 0, 0, 0, 0));
          sock.destroy();
          return;
        }
        sock.write(bytes(5, 0, 0, 1, 127, 0, 0, 1, (port >> 8) & 0xff, port & 0xff));
        const [th, tp] = this.targetAddr.split(':');
        tunnel = net.connect({ host: th, port: Number(tp) });
        tunnel.on('error', () => { try { sock.destroy(); } catch { /* ignore */ } });
        tunnel.on('data', (d) => { if (!sock.destroyed) sock.write(d); });
        tunnel.on('close', () => { try { sock.destroy(); } catch { /* ignore */ } });
        if (buf.length) tunnel.write(Buffer.from(buf));
        return;
      }
    });
  }
  stop() { this.server.close(); }
}

// Minimal fake SOCKS4 server (IPv4 targets; 4a domain form accepted).
class FakeSocks4 {
  server: net.Server;
  port = 0;
  connectAttempts = 0;
  private targetAddr: string;
  constructor(targetAddr: string) {
    this.targetAddr = targetAddr;
    this.server = net.createServer((s) => this.onConnection(s));
  }
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
    let buf = Buffer.alloc(0);
    let tunnel: net.Socket | null = null;
    sock.on('error', () => { /* ignore */ });
    sock.on('data', (c) => {
      if (tunnel != null) { tunnel.write(c); return; }
      buf = Buffer.concat([buf, c]);
      if (buf.length < 8) return;
      const port = (buf[2] << 8) | buf[3];
      const ip = [...buf.subarray(4, 8)];
      if (ip[0] === 0 && ip[1] === 0 && ip[2] === 0 && ip[3] !== 0) {
        // SOCKS4a sentinel: the live fake only serves IPv4 targets; the 4a
        // domain form is pinned by the pure byte-shape checks above.
        sock.destroy();
        return;
      }
      // Classic SOCKS4: VER CMD DSTPORT DSTIP USERID NUL — wait for the
      // terminating NUL so the request is fully consumed before tunnelling
      // (forwarding a partial request would poison the tunnel).
      const rest = buf.subarray(8);
      const z = rest.indexOf(0x00);
      if (z < 0) return;
      const consumed = 8 + z + 1;
      this.connectAttempts++;
      sock.write(bytes(0, 0x5a, 0, 0, 0, 0, 0, 0));
      const [th, tp] = this.targetAddr.split(':');
      tunnel = net.connect({ host: th, port: Number(tp) });
      tunnel.on('error', () => { try { sock.destroy(); } catch { /* ignore */ } });
      tunnel.on('data', (d) => { if (!sock.destroyed) sock.write(d); });
      tunnel.on('close', () => { try { sock.destroy(); } catch { /* ignore */ } });
      if (buf.length > consumed) tunnel.write(Buffer.from(buf.subarray(consumed)));
      buf = Buffer.alloc(0);
    });
  }
  stop() { this.server.close(); }
}

// Await a rejection; budget 30s+ (Windows loopback quirk: a just-closed port
// can take ~2s to refuse). Resolves with the error message, or 'NO-REJECT' /
// 'TIMEOUT' sentinels on wrong outcome.
async function expectReject(p: Promise<any>, budgetMs = 30000): Promise<string> {
  let result = 'NO-REJECT';
  const winner = await Promise.race([
    p.then(() => 'resolved', (e: Error) => { result = e.message; return 'settled'; }),
    new Promise<string>((resolve) => { const t = setTimeout(() => resolve('timeout'), budgetMs); t.unref?.(); }),
  ]);
  return winner === 'settled' ? result : `TIMEOUT(${winner})`;
}

function routeRequest(): RemotingCommand {
  return new RemotingCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC);
}

async function testLive(): Promise<void> {
  const target = new FakeTarget();
  await target.listen();

  // -- A: SOCKS5 no-auth, full RemotingClient roundtrip through the proxy --
  console.log('== SOCKS5 no-auth through the proxy ==');
  const p5 = new FakeSocks5(target.addr);
  await p5.listen();
  const rc5 = new RemotingClient({ connectTimeoutMillis: 10000, invokeTimeoutMillis: 20000 });
  rc5.socksProxyConfig = { addr: p5.addr } as SocksProxyConfig;
  const resp = await rc5.invokeSync(target.addr, routeRequest());
  check('invokeSync through SOCKS5 proxy succeeds', resp.code === ResponseCode.SUCCESS && resp.isResponseType(),
    `code=${resp.code}`);
  check('response body carried through the tunnel', resp.body != null && resp.body.toString().includes('socks-route-ok'));
  check('proxy saw the CONNECT to the target', p5.connectAttempts === 1 && rc5.isChannelWritable(target.addr),
    `attempts=${p5.connectAttempts}`);
  check('target received exactly one request', target.requests.length === 1, `requests=${target.requests.length}`);
  // A second invoke reuses the tunnel (no new CONNECT).
  await rc5.invokeSync(target.addr, routeRequest());
  check('second invoke reuses the tunnel (no new SOCKS handshake)', p5.connectAttempts === 1 && target.requests.length === 2,
    `connects=${p5.connectAttempts} requests=${target.requests.length}`);
  rc5.shutdown();
  p5.stop();

  // -- B: RFC 1929 username/password subnegotiation --
  console.log('== SOCKS5 user/pass subnegotiation ==');
  const p5a = new FakeSocks5(target.addr);
  p5a.requireAuth = true;
  await p5a.listen();
  const rc5a = new RemotingClient({ connectTimeoutMillis: 10000, invokeTimeoutMillis: 20000 });
  rc5a.socksProxyConfig = { addr: p5a.addr, username: 'socksu', password: 'socksp' };
  const respA = await rc5a.invokeSync(target.addr, routeRequest());
  check('auth subnegotiation + CONNECT succeed', respA.code === ResponseCode.SUCCESS, `code=${respA.code}`);
  check('proxy validated the credentials', p5a.authAttempts.length === 1
    && p5a.authAttempts[0].user === 'socksu' && p5a.authAttempts[0].pass === 'socksp',
    JSON.stringify(p5a.authAttempts));
  rc5a.shutdown();
  p5a.stop();

  // -- C: wrong password rejected --
  console.log('== rejection paths ==');
  const p5b = new FakeSocks5(target.addr);
  p5b.requireAuth = true;
  await p5b.listen();
  const rc5b = new RemotingClient({ connectTimeoutMillis: 20000, invokeTimeoutMillis: 30000 });
  rc5b.socksProxyConfig = { addr: p5b.addr, username: 'socksu', password: 'wrong' };
  const msgC = await expectReject(rc5b.invokeSync(target.addr, routeRequest()));
  check('wrong password rejected', msgC.includes('authentication failed'), msgC);
  check('proxy counted the failed auth', p5b.refusedAuthFailures === 1);
  rc5b.closeChannel(target.addr);
  rc5b.shutdown();
  p5b.stop();

  // -- D: proxy demands auth, client has none -> NO ACCEPTABLE METHODS --
  const p5c = new FakeSocks5(target.addr);
  p5c.requireAuth = true;
  await p5c.listen();
  const rc5c = new RemotingClient({ connectTimeoutMillis: 20000, invokeTimeoutMillis: 30000 });
  rc5c.socksProxyConfig = { addr: p5c.addr };
  const msgD = await expectReject(rc5c.invokeSync(target.addr, routeRequest()));
  check('no-credentials vs auth-demanding proxy rejected', msgD.includes('auth'), msgD);
  rc5c.closeChannel(target.addr);
  rc5c.shutdown();
  p5c.stop();

  // -- E: proxy refuses CONNECT (reply 0x05 connection refused) --
  const p5d = new FakeSocks5(target.addr);
  p5d.connectReplyCode = 0x05;
  await p5d.listen();
  const rc5d = new RemotingClient({ connectTimeoutMillis: 20000, invokeTimeoutMillis: 30000 });
  rc5d.socksProxyConfig = { addr: p5d.addr };
  const msgE = await expectReject(rc5d.invokeSync(target.addr, routeRequest()));
  check('proxy CONNECT refusal surfaces the reply code', msgE.includes('0x05') && msgE.includes('connection refused'), msgE);
  rc5d.closeChannel(target.addr);
  rc5d.shutdown();
  p5d.stop();

  // -- F: dead proxy port (loopback quirk: refusal can lag ~2s) --
  const dead = net.createServer();
  const deadPort: number = await new Promise((resolve) => dead.listen(0, '127.0.0.1', () => resolve((dead.address() as net.AddressInfo).port)));
  await new Promise<void>((resolve) => dead.close(() => resolve()));
  const rcDead = new RemotingClient({ connectTimeoutMillis: 20000, invokeTimeoutMillis: 30000 });
  rcDead.socksProxyConfig = { addr: `127.0.0.1:${deadPort}` };
  const msgF = await expectReject(rcDead.invokeSync(target.addr, routeRequest()));
  check('dead proxy port rejects within budget', msgF !== 'NO-REJECT' && !msgF.startsWith('TIMEOUT'), msgF);
  rcDead.shutdown();

  // -- G: SOCKS4 (IPv4 target) end to end --
  console.log('== SOCKS4 ==');
  const p4 = new FakeSocks4(target.addr);
  await p4.listen();
  const rc4 = new RemotingClient({ connectTimeoutMillis: 10000, invokeTimeoutMillis: 20000 });
  rc4.socksProxyConfig = { addr: p4.addr, version: 4 };
  const respG = await rc4.invokeSync(target.addr, routeRequest());
  check('invokeSync through SOCKS4 proxy succeeds', respG.code === ResponseCode.SUCCESS, `code=${respG.code}`);
  check('SOCKS4 proxy saw the CONNECT', p4.connectAttempts === 1, `attempts=${p4.connectAttempts}`);
  rc4.shutdown();
  p4.stop();

  target.stop();
}

// ---------------- 3. config plumbing ----------------

function testPlumbing(): void {
  console.log('== config plumbing ==');
  const producer = new DefaultMQProducer('GID_SOCKS_SMOKE');
  const cfg: SocksProxyConfig = { addr: '10.0.0.1:1080', username: 'u', password: 'p', version: 5 };
  check('producer setter returns this', producer.setSocksProxyConfig(cfg) === producer);
  check('producer getter round-trips', producer.getSocksProxyConfig() === cfg);
  check('producer default null', new DefaultMQProducer('GID_SOCKS_SMOKE2').socksProxyConfig === null);

  const consumer = new DefaultMQPushConsumer('GID_SOCKS_SMOKE');
  check('consumer setter returns this', consumer.setSocksProxyConfig(cfg) === consumer);

  // env surface: Java-shaped JSON map on ROCKETMQ_SOCKS_PROXY_CONFIG.
  process.env['ROCKETMQ_SOCKS_PROXY_CONFIG'] = JSON.stringify({ '0.0.0.0/0': { addr: '10.0.0.2:1080' } });
  const rcEnv = new RemotingClient({});
  check('RemotingClient picks up env JSON map', rcEnv.socksProxyConfig != null && rcEnv.socksProxyConfig.addr === '10.0.0.2:1080');
  delete process.env['ROCKETMQ_SOCKS_PROXY_CONFIG'];
  check('RemotingClient without env/config stays direct', new RemotingClient({}).socksProxyConfig === null);
  check('explicit opts override env', (() => {
    process.env['ROCKETMQ_SOCKS_PROXY_CONFIG'] = JSON.stringify({ addr: '10.0.0.3:1080' });
    try {
      return new RemotingClient({ socksProxyConfig: null }).socksProxyConfig === null;
    } finally { delete process.env['ROCKETMQ_SOCKS_PROXY_CONFIG']; }
  })());
  const rcOpt = new RemotingClient({ socksProxyConfig: { addr: '10.0.0.4:1080' } });
  check('RemotingClient ctor opts wired', rcOpt.socksProxyConfig != null && rcOpt.socksProxyConfig.addr === '10.0.0.4:1080');
}

(async () => {
  testPure();
  await testLive();
  testPlumbing();
  console.log(`\n${fail === 0 ? `ALL GREEN (${pass} checks)` : `${fail} failure(s)`}`);
  process.exit(fail === 0 ? 0 : 1);
})();
