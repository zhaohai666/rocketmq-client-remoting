// -*- coding: utf-8 -*-
// SOCKS proxy support for the remoting transport layer.
import net from 'node:net';
import { Buffer } from 'node:buffer';

export interface SocksProxyConfig {
  addr: string;
  // Optional RFC 1929 username (SOCKS5) / userid (SOCKS4). Empty/absent = no auth.
  username?: string | null;
  password?: string | null;
  version?: number | string | null;
}

export const SOCKS5_METHOD_NO_AUTH = 0x00;
export const SOCKS5_METHOD_GSSAPI = 0x01;
export const SOCKS5_METHOD_USER_PASS = 0x02;
export const SOCKS5_METHOD_NO_ACCEPTABLE = 0xff;

export const SOCKS4_GRANTED = 0x5a; // 90

export const SOCKS5_REPLY_NAMES: Record<number, string> = {
  0x01: 'general SOCKS server failure',
  0x02: 'connection not allowed by ruleset',
  0x03: 'network unreachable',
  0x04: 'host unreachable',
  0x05: 'connection refused',
  0x06: 'TTL expired',
  0x07: 'command not supported',
  0x08: 'address type not supported',
};

export function socks5ReplyName(code: number): string {
  return SOCKS5_REPLY_NAMES[code] != null ? SOCKS5_REPLY_NAMES[code] : `unknown reply code`;
}

// Normalize the `version` field to 4 | 5. Default 5.
export function proxyVersion(proxy: SocksProxyConfig): 4 | 5 {
  const v = proxy.version;
  if (v == null || v === '') return 5;
  const s = String(v).trim().toUpperCase();
  if (s === '5' || s === 'SOCKS5') return 5;
  if (s === '4' || s === 'SOCKS4' || s === 'SOCKS4A') return 4;
  throw new Error(`socks proxy: unsupported version '${String(v)}' (use 4/5 or 'SOCKS4'/'SOCKS5')`);
}

// 'host:port' -> [host, port] (IPv6 literal needs brackets, like _parseAddr in client.ts).
export function parseProxyAddr(addr: string | null | undefined): [string, number] {
  if (addr == null || addr === '') throw new Error('socks proxy addr is empty (expected host:port)');
  let host: string;
  let portStr: string;
  if (addr.startsWith('[')) {
    const idx = addr.indexOf(']');
    if (idx < 0) throw new Error(`socks proxy addr must be host:port, got '${addr}'`);
    host = addr.slice(1, idx);
    portStr = addr.slice(idx + 2);
  } else {
    const idx = addr.lastIndexOf(':');
    if (idx <= 0) throw new Error(`socks proxy addr must be host:port, got '${addr}'`);
    host = addr.slice(0, idx);
    portStr = addr.slice(idx + 1);
  }
  const port = Number(portStr);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`socks proxy addr has an invalid port: '${addr}'`);
  }
  if (!host) throw new Error(`socks proxy addr has an empty host: '${addr}'`);
  return [host, port];
}

function checkPort(port: number): void {
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`socks proxy: invalid target port ${port}`);
  }
}

// ---------------- SOCKS5 (RFC 1928) ----------------

// Method selection greeting. We offer no-auth always and user/pass when
// credentials are configured — the same offer space netty's Socks5ProxyHandler has.
export function buildSocks5Greeting(withUserPass: boolean): Buffer {
  return withUserPass ? Buffer.from([0x05, 0x02, SOCKS5_METHOD_NO_AUTH, SOCKS5_METHOD_USER_PASS])
    : Buffer.from([0x05, 0x01, SOCKS5_METHOD_NO_AUTH]);
}

// Server's method choice: VER METHOD. Returns the chosen method byte.
export function parseSocks5MethodSelection(buf: Buffer): number {
  if (buf.length < 2 || buf[0] !== 0x05) throw new Error('socks5: malformed method selection reply');
  return buf[1];
}

// RFC 1929 username/password subnegotiation request: VER(1) ULEN UNAME PLEN PASSWD.
export function buildSocks5UserPassAuth(username: string, password: string): Buffer {
  const u = Buffer.from(username, 'utf8');
  const p = Buffer.from(password, 'utf8');
  if (u.length > 255 || p.length > 255) throw new Error('socks5: username/password longer than 255 bytes');
  return Buffer.concat([Buffer.from([0x01, u.length]), u, Buffer.from([p.length]), p]);
}

// RFC 1929 reply: VER(1) STATUS(1); 0x00 = success.
export function parseSocks5UserPassReply(buf: Buffer): void {
  if (buf.length < 2 || buf[0] !== 0x01) throw new Error('socks5: malformed username/password auth reply');
  if (buf[1] !== 0x00) {
    throw new Error(`socks5: username/password authentication failed (status 0x${buf[1].toString(16).padStart(2, '0')})`);
  }
}

// CONNECT request: VER CMD RSV ATYP ADDR PORT. Host may be IPv4/IPv6 literal or
// a domain name (ATYP 3). SOCKS4a-style: domains stay unresolved on the wire.
export function buildSocks5Connect(host: string, port: number): Buffer {
  checkPort(port);
  const head = Buffer.from([0x05, 0x01, 0x00]);
  const tail = Buffer.from([(port >> 8) & 0xff, port & 0xff]);
  if (net.isIPv4(host)) {
    return Buffer.concat([head, Buffer.from([0x01]), Buffer.from(host.split('.').map((x) => parseInt(x, 10))), tail]);
  }
  if (net.isIPv6(host)) {
    return Buffer.concat([head, Buffer.from([0x04]), Buffer.from(new Uint8Array(ip6Bytes(host))), tail]);
  }
  const d = Buffer.from(host, 'utf8');
  if (d.length > 255) throw new Error(`socks5: target host longer than 255 bytes: '${host}'`);
  return Buffer.concat([head, Buffer.from([0x03, d.length]), d, tail]);
}

function ip6Bytes(host: string): number[] {
  const bytes: number[] = [];
  // net.isIPv6 checked by the caller; node parses via `net.isIPv6`, but there is
  // no public API for the byte form — use URL-ish split on ':' groups.
  const groups = host.replace(/^\[|\]$/g, '').split('::');
  const left = groups[0] === '' ? [] : groups[0].split(':');
  const right = groups.length > 1 ? (groups[1] === '' ? [] : groups[1].split(':')) : [];
  const mid = 8 - left.length - right.length;
  const all = [...left, ...Array.from({ length: Math.max(0, mid) }, () => '0'), ...right];
  for (const g of all) {
    const v = parseInt(g, 16);
    bytes.push((v >> 8) & 0xff, v & 0xff);
  }
  return bytes;
}

// Full success reply parse (used by tests and the handshake driver's tail):
// returns the BND.ADDR/BND.PORT the proxy reports.
export function parseSocks5ConnectReply(buf: Buffer): { boundAddr: string; boundPort: number } {
  if (buf.length < 8) throw new Error('socks5: truncated CONNECT reply');
  if (buf[0] !== 0x05) throw new Error(`socks5: bad CONNECT reply version ${buf[0]}`);
  if (buf[1] !== 0x00) {
    throw new Error(`socks5 CONNECT refused (0x${buf[1].toString(16).padStart(2, '0')}): ${socks5ReplyName(buf[1])}`);
  }
  const atyp = buf[3];
  let boundAddr: string;
  let portOff: number;
  if (atyp === 0x01) {
    boundAddr = `${buf[4]}.${buf[5]}.${buf[6]}.${buf[7]}`;
    portOff = 8;
  } else if (atyp === 0x03) {
    const len = buf[4];
    if (buf.length < 5 + len + 2) throw new Error('socks5: truncated CONNECT reply (domain)');
    boundAddr = buf.subarray(5, 5 + len).toString('utf8');
    portOff = 5 + len;
  } else if (atyp === 0x04) {
    if (buf.length < 22) throw new Error('socks5: truncated CONNECT reply (IPv6)');
    const parts: string[] = [];
    for (let i = 0; i < 16; i += 2) parts.push(((buf[4 + i] << 8) | buf[5 + i]).toString(16));
    boundAddr = parts.join(':');
    portOff = 20;
  } else {
    throw new Error(`socks5: unsupported reply address type 0x${atyp.toString(16)}`);
  }
  return { boundAddr, boundPort: (buf[portOff] << 8) | buf[portOff + 1] };
}

// ---------------- SOCKS4 / SOCKS4a ----------------

// SOCKS4 request: VER CMD DSTPORT DSTIP USERID NULL. A domain name target is
// sent as SOCKS4a: DSTIP 0.0.0.x (x != 0) sentinel + USERID NULL + DOMAIN NULL.
export function buildSocks4Connect(host: string, port: number, userid = ''): Buffer {
  checkPort(port);
  const head = Buffer.from([0x04, 0x01, (port >> 8) & 0xff, port & 0xff]);
  const user = Buffer.from(userid, 'utf8');
  if (net.isIPv4(host)) {
    const ip = Buffer.from(host.split('.').map((x) => parseInt(x, 10)));
    return Buffer.concat([head, ip, user, Buffer.from([0x00])]);
  }
  const d = Buffer.from(host, 'utf8');
  if (d.length > 255) throw new Error(`socks4a: target host longer than 255 bytes: '${host}'`);
  return Buffer.concat([head, Buffer.from([0x00, 0x00, 0x00, 0x01]), user, Buffer.from([0x00]), d, Buffer.from([0x00])]);
}

// SOCKS4 reply: 8 bytes, VN(0) CD; 0x5A (90) = granted.
export function parseSocks4ConnectReply(buf: Buffer): void {
  if (buf.length < 8 || buf[0] !== 0x00) throw new Error('socks4: malformed CONNECT reply');
  const cd = buf[1];
  if (cd === SOCKS4_GRANTED) return;
  const names: Record<number, string> = {
    0x5b: 'request rejected or failed',
    0x5c: 'request rejected: identd not reachable',
    0x5d: 'request rejected: identd user mismatch',
  };
  const why = names[cd] != null ? names[cd] : 'unknown reply code';
  throw new Error(`socks4 CONNECT refused (0x${cd.toString(16)}): ${why}`);
}

// ---------------- config parsing (env surface) ----------------

export function parseSocksProxyConfig(json: string | null | undefined): SocksProxyConfig | null {
  if (json == null) return null;
  const s = String(json).trim();
  if (s === '' || s === '{}') return null;
  let obj: any;
  try { obj = JSON.parse(s); } catch (e) {
    throw new Error(`socks proxy config is not valid JSON: ${(e as Error).message}`);
  }
  if (obj == null || typeof obj !== 'object' || Array.isArray(obj)) return null;
  if (typeof obj['addr'] === 'string') return obj as SocksProxyConfig;
  for (const v of Object.values(obj)) {
    if (v != null && typeof v === 'object' && typeof (v as any)['addr'] === 'string') {
      return v as SocksProxyConfig;
    }
  }
  return null;
}

// ---------------- handshake driver ----------------

// Streaming reader over the proxy socket: buffers inbound bytes across reads so
// no data is lost between handshake steps, and pushes unconsumed bytes back via
// socket.unshift() on release (the tunnel may carry the first remoting frame in
// the same TCP segment as the handshake reply).
class _SocksStreamReader {
  private buf: Buffer = Buffer.alloc(0);
  private failed: Error | null = null;
  private closed = false;
  private waiter: { n: number; resolve: (b: Buffer) => void; reject: (e: Error) => void; timer: NodeJS.Timeout } | null = null;
  sock: any;
  private onData: (c: Buffer) => void;
  private onErr: (e: Error) => void;
  private onClose: () => void;

  constructor(sock: any) {
    this.sock = sock;
    this.onData = (c: Buffer) => {
      this.buf = this.buf.length ? Buffer.concat([this.buf, Buffer.from(c)]) : Buffer.from(c);
      this._maybe();
    };
    this.onErr = (e: Error) => {
      this.failed = new Error(`socks proxy socket error: ${e.message}`);
      this._fail(this.failed);
    };
    this.onClose = () => {
      this.closed = true;
      this._fail(new Error('socks proxy closed the connection during handshake'));
    };
    sock.on('data', this.onData);
    sock.on('error', this.onErr);
    sock.on('close', this.onClose);
  }

  private _fail(e: Error): void {
    if (this.waiter) {
      clearTimeout(this.waiter.timer);
      const w = this.waiter;
      this.waiter = null;
      w.reject(e);
    }
  }

  private _maybe(): void {
    if (!this.waiter || this.buf.length < this.waiter.n) return;
    const w = this.waiter;
    this.waiter = null;
    clearTimeout(w.timer);
    const out = Buffer.from(this.buf.subarray(0, w.n));
    this.buf = this.buf.subarray(w.n);
    w.resolve(out);
  }

  read(n: number, budgetMs: number): Promise<Buffer> {
    if (this.failed) return Promise.reject(this.failed);
    if (this.closed) return Promise.reject(new Error('socks proxy closed the connection during handshake'));
    if (this.buf.length >= n) {
      const out = Buffer.from(this.buf.subarray(0, n));
      this.buf = this.buf.subarray(n);
      return Promise.resolve(out);
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiter = null;
        reject(new Error(`socks handshake timed out after ${budgetMs}ms`));
      }, budgetMs);
      this.waiter = { n, resolve, reject, timer };
    });
  }

  // Detach our listeners and push unconsumed bytes back into the socket so the
  // caller's own 'data' handler (frame parser) sees them.
  release(): void {
    this.sock.removeListener('data', this.onData);
    this.sock.removeListener('error', this.onErr);
    this.sock.removeListener('close', this.onClose);
    if (this.waiter) {
      clearTimeout(this.waiter.timer);
      this.waiter = null;
    }
    if (this.buf.length) {
      try { this.sock.unshift(this.buf); } catch (e) { /* stream already gone */ }
      this.buf = Buffer.alloc(0);
    }
  }
}

function writeAll(sock: any, buf: Buffer): Promise<void> {
  return new Promise((resolve, reject) => {
    sock.write(buf, (err?: Error | null) => (err ? reject(err) : resolve()));
  });
}

function assertOk(verByte: number, what: string): void {
  if (verByte !== 0x05) throw new Error(`socks5: bad ${what} reply version ${verByte}`);
}

async function _socks5Handshake(reader: _SocksStreamReader, proxy: SocksProxyConfig,
  host: string, port: number, left: () => number): Promise<void> {
  const hasAuth = (proxy.username != null && proxy.username !== '')
    || (proxy.password != null && proxy.password !== '');
  await writeAll(reader.sock, buildSocks5Greeting(hasAuth));
  const method = parseSocks5MethodSelection(await reader.read(2, left()));
  if (method === SOCKS5_METHOD_NO_ACCEPTABLE) {
    throw new Error(hasAuth
      ? 'socks5: proxy accepts none of the offered auth methods'
      : 'socks5: proxy requires an auth method but no username/password is configured');
  }
  if (method === SOCKS5_METHOD_USER_PASS) {
    if (!hasAuth) throw new Error('socks5: proxy demands username/password auth but none is configured');
    await writeAll(reader.sock, buildSocks5UserPassAuth(proxy.username ?? '', proxy.password ?? ''));
    parseSocks5UserPassReply(await reader.read(2, left()));
  } else if (method !== SOCKS5_METHOD_NO_AUTH) {
    throw new Error(`socks5: proxy chose unsupported auth method 0x${method.toString(16).padStart(2, '0')}`);
  }
  await writeAll(reader.sock, buildSocks5Connect(host, port));
  const head = await reader.read(4, left());
  assertOk(head[0], 'CONNECT');
  if (head[1] !== 0x00) {
    throw new Error(`socks5 CONNECT refused (0x${head[1].toString(16).padStart(2, '0')}): ${socks5ReplyName(head[1])}`);
  }
  // Drain the BND.ADDR/BND.PORT tail (length depends on ATYP; domain needs its
  // length byte first).
  if (head[3] === 0x03) {
    const len = (await reader.read(1, left()))[0];
    if (len > 0) await reader.read(len + 2, left());
    else await reader.read(2, left());
  } else if (head[3] === 0x01) {
    await reader.read(6, left());
  } else if (head[3] === 0x04) {
    await reader.read(18, left());
  }
}

async function _socks4Handshake(reader: _SocksStreamReader, proxy: SocksProxyConfig,
  host: string, port: number, left: () => number): Promise<void> {
  await writeAll(reader.sock, buildSocks4Connect(host, port, proxy.username ?? ''));
  parseSocks4ConnectReply(await reader.read(8, left()));
}

export async function socksHandshake(socket: any, proxy: SocksProxyConfig,
  targetHost: string, targetPort: number, timeoutMillis = 10000): Promise<void> {
  const version = proxyVersion(proxy); // validates addr too
  parseProxyAddr(proxy.addr);
  const reader = new _SocksStreamReader(socket);
  const deadline = Date.now() + Math.max(1, timeoutMillis);
  const left = () => Math.max(1, deadline - Date.now());
  try {
    if (version === 5) await _socks5Handshake(reader, proxy, targetHost, targetPort, left);
    else await _socks4Handshake(reader, proxy, targetHost, targetPort, left);
  } finally {
    reader.release();
  }
}
