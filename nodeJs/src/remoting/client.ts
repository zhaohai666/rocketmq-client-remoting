import net from 'node:net';
import tls from 'node:tls';
import fs from 'node:fs';
import { once } from 'node:events';
import { Buffer } from 'node:buffer';
import { ResponseCode } from './codes.ts';
import { RemotingCommand } from './remotingCommand.ts';
import { RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException } from './exception.ts';
import { parseProxyAddr, parseSocksProxyConfig, socksHandshake } from './socks_proxy.ts';
import type { SocksProxyConfig } from './socks_proxy.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('remoting.client');
const MAX_FRAME_LENGTH = 16 * 1024 * 1024;
// Idle-connection close default: a connection that has neither read nor written
// for this long is swept away. A configured value that is not positive falls back
// to this default (it does not disable the sweep).
const DEFAULT_MAX_IDLE_TIME_MILLIS = 120 * 1000;

interface ResponseFuture {
  opaque: number;
  timeoutMillis: number;
  resolve: (cmd: RemotingCommand) => void;
  reject: (err: Error) => void;
  onResponse?: (cmd: RemotingCommand | null, err: Error | null) => void;
  timer?: NodeJS.Timeout;
  startTime: number;
  addr: string;
  request: RemotingCommand | null;
  conn?: any;
  sendRequestOK: boolean;
  fired: boolean;
}

// TLS details:
//   - caCert given → strict verification: the broker certificate must
//     chain to that CA and the hostname/SAN must match (host is used by default; serverName can override);
//     not given → test-mode: trust self-signed (rejectUnauthorized=false, historical behavior).
//   - clientCert/clientKey → mTLS client certificate.
export interface TlsOptions {
  caCert?: string;
  clientCert?: string;
  clientKey?: string;
  serverName?: string;
}

export class RemotingClient {
  connectTimeoutMillis = 3000;
  invokeTimeoutMillis = 15000;
  enableReconnectForGoAway = true;
  // Idle-connection close limit, in milliseconds (120 s by default). A value <= 0
  // falls back to the default rather than disabling the sweep.
  maxIdleTimeMillis = DEFAULT_MAX_IDLE_TIME_MILLIS;
  // When a synchronous invoke's own wait times out, close the connection first.
  // A port that does not subtract the connect cost effectively always closes for
  // any positive timeout; the flag is the master switch for that behaviour.
  closeSocketOnSyncTimeout = true;
  tlsEnable = false;
  tlsOptions: TlsOptions | null = null;
  socksProxyConfig: SocksProxyConfig | null = null;
  private conns = new Map<string, any>();
  private buffers = new Map<string, Buffer>();
  private responseTable = new Map<number, ResponseFuture>();
  private rpcHooks: any[] = [];
  private processors = new Map<number, (cmd: RemotingCommand, addr: string) => RemotingCommand | null | Promise<RemotingCommand | null>>();
  private running = true;
  private connectPromises = new Map<string, Promise<any>>();
  // Per-connection last-activity stamp (address -> epoch millis), refreshed on
  // every successful read and every completed write; consumed by the idle sweep.
  private lastActivity = new Map<string, number>();
  // The idle sweep timer. It is unref()'d so it never keeps a test process alive,
  // and cleared on shutdown.
  private idleSweepTimer: NodeJS.Timeout | null = null;

  constructor(opts: { connectTimeoutMillis?: number; invokeTimeoutMillis?: number; maxIdleTimeMillis?: number; closeSocketOnSyncTimeout?: boolean; tlsEnable?: boolean; tlsOptions?: TlsOptions | null; socksProxyConfig?: SocksProxyConfig | null } = {}) {
    if (opts.connectTimeoutMillis != null) this.connectTimeoutMillis = opts.connectTimeoutMillis;
    if (opts.invokeTimeoutMillis != null) this.invokeTimeoutMillis = opts.invokeTimeoutMillis;
    if (opts.maxIdleTimeMillis != null) this.maxIdleTimeMillis = opts.maxIdleTimeMillis;
    if (opts.closeSocketOnSyncTimeout != null) this.closeSocketOnSyncTimeout = opts.closeSocketOnSyncTimeout;
    if (opts.tlsEnable != null) this.tlsEnable = opts.tlsEnable;
    else {
      const e = (process.env['ROCKETMQ_TLS_ENABLE'] || '').trim().toLowerCase();
      this.tlsEnable = e === '1' || e === 'true' || e === 'yes';
    }
    if (opts.tlsOptions !== undefined) this.tlsOptions = opts.tlsOptions;
    if (opts.socksProxyConfig !== undefined) this.socksProxyConfig = opts.socksProxyConfig;
    else {
      const env = (process.env['ROCKETMQ_SOCKS_PROXY_CONFIG'] || '').trim();
      if (env) this.socksProxyConfig = parseSocksProxyConfig(env);
    }
  }

  // ---------------- connection management ----------------
  private _parseAddr(addr: string): [string, number] {
    if (addr.startsWith('[')) {
      const idx = addr.indexOf(']');
      const host = addr.slice(1, idx);
      const port = parseInt(addr.slice(idx + 2), 10);
      return [host, port];
    }
    const idx = addr.lastIndexOf(':');
    return [addr.slice(0, idx), parseInt(addr.slice(idx + 1), 10)];
  }

  // TLS ConnectionOptions WITHOUT host/port/socket — shared by the direct dial
  // and the SOCKS-tunnel upgrade path (identical cert handling in both).
  private async _tlsOptions(host: string): Promise<tls.ConnectionOptions> {
    const t = this.tlsOptions;
    const strict = t?.caCert != null;
    const opts: tls.ConnectionOptions = { rejectUnauthorized: strict };
    if (strict) {
      opts.ca = [await fs.promises.readFile(t!.caCert!)];
      // servername defaults to host; set it only when explicitly overridden and different (node emits
      // DEP0123 deprecation for IP SNI, but the certificate's IP SAN check still follows host).
      if (t!.serverName != null && t!.serverName !== '' && t!.serverName !== host) {
        opts.servername = t!.serverName;
      }
      if (t!.clientCert != null) opts.cert = [await fs.promises.readFile(t!.clientCert)];
      if (t!.clientKey != null) opts.key = [await fs.promises.readFile(t!.clientKey)];
    }
    return opts;
  }

  private async _connect(addr: string): Promise<any> {
    const existing = this.conns.get(addr);
    if (existing && !existing.destroyed) return existing;
    let p = this.connectPromises.get(addr);
    if (p) return p;
    p = (async () => {
      const [host, port] = this._parseAddr(addr);
      const proxy = this.socksProxyConfig;
      let sock: any;
      if (proxy != null) {
        const [phost, pport] = parseProxyAddr(proxy.addr);
        sock = net.connect({ host: phost, port: pport });
      } else if (this.tlsEnable) {
        // caCert given → strict verification: 'secureConnect' is not emitted until the handshake
        // (certificate chain + hostname check) completes — a wrong CA / mismatched hostname errors at
        // connect time rather than on first write. test-mode (no caCert) keeps rejectUnauthorized:false.
        sock = tls.connect({ ...(await this._tlsOptions(host)), host, port });
      } else {
        sock = net.connect({ host, port });
      }
      sock.setNoDelay(true);
      const timer = setTimeout(() => sock.destroy(new Error('connect timeout')), this.connectTimeoutMillis);
      try {
        if (proxy != null) {
          // TCP to the proxy is connected → SOCKS CONNECT handshake (before the TLS handshake).
          const rawSock = sock;
          try {
            await once(rawSock, 'connect');
            await socksHandshake(rawSock, proxy, host, port, this.connectTimeoutMillis);
            if (this.tlsEnable) {
              // Upgrade to TLS inside the tunnel: SNI uses the target host (serverName override was set in _tlsOptions).
              const opts = await this._tlsOptions(host);
              if (opts.servername == null) opts.servername = host;
              opts.socket = rawSock;
              sock = tls.connect(opts);
              sock.setNoDelay(true);
              await once(sock, 'secureConnect');
            }
          } catch (e) {
            // Handshake failed: the tunnel must not enter the connection pool and must be torn down to avoid a dangling half-open connection.
            try { if (!rawSock.destroyed) rawSock.destroy(); } catch { /* ignore */ }
            throw e;
          }
        } else {
          await once(sock, this.tlsEnable && this.tlsOptions?.caCert != null ? 'secureConnect' : 'connect');
        }
      } finally {
        clearTimeout(timer);
      }
      this.conns.set(addr, sock);
      this.buffers.set(addr, Buffer.alloc(0));
      this.lastActivity.set(addr, Date.now());
      this._ensureIdleSweep();
      sock.on('data', (chunk: Buffer) => this._onData(addr, sock, chunk));
      sock.on('close', () => this._onClose(addr, sock));
      sock.on('error', (e: Error) => { logger.debug('socket error %s: %s', addr, (e as Error).message); });
      return sock;
    })();
    this.connectPromises.set(addr, p);
    try { return await p; }
    finally { this.connectPromises.delete(addr); }
  }

  isChannelWritable(addr: string): boolean {
    // "No connection yet" is not "cannot write": an absent or already-closed
    // socket reports writable (there is nothing to block on). Only a live socket
    // whose outbound buffer has hit the backpressure watermark is unwritable.
    const c = this.conns.get(addr);
    if (c == null || c.destroyed) return true;
    return !(c.writableNeedDrain === true || c.writableLength > c.writableHighWaterMark);
  }
  closeChannel(addr: string) {
    const c = this.conns.get(addr);
    this.conns.delete(addr);
    this.buffers.delete(addr);
    this.lastActivity.delete(addr);
    if (c && !c.destroyed) c.destroy();
  }

  // Take down exactly this socket the way a peer close does: destroying it fires
  // 'close' -> _onClose -> _failFast, so its pending entries fail fast while other
  // connections stay put.
  private _closeSocketForFrameError(sock: any) {
    try { if (sock && !sock.destroyed) sock.destroy(); } catch { /* ignore */ }
  }

  // ---------------- read loop ----------------
  private _onData(addr: string, sock: any, chunk: Buffer) {
    this.lastActivity.set(addr, Date.now());
    let buf = this.buffers.get(addr) || Buffer.alloc(0);
    buf = Buffer.concat([buf, chunk]);
    for (;;) {
      if (buf.length < 4) break;
      const totalLen = buf.readInt32BE(0);
      if (totalLen <= 0 || totalLen > MAX_FRAME_LENGTH) {
        // A bad length prefix desynchronises the whole stream, so it is not
        // trustworthy: log and take the connection down like a peer close.
        logger.error('illegal frame length %d from %s (must be 1..%d), closing connection', totalLen, addr, MAX_FRAME_LENGTH);
        this.buffers.set(addr, Buffer.alloc(0));
        this._closeSocketForFrameError(sock);
        return;
      }
      if (buf.length < 4 + totalLen) break;
      const frame = buf.subarray(0, 4 + totalLen);
      buf = buf.subarray(4 + totalLen);
      if (!this._dispatch(Buffer.from(frame), addr, sock)) return;
    }
    this.buffers.set(addr, buf);
  }

  private _onClose(addr: string, sock: any) {
    // Only drop the pooled entry if it still points at the socket that closed;
    // a reconnect to the same address must not be clobbered by the old socket's
    // late 'close'. The socket-bound fail-fast runs regardless.
    if (this.conns.get(addr) === sock) {
      this.conns.delete(addr);
      this.buffers.delete(addr);
      this.lastActivity.delete(addr);
    }
    this._failFast(addr, sock);
  }

  // Returns true when the connection is still usable, false when this frame tore
  // the connection down and the read loop must stop.
  private _dispatch(frame: Buffer, addr: string, sock: any): boolean {
    let cmd: RemotingCommand;
    try { cmd = RemotingCommand.decode(frame); }
    catch (e) {
      // The bytes no longer line up with the frame boundaries, so nothing that arrives
      // after this frame on this stream can be trusted: report it and take the
      // connection down the way a peer close does, leaving the other connections alone.
      logger.error('decode response failed from %s (%s bytes): %s, closing connection',
        addr, frame.length, (e instanceof Error ? e.message : String(e)));
      this.buffers.set(addr, Buffer.alloc(0));
      this._closeSocketForFrameError(sock);
      return false;
    }
    if (cmd.isResponseType()) {
      const fut = this.responseTable.get(cmd.opaque);
      if (fut) {
        this.responseTable.delete(cmd.opaque);
        fut.conn = sock;
        this._complete(fut, cmd);
      } else {
        // No pending request owns this opaque: drop it, keep the connection, but
        // say so instead of swallowing it in silence.
        logger.warning('receive response, but not matched any request: opaque=%s, addr=%s', cmd.opaque, addr);
      }
      return true;
    }
    const fut = this.responseTable.get(cmd.opaque);
    if (fut) {
      this.responseTable.delete(cmd.opaque);
      fut.conn = sock;
      this._complete(fut, cmd);
      return true;
    }
    const handler = this.processors.get(cmd.code);
    if (handler) {
      Promise.resolve(handler(cmd, addr)).then((response) => {
        if (response && !cmd.isOnewayRpc()) {
          response.opaque = cmd.opaque;
          this._write(addr, response).catch((e) => logger.warning('failed to write response: %s', (e as Error).message));
        }
      }).catch((e) => {
        logger.warning('processor for code %s raised: %s', cmd.code, (e as Error).message);
      });
    } else {
      logger.debug('no processor for request code %s (opaque=%s) from %s', cmd.code, cmd.opaque, addr);
    }
    return true;
  }

  registerProcessor(requestCode: number, handler: (cmd: RemotingCommand, addr: string) => RemotingCommand | null | Promise<RemotingCommand | null>) {
    this.processors.set(requestCode, handler);
  }
  unregisterProcessor(requestCode: number) { this.processors.delete(requestCode); }

  // ---------------- rpc hooks ----------------
  registerRpcHook(hook: any) { this.rpcHooks.push(hook); }
  unregisterRpcHook(hook: any) {
    const i = this.rpcHooks.indexOf(hook);
    if (i >= 0) this.rpcHooks.splice(i, 1);
  }
  private _applyBeforeRequestHooks(addr: string, cmd: RemotingCommand) {
    for (const hook of this.rpcHooks.slice()) hook.doBeforeRequest(addr, cmd);
  }
  private _applyAfterResponseHooks(addr: string, request: RemotingCommand | null, response: RemotingCommand | null) {
    if (!this.rpcHooks.length || request == null) return;
    for (const hook of this.rpcHooks.slice()) {
      try { hook.doAfterResponse(addr, request, response); } catch (e) { /* swallow */ }
    }
  }

  // ---------------- write ----------------
  private async _send(addr: string, request: RemotingCommand): Promise<any> {
    this._applyBeforeRequestHooks(addr, request);
    return this._write(addr, request);
  }
  private async _write(addr: string, request: RemotingCommand): Promise<any> {
    const sock = await this._connect(addr);
    const data = request.encode();
    await new Promise<void>((resolve, reject) => {
      sock.write(data, (err?: Error) => {
        if (err) reject(err);
        else { this.lastActivity.set(addr, Date.now()); resolve(); }
      });
    });
    return sock;
  }
  private async _writeResponse(addr: string, request: RemotingCommand, response: RemotingCommand) {
    try { await this._write(addr, response); }
    catch (e) { logger.warning('failed to write response (code=%s) to %s: %s', response.code, addr, (e as Error).message); }
  }

  // ---------------- invoke sync ----------------
  async invokeSync(addr: string, request: RemotingCommand, timeoutMillis?: number): Promise<RemotingCommand> {
    const timeout = timeoutMillis ?? this.invokeTimeoutMillis;
    const started = Date.now();
    const response = await this._invokeOnce(addr, request, timeout);
    if (response.code !== ResponseCode.GO_AWAY) return response;
    return this._handleGoAway(addr, request, timeout, started, response);
  }

  private async _handleGoAway(addr: string, request: RemotingCommand, timeout: number,
    started: number, _response: RemotingCommand): Promise<RemotingCommand> {
    if (!this.enableReconnectForGoAway) throw new RemotingSendRequestException(addr, `Receive GO_AWAY from channel ${addr}`);
    logger.info('receive GO_AWAY from %s, reconnect and retry once', addr);
    this.closeChannel(addr);
    const spent = Date.now() - started;
    const retryTimeout = Math.max(1, timeout - spent);
    const retry = this._retryRequest(request);
    const response = await this._invokeOnce(addr, retry, retryTimeout);
    if (response.code === ResponseCode.GO_AWAY) throw new RemotingSendRequestException(addr, `Receive GO_AWAY twice from channel ${addr}`);
    return response;
  }

  private _retryRequest(request: RemotingCommand): RemotingCommand {
    const retry = new RemotingCommand(request.code, request.customHeader, request.remark, RemotingCommand.createNewRequestId(), request.flag, request.body);
    retry.language = request.language;
    retry.version = request.version;
    retry.extFields = { ...request.extFields };
    retry.serializeTypeCurrentRpc = request.serializeTypeCurrentRpc;
    return retry;
  }

  // Whether a sync invoke that hits its own deadline should close the connection.
  // Gated by the master flag and by the time still available when the wait began:
  // close unless that budget is both <= 100 ms and <= timeout / 4. This client
  // schedules the deadline as a plain setTimeout(timeout) and does not subtract the
  // connect cost, so the budget when the wait began is the whole timeout.
  private _shouldCloseOnSyncTimeout(timeoutMillis: number): boolean {
    if (!this.closeSocketOnSyncTimeout) return false;
    const left = timeoutMillis;
    return !(left <= 100 && left <= timeoutMillis / 4);
  }

  private _invokeOnce(addr: string, request: RemotingCommand, timeout: number): Promise<RemotingCommand> {
    return new Promise((resolve, reject) => {
      const fut: ResponseFuture = {
        opaque: request.opaque, timeoutMillis: timeout, resolve, reject,
        startTime: Date.now(), addr, request, sendRequestOK: false, fired: false,
      };
      this.responseTable.set(request.opaque, fut);
      fut.timer = setTimeout(() => {
        if (!this.responseTable.delete(request.opaque)) return;
        if (this._shouldCloseOnSyncTimeout(timeout)) {
          logger.warning('invokeSync timed out, closing connection to %s (opaque=%s)', addr, request.opaque);
          this.closeChannel(addr);
        }
        logger.warning('invokeSync request expired: addr=%s, opaque=%s, timeoutMillis=%s', addr, request.opaque, timeout);
        reject(new RemotingTimeoutException(addr, timeout));
      }, timeout);
      this._send(addr, request).then((sock) => {
        fut.conn = sock;
        fut.sendRequestOK = true;
      }).catch((e) => {
        this.responseTable.delete(request.opaque);
        if (fut.timer) clearTimeout(fut.timer);
        logger.warning('invokeSync send to %s failed, tearing down the connection: %s',
          addr, (e instanceof Error ? e.message : String(e)));
        this.closeChannel(addr);
        reject(e instanceof Error ? e : new RemotingSendRequestException(addr, String(e)));
      });
    });
  }

  // ---------------- invoke async ----------------
  invokeAsync(addr: string, request: RemotingCommand,
    callback: (cmd: RemotingCommand | null, err: Error | null) => void, timeoutMillis?: number) {
    const timeout = timeoutMillis ?? this.invokeTimeoutMillis;
    this._invokeAsyncOnce(addr, request, callback, timeout, Date.now());
  }

  private _invokeAsyncOnce(addr: string, request: RemotingCommand,
    callback: (cmd: RemotingCommand | null, err: Error | null) => void, timeout: number, started: number) {
    const onResponse = (cmd: RemotingCommand | null, err: Error | null) => {
      if (cmd == null || err != null || cmd.code !== ResponseCode.GO_AWAY) { callback(cmd, err); return; }
      // GO_AWAY: retry once on a fresh thread to avoid blocking this path
      setTimeout(() => {
        try { this._handleGoAway(addr, request, timeout, started, cmd).then((r) => callback(r, null)).catch((e) => callback(null, e)); }
        catch (e) { callback(null, e as Error); }
      }, 0);
    };
    const fut: ResponseFuture = {
      opaque: request.opaque, timeoutMillis: timeout, resolve: () => {}, reject: () => {},
      onResponse, startTime: Date.now(), addr, request, sendRequestOK: false, fired: false,
    };
    this.responseTable.set(request.opaque, fut);
    fut.timer = setTimeout(() => {
      if (!this.responseTable.delete(request.opaque)) return;
      logger.warning('invokeAsync request expired: addr=%s, opaque=%s, timeoutMillis=%s', addr, request.opaque, timeout);
      // This body runs on Node's timer stack: a throw from the caller's callback
      // would surface as an uncaughtException and kill the process, so it must not
      // escape here.
      try {
        callback(null, new RemotingTimeoutException(addr, timeout));
      } catch (e) {
        logger.error('invokeAsync timeout callback threw for %s: %s', addr, (e as Error).message);
      }
    }, timeout);
    this._send(addr, request).then((sock) => {
      fut.conn = sock;
      fut.sendRequestOK = true;
    }).catch((e) => {
      this.responseTable.delete(request.opaque);
      if (fut.timer) clearTimeout(fut.timer);
      callback(null, e instanceof Error ? e : new RemotingSendRequestException(addr, String(e)));
    });
  }

  // ---------------- invoke oneway ----------------
  async invokeOneway(addr: string, request: RemotingCommand): Promise<void> {
    request.markOnewayRpc();
    await this._send(addr, request);
  }

  // ---------------- completion ----------------
  private _complete(fut: ResponseFuture, cmd: RemotingCommand) {
    if (fut.timer) clearTimeout(fut.timer);
    this._applyAfterResponseHooks(fut.addr, fut.request, cmd);
    if (fut.onResponse) { fut.onResponse(cmd, null); return; }
    fut.resolve(cmd);
  }

  private _failFast(addr: string, sock: any): number {
    const doomed: ResponseFuture[] = [];
    for (const [opaque, fut] of this.responseTable) {
      if (fut.conn === sock) {
        if (this.responseTable.delete(opaque)) doomed.push(fut);
      }
    }
    for (const fut of doomed) {
      fut.sendRequestOK = false;
      fut.conn = undefined;
      if (fut.onResponse) { fut.onResponse(null, new RemotingSendRequestException(fut.addr, 'connection closed')); }
      else fut.reject(new RemotingSendRequestException(fut.addr, 'connection closed'));
    }
    if (doomed.length) logger.warning('connection to %s closed, %d in-flight request(s) failed fast', addr, doomed.length);
    return doomed.length;
  }

  // ---------------- idle connection sweep ----------------
  // The effective idle limit: a non-positive configured value falls back to the
  // default (it does not disable the sweep — the sweep is always installed).
  private _idleLimitMillis(): number {
    return this.maxIdleTimeMillis > 0 ? this.maxIdleTimeMillis : DEFAULT_MAX_IDLE_TIME_MILLIS;
  }

  // Install the sweep once, lazily, on the first connection. The interval is a
  // fraction of the idle limit so detection is prompt for small limits yet cheap for
  // the 120 s default. unref() keeps a test process from hanging on this timer.
  private _ensureIdleSweep() {
    if (this.idleSweepTimer != null || !this.running) return;
    const tick = Math.max(200, Math.floor(this._idleLimitMillis() / 4));
    const timer = setInterval(() => this._sweepIdleConnections(), tick);
    if (typeof timer.unref === 'function') timer.unref();
    this.idleSweepTimer = timer;
  }

  // Close any connection idle strictly past the configured limit, WARNing the
  // address; the close goes through the peer-close route (destroy -> 'close' ->
  // _onClose -> _failFast), so its pending entries fail fast and other connections
  // are untouched.
  private _sweepIdleConnections() {
    const limit = this._idleLimitMillis();
    const now = Date.now();
    for (const [addr, sock] of this.conns) {
      const last = this.lastActivity.get(addr);
      if (last == null) { this.lastActivity.set(addr, now); continue; }
      if (now - last > limit) {
        logger.warning('closing idle connection to %s: no traffic for %s ms (limit %s ms)', addr, now - last, limit);
        try { if (!sock.destroyed) sock.destroy(); } catch { /* ignore */ }
      }
    }
  }

  shutdown() {
    this.running = false;
    if (this.idleSweepTimer != null) { clearInterval(this.idleSweepTimer); this.idleSweepTimer = null; }
    for (const fut of this.responseTable.values()) {
      if (fut.timer) clearTimeout(fut.timer);
    }
    this.responseTable.clear();
    for (const [, sock] of this.conns) {
      try { if (!sock.destroyed) sock.destroy(); } catch (e) { /* ignore */ }
    }
    this.conns.clear();
    this.buffers.clear();
    this.lastActivity.clear();
  }
}

export default RemotingClient;
