// -*- coding: utf-8 -*-
// Socket long-connection client (org.apache.rocketmq.remoting.netty.NettyRemotingClient core).
// Provides: connection mgmt (lazy connect + reuse), invokeSync / invokeAsync / invokeOneway,
// opaque routing, timeout, GO_AWAY reconnect-and-retry-once, fail-fast on disconnect, TLS,
// and broker-initiated request processors.
import net from 'node:net';
import tls from 'node:tls';
import { once } from 'node:events';
import { Buffer } from 'node:buffer';
import { ResponseCode } from './codes.ts';
import { RemotingCommand } from './remotingCommand.ts';
import { RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException } from './exception.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('remoting.client');
const MAX_FRAME_LENGTH = 16 * 1024 * 1024;

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

export class RemotingClient {
  connectTimeoutMillis = 3000;
  invokeTimeoutMillis = 15000;
  enableReconnectForGoAway = true;
  tlsEnable = false;
  private conns = new Map<string, any>();
  private buffers = new Map<string, Buffer>();
  private responseTable = new Map<number, ResponseFuture>();
  private rpcHooks: any[] = [];
  private processors = new Map<number, (cmd: RemotingCommand, addr: string) => RemotingCommand | null | Promise<RemotingCommand | null>>();
  private running = true;
  private connectPromises = new Map<string, Promise<any>>();

  constructor(opts: { connectTimeoutMillis?: number; invokeTimeoutMillis?: number; tlsEnable?: boolean } = {}) {
    if (opts.connectTimeoutMillis != null) this.connectTimeoutMillis = opts.connectTimeoutMillis;
    if (opts.invokeTimeoutMillis != null) this.invokeTimeoutMillis = opts.invokeTimeoutMillis;
    if (opts.tlsEnable != null) this.tlsEnable = opts.tlsEnable;
    else {
      const e = (process.env['ROCKETMQ_TLS_ENABLE'] || '').trim().toLowerCase();
      this.tlsEnable = e === '1' || e === 'true' || e === 'yes';
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

  private async _connect(addr: string): Promise<any> {
    const existing = this.conns.get(addr);
    if (existing && !existing.destroyed) return existing;
    let p = this.connectPromises.get(addr);
    if (p) return p;
    p = (async () => {
      const [host, port] = this._parseAddr(addr);
      let sock: any;
      if (this.tlsEnable) {
        sock = tls.connect({ host, port, rejectUnauthorized: false });
      } else {
        sock = net.connect({ host, port });
      }
      sock.setNoDelay(true);
      const timer = setTimeout(() => sock.destroy(new Error('connect timeout')), this.connectTimeoutMillis);
      try {
        await once(sock, 'connect');
      } finally {
        clearTimeout(timer);
      }
      this.conns.set(addr, sock);
      this.buffers.set(addr, Buffer.alloc(0));
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
    const c = this.conns.get(addr);
    return !!c && !c.destroyed;
  }
  closeChannel(addr: string) {
    const c = this.conns.get(addr);
    this.conns.delete(addr);
    this.buffers.delete(addr);
    if (c && !c.destroyed) c.destroy();
  }

  // ---------------- read loop ----------------
  private _onData(addr: string, sock: any, chunk: Buffer) {
    let buf = this.buffers.get(addr) || Buffer.alloc(0);
    buf = Buffer.concat([buf, chunk]);
    for (;;) {
      if (buf.length < 4) break;
      const totalLen = buf.readInt32BE(0);
      if (totalLen <= 0 || totalLen > MAX_FRAME_LENGTH) { buf = Buffer.alloc(0); break; }
      if (buf.length < 4 + totalLen) break;
      const frame = buf.subarray(0, 4 + totalLen);
      buf = buf.subarray(4 + totalLen);
      this._dispatch(Buffer.from(frame), addr, sock);
    }
    this.buffers.set(addr, buf);
  }

  private _onClose(addr: string, sock: any) {
    this.conns.delete(addr);
    this.buffers.delete(addr);
    this._failFast(addr, sock);
  }

  private _dispatch(frame: Buffer, addr: string, sock: any) {
    let cmd: RemotingCommand;
    try { cmd = RemotingCommand.decode(frame); } catch (e) { return; }
    if (cmd.isResponseType()) {
      const fut = this.responseTable.get(cmd.opaque);
      if (fut) {
        this.responseTable.delete(cmd.opaque);
        fut.conn = sock;
        this._complete(fut, cmd);
      }
      return;
    }
    const fut = this.responseTable.get(cmd.opaque);
    if (fut) {
      this.responseTable.delete(cmd.opaque);
      fut.conn = sock;
      this._complete(fut, cmd);
      return;
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
        else resolve();
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

  private _invokeOnce(addr: string, request: RemotingCommand, timeout: number): Promise<RemotingCommand> {
    return new Promise((resolve, reject) => {
      const fut: ResponseFuture = {
        opaque: request.opaque, timeoutMillis: timeout, resolve, reject,
        startTime: Date.now(), addr, request, sendRequestOK: false, fired: false,
      };
      this.responseTable.set(request.opaque, fut);
      fut.timer = setTimeout(() => {
        if (this.responseTable.delete(request.opaque)) {
          reject(new RemotingTimeoutException(addr, timeout));
        }
      }, timeout);
      this._send(addr, request).then((sock) => {
        fut.conn = sock;
        fut.sendRequestOK = true;
      }).catch((e) => {
        this.responseTable.delete(request.opaque);
        if (fut.timer) clearTimeout(fut.timer);
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
      if (this.responseTable.delete(request.opaque)) callback(null, new RemotingTimeoutException(addr, timeout));
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

  shutdown() {
    this.running = false;
    for (const fut of this.responseTable.values()) {
      if (fut.timer) clearTimeout(fut.timer);
    }
    this.responseTable.clear();
    for (const [, sock] of this.conns) {
      try { if (!sock.destroyed) sock.destroy(); } catch (e) { /* ignore */ }
    }
    this.conns.clear();
    this.buffers.clear();
  }
}

export default RemotingClient;
