// -*- coding: utf-8 -*-
// Client logging (Java rocketmq-client's logging.basicConfig equivalent, env
// knobs exactly like go/common/logging.go, rotation like csharp ClientLog).
//
//   ROCKETMQ_CLIENT_LOG_LEVEL        TRACE|DEBUG|INFO|WARN|ERROR (default INFO)
//   ROCKETMQ_CLIENT_LOG_DIR          log directory (default $HOME/logs/rocketmqlogs)
//   ROCKETMQ_CLIENT_LOG_FILE         file name (default rocketmq_node_client.log;
//                                    '' / OFF / NONE disables the file sink)
//   ROCKETMQ_CLIENT_LOG_USE_STDOUT   any non-empty value -> stdout instead of file
//
// The file name is deliberately NOT Java's rocketmq_client.log: on a machine
// running the Java client at the same time, the two processes would interleave
// into (and roll each other's) one file — the same rationale as the C# port
// (csharp/src/RocketMQ.Client/Common/ClientLog.cs, "文件名刻意与 Java 的
// rocketmq_client.log 区分" comment).
//
// Rotation is SIZE based with a fixed backup window (csharp RollLocked):
//   <file>.N oldest (deleted first), .N-1 -> .N ... .1 -> .2, base -> .1.
//   ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE   default 64MB
//   ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX  default 10 backups
//
// Thread safety: Node runs the JS program on a single event-loop thread and
// every write below is a synchronous fs.writeSync of one line — the process is
// the single writer, so no further locking is required (the Go port needs a
// mutex for the same guarantee).
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { format } from 'node:util';

const LEVELS = { TRACE: 5, DEBUG: 10, INFO: 20, WARN: 30, ERROR: 40 };

function levelFromEnv() {
  const l = (process.env.ROCKETMQ_CLIENT_LOG_LEVEL || 'INFO').toUpperCase();
  return LEVELS[l] != null ? LEVELS[l] : 20;
}

function envInt(name: string, def: number): number {
  const raw = process.env[name];
  if (raw == null || raw === '') return def;
  const v = parseInt(raw, 10);
  return Number.isFinite(v) && v > 0 ? v : def;
}

export const DEFAULT_LOG_DIR = path.join(os.homedir(), 'logs', 'rocketmqlogs');
// Distinct from the Java client's rocketmq_client.log — see the header comment.
export const DEFAULT_LOG_FILE = 'rocketmq_node_client.log';

class FileSink {
  path: string;
  fd: number | null = null;
  size = 0;
  failed = false;
  maxSize: number;
  maxIndex: number;

  constructor(filePath: string) {
    this.path = filePath;
    this.maxSize = envInt('ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE', 64 * 1024 * 1024);
    this.maxIndex = envInt('ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX', 10);
  }

  open(): boolean {
    if (this.failed) return false;
    if (this.fd != null) return true;
    try {
      fs.mkdirSync(path.dirname(this.path), { recursive: true });
      try { this.size = fs.statSync(this.path).size; } catch (e) { this.size = 0; }
      this.fd = fs.openSync(this.path, 'a');
      return true;
    } catch (e) {
      // Non-fatal: keep console-only logging, never try again this process.
      this.failed = true;
      console.error('failed to open rocketmq client log file:', (e as Error).message);
      return false;
    }
  }

  // rollLocked — csharp ClientLog.RollLocked: delete .N, shift .N-1..1 up,
  // move base to .1, then reopen a fresh file.
  roll(): void {
    if (this.fd != null) { try { fs.closeSync(this.fd); } catch (e) { /* best effort */ } this.fd = null; }
    try {
      const top = `${this.path}.${this.maxIndex}`;
      if (fs.existsSync(top)) { try { fs.unlinkSync(top); } catch (e) { /* best effort */ } }
      for (let i = this.maxIndex - 1; i >= 1; i--) {
        const src = `${this.path}.${i}`;
        if (!fs.existsSync(src)) continue;
        const dst = `${this.path}.${i + 1}`;
        if (fs.existsSync(dst)) { try { fs.unlinkSync(dst); } catch (e) { /* best effort */ } }
        try { fs.renameSync(src, dst); } catch (e) { /* best effort */ }
      }
      if (fs.existsSync(this.path)) {
        try { fs.renameSync(this.path, `${this.path}.1`); } catch (e) { /* best effort */ }
      }
    } catch (e) { /* rotation failure degrades to appending to the current file */ }
    this.size = 0;
    this.open();
  }

  write(line: string): void {
    if (!this.open()) return;
    const buf = Buffer.from(line + '\n', 'utf8');
    if (this.maxSize > 0 && this.maxIndex > 0 && this.size + buf.length > this.maxSize) {
      this.roll();
      if (!this.open()) return;
    }
    try {
      fs.writeSync(this.fd!, buf);
      this.size += buf.length;
    } catch (e) {
      this.failed = true;
      try { if (this.fd != null) fs.closeSync(this.fd); } catch (e2) { /* ignore */ }
      this.fd = null;
    }
  }

  close(): void {
    if (this.fd != null) { try { fs.closeSync(this.fd); } catch (e) { /* ignore */ } this.fd = null; }
  }
}

// Resolve the sink once at module load, exactly like the Go port's sync.Once.
function buildSink(): FileSink | null {
  if (process.env.ROCKETMQ_CLIENT_LOG_USE_STDOUT) return null;
  let file = process.env.ROCKETMQ_CLIENT_LOG_FILE;
  if (file === undefined) file = DEFAULT_LOG_FILE;
  if (file === '' || file.toUpperCase() === 'OFF' || file.toUpperCase() === 'NONE') return null;
  const dir = process.env.ROCKETMQ_CLIENT_LOG_DIR || DEFAULT_LOG_DIR;
  // A file value containing a separator is treated as a full path, else it is
  // joined under the log dir (go's behaviour).
  const p = path.isAbsolute(file) || file.includes('/') || file.includes('\\')
    ? file : path.join(dir, file);
  return new FileSink(p);
}

const sink = buildSink();
if (sink != null) {
  const onExit = () => { sink.close(); };
  process.once('exit', onExit);
}

let gLevel = levelFromEnv();

export function setLevel(level: string | number) {
  if (typeof level === 'number' && LEVELS.TRACE <= level && level <= LEVELS.ERROR) gLevel = level;
  else if (typeof level === 'string' && LEVELS[level] != null) gLevel = LEVELS[level];
}

function pad2(n: number) { return n < 10 ? '0' + n : '' + n; }
function ts() {
  const d = new Date();
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ` +
         `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}.` +
         `${String(d.getMilliseconds()).padStart(3, '0')}`;
}

export class Logger {
  name: string;
  constructor(name = 'rocketmq') {
    this.name = name;
  }

  _log(level: number, levelName: string, args: any[]) {
    if (level < gLevel) return;
    // ~70 call sites across the client are written Python-style
    // (logger.info('rebalance changed, group=%s assigned=%d', g, n)) because the
    // port mirrors Python's logging API. Plain space-joining printed the `%s`
    // literally and dumped the values behind it, so every one of those lines was
    // unreadable in rocketmq_node_client.log. util.format substitutes %s/%d/%j
    // and, when the first argument is not a string, degrades to exactly the old
    // space join — so multi-arg logging keeps working unchanged.
    const msg = (args.length > 1 && typeof args[0] === 'string')
      ? format(...args)
      : args.map(a => (typeof a === 'string' ? a : safeStringify(a))).join(' ');
    const line = `${ts()} ${levelName.padEnd(5)} ${this.name} - ${msg}`;
    if (level >= LEVELS.ERROR) console.error(line);
    else console.log(line);
    if (sink) sink.write(line);
  }

  debug(...args: any[]) { this._log(LEVELS.DEBUG, 'DEBUG', args); }
  info(...args: any[]) { this._log(LEVELS.INFO, 'INFO', args); }
  warn(...args: any[]) { this._log(LEVELS.WARN, 'WARN', args); }
  // `warning` alias: the Python logging API name — ~30 call sites across the
  // client use it (offset store, trace dispatcher, consumer loops). Missing
  // alias = TypeError on the error path itself.
  warning(...args: any[]) { this._log(LEVELS.WARN, 'WARN', args); }
  error(...args: any[]) { this._log(LEVELS.ERROR, 'ERROR', args); }
}

function safeStringify(o: any) {
  try {
    return JSON.stringify(o);
  } catch (e) {
    return String(o);
  }
}

const cache = new Map<string, Logger>();
export function getLogger(name = 'rocketmq'): Logger {
  let l = cache.get(name);
  if (!l) {
    l = new Logger(name);
    cache.set(name, l);
  }
  return l;
}

// Test/diagnostic hook: the resolved sink path (null = stdout mode or disabled).
export function logFilePath(): string | null {
  return sink != null ? sink.path : null;
}

export default { getLogger, setLevel, Logger, logFilePath, DEFAULT_LOG_DIR, DEFAULT_LOG_FILE };
