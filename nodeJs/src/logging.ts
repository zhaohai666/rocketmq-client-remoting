// -*- coding: utf-8 -*-
// 客户端日志：支持环境变量开关、按大小轮转的文件 sink。
//
//   ROCKETMQ_CLIENT_LOG_LEVEL        TRACE|DEBUG|INFO|WARN|ERROR（默认 INFO）
//   ROCKETMQ_CLIENT_LOG_DIR          日志目录（默认 $HOME/logs/rocketmqlogs）
//   ROCKETMQ_CLIENT_LOG_FILE         文件名（默认 rocketmq_node_client.log；
//                                    设为 '' / OFF / NONE 则关闭文件 sink）
//   ROCKETMQ_CLIENT_LOG_USE_STDOUT   任意非空值 -> 输出到 stdout 而非文件
//
// 文件名刻意不用 rocketmq_client.log：同一台机器上若与经典客户端进程并发运行，
// 两个进程会交替写入并轮转同一个文件，互相破坏对方的日志。
//
// 轮转按 SIZE 触发，固定备份窗口（RollLocked 式）：
//   <file>.N 最旧（最先删除），.N-1 -> .N ... .1 -> .2，base -> .1。
//   ROCKETMQ_CLIENT_LOG_FILE_MAX_SIZE   默认 64MB
//   ROCKETMQ_CLIENT_LOG_FILE_MAX_INDEX  默认 10 个备份
//
// 线程安全：Node 的 JS 程序运行在单一事件循环线程上，下面的每次写入都是
// 单行的同步 fs.writeSync——本进程是唯一写入者，无需额外加锁。
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
// 默认文件名带 node 标识，与通用的 rocketmq_client.log 刻意区分——见文件头注释。
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

// Resolve the sink once at module load.
function buildSink(): FileSink | null {
  if (process.env.ROCKETMQ_CLIENT_LOG_USE_STDOUT) return null;
  let file = process.env.ROCKETMQ_CLIENT_LOG_FILE;
  if (file === undefined) file = DEFAULT_LOG_FILE;
  if (file === '' || file.toUpperCase() === 'OFF' || file.toUpperCase() === 'NONE') return null;
  const dir = process.env.ROCKETMQ_CLIENT_LOG_DIR || DEFAULT_LOG_DIR;
  // A file value containing a separator is treated as a full path, else it is
  // joined under the log dir.
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
    // 客户端内约 70 处调用点使用 printf 风格的占位符
    // （logger.info('rebalance changed, group=%s assigned=%d', g, n)）。
    // 若只是空格拼接输出，`%s` 会被原样打印、参数堆在其后，日志完全不可读。
    // util.format 会替换 %s/%d/%j，且当首参不是字符串时退化为原来的空格拼接——
    // 因此多参数日志的用法保持不变即可正常工作。
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
  // `warning` 别名：客户端约 30 处调用点使用这个名字（offset store、
  // trace dispatcher、消费循环等）。缺少别名会让错误路径本身抛出 TypeError。
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
