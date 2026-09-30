// -*- coding: utf-8 -*-
// Simple leveled logger (mirrors the other language ports).
// Configurable via env:
//   ROCKETMQ_CLIENT_LOG_LEVEL  DEBUG|INFO|WARN|ERROR  (default INFO)
//   ROCKETMQ_CLIENT_LOG_DIR    optional directory for a rolling file appender
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const LEVELS = { DEBUG: 10, INFO: 20, WARN: 30, ERROR: 40 };

function levelFromEnv() {
  const l = (process.env.ROCKETMQ_CLIENT_LOG_LEVEL || 'INFO').toUpperCase();
  return LEVELS[l] != null ? LEVELS[l] : 20;
}

let gLevel = levelFromEnv();
const logDir = process.env.ROCKETMQ_CLIENT_LOG_DIR || '';
let logFileStream = null;
if (logDir) {
  try {
    fs.mkdirSync(logDir, { recursive: true });
    const fp = path.join(logDir, 'rocketmq-client.log');
    logFileStream = fs.createWriteStream(fp, { flags: 'a' });
  } catch (e) {
    // Non-fatal: keep console-only logging.
    console.error('failed to open log file:', e);
  }
}

export function setLevel(level) {
  if (LEVELS[level] != null) gLevel = LEVELS[level];
}

function pad2(n) { return n < 10 ? '0' + n : '' + n; }
function ts() {
  const d = new Date();
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ` +
         `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}.` +
         `${String(d.getMilliseconds()).padStart(3, '0')}`;
}

export class Logger {
  constructor(name = 'rocketmq') {
    this.name = name;
  }

  _log(level, levelName, args) {
    if (level < gLevel) return;
    const msg = args.map(a => (typeof a === 'string' ? a : safeStringify(a))).join(' ');
    const line = `${ts()} ${levelName.padEnd(5)} ${this.name} - ${msg}`;
    if (level >= LEVELS.ERROR) console.error(line);
    else console.log(line);
    if (logFileStream) logFileStream.write(line + '\n');
  }

  debug(...args) { this._log(LEVELS.DEBUG, 'DEBUG', args); }
  info(...args) { this._log(LEVELS.INFO, 'INFO', args); }
  warn(...args) { this._log(LEVELS.WARN, 'WARN', args); }
  // `warning` alias: the Python logging API name — ~30 call sites across the
  // client use it (offset store, trace dispatcher, consumer loops). Missing
  // alias = TypeError on the error path itself.
  warning(...args) { this._log(LEVELS.WARN, 'WARN', args); }
  error(...args) { this._log(LEVELS.ERROR, 'ERROR', args); }
}

function safeStringify(o) {
  try {
    return JSON.stringify(o);
  } catch (e) {
    return String(o);
  }
}

const cache = new Map();
export function getLogger(name = 'rocketmq') {
  let l = cache.get(name);
  if (!l) {
    l = new Logger(name);
    cache.set(name, l);
  }
  return l;
}

export default { getLogger, setLevel, Logger };
