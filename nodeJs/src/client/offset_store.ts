import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { MessageQueue } from '../common/message.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.offset_store');

export const ReadOffsetMode = {
  READ_FROM_MEMORY: 0,
  READ_FROM_STORE: 1,
  READ_FROM_MEMORY_THEN_STORE: 2,
} as const;
export type ReadOffsetMode = number;

// Canonical map key for a MessageQueue — the (topic, brokerName, queueId)
// triple. NEVER key only by queueId: the implicit %RETRY%<group> subscription
// shares queueIds with the business topic and would produce phantom overlaps.
export function mqKey(mq: MessageQueue): string {
  return `${mq.getTopic()}@${mq.getBrokerName()}@${mq.getQueueId()}`;
}

export interface OffsetStore {
  load(): Promise<void>;
  updateOffset(mq: MessageQueue, offset: number, increaseOnly: boolean): void;
  readOffset(mq: MessageQueue, mode: ReadOffsetMode): Promise<number>;
  persistAll(mqs: MessageQueue[]): Promise<void>;
  persist(mq: MessageQueue): Promise<void>;
}

// ---------------------------------------------------------------- remote store

// RemoteBrokerOffsetStore backs CLUSTERING consumers.
export class RemoteBrokerOffsetStore implements OffsetStore {
  private mqClient: any;
  private group: string;
  private table = new Map<string, number>();

  constructor(mqClient: any, group: string) {
    this.mqClient = mqClient;
    this.group = group;
  }

  // Load is a no-op: remote offsets are fetched on demand.
  async load(): Promise<void> { /* nothing to do */ }

  updateOffset(mq: MessageQueue, offset: number, increaseOnly: boolean): void {
    const key = mqKey(mq);
    const old = this.table.get(key);
    if (old !== undefined) {
      if (increaseOnly && offset < old) return;
      this.table.set(key, offset);
      return;
    }
    this.table.set(key, offset);
  }

  // ReadOffset answers from the table (memory modes) or the broker.
  // -1 = unknown (no entry / broker says QUERY_NOT_FOUND); -2 = the broker read
  // itself failed, which is NOT "nobody committed" and must stay distinguishable.
  async readOffset(mq: MessageQueue, mode: ReadOffsetMode): Promise<number> {
    if (mode === ReadOffsetMode.READ_FROM_MEMORY || mode === ReadOffsetMode.READ_FROM_MEMORY_THEN_STORE) {
      const offset = this.table.get(mqKey(mq));
      if (offset !== undefined) return offset;
      if (mode === ReadOffsetMode.READ_FROM_MEMORY) return -1;
    }
    // READ_FROM_STORE (and MEMORY_FIRST on a table miss): ask the broker.
    const addr = this.mqClient.findBrokerAddrForQueue(mq);
    if (!addr) {
      logger.warning('readOffset: broker[%s] not exist for %s', mq.getBrokerName(), mqKey(mq));
      return -2;
    }
    try {
      const { found, offset } = await this.mqClient.queryConsumerOffset(addr, this.group, mq);
      if (!found) return -1;
      this.updateOffset(mq, offset, false);
      return offset;
    } catch (e) {
      logger.warning('readOffset failed for %s: %s', mqKey(mq), (e as Error).message);
      return -2;
    }
  }

  async persistAll(mqs: MessageQueue[]): Promise<void> {
    if (mqs.length === 0) return;
    let firstErr: Error | null = null;
    for (const mq of mqs) {
      const offset = this.table.get(mqKey(mq));
      if (offset === undefined || offset < 0) continue;
      try {
        await this.updateConsumeOffsetToBroker(mq, offset);
      } catch (e) {
        if (!firstErr) firstErr = e as Error;
      }
    }
    const requested = new Set<string>();
    for (const mq of mqs) requested.add(mqKey(mq));
    for (const key of Array.from(this.table.keys())) {
      if (!requested.has(key)) this.table.delete(key);
    }
    if (firstErr) throw firstErr;
  }

  // Persist flushes one queue.
  async persist(mq: MessageQueue): Promise<void> {
    const offset = this.table.get(mqKey(mq));
    if (offset === undefined || offset < 0) return;
    return this.updateConsumeOffsetToBroker(mq, offset);
  }

  // Table snapshots the cached table (checks and tests).
  tableSnapshot(): Map<string, number> {
    return new Map(this.table);
  }

  private async updateConsumeOffsetToBroker(mq: MessageQueue, offset: number): Promise<void> {
    const addr = this.mqClient.findBrokerAddrForQueue(mq);
    if (!addr) throw new Error(`The broker[${mq.getBrokerName()}] not exist`);
    await this.mqClient.updateConsumerOffset(addr, this.group, mq, offset);
  }
}

// ---------------------------------------------------------------- local store

// LocalFileOffsetStore backs BROADCASTING consumers with a JSON file:
// ~/.rocketmq_offsets/<clientId>/<group>/offsets.json.
export class LocalFileOffsetStore implements OffsetStore {
  private clientID: string;
  private group: string;
  private table = new Map<string, number>(); // mqKey -> offset
  private storePath: string;

  constructor(clientID: string, group: string, baseDir?: string) {
    this.clientID = clientID;
    this.group = group;
    let base = baseDir || process.env['rocketmq.client.localOffsetStoreDir'] || '';
    if (!base) base = path.join(os.homedir(), '.rocketmq_offsets');
    this.storePath = path.join(base, clientID, group, 'offsets.json');
  }

  async load(): Promise<void> {
    let data: Buffer | null = null;
    try {
      data = fs.readFileSync(this.storePath);
    } catch (e1) {
      try {
        data = fs.readFileSync(this.storePath + '.bak');
      } catch (e2) {
        return; // both missing -> empty table
      }
    }
    try {
      this.table = decodeLocalOffsetTable(data);
    } catch (e) {
      logger.warning('load local offset store %s failed: %s', this.storePath, (e as Error).message);
      this.table = new Map();
    }
  }

  // UpdateOffset records the offset (increaseOnly keeps the max).
  updateOffset(mq: MessageQueue, offset: number, increaseOnly: boolean): void {
    const key = mqKey(mq);
    const old = this.table.get(key);
    if (old !== undefined) {
      if (increaseOnly && offset < old) return;
      this.table.set(key, offset);
      return;
    }
    this.table.set(key, offset);
  }

  // ReadOffset answers from memory only; -1 when the queue has no entry.
  async readOffset(mq: MessageQueue, _mode: ReadOffsetMode): Promise<number> {
    const offset = this.table.get(mqKey(mq));
    return offset === undefined ? -1 : offset;
  }

  // PersistAll merges with the on-disk table: the file still holds offsets for
  // queues this round no longer owns, so read it back and overlay only the
  // flushed queues instead of replacing the file with the in-memory table.
  async persistAll(mqs: MessageQueue[]): Promise<void> {
    if (mqs.length === 0) return;
    const merged = this.readDiskTable();
    for (const mq of mqs) {
      const key = mqKey(mq);
      const offset = this.table.get(key);
      if (offset === undefined) continue;
      merged.set(key, offset);
    }
    this.writeTable(merged);
  }

  async persist(mq: MessageQueue): Promise<void> {
    const key = mqKey(mq);
    const offset = this.table.get(key);
    if (offset === undefined) return;
    const merged = this.readDiskTable();
    merged.set(key, offset);
    this.writeTable(merged);
  }

  private readDiskTable(): Map<string, number> {
    let data: Buffer | null = null;
    try {
      data = fs.readFileSync(this.storePath);
    } catch (e1) {
      try {
        data = fs.readFileSync(this.storePath + '.bak');
      } catch (e2) {
        return new Map(); // both missing -> nothing on disk yet
      }
    }
    try {
      return decodeLocalOffsetTable(data);
    } catch (e) {
      logger.warning('read local offset store %s failed: %s', this.storePath, (e as Error).message);
      return new Map();
    }
  }

  private writeTable(table: Map<string, number>): void {
    const body = encodeLocalOffsetTable(table);
    if (fs.existsSync(this.storePath)) {
      try { fs.writeFileSync(this.storePath + '.bak', fs.readFileSync(this.storePath)); } catch (e) { /* best effort */ }
    }
    fs.mkdirSync(path.dirname(this.storePath), { recursive: true });
    fs.writeFileSync(this.storePath, body, 'utf-8');
  }

  storePathValue(): string { return this.storePath; }

  // Test/diagnostic accessor.
  tableSnapshot(): Map<string, number> {
    return new Map(this.table);
  }
}

// ------------------------------------------------------------- file format

// Encoder for the inline-object MessageQueue key form used by the local offset
// file: {"brokerName":"b","queueId":0,"topic":"T"}. Also reused by the running
// info table.
export function messageQueueKeyJson(mq: MessageQueue): string {
  return JSON.stringify({
    brokerName: mq.getBrokerName(),
    queueId: mq.getQueueId(),
    topic: mq.getTopic(),
  });
}

// The table is written with inline JSON object keys, which JSON.stringify
// cannot produce (it would quote and escape the key text). Serialize the outer
// document by hand so the keys stay raw objects.
function encodeLocalOffsetTable(table: Map<string, number>): string {
  const entries: string[] = [];
  for (const [key, offset] of table) {
    const mq = parseMqKey(key);
    if (!mq) continue;
    entries.push(`${messageQueueKeyJson(mq)}:${offset}`);
  }
  const body = entries.length === 0 ? '' : `\n\t${entries.join(',\n\t')}\n`;
  return `{\n\t"offsetTable":{${body}}\n}\n`;
}

// Reads the offset table accepting all three key spellings seen on disk:
// an inline object key, a quoted-string key holding that same JSON text, and
// the legacy topic@brokerName@queueId text.
function decodeLocalOffsetTable(data: Buffer): Map<string, number> {
  const out = new Map<string, number>();
  const text = data.toString('utf-8');
  const marker = /"offsetTable"\s*:\s*\{/.exec(text);
  if (!marker) return out;
  let i = marker.index + marker[0].length;
  while (i < text.length) {
    while (i < text.length && (text[i] === ',' || /\s/.test(text[i]))) i++;
    if (i >= text.length || text[i] === '}') break;
    let keyText: string;
    if (text[i] === '{') {
      const end = scanBalancedObject(text, i);
      if (end < 0) break;
      keyText = text.slice(i, end + 1);
      i = end + 1;
    } else if (text[i] === '"') {
      const parsed = readJsonString(text, i);
      if (parsed.next < 0) break;
      keyText = parsed.value;
      i = parsed.next;
    } else {
      break;
    }
    while (i < text.length && /\s/.test(text[i])) i++;
    if (text[i] !== ':') break;
    i++;
    while (i < text.length && /\s/.test(text[i])) i++;
    const numStart = i;
    while (i < text.length && /[-+0-9.eE]/.test(text[i])) i++;
    const offset = Number(text.slice(numStart, i));
    if (Number.isNaN(offset)) continue;
    const mq = parseFileKey(keyText);
    if (mq) out.set(mqKey(mq), offset);
  }
  return out;
}

function parseFileKey(keyText: string): MessageQueue | null {
  const trimmed = keyText.trim();
  if (trimmed.startsWith('{')) {
    try {
      const inner = JSON.parse(trimmed);
      if (!inner || typeof inner !== 'object') return null;
      return new MessageQueue(
        String(inner['topic'] ?? ''),
        String(inner['brokerName'] ?? ''),
        Number(inner['queueId'] ?? 0) | 0,
      );
    } catch (e) {
      return null;
    }
  }
  return parseMqKey(trimmed);
}

function scanStringEnd(text: string, start: number): number {
  let i = start + 1;
  while (i < text.length) {
    if (text[i] === '\\') { i += 2; continue; }
    if (text[i] === '"') return i;
    i++;
  }
  return -1;
}

function readJsonString(text: string, start: number): { value: string; next: number } {
  const end = scanStringEnd(text, start);
  if (end < 0) return { value: '', next: -1 };
  try {
    return { value: String(JSON.parse(text.slice(start, end + 1))), next: end + 1 };
  } catch (e) {
    return { value: '', next: -1 };
  }
}

function scanBalancedObject(text: string, start: number): number {
  let depth = 0;
  let i = start;
  while (i < text.length) {
    const ch = text[i];
    if (ch === '"') {
      const end = scanStringEnd(text, i);
      if (end < 0) return -1;
      i = end + 1;
      continue;
    }
    if (ch === '{') depth++;
    else if (ch === '}') {
      depth--;
      if (depth === 0) return i;
    }
    i++;
  }
  return -1;
}

// parseMqKey reverses mqKey() (test/file helpers).
function parseMqKey(key: string): MessageQueue | null {
  const idx1 = key.indexOf('@');
  const idx2 = key.lastIndexOf('@');
  if (idx1 < 0 || idx2 <= idx1) return null;
  const topic = key.slice(0, idx1);
  const brokerName = key.slice(idx1 + 1, idx2);
  const queueId = parseInt(key.slice(idx2 + 1), 10);
  if (Number.isNaN(queueId)) return null;
  return new MessageQueue(topic, brokerName, queueId);
}
