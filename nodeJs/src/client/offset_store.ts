// Consumer offset stores (Java org.apache.rocketmq.client.consumer.store.*).
//
// RemoteBrokerOffsetStore is the CLUSTERING store: offsets live broker-side,
// the local table is a write-through cache. LocalFileOffsetStore is the
// BROADCASTING store: offsets live under
// $HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json in Java fastjson2's
// MessageQueue-as-object-key format, with offsets.json.bak as the previous
// generation.
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { MessageQueue } from '../common/message.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.offset_store');

// Java OffsetStore.ReadOffsetMode.
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

  // UpdateOffset records the offset. A missing entry is created even for
  // offset 0 (Java updateOffset's putIfAbsent — the second commit of a pair
  // may carry the lower value and must not erase the first).
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
  // -1 = unknown (no entry / broker says QUERY_NOT_FOUND).
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
      return -1;
    }
    try {
      const { found, offset } = await this.mqClient.queryConsumerOffset(addr, this.group, mq);
      if (!found) return -1;
      this.updateOffset(mq, offset, false);
      return offset;
    } catch (e) {
      logger.warning('readOffset failed for %s: %s', mqKey(mq), (e as Error).message);
      return -1;
    }
  }

  // PersistAll flushes the given queues to their brokers and removes table
  // entries for queues not in the list (Java's "remove unused mq" —
  // rebalanced-away queues must not re-appear on the next load). Broadcasting
  // is the caller's concern; this store only ever runs for clustering
  // consumers.
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
    // Drop entries the caller did not name — Java's removal condition is
    // literally `!mqs.contains(mq)`, so a named queue keeps its table slot
    // even when its offset was negative and never committed.
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

  // updateConsumeOffsetToBroker picks the address Java-style: master from the
  // publish table first, then "any addr for this broker in a cached route".
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

  // Builds the store under the default path (~/.rocketmq_offsets). An env
  // override rocketmq.client.localOffsetStoreDir takes the place of the base
  // directory, matching Java. `baseDir` is a swappable test hook.
  constructor(clientID: string, group: string, baseDir?: string) {
    this.clientID = clientID;
    this.group = group;
    let base = baseDir || process.env['rocketmq.client.localOffsetStoreDir'] || '';
    if (!base) base = path.join(os.homedir(), '.rocketmq_offsets');
    this.storePath = path.join(base, clientID, group, 'offsets.json');
  }

  // Load reads offsets.json, falling back to offsets.json.bak when the main
  // file is missing; both missing is an empty table, not an error (Java same).
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
      // A corrupt file must not brick the consumer; Java logs and keeps an
      // empty table.
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

  // PersistAll writes the whole table (all entries ARE this consumer's
  // queues).
  async persistAll(_mqs: MessageQueue[]): Promise<void> {
    return this.persistFile();
  }

  // Persist writes the whole table too (Java keeps one file per group).
  async persist(_mq: MessageQueue): Promise<void> {
    return this.persistFile();
  }

  private async persistFile(): Promise<void> {
    const entries: Record<string, number> = {};
    for (const [key, offset] of this.table) {
      const parsed = parseMqKey(key);
      if (parsed) entries[messageQueueKeyJson(parsed)] = offset;
    }
    const body = JSON.stringify({ offsetTable: entries });
    // Java MixAll.string2File: the FIRST write creates the file, every later
    // write rolls the previous content to offsets.json.bak before replacing.
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

// messageQueueKeyJson writes Java fastjson2's shape:
//   {"brokerName":"b","queueId":1,"topic":"T"}
// object keys alphabetically: brokerName, queueId, topic.
function messageQueueKeyJson(mq: MessageQueue): string {
  return JSON.stringify({
    brokerName: mq.getBrokerName(),
    queueId: mq.getQueueId(),
    topic: mq.getTopic(),
  });
}

// decodeLocalOffsetTable parses Java's compact and pretty variants. Legacy
// flat "topic+broker+queueId" string keys cannot round-trip through a
// MessageQueue-keyed table and are skipped (the file is rewritten in the Java
// shape on the next persist).
function decodeLocalOffsetTable(data: Buffer): Map<string, number> {
  const out = new Map<string, number>();
  const obj = JSON.parse(data.toString('utf-8'));
  const raw = obj && obj['offsetTable'];
  if (!raw || typeof raw !== 'object') return out;
  for (const [k, v] of Object.entries(raw)) {
    let inner: any;
    try { inner = JSON.parse(k); } catch (e) { continue; }
    if (!inner || typeof inner !== 'object') continue;
    const mq = new MessageQueue(
      String(inner['topic'] ?? ''),
      String(inner['brokerName'] ?? ''),
      Number(inner['queueId'] ?? 0) | 0,
    );
    const offset = typeof v === 'number' ? v : parseInt(String(v), 10);
    if (Number.isNaN(offset)) continue;
    out.set(mqKey(mq), offset);
  }
  return out;
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
