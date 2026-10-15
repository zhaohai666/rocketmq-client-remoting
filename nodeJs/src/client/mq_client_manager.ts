import type { MQClient } from './mq_client.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('mqclient.manager');

type MQClientFactory = () => MQClient;

// Cache entry: instance + in-use facade count (reference count).
class FactoryEntry {
  instance: MQClient;
  refcount: number;
  constructor(instance: MQClient) {
    this.instance = instance;
    this.refcount = 0;
  }
}

export class MQClientManager {
  private static _singleton: MQClientManager | null = null;

  static getInstance(): MQClientManager {
    if (MQClientManager._singleton == null) {
      MQClientManager._singleton = new MQClientManager();
    }
    return MQClientManager._singleton;
  }

  private _factoryTable: Map<string, FactoryEntry>;

  private constructor() {
    this._factoryTable = new Map();
  }

  getOrCreateMQClientInstance(clientId: string, factory: MQClientFactory): { instance: MQClient; created: boolean } {
    if (clientId == null || !String(clientId).trim()) {
      throw new Error('clientId is empty');
    }
    const entry = this._factoryTable.get(clientId);
    if (entry != null) {
      logger.debug('reuse MQClientInstance for clientId: %s', clientId);
      return { instance: entry.instance, created: false };
    }
    const instance = factory();
    // A synchronous event loop guarantees nobody can slip in, but re-check the
    // table with a "only the winner takes effect" rule anyway, to guard
    // against a possible future await-ification refactor.
    const existing = this._factoryTable.get(clientId);
    if (existing != null) {
      return { instance: existing.instance, created: false };
    }
    this._factoryTable.set(clientId, new FactoryEntry(instance));
    logger.info('Created new MQClientInstance for clientId: %s', clientId);
    return { instance, created: true };
  }

  // ---------------- reference counting ----------------
  // Called on facade start(): in-use count +1.
  // Instances that never went through getOrCreateMQClientInstance (test
  // doubles / legacy callers constructing directly) are registered the same
  // way, keeping unregister's count balanced.
  registerClient(clientId: string, instance: MQClient): void {
    const entry = this._factoryTable.get(clientId);
    if (entry == null || entry.instance !== instance) {
      this._factoryTable.set(clientId, new FactoryEntry(instance));
      const fresh = this._factoryTable.get(clientId)!;
      fresh.refcount = 1;
      return;
    }
    entry.refcount += 1;
  }

  // Called on facade shutdown(): count -1; the real close happens ONLY when it
  // hits ZERO.
  // On zero, remove from the table first, then shutdown() — after removal,
  // other facades' getOrCreate can only create a new instance.
  // When the instance is not in the table (test double / an old instance
  // already displaced or removed), keep the facade's old shutdown semantics:
  // close it directly (if it has a close method); the caller needs no
  // distinction.
  // Returns true when this call actually shut the instance down.
  unregisterClient(clientId: string, instance: MQClient | null): boolean {
    if (instance == null) return false;
    const entry = this._factoryTable.get(clientId);
    if (entry == null || entry.instance !== instance) {
      // Not the instance in this manager's cache: leave any other entry in the
      // cache alone, and just close the instance the caller holds, per the old
      // semantics.
      this._shutdownQuietly(instance);
      return true;
    }
    entry.refcount -= 1;
    if (entry.refcount > 0) {
      logger.debug('MQClientInstance %s still held by %d client(s)', clientId, entry.refcount);
      return false;
    }
    this._factoryTable.delete(clientId);
    logger.info('the client factory [%s] shutdown OK', clientId);
    this._shutdownQuietly(instance);
    return true;
  }

  private _shutdownQuietly(instance: MQClient): void {
    try {
      if (instance != null && typeof (instance as any).shutdown === 'function') {
        (instance as any).shutdown();
      }
    } catch (e) {
      logger.warn('MQClientInstance shutdown raised: %s', (e as Error).message);
    }
  }

  removeClientFactory(clientId: string, instance?: MQClient | null): void {
    const entry = this._factoryTable.get(clientId);
    if (entry == null) return;
    if (instance != null && entry.instance !== instance) return;
    this._factoryTable.delete(clientId);
  }

  // ---------------- observability / tests ----------------
  getClientFactory(clientId: string): MQClient | null {
    const entry = this._factoryTable.get(clientId);
    return entry != null ? entry.instance : null;
  }

  // The current in-use count for this clientId (observability / unit tests).
  refcountOf(clientId: string): number {
    const entry = this._factoryTable.get(clientId);
    return entry != null ? entry.refcount : 0;
  }

  // The number of currently live instances.
  instanceCount(): number {
    return this._factoryTable.size;
  }

  // Clear the cache (for test isolation). Only removes table entries, no
  // shutdown — matching the old behavior; leaked instances are reclaimed at
  // process exit without introducing new close side effects.
  reset(): void {
    this._factoryTable.clear();
  }
}

export default MQClientManager;
