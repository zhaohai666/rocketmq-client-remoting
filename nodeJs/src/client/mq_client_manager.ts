import type { MQClient } from './mq_client.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('mqclient.manager');

type MQClientFactory = () => MQClient;

// 缓存条目：实例 + 在用门面数（引用计数）。
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
    // 同步事件循环保底不会有人插队，但仍按「只有赢家生效」重查一遍表，
    // 防御未来可能的 await 化改造。
    const existing = this._factoryTable.get(clientId);
    if (existing != null) {
      return { instance: existing.instance, created: false };
    }
    this._factoryTable.set(clientId, new FactoryEntry(instance));
    logger.info('Created new MQClientInstance for clientId: %s', clientId);
    return { instance, created: true };
  }

  // ---------------- 引用计数 ----------------
  // 门面 start() 时调用：在用计数 +1。
  // 未经 getOrCreateMQClientInstance 的实例（测试替身 / 旧调用方直接构造）也照常
  // 登记，保证 unregister 的计数平衡。
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

  // 门面 shutdown() 时调用：计数 -1；**归零才真正关闭**。
  // 归零时先摘表再 shutdown()——摘表后其他门面的 getOrCreate 只会建新实例。
  // 实例不在表中（测试替身 / 已被顶替或摘除的旧实例）时保持门面旧的 shutdown
  // 语义：直接关它（若它有关闭方法），调用方无需分辨。
  // 返回 true 表示本次调用真的关闭了实例。
  unregisterClient(clientId: string, instance: MQClient | null): boolean {
    if (instance == null) return false;
    const entry = this._factoryTable.get(clientId);
    if (entry == null || entry.instance !== instance) {
      // 不是本 manager 缓存里的那份实例：不动缓存里可能存在的其他条目，
      // 只按旧语义直接关调用方手里的实例。
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

  // ---------------- 观测 / 测试 ----------------
  getClientFactory(clientId: string): MQClient | null {
    const entry = this._factoryTable.get(clientId);
    return entry != null ? entry.instance : null;
  }

  // 该 clientId 当前的在用计数（观测 / 单测用）。
  refcountOf(clientId: string): number {
    const entry = this._factoryTable.get(clientId);
    return entry != null ? entry.refcount : 0;
  }

  // 当前存活的实例数。
  instanceCount(): number {
    return this._factoryTable.size;
  }

  // 清空缓存（测试隔离用）。只摘表、不 shutdown——与旧行为一致，
  // 泄漏的实例交由进程退出回收，不引入新的关闭副作用。
  reset(): void {
    this._factoryTable.clear();
  }
}

export default MQClientManager;
