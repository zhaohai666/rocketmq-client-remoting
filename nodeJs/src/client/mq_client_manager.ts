// -*- coding: utf-8 -*-
// MQClientManager — 同进程内按 clientId 复用 MQClient（org.apache.rocketmq.client.impl.MQClientManager）。
//
// Java 的经典设计：同进程内同 clientId（`<ip>@<instanceName>[@<unitName>][@STREAM]`，
// 见 MixAll.clientIdFor）的 producer / consumer / admin 共享**同一个** MQClient 实例
// （factoryTable：clientId → instance）——路由缓存、broker 地址表、心跳与路由刷新定时器、
// 与 broker 的长连接、client 级反向请求处理器（39/326/36/220/221/307/309）都只有一份；
// 引用计数归零时才真正 shutdown()（停后台定时器、断连接），并把条目从表里摘掉。
//
// 生命周期契约（与 Java getOrCreateMQClientInstance / MQClientInstance.shutdown 一致，
// 语义对齐本仓库 python/client/mq_client_manager.py 与 csharp MQClientManager）：
//
// * 门面 start()：getOrCreateMQClientInstance 拿实例（没有就建）→ registerClient 计数 +1
//   → instance.start()（幂等，复用时不会重复拉起路由刷新 / name server 寻址 /
//   线程池巡检这些**实例级**后台定时器）。
// * 门面 shutdown()：unregisterClient 计数 -1；**归零**时把实例摘出缓存并真正 shutdown()；
//   仍有其他门面持有（计数 > 0）时实例继续存活，其后台定时器与连接不受影响。
// * 配置以**第一次创建**时的快照为准（Java cloneClientConfig 同口径）：复用方后设的
//   tls / 代理 / 超时等不会改写已存在的实例；rpcHook 同理只在 created=true 时注册。
//
// Node 是单事件循环，不存在 Java/C#/Python 那把表锁；"归零 → 摘表 → shutdown" 在同步
// 代码段里天然原子。真正的 shutdown()（销毁 socket）统一在表更新之后执行：摘表后其他
// 门面的 getOrCreateMQClientInstance 只会建新实例，不会拿到半死的旧实例。
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

  // 进程级单例（对应 Java MQClientManager.getInstance()）。
  static getInstance(): MQClientManager {
    if (MQClientManager._singleton == null) {
      MQClientManager._singleton = new MQClientManager();
    }
    return MQClientManager._singleton;
  }

  // 对应 Java factoryTable：clientId → 条目。
  private _factoryTable: Map<string, FactoryEntry>;

  private constructor() {
    this._factoryTable = new Map();
  }

  // ---------------- 查询 / 创建 ----------------
  // 按 clientId 取缓存实例；没有就用 factory() 建一个并落表。**不**加引用计数 ——
  // 计数由门面显式 registerClient（与 Python 端同口径），便于 start() 失败路径
  // 精确回退。返回 { instance, created }：created=true 表示本调用方是创建者，
  // 门面据此决定要不要注册自己的 rpcHook / TLS 快照（复用方不注册，与 Java 只认
  // 创建时那份同口径）。
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

  // 把实例从缓存摘掉但**不** shutdown（对应 Java removeClientFactory）。
  // 门面 start() 失败时由调用方收尾用：半启动的实例不能留在表里毒化后续的
  // getOrCreate；传 instance 时只摘自己的条目，防止误删一个已顶替上来的新实例。
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
