// 队列分配策略（消费组内的队列切分算法）。
//
// 统一的行为约定：入参非法（currentCID 不在 cidAll 里、mqAll/cidAll 为空）
// 时不抛异常，策略一律返回空列表——rebalance 是后台定时器，一次脏输入不能
// 把消费者弄死。
//
// 确定性才是关键：组内每个实例都以相同方式排序 mqAll 与 cidAll、跑同样的
// 下标运算，否则两个实例算出重叠的分配，每条消息都被消费两遍。
import { createHash } from 'node:crypto';
import { MessageQueue } from '../common/message.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.allocate');

export interface AllocateMessageQueueStrategy {
  allocate(consumerGroup: string, currentCID: string, mqAll: MessageQueue[], cidAll: string[]): MessageQueue[];
  name(): string;
}

// checkCid 是所有策略的前置校验：currentCID 必须在 cidAll 里，不在时打出
// [BUG] 日志并让策略返回空分配。
function checkCid(consumerGroup: string, currentCID: string, mqAll: MessageQueue[], cidAll: string[]): number {
  if (!currentCID || mqAll.length === 0 || cidAll.length === 0) return -1;
  const index = cidAll.indexOf(currentCID);
  if (index < 0) {
    logger.info('[BUG] ConsumerGroup: %s The consumerId: %s not in cidAll: %j', consumerGroup, currentCID, cidAll);
    return -1;
  }
  return index;
}

function sortMqAll(mqAll: MessageQueue[]): MessageQueue[] {
  return [...mqAll].sort((a, b) => a.compareTo(b));
}

function sortCidAll(cidAll: string[]): string[] {
  return [...cidAll].sort();
}

// ------------------------------------------------------------- AVG

// AVG：把排序后的队列按连续区间平均分给组内消费者，余数分给前 mod 个。
export class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
  name(): string { return 'AVG'; }

  allocate(consumerGroup: string, currentCID: string, mqAllRaw: MessageQueue[], cidAllRaw: string[]): MessageQueue[] {
    const mqAll = sortMqAll(mqAllRaw);
    const cidAll = sortCidAll(cidAllRaw);
    const index = checkCid(consumerGroup, currentCID, mqAll, cidAll);
    if (index < 0) return [];
    const mod = mqAll.length % cidAll.length;
    const averageSize = Math.floor(mqAll.length / cidAll.length);
    if (averageSize === 0) {
      if (index < mqAll.length) return [mqAll[index]];
      return [];
    }
    let start: number, end: number;
    if (mod > 0 && index < mod) {
      start = index * (averageSize + 1);
      end = start + averageSize + 1;
    } else {
      start = mod * (averageSize + 1) + (index - mod) * averageSize;
      end = start + averageSize;
    }
    return mqAll.slice(start, end);
  }
}

// ------------------------------------------------ AVG_BY_CIRCLE

// AVG_BY_CIRCLE：按排序后的 cidAll 环形轮流取队列。
export class AllocateMessageQueueAveragelyByCircle implements AllocateMessageQueueStrategy {
  name(): string { return 'AVG_BY_CIRCLE'; }

  allocate(consumerGroup: string, currentCID: string, mqAllRaw: MessageQueue[], cidAllRaw: string[]): MessageQueue[] {
    const mqAll = sortMqAll(mqAllRaw);
    const cidAll = sortCidAll(cidAllRaw);
    const index = checkCid(consumerGroup, currentCID, mqAll, cidAll);
    if (index < 0) return [];
    const out: MessageQueue[] = [];
    for (let i = index; i < mqAll.length; i += cidAll.length) {
      out.push(mqAll[i]);
    }
    return out;
  }
}

// ------------------------------------------------------------- CONFIG

// CONFIG：直接返回调用方配置好的队列列表。注意这里【不做任何校验】——
// 空的 group 或 cidAll 也照样返回配置列表。
export class AllocateMessageQueueByConfig implements AllocateMessageQueueStrategy {
  messageQueueList: MessageQueue[];

  constructor(mqs: MessageQueue[]) {
    this.messageQueueList = [...mqs];
  }

  name(): string { return 'CONFIG'; }

  // 忽略全部入参（这里不跑任何校验）。
  allocate(_consumerGroup: string, _currentCID: string, _mqAll: MessageQueue[], _cidAll: string[]): MessageQueue[] {
    return [...this.messageQueueList];
  }
}

// ------------------------------------------------- consistent hash ring

export interface HashFunction {
  hash(key: string): number; // uint32
}

// md5 哈希：取 MD5 摘要的【前四个字节】、大端序拼成 uint32。取整段 128 位
// （或别的切片）会落在与既有客户端不同的哈希环上，队列归属就对不上了。
class Md5Hash implements HashFunction {
  hash(key: string): number {
    const sum = createHash('md5').update(key, 'utf8').digest();
    return sum.readUInt32BE(0) >>> 0;
  }
}

interface VirtualNode {
  key: string;
  physicalKey: string;
}

class ConsistentHashRouter {
  private hashFn: HashFunction;
  private ring = new Map<number, VirtualNode>(); // key: uint32 hash
  private keys: number[] = []; // sorted ascending

  constructor(pNodes: string[], vNodeCount: number, fn?: HashFunction) {
    this.hashFn = fn || new Md5Hash();
    for (const node of pNodes) this.addNode(node, vNodeCount);
  }

  addNode(pNode: string, vNodeCount: number): void {
    const existing = this.existingReplicas(pNode);
    for (let i = 0; i < vNodeCount; i++) {
      const v: VirtualNode = { key: virtualNodeRingKey(pNode, i + existing), physicalKey: pNode };
      const key = this.hashFn.hash(v.key);
      if (!this.ring.has(key)) {
        this.keys = insertU32(this.keys, key);
      }
      // 哈希重复时直接在原键上覆盖旧节点（有序表 put 的语义）。
      this.ring.set(key, v);
    }
  }

  removeNode(pNode: string): void {
    const kept: number[] = [];
    for (const key of this.keys) {
      if (this.ring.get(key)!.physicalKey === pNode) {
        this.ring.delete(key);
        continue;
      }
      kept.push(key);
    }
    this.keys = kept;
  }

  // routeNode：取环上第一个 >= hash 的键（尾区间是【闭】区间，即含端点），
  // 越界则回绕到第一个键。
  routeNode(objectKey: string): string | null {
    if (this.keys.length === 0) return null;
    let idx = lowerBound(this.keys, this.hashFn.hash(objectKey));
    if (idx === this.keys.length) idx = 0;
    return this.ring.get(this.keys[idx])!.physicalKey;
  }

  existingReplicas(pNode: string): number {
    let n = 0;
    for (const v of this.ring.values()) {
      if (v.physicalKey === pNode) n++;
    }
    return n;
  }
}

function virtualNodeRingKey(pNode: string, replicaIndex: number): string {
  return `${pNode}-${replicaIndex}`;
}

function insertU32(sorted: number[], v: number): number[] {
  const i = lowerBound(sorted, v);
  sorted.splice(i, 0, v);
  return sorted;
}

function lowerBound(sorted: number[], v: number): number {
  let lo = 0, hi = sorted.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (sorted[mid] < v) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

// 队列在哈希环上的键字符串：`MessageQueue [topic=..., brokerName=...,
// queueId=...]`，必须与经典 remoting 协议的既有形态逐字符一致。
function queueRingKeyString(mq: MessageQueue): string {
  return `MessageQueue [topic=${mq.getTopic()}, brokerName=${mq.getBrokerName()}, queueId=${mq.getQueueId()}]`;
}

// ------------------------------------------- CONSISTENT_HASH

// CONSISTENT_HASH：一致性哈希策略。要点不是均匀而是【稳定】：队列数或消费者
// 数变化时，只有受影响弧段上的队列会换主人，而 AVG 会移动所有人的边界。
export class AllocateMessageQueueConsistentHash implements AllocateMessageQueueStrategy {
  virtualNodeCnt: number;
  customHashFunction?: HashFunction;

  constructor(virtualNodeCnt = 10) {
    this.virtualNodeCnt = virtualNodeCnt;
  }

  name(): string { return 'CONSISTENT_HASH'; }

  allocate(consumerGroup: string, currentCID: string, mqAllRaw: MessageQueue[], cidAllRaw: string[]): MessageQueue[] {
    const mqAll = sortMqAll(mqAllRaw);
    const cidAll = sortCidAll(cidAllRaw);
    if (checkCid(consumerGroup, currentCID, mqAll, cidAll) < 0) return [];
    const router = new ConsistentHashRouter(cidAll, this.virtualNodeCnt, this.customHashFunction);
    const out: MessageQueue[] = [];
    for (const mq of mqAll) {
      const node = router.routeNode(queueRingKeyString(mq));
      if (node !== null && node === currentCID) out.push(mq);
    }
    return out;
  }
}

// ------------------------------------------------- MACHINE_ROOM

// splitTrimmingTrailingEmpty：按分隔符切分并丢掉【末尾】空段（limit=0 的
// split 语义）；但串里没有分隔符时原样整段返回——所以 "" 仍是 [""]，不会
// 变成空数组。
function splitTrimmingTrailingEmpty(text: string, sep: string): string[] {
  const parts = text.split(sep);
  if (parts.length === 1) return parts;
  while (parts.length > 0 && parts[parts.length - 1] === '') parts.pop();
  return parts;
}

// MACHINE_ROOM：broker 名约定为 `<idc>@<brokerName>`；只有 idc 前缀在
// ConsumerIDCs 集合里的队列参与分配，然后按类 AVG 规则切分，余数分给前
// `mod` 个消费者（比较用 `rem > index`，不是 `>=`）。
export class AllocateMessageQueueByMachineRoom implements AllocateMessageQueueStrategy {
  consumerIDCs: Set<string>;

  constructor(idcs: string[]) {
    this.consumerIDCs = new Set(idcs);
  }

  name(): string { return 'MACHINE_ROOM'; }

  allocate(consumerGroup: string, currentCID: string, mqAllRaw: MessageQueue[], cidAllRaw: string[]): MessageQueue[] {
    const mqAll = sortMqAll(mqAllRaw);
    const cidAll = sortCidAll(cidAllRaw);
    const index = checkCid(consumerGroup, currentCID, mqAll, cidAll);
    if (index < 0) return [];
    // consumerIDCs 没有内建默认值（缺省会让 idc 匹配直接炸），这里默认空
    // 集合，即"什么都不分"。
    const candidates: MessageQueue[] = [];
    for (const mq of mqAll) {
      const parts = splitTrimmingTrailingEmpty(mq.getBrokerName(), '@');
      if (parts.length !== 2) continue;
      if (!this.consumerIDCs.has(parts[0])) continue;
      if (parts[1] === '') continue;
      candidates.push(mq);
    }
    const rem = candidates.length % cidAll.length;
    const size = Math.floor(candidates.length / cidAll.length);
    let start = index * size;
    if (rem > index) start += index;
    else start += rem;
    let end = start + size;
    if (rem > index) end++;
    const out: MessageQueue[] = [];
    for (let i = start; i < end; i++) out.push(candidates[i]);
    return out;
  }
}

// 策略工厂：名字 -> 实例（覆盖上面几个内置策略）。
export const ALLOCATE_STRATEGIES: Record<string, () => AllocateMessageQueueStrategy> = {
  AVG: () => new AllocateMessageQueueAveragely(),
  AVG_BY_CIRCLE: () => new AllocateMessageQueueAveragelyByCircle(),
  CONFIG: () => new AllocateMessageQueueByConfig([]),
  MACHINE_ROOM: () => new AllocateMessageQueueByMachineRoom([]),
  CONSISTENT_HASH: () => new AllocateMessageQueueConsistentHash(),
  // MACHINE_ROOM_NEARBY is a PROXY: it needs an inner strategy plus a
  // MachineRoomResolver, neither of which the zero-arg factory signature can
  // supply. It is registered with the same defaults a caller would pass
  // explicitly (AVG inside, brokerName as the room) so a name-based lookup
  // still resolves instead of silently degrading to AVG — which would
  // distribute queues across machine rooms and defeat the whole point.
  MACHINE_ROOM_NEARBY: () => new AllocateMachineRoomNearby(
    new AllocateMessageQueueAveragely(),
    {
      brokerDeployIn: (mq) => mq.getBrokerName(),
      consumerDeployIn: (cid) => cid,
    },
  ),
};

export function createAllocateStrategy(name: string): AllocateMessageQueueStrategy {
  const f = ALLOCATE_STRATEGIES[name];
  return f ? f() : new AllocateMessageQueueAveragely();
}

// --------------------------------------------- MACHINE_ROOM_NEARBY

// MachineRoomResolver：告诉策略某个队列 / 某个消费者住在哪个机房。
// 两个方法都不允许返回 null/空串——机房解析为空会让整轮分配带错终止，
// 消费者把异常抛出去，rebalance 这一轮因而保留现有分配。
export interface MachineRoomResolver {
  brokerDeployIn(messageQueue: MessageQueue): string;
  consumerDeployIn(clientID: string): string;
}

// AllocateMachineRoomNearby：代理在另一个策略外面的"就近机房"分配。队列与
// 消费者先按机房分组，然后
//
//  1. 本消费者所在机房的队列，只分给该机房的消费者（走内层策略）；
//  2. 没有任何存活消费者的机房，其队列由【全部】消费者分摊（同样走内层
//     策略）——否则那些队列就没人消费了。
//
// name() 是 "MACHINE_ROOM_NEARBY" + "-" + <内层名>，日志能看出实际做切分的
// 是哪个算法。
//
// 构造参数为空时抛错；resolver 返回空机房时也抛错。allocate 期间的失败会
// 直接从 allocate() 抛出——rebalance 循环捕获后保留现有分配（异常终止本轮）。
export class AllocateMachineRoomNearby implements AllocateMessageQueueStrategy {
  inner: AllocateMessageQueueStrategy;
  resolver: MachineRoomResolver;

  constructor(inner: AllocateMessageQueueStrategy, resolver: MachineRoomResolver) {
    if (!inner) throw new Error('allocateMessageQueueStrategy is null');
    if (!resolver) throw new Error('machineRoomResolver is null');
    this.inner = inner;
    this.resolver = resolver;
  }

  name(): string { return `MACHINE_ROOM_NEARBY-${this.inner.name()}`; }

  allocate(consumerGroup: string, currentCID: string, mqAllRaw: MessageQueue[], cidAllRaw: string[]): MessageQueue[] {
    const mqAll = sortMqAll(mqAllRaw);
    const cidAll = sortCidAll(cidAllRaw);
    if (checkCid(consumerGroup, currentCID, mqAll, cidAll) < 0) return [];

    // 队列按机房分组：机房间按字典序处理——靠输入排序 + Map 的插入顺序保证
    // 确定性。
    const mr2Mq = new Map<string, MessageQueue[]>();
    for (const mq of mqAll) {
      const room = this.resolver.brokerDeployIn(mq);
      if (!room) {
        throw new Error(`Machine room is null for mq ${queueRingKeyString(mq)}`);
      }
      let list = mr2Mq.get(room);
      if (!list) mr2Mq.set(room, (list = []));
      list.push(mq);
    }
    // Group consumers by machine room (same non-empty rule).
    const mr2C = new Map<string, string[]>();
    for (const cid of cidAll) {
      const room = this.resolver.consumerDeployIn(cid);
      if (!room) {
        throw new Error(`Machine room is null for consumer id ${cid}`);
      }
      let list = mr2C.get(room);
      if (!list) mr2C.set(room, (list = []));
      list.push(cid);
    }

    const out: MessageQueue[] = [];
    // 1. This consumer's room: queues split among same-room consumers only.
    const currentRoom = this.resolver.consumerDeployIn(currentCID);
    const mqHere = mr2Mq.get(currentRoom);
    mr2Mq.delete(currentRoom);
    if (mqHere && mqHere.length > 0) {
      out.push(...this.inner.allocate(consumerGroup, currentCID, mqHere, mr2C.get(currentRoom) ?? []));
    }
    // 2. Rooms with no alive consumer: every consumer shares their queues.
    for (const [room, roomMqs] of mr2Mq) {
      if (!mr2C.has(room)) {
        out.push(...this.inner.allocate(consumerGroup, currentCID, roomMqs, cidAll));
      }
    }
    return out;
  }
}
