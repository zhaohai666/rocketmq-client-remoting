// Queue allocation strategies (Java
// org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueue*).
//
// Deliberate deviation from Java, shared by all ports: Java's
// AbstractAllocateMessageQueueStrategy#check throws IllegalArgumentException
// on bad input; here every strategy returns an empty list instead. Rebalance is
// a background timer — one dirty input must not kill the consumer.
//
// Determinism is the whole point: every instance in a group sorts mqAll and
// cidAll the same way and runs the same index math, otherwise two instances
// compute overlapping assignments and duplicate every message.
import { createHash } from 'node:crypto';
import { MessageQueue } from '../common/message.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.allocate');

export interface AllocateMessageQueueStrategy {
  allocate(consumerGroup: string, currentCID: string, mqAll: MessageQueue[], cidAll: string[]): MessageQueue[];
  name(): string;
}

// checkCid reproduces Java AbstractAllocateMessageQueueStrategy#check: the
// currentCID must be IN cidAll, and Java logs the [BUG] line when it is not.
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

// AllocateMessageQueueAveragely is Java AllocateMessageQueueAveragely.
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

// AllocateMessageQueueAveragelyByCircle is Java
// AllocateMessageQueueAveragelyByCircle.
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

// AllocateMessageQueueByConfig is Java AllocateMessageQueueByConfig: whatever
// the caller configured. Note Java does NO check here — an empty group or
// cidAll still returns the configured list.
export class AllocateMessageQueueByConfig implements AllocateMessageQueueStrategy {
  messageQueueList: MessageQueue[];

  constructor(mqs: MessageQueue[]) {
    this.messageQueueList = [...mqs];
  }

  name(): string { return 'CONFIG'; }

  // Ignores every argument (Java does too — no check runs here).
  allocate(_consumerGroup: string, _currentCID: string, _mqAll: MessageQueue[], _cidAll: string[]): MessageQueue[] {
    return [...this.messageQueueList];
  }
}

// ------------------------------------------------- consistent hash ring

export interface HashFunction {
  hash(key: string): number; // uint32
}

// md5Hash is Java ConsistentHashRouter.MD5Hash: the FIRST FOUR BYTES of the MD5
// digest, big endian. Taking the full 128 bits (or a different slice) puts you
// on a different ring than every Java client.
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
      const v: VirtualNode = { key: javaVirtualNodeKey(pNode, i + existing), physicalKey: pNode };
      const key = this.hashFn.hash(v.key);
      if (!this.ring.has(key)) {
        this.keys = insertU32(this.keys, key);
      }
      // Java TreeMap.put: a duplicate hash is overwritten in place.
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

  // routeNode is Java ConsistentHashRouter#routeNode: tailMap(hash).firstKey(),
  // and TreeMap's tailMap is INCLUSIVE of the endpoint, so the lookup is a
  // "first key >= hash" search. Wrapping past the end returns the first key.
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

function javaVirtualNodeKey(pNode: string, replicaIndex: number): string {
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

// javaMessageQueueString is Java MessageQueue#toString — the string the ring
// hashes. Must match character for character.
function javaMessageQueueString(mq: MessageQueue): string {
  return `MessageQueue [topic=${mq.getTopic()}, brokerName=${mq.getBrokerName()}, queueId=${mq.getQueueId()}]`;
}

// ------------------------------------------- CONSISTENT_HASH

// AllocateMessageQueueConsistentHash is Java
// AllocateMessageQueueConsistentHash. Its point is not evenness but STABILITY:
// when the queue or consumer count changes, only the queues on the affected arc
// switch owner, while AVG moves everybody's boundary.
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
      const node = router.routeNode(javaMessageQueueString(mq));
      if (node !== null && node === currentCID) out.push(mq);
    }
    return out;
  }
}

// ------------------------------------------------- MACHINE_ROOM

// javaSplit mirrors Java String#split(sep) with limit 0: trailing empty
// segments are DROPPED, but a string with no separator is returned whole
// (Java's "if no match was found, return this" early return) — so "" stays [""]
// and does not become empty.
function javaSplit(text: string, sep: string): string[] {
  const parts = text.split(sep);
  if (parts.length === 1) return parts;
  while (parts.length > 0 && parts[parts.length - 1] === '') parts.pop();
  return parts;
}

// AllocateMessageQueueByMachineRoom is Java
// AllocateMessageQueueByMachineRoom. Broker names are expected to look like
// `<idc>@<brokerName>`; only queues whose idc prefix is in ConsumerIDCs take
// part, and those are then split by an AVG-like rule whose remainder goes to
// the FIRST `mod` consumers (java uses `rem > currentIndex`, not `>=`).
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
    // Java's consumeridcs has no default (a nil field NPEs on contains); this
    // port defaults to an empty set, i.e. "nothing is allocated".
    const candidates: MessageQueue[] = [];
    for (const mq of mqAll) {
      const parts = javaSplit(mq.getBrokerName(), '@');
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

// AllocateMessageQueueStrategyFactory: name -> instance (Java
// AllocateMessageQueueStrategyFactory subset the other ports ship).
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

// MachineRoomResolver is Java AllocateMachineRoomNearBy.MachineRoomResolver:
// it tells the strategy which machine room a queue or a consumer lives in.
// Java's javadoc is explicit that neither method may return null/empty — an
// empty room aborts the allocation with an error, which the consumer surfaces
// so the rebalance round can keep the current assignment.
export interface MachineRoomResolver {
  brokerDeployIn(messageQueue: MessageQueue): string;
  consumerDeployIn(clientID: string): string;
}

// AllocateMachineRoomNearby is Java AllocateMachineRoomNearBy: a proxy around
// another strategy. Queues and consumers are grouped by machine room, then
//
//  1. the queues in THIS consumer's room are split among the consumers of that
//     room only (through the inner strategy);
//  2. queues in a room with NO alive consumer at all are shared among ALL
//     consumers (again through the inner strategy) — otherwise nobody would
//     consume them.
//
// name() is "MACHINE_ROOM_NEARBY" + "-" + <inner name> (Java likewise), so
// logs can tell which algorithm actually did the splitting.
//
// Java throws NullPointerException when either constructor argument is null
// and IllegalArgumentException when the resolver returns an empty room; the
// constructor mirrors that as an Error, and the per-allocate failure throws
// out of allocate() — the rebalance loop catches it and keeps the existing
// assignment, which is the Java semantics (an exception aborts the round).
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

    // Group queues by machine room. Java uses TreeMap, i.e. lexicographic room
    // order — the sorted input and Map insertion order keep it deterministic.
    const mr2Mq = new Map<string, MessageQueue[]>();
    for (const mq of mqAll) {
      const room = this.resolver.brokerDeployIn(mq);
      if (!room) {
        throw new Error(`Machine room is null for mq ${javaMessageQueueString(mq)}`);
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
