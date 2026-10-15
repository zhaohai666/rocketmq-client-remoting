// Queue allocation strategies (the queue-splitting algorithms inside a consumer
// group).
//
// One behavioural convention holds everywhere: on illegal input (currentCID not
// in cidAll, empty mqAll/cidAll) nothing is thrown and the strategy returns an
// empty list — rebalance is a background timer, and one dirty input must not kill
// the consumer.
//
// Determinism is what matters: every instance in the group must sort mqAll and
// cidAll the same way and run the same index arithmetic, otherwise two instances
// compute overlapping allocations and every message is consumed twice.
import { createHash } from 'node:crypto';
import { MessageQueue } from '../common/message.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('client.allocate');

export interface AllocateMessageQueueStrategy {
  allocate(consumerGroup: string, currentCID: string, mqAll: MessageQueue[], cidAll: string[]): MessageQueue[];
  name(): string;
}

// checkCid is the shared pre-check for all strategies: currentCID must be in
// cidAll; when it is not, log a [BUG] line and let the strategy return an empty
// allocation.
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

// AVG: hands contiguous ranges of the sorted queues to the consumers of the
// group, with the remainder going to the first `mod` of them.
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

// AVG_BY_CIRCLE: takes queues in a round-robin circle over the sorted cidAll.
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

// CONFIG: returns the queue list the caller configured, verbatim. Note that it
// does **no validation at all** here — an empty group or cidAll still returns
// the configured list.
export class AllocateMessageQueueByConfig implements AllocateMessageQueueStrategy {
  messageQueueList: MessageQueue[];

  constructor(mqs: MessageQueue[]) {
    this.messageQueueList = [...mqs];
  }

  name(): string { return 'CONFIG'; }

  // Every argument is ignored (no validation runs here).
  allocate(_consumerGroup: string, _currentCID: string, _mqAll: MessageQueue[], _cidAll: string[]): MessageQueue[] {
    return [...this.messageQueueList];
  }
}

// ------------------------------------------------- consistent hash ring

export interface HashFunction {
  hash(key: string): number; // uint32
}

// md5 hash: takes the **first four bytes** of the MD5 digest and assembles them
// big-endian into a uint32. Using the whole 128-bit value (or a different slice)
// lands on a different hash ring than existing clients use, and queue ownership
// no longer lines up.
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
      // On a hash collision the old node is overwritten on the same key (the
      // semantics of an ordered-map put).
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

  // routeNode: takes the first key on the ring that is >= the hash (the wrap-around
  // range is a **closed** interval, endpoint included), wrapping to the first key
  // when out of range.
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

// The key string a queue is hashed by on the ring:
// `MessageQueue [topic=..., brokerName=..., queueId=...]` — it must match the
// existing shape of the classic remoting protocol character for character.
function queueRingKeyString(mq: MessageQueue): string {
  return `MessageQueue [topic=${mq.getTopic()}, brokerName=${mq.getBrokerName()}, queueId=${mq.getQueueId()}]`;
}

// ------------------------------------------- CONSISTENT_HASH

// CONSISTENT_HASH: the consistent-hash strategy. The point is not uniformity but
// **stability**: when the queue count or the consumer count changes, only the
// queues on the affected arc change owner, whereas AVG moves everyone's boundary.
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

// splitTrimmingTrailingEmpty: splits on the separator and drops **trailing**
// empty segments (the limit=0 split semantics); but with no separator present the
// whole string is returned as one segment — so "" stays [""], it does not become
// an empty array.
function splitTrimmingTrailingEmpty(text: string, sep: string): string[] {
  const parts = text.split(sep);
  if (parts.length === 1) return parts;
  while (parts.length > 0 && parts[parts.length - 1] === '') parts.pop();
  return parts;
}

// MACHINE_ROOM: broker names follow the `<idc>@<brokerName>` convention; only
// queues whose idc prefix is in the ConsumerIDCs set take part in allocation, and
// they are then split by the AVG-like rule with the remainder going to the first
// `mod` consumers (the comparison is `rem > index`, not `>=`).
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
    // consumerIDCs has no built-in default (omitting it would blow up idc
    // matching); the default here is the empty set, i.e. "assign nothing".
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

// Strategy factory: name -> instance (covering the built-in strategies above).
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

// MachineRoomResolver: tells the strategy which machine room a given queue or a
// given consumer lives in. Neither method may return null/an empty string — an
// empty room would make the whole allocation round carry a wrong terminator, the
// consumer throws, and this rebalance round therefore keeps the existing
// allocation.
export interface MachineRoomResolver {
  brokerDeployIn(messageQueue: MessageQueue): string;
  consumerDeployIn(clientID: string): string;
}

// AllocateMachineRoomNearby: "nearby machine room" allocation as a proxy in
// front of another strategy. Queues and consumers are first grouped by machine
// room, then
//
//  1. the queues of this consumer's room go only to the consumers of that room
//     (handled by the inner strategy);
//  2. the queues of any room with no live consumer are shared by **all**
//     consumers (also via the inner strategy) — otherwise those queues would
//     have nobody consuming them.
//
// name() is "MACHINE_ROOM_NEARBY" + "-" + <inner name>, so logs reveal which
// algorithm actually does the splitting.
//
// The constructor throws on empty arguments, and so does a resolver that returns
// an empty machine room. Failures during allocate propagate straight out of
// allocate() — the rebalance loop catches them and keeps the existing allocation
// (aborting the round).
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

    // Group queues by machine room: the rooms are processed in lexicographic order —
    // determinism comes from sorting the input plus Map insertion order.
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
