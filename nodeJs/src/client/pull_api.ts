// Pull path (Java org.apache.rocketmq.client.impl.consumer.PullAPIWrapper).
//
// Three rules here are easy to get wrong and expensive to debug — they are
// called out again at each site:
//
//  1. pull() being a SHORT poll has nothing to do with the push consumer's
//     LONG poll. What matters here is that the COMMIT_OFFSET bit is cleared
//     whenever the request lands on a slave — committing an offset to a slave
//     is pointless.
//  2. The `subscription` extField is written ONLY when the SUBSCRIPTION sysFlag
//     bit is on. The broker branches on the bit, so an expression that travels
//     without the bit is ignored, and a bit that travels without the value
//     makes the broker reject the request with SUBSCRIPTION_NOT_EXIST. The
//     pull / lite-pull consumers ALWAYS send the expression.
//  3. Every response rewrites pullFromWhichNodeTable from suggestWhichBrokerId,
//     and a MISSING field means master(0) — not "keep the previous value".
//     The next round uses that id to pick master-or-slave.
import { Buffer } from 'node:buffer';
import type { MQClient } from './mq_client.ts';
import { PullStatus, PullResult } from './consumer_result.ts';
import { PullMessageRequestHeader } from '../remoting/headers.ts';
import { RemotingCommand } from '../remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../remoting/codes.ts';
import { findBrokerAddressInSubscribe } from '../remoting/route.ts';
import { PullSysFlag } from '../common/sysflag.ts';
import { MessageExt, MessageQueue } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';
import { MixAll } from '../common/mixAll.ts';
import { SubscriptionData, ExpressionType } from '../common/subscriptionData.ts';
import { FilterMessageContext, FilterMessageHook } from './hook.ts';
import { decodeMessages } from '../common/messageDecoder.ts';
import { getLogger } from '../logging.ts';
import { mqKey } from './offset_store.ts';

const logger = getLogger('client.pull_api');

export class PullAPI {
  private mqClient: MQClient;
  private group: string;
  private unitMode = false;
  hooks: FilterMessageHook[] = [];

  // pullFromWhichNode maps mqKey -> the brokerId to ask next round.
  private pullFromWhichNode = new Map<string, number>();

  constructor(mqClient: MQClient, group: string) {
    this.mqClient = mqClient;
    this.group = group;
  }

  // recalculatePullFromWhichNode: the table value, or MASTER_ID when the queue
  // has no entry.
  recalculatePullFromWhichNode(mq: MessageQueue): number {
    const id = this.pullFromWhichNode.get(mqKey(mq));
    return id === undefined ? MixAll.MASTER_ID : id;
  }

  pullFromWhichNode(mq: MessageQueue): number | null {
    const id = this.pullFromWhichNode.get(mqKey(mq));
    return id === undefined ? null : id;
  }

  updatePullFromWhichNode(mq: MessageQueue, brokerID: number): void {
    this.pullFromWhichNode.set(mqKey(mq), brokerID);
  }

  // forgetPullFromWhichNode drops one queue's entry (Java's removeProcessQueue
  // path rebuilds the entry from scratch).
  forgetPullFromWhichNode(mq: MessageQueue): void {
    this.pullFromWhichNode.delete(mqKey(mq));
  }

  // pulledQueues is the table's key set rebuilt as MessageQueues — Java's
  // pullFromWhichNodeTable keys, i.e. "the assignment this consumer is actually
  // working". mqKey is the canonical topic@brokerName@queueId triple and neither
  // name may contain '@' (Validators), so the split is unambiguous.
  pulledQueues(): MessageQueue[] {
    const out: MessageQueue[] = [];
    for (const key of this.pullFromWhichNode.keys()) {
      const first = key.indexOf('@');
      const last = key.lastIndexOf('@');
      if (first <= 0 || last <= first) continue;
      out.push(new MessageQueue(key.slice(0, first), key.slice(first + 1, last),
        parseInt(key.slice(last + 1), 10) || 0));
    }
    return out;
  }

  // findBrokerAddressInSubscribe mirrors Java MQClientInstance
  // #findBrokerAddressInSubscribe. Returns (addr, isSlave) or null. The
  // isSlave flag comes from the id that was ACTUALLY matched.
  private findAddrInSubscribe(brokerName: string, brokerId: number,
    onlyThisBroker: boolean): { addr: string; isSlave: boolean } | null {
    const addrs = this.mqClient.getBrokerAddrTable().get(brokerName);
    if (!addrs || Object.keys(addrs).length === 0) return null;
    const snapshot = new Map<number, string>();
    for (const [id, addr] of Object.entries(addrs)) snapshot.set(parseInt(id, 10), addr);
    const hit = snapshot.get(brokerId);
    if (hit != null) return { addr: hit, isSlave: brokerId !== MixAll.MASTER_ID };
    if (brokerId !== MixAll.MASTER_ID) {
      const next = snapshot.get(brokerId + 1);
      if (next != null) return { addr: next, isSlave: true };
    }
    if (!onlyThisBroker) {
      let minID = 0;
      let first = true;
      for (const id of snapshot.keys()) {
        if (first || id < minID) { minID = id; first = false; }
      }
      return { addr: snapshot.get(minID)!, isSlave: minID !== MixAll.MASTER_ID };
    }
    return null;
  }

  // pullKernel mirrors Java PullAPIWrapper#pullKernelImpl.
  //
  // suspendTimeoutMillis > 0 makes the broker hold the request (LONG poll);
  // the short-poll callers pass 0 with suspend=false in sysFlag.
  async pullKernel(mq: MessageQueue, offset: number, sub: SubscriptionData,
    sysFlag: number, commitOffset: number, maxMsgNums: number,
    suspendTimeoutMillis: number, timeoutMillis: number,
    requestCode: number = RequestCode.PULL_MESSAGE, maxMsgBytes = -1): Promise<PullResult> {
    const resolved = this.findAddrInSubscribe(mq.getBrokerName(), this.recalculatePullFromWhichNode(mq), false);
    let addr: string;
    let sysFlagFinal = sysFlag;
    if (!resolved) {
      await this.mqClient.updateTopicRouteInfoFromNameServer(mq.getTopic(), false).catch(() => {});
      const retry = this.findAddrInSubscribe(mq.getBrokerName(), this.recalculatePullFromWhichNode(mq), false);
      if (!retry) throw new Error(`The broker[${mq.getBrokerName()}] not exist`);
      addr = retry.addr;
      if (retry.isSlave) sysFlagFinal = PullSysFlag.clearCommitOffsetFlag(sysFlagFinal);
    } else {
      addr = resolved.addr;
      if (resolved.isSlave) sysFlagFinal = PullSysFlag.clearCommitOffsetFlag(sysFlagFinal);
    }

    const header = new PullMessageRequestHeader();
    header.consumerGroup = this.group;
    header.topic = mq.getTopic();
    header.queueId = mq.getQueueId();
    header.queueOffset = offset;
    header.maxMsgNums = maxMsgNums;
    header.sysFlag = sysFlagFinal;
    header.commitOffset = commitOffset;
    header.suspendTimeoutMillis = suspendTimeoutMillis;
    header.subVersion = sub.subVersion;
    header.expressionType = sub.expressionType || ExpressionType.TAG;
    header.maxMsgBytes = maxMsgBytes;
    // Rule 2: the expression rides the wire only when the SUBSCRIPTION bit is
    // on. The key must not be present at all when the bit is off.
    if (PullSysFlag.hasSubscriptionFlag(sysFlagFinal)) {
      header.subscription = sub.subString;
    }
    const request = RemotingCommand.createRequestCommand(requestCode, header);
    const response = await this.mqClient.remotingClient.invokeSync(addr, request, timeoutMillis);
    let status: number;
    if (response.code === ResponseCode.SUCCESS) status = PullStatus.FOUND;
    else if (response.code === ResponseCode.PULL_NOT_FOUND) status = PullStatus.NO_NEW_MSG;
    else if (response.code === ResponseCode.PULL_OFFSET_MOVED) status = PullStatus.OFFSET_ILLEGAL;
    else if (response.code === ResponseCode.PULL_RETRY_IMMEDIATELY) status = PullStatus.NO_MATCHED_MSG;
    else throw new Error(`pull failed: CODE ${response.code} ${response.remark || ''}`);
    const ext = response.extFields || {};
    const num = (k: string, d: number) => (ext[k] != null ? parseInt(ext[k], 10) : d);
    let found: MessageExt[] = [];
    if (response.body && response.body.length > 0) {
      found = decodeMessages(response.body);
      for (const msg of found) {
        msg.setBrokerName(mq.getBrokerName());
        msg.setQueueId(mq.getQueueId());
      }
    }
    const suggest = ext['suggestWhichBrokerId'] != null ? parseInt(ext['suggestWhichBrokerId'], 10) : null;
    return new PullResult(status, num('nextBeginOffset', 0), num('minOffset', 0),
      num('maxOffset', 0), found, suggest);
  }

  // pullKernelLite is the LITE_PULL_MESSAGE(361) flavour the lite pull
  // consumer uses: identical header and response shape, and the caller has
  // already set the FLAG_LITE_PULL_MESSAGE bit in sysFlag. The pull is always
  // a SHORT poll with no inline offset commit, so commitOffset is fixed at 0
  // and the broker hold budget at 15s.
  async pullKernelLite(mq: MessageQueue, offset: number, sub: SubscriptionData,
    sysFlag: number, maxMsgNums: number, timeoutMillis: number): Promise<PullResult> {
    return this.pullKernel(mq, offset, sub, sysFlag, 0, maxMsgNums, 15000, timeoutMillis,
      RequestCode.LITE_PULL_MESSAGE);
  }

  // processPullResult mirrors Java PullAPIWrapper#processPullResult. It MUST
  // run for every response, not only FOUND ones — rule 3: the node table is
  // fed by every reply, and absent means master.
  processPullResult(mq: MessageQueue, result: PullResult, sub: SubscriptionData): PullResult {
    const brokerId = result.suggestWhichBrokerId != null ? result.suggestWhichBrokerId : MixAll.MASTER_ID;
    this.updatePullFromWhichNode(mq, brokerId);
    if (result.status !== PullStatus.FOUND || result.msgFoundList.length === 0) return result;
    let msgs = clientSideTagFilter(sub, result.msgFoundList);
    if (this.hooks.length > 0) {
      // FilterMessageHook MUST swallow exceptions and continue: the pull path
      // silently skips filtered messages.
      const ctx = new FilterMessageContext();
      ctx.consumerGroup = this.group;
      ctx.msgList = msgs;
      ctx.mq = mq;
      ctx.unitMode = this.unitMode;
      ctx.accessChannel = 'LOCAL';
      for (const hook of this.hooks.slice()) {
        try { hook.filterMessage(ctx); } catch (e) { /* swallowed by contract */ }
      }
      msgs = ctx.msgList;
    }
    for (const msg of msgs) {
      if (msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED) === 'true') {
        const uniq = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if (uniq) msg.setTransactionId(uniq);
      }
      msg.putProperty(MessageConst.PROPERTY_MIN_OFFSET, String(result.minOffset));
      msg.putProperty(MessageConst.PROPERTY_MAX_OFFSET, String(result.maxOffset));
      msg.setBrokerName(mq.getBrokerName());
    }
    result.msgFoundList = msgs;
    return result;
  }
}

// clientSideTagFilter is Java PullAPIWrapper.processPullResult:113-122 — the
// second, string-level tag check. The guard `!tagsSet.isEmpty()` is why
// FilterAPI's SUB_ALL path must keep tagsSet EMPTY: subscribing to "*" turns
// the filter off entirely rather than filtering everything out.
function clientSideTagFilter(sub: SubscriptionData, msgs: MessageExt[]): MessageExt[] {
  // tagsSet is a Set in this port (Java: HashSet) — use .size, NOT .length.
  // A `.length === 0` guard on a Set is always false, which silently turns
  // the '*' subscription (empty tagsSet → filter OFF) into "filter everything".
  const tagsSet: any = sub ? (sub as any).tagsSet : null;
  const empty = tagsSet == null ||
    (typeof tagsSet.size === 'number' ? tagsSet.size === 0 : tagsSet.length === 0);
  if (msgs.length === 0 || !sub || empty || sub.classFilterMode) {
    return msgs;
  }
  const accepted: Set<string> = typeof (tagsSet as any).has === 'function'
    ? tagsSet : new Set(tagsSet);
  const out: MessageExt[] = [];
  for (const msg of msgs) {
    const tags = msg.getProperty(MessageConst.PROPERTY_TAGS);
    if (!tags) continue;
    if (accepted.has(tags)) out.push(msg);
  }
  return out;
}
