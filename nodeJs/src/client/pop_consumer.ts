// -*- coding: utf-8 -*-
// POP-mode consumption (Java
// ConsumeMessagePopConcurrentlyService + DefaultMQPushConsumerImpl.popMessage /
// ackAsync / changePopInvisibleTimeAsync / checkNeedAckOrDelay; port of
// go/client/pop_consumer.go).
//
// POP differs from pull in ways that make a "just reuse the pull loop" port
// wrong at every step:
//
//  1. There is no client-side offset. The broker hands out an INVISIBLE batch
//     and the client either ACKs it or asks for more invisible time. A queue's
//     "cursor" is the broker's revive queue, not a number we store.
//  2. Flow control is on the ANSWER DEBT (waitAckCounter >
//     popThresholdForQueue), not on buffered messages.
//  3. A batch can expire WHILE the listener runs (invisibleTime is wall
//     clock). Such a batch must not be acked — the broker has already taken it
//     back and someone else may own it. Java checks this twice: before the
//     listener and again after.
//  4. Redelivery is "extend the invisibility window", never a send-back. The
//     broker's revive logic re-pops the message once the window closes.
//
// Orderly POP exists in Java as a stub ("POPTODO think of pop mode orderly
// implementation later") and is rejected here at config time rather than
// half-implemented.
import type { DefaultMQPushConsumer } from './consumer.ts';
import { MessageExt, MessageQueue } from '../common/message.ts';
import { MessageConst } from '../common/messageConst.ts';
import { MixAll } from '../common/mixAll.ts';
import { mqKey } from './offset_store.ts';
import { PopProcessQueue } from './pop_process_queue.ts';
import {
  PopStatus, PopResult, POP_DELAY_LEVEL, popDelayLevelSeconds, popDelayLevelForElapsed,
  popBatchTimedOut, popInvisibleTimeOf, ckParse, getRealTopicFromCk,
} from './pop_api.ts';
import { AckMessageRequestHeader, ChangeInvisibleTimeRequestHeader, PopMessageRequestHeader } from '../remoting/headers.ts';
import { MESSAGE_REQUEST_MODE_POP } from '../remoting/pop_bodies.ts';
import { ResponseCode } from '../remoting/codes.ts';
import { RemotingTimeoutException } from '../remoting/exception.ts';
import { ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus } from './consumer_result.ts';
import { ConsumeMessageContext } from './hook.ts';
import { getLogger } from '../logging.ts';

const logger = getLogger('pop');

// POP timing constants (Java DefaultMQPushConsumerImpl:105-140).
export const POP_BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15; // long-poll budget sent as pollTime
export const POP_ASYNC_TIMEOUT_MILLIS = 3000;               // ACK and CHANGE_INVISIBLETIME
export const POP_REQUEST_EXTRA_NETWORK_MILLIS = 10 * 1000;  // "+10s" on the long-poll timeout

export const POP_MIN_INVISIBLE_TIME = 5000;
export const POP_MAX_INVISIBLE_TIME = 300000;
// Fallback Java applies when the configured value is out of range — NOT an
// error at request time.
export const POP_DEFAULT_INVISIBLE_TIME = 60000;

export const POP_DELAY_WHEN_CACHE_FLOW_CONTROL = 50;
export const POP_DELAY_WHEN_BROKER_FLOW = 20;
export const POP_DELAY_WHEN_SUSPEND = 1000;
export const POP_DELAY_WHEN_EXCEPTION = 3000;

// ---------------------------------------------------------------------------
// pop loop
// ---------------------------------------------------------------------------

// queuePopLoop pops one queue until it is retired or the consumer stops.
export async function queuePopLoop(c: DefaultMQPushConsumer, mq: MessageQueue, stopFlag: string): Promise<void> {
  const key = mqKey(mq);
  while (!c._isStopped(stopFlag)) {
    const pq = c.popQueueTable.get(key);
    if (!pq || pq.isDropped()) return;
    pq.setLastPopTimestamp(Date.now());

    const sub = c.subscription.get(mq.getTopic());
    if (!sub) return;
    // Flow control on the outstanding-ACK debt. Java uses a strict ">"
    // against popThresholdForQueue.
    if (pq.waitAckMsgCount() > c.popThresholdForQueue) {
      logger.debug('pop flow control: queue %s waiting-ack=%d exceeds threshold %d',
        key, pq.waitAckMsgCount(), c.popThresholdForQueue);
      if (await c._sleepOrStop(POP_DELAY_WHEN_CACHE_FLOW_CONTROL, stopFlag)) return;
      continue;
    }

    // Java:541 stamps beginTimestamp immediately before the POP request, so
    // the POP round trip includes the broker's poll hold.
    const begin = Date.now();
    let result: PopResult;
    try {
      result = await popOnce(c, mq, sub);
    } catch (e) {
      const code = (e as any).code;
      if (code === ResponseCode.FLOW_CONTROL) {
        // Broker-side flow control: back off briefly, do not rescan.
        if (await c._sleepOrStop(POP_DELAY_WHEN_BROKER_FLOW, stopFlag)) return;
        continue;
      }
      if (e instanceof RemotingTimeoutException) {
        // A long poll timing out while idle is normal (the broker clamps the
        // suspend time); keep it at debug so the run log stays clean.
        logger.debug('pop long-poll timeout for %s (benign, will retry): %s', key, (e as Error).message);
      } else {
        logger.debug('pop error for %s: %s', key, (e as Error).message);
        if (await c._sleepOrStop(POP_DELAY_WHEN_EXCEPTION, stopFlag)) return;
      }
      continue;
    }

    if (c._isStopped(stopFlag)) return;
    switch (result.popStatus) {
      case PopStatus.FOUND: {
        // Java DefaultMQPushConsumerImpl:555-563 — the RT counter is bumped
        // for a FOUND answer even when the list is empty; only the TPS
        // counter is conditioned on a non-empty list.
        c._incPullRT(mq.getTopic(), Date.now() - begin);
        if (result.msgFoundList.length === 0) {
          // Java: FOUND with an empty list retries immediately.
          continue;
        }
        c._incPullTPS(mq.getTopic(), result.msgFoundList.length);
        pq.incFoundMsg(result.msgFoundList.length);
        submitPopConsume(c, mq, result.msgFoundList, pq);
        if (c.pullInterval > 0 && await c._sleepOrStop(c.pullInterval, stopFlag)) return;
        break;
      }
      case PopStatus.NO_NEW_MSG:
      case PopStatus.POLLING_NOT_FOUND:
        // Retry immediately: the broker already held the poll open.
        continue;
      default:
        // POLLING_FULL and anything else: back off.
        if (await c._sleepOrStop(POP_DELAY_WHEN_EXCEPTION, stopFlag)) return;
        break;
    }
  }
}

// popOnce issues one POP_MESSAGE and returns the processed result.
async function popOnce(c: DefaultMQPushConsumer, mq: MessageQueue, sub: any): Promise<PopResult> {
  const client = c.mqClient;
  if (!client) throw new Error('consumer not started');
  // Java DefaultMQPushConsumerImpl:608-611 — an out-of-range configured value
  // is replaced by 60s at REQUEST time; checkConfig has already rejected the
  // startup, so this only bites when the setter runs mid-flight.
  let invisibleTime = c.popInvisibleTime;
  if (invisibleTime < POP_MIN_INVISIBLE_TIME || invisibleTime > POP_MAX_INVISIBLE_TIME) {
    invisibleTime = POP_DEFAULT_INVISIBLE_TIME;
  }

  let resolved = c._findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
  if (!resolved) {
    await client.updateTopicRouteInfoFromNameServer(mq.getTopic(), false).catch(() => {});
    resolved = c._findBrokerAddressInSubscribe(mq.getBrokerName(), MixAll.MASTER_ID, true);
    if (!resolved) {
      throw new Error('The broker[' + mq.getBrokerName() + '] not exist');
    }
  }

  const header = new PopMessageRequestHeader();
  header.bname = mq.getBrokerName();
  header.consumerGroup = c.consumerGroup;
  header.topic = mq.getTopic();
  header.queueId = mq.getQueueId();
  header.maxMsgNums = c.popBatchNums;
  header.invisibleTime = invisibleTime;
  header.pollTime = POP_BROKER_SUSPEND_MAX_TIME_MILLIS;
  header.bornTime = Date.now();
  header.initMode = 0;
  // order stays false: this port does not do orderly POP.
  header.order = false;
  if (sub) {
    header.expType = sub.expressionType;
    header.exp = sub.subString;
  }
  // Java PullAPIWrapper.popAsync:386-392 — poll=true always here, and the
  // network timeout gets +10s on top of the broker hold budget.
  const timeout = POP_BROKER_SUSPEND_MAX_TIME_MILLIS + POP_REQUEST_EXTRA_NETWORK_MILLIS;
  return client.popMessage(mq.getBrokerName(), resolved.addr, header, c.namespace, timeout);
}

// ---------------------------------------------------------------------------
// consume
// ---------------------------------------------------------------------------

// submitPopConsume splits one pop batch into consumeMessageBatchMaxSize chunks
// and consumes them, mirroring
// ConsumeMessagePopConcurrentlyService#submitPopConsumeRequest.
function submitPopConsume(c: DefaultMQPushConsumer, mq: MessageQueue, msgs: MessageExt[], pq: PopProcessQueue): void {
  const batchSize = Math.max(1, c.consumeMessageBatchMaxSize);
  for (let total = 0; total < msgs.length;) {
    const end = Math.min(total + batchSize, msgs.length);
    const chunk = msgs.slice(total, end);
    total = end;
    if (!c._beginInFlight()) {
      // Shutdown froze new work: hand the debt back so nothing is left
      // waiting for an ACK that will never come.
      pq.decFoundMsg(-chunk.length);
      return;
    }
    void consumePopBatch(c, mq, chunk, pq)
      .catch((e) => {
        logger.error('pop consume batch error for %s: %s', mqKey(mq), (e as Error).message);
        pq.decFoundMsg(-chunk.length);
      })
      .finally(() => c._endInFlight());
  }
}

// consumePopBatch is Java ConsumeMessagePopConcurrentlyService$ConsumeRequest
// .run + processConsumeResult.
async function consumePopBatch(c: DefaultMQPushConsumer, mq: MessageQueue, batch: MessageExt[], pq: PopProcessQueue): Promise<void> {
  if (batch.length === 0) return;
  const key = mqKey(mq);
  if (pq.isDropped()) {
    logger.debug('pop batch dropped for %s (queue retired)', key);
    return;
  }
  // The window can already have closed while this batch sat in the dispatch
  // queue. Java aborts here without acking — the broker has revived the
  // messages and another consumer may hold them.
  if (popBatchTimedOut(batch)) {
    logger.debug('the pop message time out so abort consume, mq=%s', key);
    pq.decFoundMsg(-batch.length);
    return;
  }

  c._resetRetryTopicAndNamespace(batch);
  const ctx = new ConsumeConcurrentlyContext(mq);

  const hooks = c.consumeMessageHookList;
  let hookCtx: ConsumeMessageContext | null = null;
  if (hooks.length > 0) {
    hookCtx = new ConsumeMessageContext(c.consumerGroup, batch, mq.getTopic());
    hookCtx.mq = mq;
    hookCtx.success = false;
    hookCtx.props = {};
    for (const hook of hooks.slice()) {
      try { hook.consumeMessageBefore(hookCtx); } catch (e) { /* hook errors don't stop delivery */ }
    }
  }

  const begin = Date.now();
  // ConsumeMessagePopConcurrentlyService:380-382 — same CONSUME_START_TIME
  // stamping as the pull path.
  for (const msg of batch) {
    msg.putProperty('CONSUME_START_TIME', String(Date.now()));
  }

  const { status: rawStatus, threw } = c._listenerCall(batch, ctx);
  let status = rawStatus;
  if (threw) {
    logger.debug('pop listener error, treat as RECONSUME_LATER: mq=%s', key);
    status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
  }
  const consumeRT = Date.now() - begin;
  if (status !== ConsumeConcurrentlyStatus.CONSUME_SUCCESS
    && status !== ConsumeConcurrentlyStatus.RECONSUME_LATER) {
    logger.warning('consumeMessage return unknown status, Group: %s Msgs: %d MQ: %s',
      c.consumerGroup, batch.length, key);
    status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
  }
  const invisibleTime = popInvisibleTimeOf(batch);
  if (hookCtx) {
    // The POP flavour of the consume-hook epilogue. It cannot reuse the pull
    // path's rule: the pull path declares TIME_OUT when the listener took
    // longer than consumeTimeout MINUTES, while the POP path declares it when
    // the listener took longer than the batch's INVISIBILITY window — which is
    // seconds, not minutes. Sharing the rule would make a POP trace report
    // SUCCESS for a listener that overran its window.
    const unknown = rawStatus !== ConsumeConcurrentlyStatus.CONSUME_SUCCESS
      && rawStatus !== ConsumeConcurrentlyStatus.RECONSUME_LATER;
    let type: string;
    if (unknown) {
      type = threw ? 'EXCEPTION' : 'RETURNNULL';
    } else if (consumeRT >= invisibleTime * 1000) {
      type = 'TIME_OUT';
    } else if (status === ConsumeConcurrentlyStatus.RECONSUME_LATER) {
      type = 'FAILED';
    } else {
      type = 'SUCCESS';
    }
    hookCtx.props['ConsumeContextType'] = type;
    hookCtx.success = status === ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    hookCtx.status = status === ConsumeConcurrentlyStatus.CONSUME_SUCCESS
      ? 'CONSUME_SUCCESS' : 'RECONSUME_LATER';
    for (const hook of hooks.slice()) {
      try { hook.consumeMessageAfter(hookCtx); } catch (e) { /* hooks never break delivery */ }
    }
  }
  // Java ConsumeMessagePopConcurrentlyService:424-425 — CONSUME_RT is bumped
  // after the hook and before the validity re-check, so a batch that expired
  // while the listener ran still contributes its RT (the listener DID run).
  c._incConsumeRT(mq.getTopic(), consumeRT);

  // Java checks the window AGAIN after the listener: a slow listener must not
  // ACK messages the broker already owns.
  if (pq.isDropped() || popBatchTimedOut(batch)) {
    logger.warning('processQueue invalid or popTimeout, mq=%s', key);
    pq.decFoundMsg(-batch.length);
    return;
  }
  processPopConsumeResult(c, mq, batch, pq, status, ctx);
}

// processPopConsumeResult is Java
// ConsumeMessagePopConcurrentlyService#processConsumeResult.
//
// Two loops, and EVERY message decrements the debt exactly once, so the
// counter returns to zero whatever the outcome:
//
//   - [0, ackIndex] are ACKed;
//   - (ackIndex, size) are re-hidden: either with a longer invisibility window
//     (changePopInvisibleTime) or — once the message has exhausted its retry
//     budget — by checkNeedAckOrDelay, which gives up and ACKs when the message
//     is older than twice the longest delay.
function processPopConsumeResult(c: DefaultMQPushConsumer, mq: MessageQueue, batch: MessageExt[],
  pq: PopProcessQueue, status: number, ctx: ConsumeConcurrentlyContext): void {

  let ackIndex = -1;
  if (status === ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
    ackIndex = ctx.ackIndex;
    if (ackIndex >= batch.length) ackIndex = batch.length - 1;
  }
  // Java ConsumeMessagePopConcurrentlyService:186-203 — the same OK/Failed
  // accounting as the pull path, from the same clamped ackIndex the ACK loop
  // below uses, and likewise before the loop.
  if (status === ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
    c._incConsumeOKTPS(mq.getTopic(), ackIndex + 1);
    if (batch.length - ackIndex - 1 > 0) {
      c._incConsumeFailedTPS(mq.getTopic(), batch.length - ackIndex - 1);
    }
  } else {
    c._incConsumeFailedTPS(mq.getTopic(), batch.length);
  }

  for (let i = 0; i < batch.length; i++) {
    const msg = batch[i];
    if (i <= ackIndex) {
      ackMessagePop(c, msg);
    } else if (msg.getReconsumeTimes() >= popMaxReconsumeTimes(c)) {
      // Retry budget exhausted. checkNeedAckOrDelay either ACKs the message
      // for good or extends its invisibility by the next notch.
      checkNeedAckOrDelay(c, msg);
    } else {
      const delayLevel = ctx.delayLevelWhenNextConsume;
      changePopInvisibleTime(c, msg, delayLevel);
    }
    pq.ack();
  }
}

// popMaxReconsumeTimes is Java DefaultMQPushConsumerImpl.getMaxReconsumeTimes
// for the POP service: -1 means 16 (the concurrent rule), NOT Number.MAX_VALUE.
function popMaxReconsumeTimes(c: DefaultMQPushConsumer): number {
  return c.maxReconsumeTimes === -1 ? 16 : c.maxReconsumeTimes;
}

// ---------------------------------------------------------------------------
// ack / extend
// ---------------------------------------------------------------------------

// popAckAddress resolves the broker address an ACK/CHANGE_INVISIBLETIME
// belongs to. Java finds it by the LOGICAL broker name from checkpoint segment
// 5 against the master, refreshing the route once.
//
// Deliberate divergence: Java's ackAsync/changePopInvisibleTimeAsync first map
// a checkpoint broker name beginning with "__syslo__" (MixAll
// .LOGICAL_QUEUE_MOCK_BROKER_PREFIX) through the topicEndPointsTable built from
// TopicRouteData.topicQueueMapping. This port decodes topicQueueMappingByBroker
// as an opaque MappingEntry list and keeps no topicEndPointsTable, so there is
// no address to map to and the branch is not taken. The static-topic /
// logical-queue routing feature is therefore a known P3 gap, not a silent
// fallback: the header still carries the LOGICAL name Java would send.
async function popAckAddress(c: DefaultMQPushConsumer, brokerName: string, topic: string): Promise<string> {
  const client = c.mqClient;
  if (!client) throw new Error('consumer not started');
  let resolved = c._findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
  if (!resolved) {
    await client.updateTopicRouteInfoFromNameServer(topic, false).catch(() => {});
    resolved = c._findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
  }
  if (!resolved) {
    throw new Error('The broker[' + brokerName + '] master node does not exist');
  }
  return resolved.addr;
}

// ackMessagePop is Java DefaultMQPushConsumerImpl#ackAsync.
//
// The request fields are read back out of the checkpoint, with two exceptions:
// the topic is re-derived through getRealTopic (so a retried message ACKs on
// the pop-retry topic) and the offset is segment 7 — the message's own queue
// offset, never the batch start.
export async function ackMessagePop(c: DefaultMQPushConsumer, msg: MessageExt): Promise<void> {
  const extraInfo = msg.getProperty(MessageConst.PROPERTY_POP_CK);
  if (!extraInfo) {
    logger.warning('ack skipped: message %s carries no POP_CK', msg.getMsgId());
    return;
  }
  const parsed = ckParse(extraInfo);
  if (!parsed) {
    logger.warning('ack failed to parse POP_CK %q', extraInfo);
    return;
  }
  const { brokerName, queueId, queueOffset } = parsed;
  // Java resolves the topic through the checkpoint's retry marker because the
  // message's own topic has been rewritten to the request topic.
  const topic = getRealTopicFromCk(extraInfo, msg.getTopic(), c.consumerGroup);

  let addr: string;
  try {
    addr = await popAckAddress(c, brokerName, msg.getTopic());
  } catch (e) {
    logger.warning('ack failed, broker %s not resolvable: %s', brokerName, (e as Error).message);
    return;
  }
  const header = new AckMessageRequestHeader();
  header.bname = brokerName;
  header.consumerGroup = c.consumerGroup;
  header.topic = topic;
  header.queueId = queueId;
  header.extraInfo = extraInfo;
  header.offset = queueOffset;
  try {
    await c.mqClient!.ackMessage(addr, header, POP_ASYNC_TIMEOUT_MILLIS);
  } catch (e) {
    logger.warning('Ack message fail. extraInfo: %s error: %s', extraInfo, (e as Error).message);
  }
}

// changePopInvisibleTime is Java
// ConsumeMessagePopConcurrentlyService#changePopInvisibleTime.
//
// delayLevel 0 means "not chosen": it is replaced by the message's own
// reconsume count, which is what makes the back-off grow with each retry. The
// value is then looked up in the SECONDS table and sent as MILLISECONDS — the
// unit flip is Java's, and sending seconds here would hold messages for
// milliseconds.
export function changePopInvisibleTime(c: DefaultMQPushConsumer, msg: MessageExt, delayLevel: number): void {
  if (delayLevel === 0) {
    delayLevel = msg.getReconsumeTimes();
  }
  changePopInvisibleTimeAtLevel(c, msg, delayLevel);
}

// changePopInvisibleTimeAtLevel is the shared body with the level ALREADY
// resolved: the "0 means unset" rule must not be applied a second time here,
// or a deliberate level-0 (the 10s back-off) would be re-read as "use the
// reconsume count" and every give-up path would silently wait much longer.
function changePopInvisibleTimeAtLevel(c: DefaultMQPushConsumer, msg: MessageExt, delayLevel: number): void {
  const delaySecond = popDelayLevelSeconds(delayLevel);
  const extraInfo = msg.getProperty(MessageConst.PROPERTY_POP_CK);
  if (!extraInfo) {
    logger.warning('changePopInvisibleTime skipped: message %s carries no POP_CK', msg.getMsgId());
    return;
  }
  const parsed = ckParse(extraInfo);
  if (!parsed) {
    logger.warning('changePopInvisibleTime failed to parse POP_CK %q', extraInfo);
    return;
  }
  const { brokerName, queueId, queueOffset } = parsed;
  const topic = getRealTopicFromCk(extraInfo, msg.getTopic(), c.consumerGroup);
  void (async () => {
    let addr: string;
    try {
      addr = await popAckAddress(c, brokerName, msg.getTopic());
    } catch (e) {
      logger.warning('changePopInvisibleTime failed, broker %s not resolvable: %s',
        brokerName, (e as Error).message);
      return;
    }
    const header = new ChangeInvisibleTimeRequestHeader();
    header.bname = brokerName;
    header.consumerGroup = c.consumerGroup;
    header.topic = topic;
    header.queueId = queueId;
    header.extraInfo = extraInfo;
    header.offset = queueOffset;
    header.invisibleTime = delaySecond * 1000;
    header.suspend = false;
    try {
      await c.mqClient!.changeInvisibleTime(addr, header, POP_ASYNC_TIMEOUT_MILLIS);
    } catch (e) {
      logger.error('changePopInvisibleTimeAsync fail, group:%s msg:%s error:%s',
        c.consumerGroup, msg.getMsgId(), (e as Error).message);
    }
  })();
}

// checkNeedAckOrDelay is Java
// ConsumeMessagePopConcurrentlyService#checkNeedAckOrDelay.
//
// Called once the retry budget is spent. If the message has been bouncing for
// longer than twice the longest back-off there is no point extending again —
// ACK it and let it go. Otherwise extend by the next notch above the elapsed
// time.
//
// The Java loop can exit with delayLevel == -1 (the elapsed time is below the
// first notch, i.e. 10s) and then index the table with -1, which throws
// ArrayIndexOutOfBoundsException. This port clamps to the first notch instead:
// the same "wait at least 10s" intent, without the crash.
//
// The clamp goes through changePopInvisibleTimeAtLevel rather than
// changePopInvisibleTime on purpose: the latter treats 0 as "the caller did
// not choose a level, use the reconsume count", which would turn this clamp
// into a back-off of table[reconsumeTimes] seconds. Java never reaches that
// substitution on this path (it crashes first), so 10s is the honest reading
// of the clamp.
function checkNeedAckOrDelay(c: DefaultMQPushConsumer, msg: MessageExt): void {
  const last = POP_DELAY_LEVEL[POP_DELAY_LEVEL.length - 1];
  const elapsed = Date.now() - msg.getBornTimestamp();
  if (elapsed > last * 1000 * 2) {
    logger.warning('Consume too many times, ack message async. message %s', msg.getMsgId());
    void ackMessagePop(c, msg);
    return;
  }
  let delayLevel = popDelayLevelForElapsed(elapsed);
  if (delayLevel < 0) delayLevel = 0;
  changePopInvisibleTimeAtLevel(c, msg, delayLevel);
  logger.warning('Consume too many times, but delay time %d not enough. changePopInvisibleTime to delayLevel %d . message key:%s',
    elapsed, delayLevel, msg.getProperty(MessageConst.PROPERTY_KEYS) || '');
}

// ---------------------------------------------------------------------------
// rebalance integration
// ---------------------------------------------------------------------------

// syncPopLoops is syncPullLoops' POP twin: retire the queues we no longer own,
// then start a pop loop for each newly assigned one.
//
// Order matters for the same reason as the pull path (retire before add), and
// there is no offset to settle: POP keeps no client cursor, so retiring only
// has to stop the loop and mark the queue dropped so an in-flight batch aborts
// instead of ACKing a batch the broker has taken back.
export function syncPopLoops(c: DefaultMQPushConsumer): void {
  const current = new Set(c.assigned.map(mqKey));

  for (const key of Array.from(c.popQueueTable.keys())) {
    if (!current.has(key)) {
      retirePopQueue(c, key);
    }
  }

  for (const mq of c.assigned) {
    const key = mqKey(mq);
    if (c.popQueueTable.has(key)) continue;
    const pq = new PopProcessQueue();
    c.popQueueTable.set(key, pq);
    const stopFlag = c._newStopFlag();
    c.popQueueStopFlags.set(key, stopFlag);
    void queuePopLoop(c, mq, stopFlag).catch((e) =>
      logger.error('pop loop error for %s: %s', key, (e as Error).message || e));
  }
  c._notifyQueueChanged();
}

// retirePopQueue drops one queue's POP state.
export function retirePopQueue(c: DefaultMQPushConsumer, key: string): void {
  const stopFlag = c.popQueueStopFlags.get(key);
  if (stopFlag) {
    c._stopFlags.delete(stopFlag); // signal the pop loop to exit
    c.popQueueStopFlags.delete(key);
  }
  const pq = c.popQueueTable.get(key);
  if (pq) pq.setDropped(true);
  c.popQueueTable.delete(key);
}

// ---------------------------------------------------------------------------
// broker mode switch
// ---------------------------------------------------------------------------

// setMessageRequestModeOnBroker sends SET_MESSAGE_REQUEST_MODE(401) for every
// subscribed business topic, telling each broker to serve this (group, topic)
// pair in POP mode.
//
// This is an ADDITION to Java's client surface: there, the mode is set by the
// operator (mqadmin / the console) and the client only reads it back from the
// assignment. Without the call a client-side setPopMode(true) would pop a
// broker that still answers PULL requests, which looks like "POP returns
// nothing" rather than an error.
//
// The %RETRY% topics are skipped: the POP retry topic is
// %RETRY%<group>_<topic>, created by the broker, and its request mode follows
// the business topic.
export async function setMessageRequestModeOnBroker(c: DefaultMQPushConsumer, timeoutMillis: number): Promise<Error | null> {
  const client = c.mqClient;
  if (!client) return new Error('consumer not started');
  const topics: string[] = [];
  for (const topic of c.subscription.keys()) {
    if (MixAll.isRetryTopic(topic) || MixAll.isDlqTopic(topic)) continue;
    topics.push(topic);
  }
  topics.sort();

  let firstErr: Error | null = null;
  for (const topic of topics) {
    // Resolve the master address the way Java does: by the topic's route.
    const route = client.getTopicRouteData(topic);
    let masterAddr: string | null = null;
    if (route) {
      for (const bd of route.brokerDatas || []) {
        const master = bd.brokerAddrs && bd.brokerAddrs[MixAll.MASTER_ID];
        if (master != null) { masterAddr = master; break; }
      }
    }
    if (!masterAddr) {
      logger.warning('set message request mode POP: no broker for %s', topic);
      if (!firstErr) firstErr = new Error('no broker for topic ' + topic);
      continue;
    }
    try {
      await client.setMessageRequestMode(masterAddr, topic, c.consumerGroup,
        MESSAGE_REQUEST_MODE_POP, c.popShareQueueNum, timeoutMillis);
    } catch (e) {
      logger.warning('set message request mode POP failed for %s/%s: %s',
        c.consumerGroup, topic, (e as Error).message);
      if (!firstErr) firstErr = e as Error;
    }
  }
  return firstErr;
}
