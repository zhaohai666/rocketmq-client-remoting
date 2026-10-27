// A retried POP row is hidden by the broker under a retry-named topic; the
// listener is handed the BUSINESS topic it was subscribed to instead. The step
// runs at the concurrent delivery entry — for a pull batch and for a POP batch —
// and never on the orderly delivery path.
//
//   node --experimental-strip-types test/pop_retry_topic_reset_smoke.ts
//
// The client-side rule pinned down here:
//
//   * `%RETRY%<group>+<topic>` -> `<topic>`   (the '+' retry naming)
//   * `%RETRY%<group>_<topic>` -> `<topic>`   (the '_' retry naming)
//   * `%RETRY%<group>` exactly -> untouched by THIS step: it carries no
//     separator, and the RETRY_TOPIC-property restore that follows it is what
//     names its business topic;
//   * an ordinary topic -> untouched, which is exactly what makes the step a
//     no-op for a pull batch;
//   * a retry topic of ANOTHER group is not this consumer's to rewrite;
//   * with a namespace the local prefix can sit on either side of the separator,
//     so the prefix is stripped first, the name normalised, and the prefix
//     stripped from the result;
//   * everything is decided per row, so one batch can mix all of the shapes.
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { MessageExt, MessageQueue } from '../src/common/message.ts';
import { FilterAPI } from '../src/common/subscriptionData.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { ConsumerStatsManager } from '../src/client/consumer_stats.ts';
import { PopProcessQueue } from '../src/client/pop_process_queue.ts';
import { PopResult, PopStatus } from '../src/client/pop_api.ts';
import { buildExtraInfo } from '../src/remoting/extra_info.ts';
import { queuePopLoop } from '../src/client/pop_consumer.ts';
import { ConsumeConcurrentlyStatus, ConsumeOrderlyStatus } from '../src/client/consumer_result.ts';
import { mqKey } from '../src/client/offset_store.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const GROUP = 'GID_pop_retry_reset';
const BIZ = 'PopRetryResetTopic';
const BROKER = 'broker-a';
const RETRY = MixAll.RETRY_GROUP_TOPIC_PREFIX;
const NS = 'MQ_INST_pop_retry';

function row(topic: string, offset: number, props: Record<string, string> = {}): MessageExt {
  const m = new MessageExt(topic, Buffer.from('body-' + offset), 'TagA');
  m.setQueueId(0);
  m.setQueueOffset(offset);
  m.setCommitLogOffset(offset);
  m.setBrokerName(BROKER);
  m.setBornTimestamp(Date.now());
  m.setStoreHost('127.0.0.1');
  m.setBornHost('127.0.0.1');
  for (const [k, v] of Object.entries(props)) m.putProperty(k, v);
  return m;
}

// normalise is the step on its own, reached exactly the way the delivery entry
// reaches it, over one row per input topic.
function normalise(topics: string[], group = GROUP, namespace = ''): string[] {
  const c = new DefaultMQPushConsumer(group);
  c.namespace = namespace;
  const msgs = topics.map((t, i) => row(t, i));
  (c as any)._resetPopRetryTopic(msgs);
  return msgs.map((m) => m.getTopic());
}

function sameStringList(actual: string[], expected: string[]): boolean {
  return actual.length === expected.length && actual.every((v, i) => v === expected[i]);
}

async function main() {
  // ---- 1. the four topic shapes, decided by the name alone
  {
    const plus = normalise([`${RETRY}${GROUP}+${BIZ}`])[0];
    check('a row under the plus-separated retry naming is handed over as the business topic',
      plus === BIZ, plus);

    const underscore = normalise([`${RETRY}${GROUP}_${BIZ}`])[0];
    check('and so is a row under the underscore-separated retry naming',
      underscore === BIZ, underscore);

    const bare = normalise([`${RETRY}${GROUP}`])[0];
    check('the bare %RETRY%<group>, which carries no separator, is left to the next step',
      bare === `${RETRY}${GROUP}`, bare);

    const plain = normalise([BIZ])[0];
    check('an ordinary topic is untouched — the step is a no-op for a pull-shaped row',
      plain === BIZ, plain);

    const both = normalise([`${RETRY}${GROUP}+${BIZ}`, `${RETRY}${GROUP}_${BIZ}`, `${RETRY}${GROUP}`, BIZ]);
    check('one batch can mix every shape and each row is decided on its own',
      sameStringList(both, [BIZ, BIZ, `${RETRY}${GROUP}`, BIZ]), both.join(','));
  }

  // ---- 2. names that are not this consumer's retry topic
  {
    const otherGroup = normalise([`${RETRY}GID_somebody_else+${BIZ}`])[0];
    check('another group\'s retry topic is not rewritten',
      otherGroup === `${RETRY}GID_somebody_else+${BIZ}`, otherGroup);

    const empty = normalise([`${RETRY}${GROUP}+`])[0];
    check('nothing after the separator is not a topic, so the row keeps its name',
      empty === `${RETRY}${GROUP}+`, empty);

    const dlq = normalise([`${MixAll.DLQ_GROUP_TOPIC_PREFIX}${GROUP}_${BIZ}`])[0];
    check('a dead-letter name is not a retry name and is left alone',
      dlq === `${MixAll.DLQ_GROUP_TOPIC_PREFIX}${GROUP}_${BIZ}`, dlq);
  }

  // ---- 3. with a namespace: strip, normalise, strip the result
  {
    // start() may wrap the namespace INTO consumerGroup, so the retry topic the
    // broker names carries the plain group inside the wrapped prefix.
    const wrapped = `${NS}%${GROUP}`;
    const bothSides = normalise([`${RETRY}${NS}%${GROUP}+${NS}%${BIZ}`], wrapped, NS)[0];
    check('a prefix on both sides of the separator comes off on both sides',
      bothSides === BIZ, bothSides);

    const oneSide = normalise([`${RETRY}${NS}%${GROUP}_${BIZ}`], wrapped, NS)[0];
    check('a prefix only inside the group part still normalises',
      oneSide === BIZ, oneSide);

    const bareNs = normalise([`${RETRY}${NS}%${GROUP}`], wrapped, NS)[0];
    check('the bare namespaced retry group is again left to the next step',
      bareNs === `${RETRY}${NS}%${GROUP}`, bareNs);

    const plainNs = normalise([`${NS}%${BIZ}`], wrapped, NS)[0];
    check('control: an ordinary namespaced topic keeps its prefix here — '
      + 'stripping it is the namespace step that follows, not this one',
      plainNs === `${NS}%${BIZ}`, plainNs);

    const unwrappedGroup = normalise([`${RETRY}${GROUP}+${NS}%${BIZ}`], GROUP, NS)[0];
    check('the group part is matched plain even when the setting kept it unwrapped',
      unwrappedGroup === BIZ, unwrappedGroup);
  }

  // ---- 4. the concurrent delivery entry runs the step; the orderly one does not
  {
    const concurrent = await deliver(0);
    check('a concurrent batch reaches the listener under the business topic for both '
      + 'retry namings and for the ordinary row',
      sameStringList(concurrent, [BIZ, BIZ, BIZ, BIZ]), concurrent.join(','));
    check('and the bare %RETRY%<group> row is restored from its RETRY_TOPIC property '
      + 'by the step that follows the normalisation',
      concurrent.length === 4 && concurrent[3] === BIZ, String(concurrent[3]));

    const orderly = await deliver(1);
    check('control: the orderly delivery path never runs the step, so the retry '
      + 'names reach the listener as they arrived',
      sameStringList(orderly,
        [`${RETRY}${GROUP}+${BIZ}`, `${RETRY}${GROUP}_${BIZ}`, BIZ, BIZ]),
      orderly.join(','));
  }

  // ---- 5. the POP consume entry runs it too
  {
    const topics = await popDelivery();
    check('a POP batch reaches the listener under the business topic',
      sameStringList(topics, [BIZ, BIZ, BIZ]), topics.join(','));
  }

  console.log(`\npop retry topic normalisation: ${pass} checks, ${fail} failed`);
  if (fail > 0) process.exitCode = 1;
}

// deliver drives ONE batch through the real delivery entry (_consumeBatch) and
// reports the topics the listener was handed, in order. `mode` 0 is concurrent,
// 1 orderly. The settle that follows the listener needs a live client, so a
// failure there is swallowed: only the handover is under test.
async function deliver(mode: number): Promise<string[]> {
  const c = new DefaultMQPushConsumer(GROUP);
  const anyC = c as any;
  c.started = true;
  c._statsManager = new ConsumerStatsManager();
  anyC._sleepOrStop = async () => false;

  const topics: string[] = [];
  c.registerMessageListener({
    consumeMessage: (msgs: MessageExt[]) => {
      for (const m of msgs) topics.push(m.getTopic());
      return mode === 1 ? ConsumeOrderlyStatus.SUCCESS : ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    },
  }, mode === 1);

  const batch = [
    row(`${RETRY}${GROUP}+${BIZ}`, 0),
    row(`${RETRY}${GROUP}_${BIZ}`, 1),
    row(BIZ, 2),
    // The bare retry group is the shape the RETRY_TOPIC property names.
    row(`${RETRY}${GROUP}`, 3, { [MessageConst.PROPERTY_RETRY_TOPIC]: BIZ }),
  ];
  const mq = new MessageQueue(BIZ, BROKER, 0);
  try {
    await anyC._consumeBatch(mq, batch, 0);
  } catch (e) {
    // the offset commit and the send-back have no client to talk to
  }
  return topics;
}

// popRow is one popped row of a retry-named queue: the checkpoint is what the
// consume entry checks before it will run the listener at all.
function popRow(topic: string, offset: number, popTime: number): MessageExt {
  const ck = buildExtraInfo(offset, popTime, 60000, 0, topic, BROKER, 0, offset);
  return row(topic, offset, { [MessageConst.PROPERTY_POP_CK]: `${ck} ${offset}` });
}

// popDelivery drives one real POP round whose rows all arrive under retry
// namings, and reports the topics the listener was handed.
async function popDelivery(): Promise<string[]> {
  const c = new DefaultMQPushConsumer(GROUP);
  const anyC = c as any;
  c.pullInterval = 0;
  c.subscription.set(BIZ, FilterAPI.buildSubscriptionData(BIZ, '*'));
  c.started = true;
  c._statsManager = new ConsumerStatsManager();

  const mq = new MessageQueue(BIZ, BROKER, 0);
  const pq = new PopProcessQueue();
  c.popQueueTable.set(mqKey(mq), pq);
  const flag = anyC._newStopFlag();

  const popTime = Date.now();
  const found = new PopResult();
  found.popStatus = PopStatus.FOUND;
  found.popTime = popTime;
  found.invisibleTime = 60000;
  found.msgFoundList = [
    popRow(`${RETRY}${GROUP}+${BIZ}`, 0, popTime),
    popRow(`${RETRY}${GROUP}_${BIZ}`, 1, popTime),
    popRow(BIZ, 2, popTime),
  ];

  let rounds = 0;
  c.mqClient = {
    popMessage: async () => {
      rounds++;
      if (rounds > 1) {
        anyC._stopFlags.delete(flag);
        return new PopResult();
      }
      return found;
    },
    ackMessage: async () => ({}),
    changeInvisibleTime: async () => ({ popTime: Date.now(), invisibleTime: 60000, reviveQid: 0 }),
  } as never;
  anyC._findBrokerAddressInSubscribe = () => ({ addr: '127.0.0.1:10911', isSlave: false });
  anyC._sleepOrStop = async () => {
    anyC._stopFlags.delete(flag);
    return true;
  };

  const topics: string[] = [];
  c.registerMessageListener({
    consumeMessage: (msgs: MessageExt[]) => {
      for (const m of msgs) topics.push(m.getTopic());
      return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    },
  });

  await queuePopLoop(c, mq, flag);
  return topics;
}

main().catch((e) => { console.error(e); process.exitCode = 1; });
