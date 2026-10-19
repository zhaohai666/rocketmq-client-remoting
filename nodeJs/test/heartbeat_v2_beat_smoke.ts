// Heartbeat V2 beat gating — offline test of MQClient's heartbeat decision.
//
// Behaviour baseline (this client's beat decision plus the broker's reply to a
// HEART_BEAT request):
//   * a fingerprint is stamped on the body ONLY when useHeartbeatV2 is on. With
//     the switch off the body stays exactly what a V1 client sent (subscriptions,
//     fingerprint 0) even when the broker declares V2 support;
//   * the broker answers IS_SUPPORT_HEART_BEAT_V2 / IS_SUB_CHANGE as TEXT
//     booleans; an address is remembered as V2-capable only after it said so;
//   * a beat may drop its subscription sets only when THIS address is known
//     V2-capable AND its cached fingerprint equals the current one;
//   * IS_SUB_CHANGE removes the cached fingerprint, so the next beat reports the
//     subscriptions again; a rebalance resets the whole table;
//   * toggling the switch clears both caches;
//   * the beat loop guards: nothing registered means no beat at all, and a
//     producer-only client beats the master only.
//
// Run: node --experimental-strip-types test/heartbeat_v2_beat_smoke.ts
import { MQClient } from '../src/client/mq_client.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import {
  HeartbeatData, heartbeatExtIsSupportV2, heartbeatExtIsSubChange,
} from '../src/remoting/heartbeat.ts';
import { SubscriptionData } from '../src/common/subscriptionData.ts';
import { MixAll } from '../src/common/mixAll.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const MASTER = '127.0.0.1:19100';
const SLAVE = '127.0.0.1:19101';

type BeatRecord = {
  addr: string;
  withSub: boolean;
  fingerprint: number;
  subscriptionCount: number;
  producerGroups: string[];
  consumerGroups: string[];
};

// Stands in for the transport: decodes the beat body the same way a broker would
// and answers with the ext fields the caller asked that address to declare.
class FakeBroker {
  addr: string;
  supportV2: boolean | null;
  beats: BeatRecord[] = [];
  // When true, every answer also reports IS_SUB_CHANGE.
  reportSubChange = false;

  constructor(addr: string, supportV2: boolean | null) {
    this.addr = addr;
    this.supportV2 = supportV2;
  }

  oneway(): void { /* the beat path never goes oneway */ }

  async invokeSync(addr: string, request: RemotingCommand): Promise<RemotingCommand> {
    if (request.code !== RequestCode.HEART_BEAT) {
      throw new Error(`unexpected request code ${request.code} in the heartbeat test`);
    }
    const hb = HeartbeatData.decode(request.body as Buffer);
    this.beats.push({
      addr,
      withSub: !hb.withoutSub,
      fingerprint: hb.heartbeatFingerprint,
      subscriptionCount: hb.consumerDataSet.reduce((n, c) => n + c.subscriptionDataSet.length, 0),
      producerGroups: hb.producerDataSet.map((p) => p.groupName).sort(),
      consumerGroups: hb.consumerDataSet.map((c) => c.groupName).sort(),
    });
    const response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, '')!;
    response.opaque = request.opaque;
    if (this.supportV2 != null) {
      response.addExtField(heartbeatExtIsSupportV2, String(this.supportV2));
      response.addExtField(heartbeatExtIsSubChange, String(this.reportSubChange));
    }
    return response;
  }

  last(): BeatRecord { return this.beats[this.beats.length - 1]; }
  clear(): void { this.beats.length = 0; }
}

function clientFor(brokerAddrTable: Array<{ brokerName: string; addrs: Record<number, string> }>,
  brokers: FakeBroker[]): MQClient {
  const byAddr = new Map<string, FakeBroker>();
  for (const b of brokers) byAddr.set(b.addr, b);
  const remoting: any = {
    invokeSync(addr: string, request: RemotingCommand): Promise<RemotingCommand> {
      const broker = byAddr.get(addr);
      if (broker == null) return Promise.reject(new Error(`no broker answers at ${addr}`));
      return broker.invokeSync(addr, request);
    },
    invokeOneway(): Promise<void> { return Promise.resolve(); },
  };
  const client = new MQClient(`node-v2-test@${process.pid}`, '127.0.0.1:1', remoting);
  for (const entry of brokerAddrTable) client.brokerAddrTable.set(entry.brokerName, entry.addrs);
  return client;
}

function subscribe(group: string, topics: string[]): any {
  const consumer: any = { groupName: group, subscriptionDataSet: [], unitMode: false };
  for (const topic of topics) {
    const sub = new SubscriptionData(topic, 'tagA||tagB');
    sub.tagsSet.add('tagA');
    sub.tagsSet.add('tagB');
    consumer.subscriptionDataSet.push(sub);
  }
  return consumer;
}

async function main() {
  // ------------------------------------------------ 1. V1: the switch is off
  {
    const master = new FakeBroker(MASTER, true);
    const client = clientFor([{ brokerName: 'brokerA', addrs: { [MixAll.MASTER_ID]: MASTER } }], [master]);
    client.registerConsumer('GID_v1', subscribe('GID_v1', ['TopicA']));
    await client.sendHeartbeatToAllBrokers();
    await client.sendHeartbeatToAllBrokers();
    check('V1 beat carries the subscriptions', master.beats.length === 2 &&
      master.beats.every((b) => b.withSub && b.subscriptionCount === 1), JSON.stringify(master.beats));
    check('V1 beat reports fingerprint 0', master.beats.every((b) => b.fingerprint === 0));
    client.setUseHeartbeatV2(true);
    await client.sendHeartbeatToAllBrokers();
    check('turning V2 on re-registers with the subscriptions',
      master.last().withSub && master.last().subscriptionCount === 1);
  }

  // --------------------------------- 2. V2 learns support, then drops subs
  {
    const master = new FakeBroker(MASTER, true);
    const client = clientFor([{ brokerName: 'brokerA', addrs: { [MixAll.MASTER_ID]: MASTER } }], [master]);
    client.setUseHeartbeatV2(true);
    const consumer = subscribe('GID_v2', ['TopicA', 'TopicB']);
    client.registerConsumer('GID_v2', consumer);
    await client.sendHeartbeatToAllBrokers();
    const first = master.last();
    check('V2 first beat keeps the subscriptions and stamps a fingerprint',
      first.withSub && first.subscriptionCount === 2 && first.fingerprint !== 0, JSON.stringify(first));
    await client.sendHeartbeatToAllBrokers();
    const second = master.last();
    check('V2 second beat is sent without subscription sets',
      !second.withSub && second.subscriptionCount === 0, JSON.stringify(second));
    check('the fingerprint rides on the without-subscription beat too',
      second.fingerprint === first.fingerprint && second.fingerprint !== 0,
      `${first.fingerprint} vs ${second.fingerprint}`);
    check('the without-subscription beat still registers the consumer group',
      second.consumerGroups.length === 1 && second.consumerGroups[0] === 'GID_v2');

    // A broker that says the subscriptions changed makes the client re-report.
    master.reportSubChange = true;
    await client.sendHeartbeatToAllBrokers();
    check('a cached fingerprint still buys the short beat, and IS_SUB_CHANGE ends it',
      !master.last().withSub && master.last().subscriptionCount === 0, JSON.stringify(master.last()));
    master.reportSubChange = false;
    await client.sendHeartbeatToAllBrokers();
    check('IS_SUB_CHANGE drops the cached fingerprint, so the next beat re-reports',
      master.last().withSub && master.last().subscriptionCount === 2, JSON.stringify(master.last()));
    await client.sendHeartbeatToAllBrokers();
    check('and the beat after that goes back to the short form',
      !master.last().withSub && master.last().subscriptionCount === 0);

    // Changing what is subscribed changes the fingerprint, so the next beat
    // carries the new set instead of asking the broker to skip registration.
    consumer.subscriptionDataSet.push(new SubscriptionData('TopicC', 'tagC'));
    await client.sendHeartbeatToAllBrokers();
    check('a new subscription is reported on the next beat',
      master.last().withSub && master.last().subscriptionCount === 3, JSON.stringify(master.last()));
    const afterChange = master.last().fingerprint;
    check('and its fingerprint differs from the stale one',
      afterChange !== first.fingerprint && afterChange !== 0);
    await client.sendHeartbeatToAllBrokers();
    check('the fresh fingerprint is then cached again',
      !master.last().withSub && master.last().fingerprint === afterChange);
  }

  // ------------------------------------------ 3. an old broker is never V2
  {
    const master = new FakeBroker(MASTER, null);  // answers nothing: pre-V2 broker
    const client = clientFor([{ brokerName: 'brokerA', addrs: { [MixAll.MASTER_ID]: MASTER } }], [master]);
    client.setUseHeartbeatV2(true);
    client.registerConsumer('GID_old', subscribe('GID_old', ['TopicA']));
    for (let i = 0; i < 3; i++) await client.sendHeartbeatToAllBrokers();
    check('a broker that never declares support always gets the subscriptions',
      master.beats.length === 3 && master.beats.every((b) => b.withSub && b.subscriptionCount === 1),
      JSON.stringify(master.beats));
    check('a fingerprint is still stamped, so the body shape stays V2',
      master.beats.every((b) => b.fingerprint !== 0));
  }

  // --------------------------------- 4. per-broker caches, master-only beats
  {
    const master = new FakeBroker(MASTER, true);
    const slave = new FakeBroker(SLAVE, null);
    const client = clientFor(
      [{ brokerName: 'brokerA', addrs: { [MixAll.MASTER_ID]: MASTER, 1: SLAVE } }], [master, slave]);
    client.setUseHeartbeatV2(true);
    client.registerConsumer('GID_two', subscribe('GID_two', ['TopicA']));
    await client.sendHeartbeatToAllBrokers();
    check('both the master and the slave get the first beat',
      master.beats.length === 1 && slave.beats.length === 1);
    await client.sendHeartbeatToAllBrokers();
    check('support is cached per address: the V2 master goes short',
      !master.last().withSub && master.last().subscriptionCount === 0);
    check('while the pre-V2 slave keeps the subscriptions',
      slave.last().withSub && slave.last().subscriptionCount === 1, JSON.stringify(slave.last()));

    // A producer-only client has no consumer data, so a slave gets no beat.
    client.unregisterConsumer('GID_two');
    client.registerProducer('GID_prod', { groupName: 'GID_prod' });
    master.clear();
    slave.clear();
    await client.sendHeartbeatToAllBrokers();
    check('a producer-only client beats the master only',
      master.beats.length === 1 && slave.beats.length === 0 &&
      master.last().producerGroups[0] === 'GID_prod', JSON.stringify({ m: master.beats, s: slave.beats }));

    // Nothing registered at all: no beat goes out.
    client.unregisterProducer('GID_prod');
    master.clear();
    await client.sendHeartbeatToAllBrokers();
    check('an idle client sends no heartbeat', master.beats.length === 0);
  }

  // -------------------------------- 5. toggling the switch clears the caches
  {
    const master = new FakeBroker(MASTER, true);
    const client = clientFor([{ brokerName: 'brokerA', addrs: { [MixAll.MASTER_ID]: MASTER } }], [master]);
    client.setUseHeartbeatV2(true);
    client.registerConsumer('GID_toggle', subscribe('GID_toggle', ['TopicA']));
    await client.sendHeartbeatToAllBrokers();
    await client.sendHeartbeatToAllBrokers();
    check('before the toggle the second beat is the short one', !master.last().withSub);
    client.setUseHeartbeatV2(false);
    await client.sendHeartbeatToAllBrokers();
    check('switching off reverts to a full V1 body with fingerprint 0',
      master.last().withSub && master.last().subscriptionCount === 1 &&
      master.last().fingerprint === 0, JSON.stringify(master.last()));
    client.setUseHeartbeatV2(true);
    await client.sendHeartbeatToAllBrokers();
    check('switching on starts the conversation over: subscriptions first',
      master.last().withSub && master.last().subscriptionCount === 1);
  }

  // ------------------------------- 6. a rebalance beat clears every fingerprint
  {
    const master = new FakeBroker(MASTER, true);
    const other = new FakeBroker(SLAVE, true);
    const client = clientFor([
      { brokerName: 'brokerA', addrs: { [MixAll.MASTER_ID]: MASTER } },
      { brokerName: 'brokerB', addrs: { [MixAll.MASTER_ID]: SLAVE } },
    ], [master, other]);
    client.setUseHeartbeatV2(true);
    client.registerConsumer('GID_rebalance', subscribe('GID_rebalance', ['TopicA']));
    await client.sendHeartbeatToAllBrokers();
    await client.sendHeartbeatToAllBrokers();
    check('both brokers reach the short form',
      !master.last().withSub && !other.last().withSub);
    await client.sendHeartbeatToAllBrokers(true);
    check('a rebalance beat re-reports subscriptions to every broker',
      master.last().withSub && other.last().withSub,
      JSON.stringify({ m: master.last(), o: other.last() }));
    await client.sendHeartbeatToAllBrokers();
    check('and the short form resumes right after',
      !master.last().withSub && !other.last().withSub);
  }

  // ------------------- 7. the fingerprint ignores per-beat and per-client noise
  {
    const a = new MQClient('node-v2-a@1', '127.0.0.1:1', { invokeSync: () => Promise.resolve(null) } as any);
    const b = new MQClient('node-v2-b@2', '127.0.0.1:1', { invokeSync: () => Promise.resolve(null) } as any);
    const subA = subscribe('GID_x', ['TopicA', 'TopicB']);
    const subB = subscribe('GID_x', ['TopicB', 'TopicA']);
    subA.subscriptionDataSet[0].subVersion = 1_700_000_000_000;
    subB.subscriptionDataSet[1].subVersion = 1_700_000_999_999;
    a.registerConsumer('GID_x', subA);
    b.registerConsumer('GID_x', subB);
    const fpA = a.buildHeartbeatData(false).computeHeartbeatFingerprint();
    const fpB = b.buildHeartbeatData(false).computeHeartbeatFingerprint();
    check('subscription order and registration time do not move the fingerprint',
      fpA === fpB && fpA !== 0, `${fpA} vs ${fpB}`);
    const clientC = new MQClient('node-v2-c@3', '127.0.0.1:1', { invokeSync: () => Promise.resolve(null) } as any);
    clientC.registerConsumer('GID_y', subscribe('GID_y', ['TopicA', 'TopicB']));
    check('a different group hashes differently',
      clientC.buildHeartbeatData(false).computeHeartbeatFingerprint() !== fpA);
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
