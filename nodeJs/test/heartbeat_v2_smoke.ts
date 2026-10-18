// Heartbeat V2 (fingerprint) unit test — no broker involved.
//
// Reference model: the protocol heartbeat body carries a subscription snapshot
// fingerprint plus a `withoutSub` flag, and the fingerprint is hashed from a
// NORMALISED rendering of the body — subscription versions, client id, the
// without-sub flag and the fingerprint field itself are all stripped, because
// they either change on every beat or per client.
//
// The value only has to be stable for one client while its subscriptions do not
// change, and different as soon as they do (a broker compares a client's beats to
// each other, never across languages). So, unlike a naive rendering, sets are put
// in a defined order here: a registration order that flipped would report a
// subscription change on every single beat.
//
// Run: node --experimental-strip-types test/heartbeat_v2_smoke.ts
import {
  HeartbeatData, ConsumerData, ProducerData, ConsumeType, MessageModel, ConsumeFromWhere,
  javaStringHash, heartbeatExtIsSupportV2, heartbeatExtIsSubChange, decodeHeartbeatV2Flags,
} from '../src/remoting/heartbeat.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { ResponseCode } from '../src/remoting/codes.ts';
import { SubscriptionData } from '../src/common/subscriptionData.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

function body(clientID = 'client-1'): HeartbeatData {
  const hb = new HeartbeatData(clientID);
  hb.producerDataSet.push(new ProducerData('GID_prod'));
  const cd = new ConsumerData('GID_cons', ConsumeType.CONSUME_PASSIVELY,
    MessageModel.CLUSTERING, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
  cd.subscriptionDataSet.push(new SubscriptionData('TopicA', 'tagA || tagB'));
  hb.consumerDataSet.push(cd);
  return hb;
}

async function main() {
  // ------------------------------------------------------ 1. stability
  {
    const a = body(), b = body();
    check('same subscriptions → same fingerprint',
      a.computeHeartbeatFingerprint() === b.computeHeartbeatFingerprint());
    const c = body();
    check('registering the same group twice in another order is still stable', (() => {
      const x = new HeartbeatData('client-1');
      for (const g of ['GID_a', 'GID_b']) {
        x.consumerDataSet.push(new ConsumerData(g, ConsumeType.CONSUME_PASSIVELY,
          MessageModel.CLUSTERING, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET));
      }
      const y = new HeartbeatData('client-1');
      for (const g of ['GID_b', 'GID_a']) {
        y.consumerDataSet.push(new ConsumerData(g, ConsumeType.CONSUME_PASSIVELY,
          MessageModel.CLUSTERING, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET));
      }
      void c;
      return x.computeHeartbeatFingerprint() === y.computeHeartbeatFingerprint();
    })());
  }

  // ------------------------------------------------------- 2. sensitivity
  {
    const base = body().computeHeartbeatFingerprint();
    const extraSub = body();
    extraSub.consumerDataSet[0].subscriptionDataSet.push(new SubscriptionData('TopicB', '*'));
    check('one more subscription changes the fingerprint',
      extraSub.computeHeartbeatFingerprint() !== base);

    const otherTag = body();
    otherTag.consumerDataSet[0].subscriptionDataSet[0] = new SubscriptionData('TopicA', 'tagA');
    check('a changed subString changes the fingerprint',
      otherTag.computeHeartbeatFingerprint() !== base);

    const otherModel = body();
    otherModel.consumerDataSet[0].messageModel = MessageModel.BROADCASTING;
    check('a changed messageModel changes the fingerprint',
      otherModel.computeHeartbeatFingerprint() !== base);

    const anotherClient = body('client-2');
    check('the client id is NOT part of the fingerprint (it differs per process)',
      anotherClient.computeHeartbeatFingerprint() === base);

    const stampA = body();
    stampA.consumerDataSet[0].subscriptionDataSet[0].subVersion = Date.now();
    check('subVersion is normalised out: a re-registration must not look like a change',
      stampA.computeHeartbeatFingerprint() === base);

    const flipped = body();
    flipped.withoutSub = true;
    flipped.heartbeatFingerprint = 12345;
    check('withoutSub / heartbeatFingerprint are normalised out too',
      flipped.computeHeartbeatFingerprint() === base);

    const empty = new HeartbeatData('client-1');
    check('an empty body still hashes to a defined value',
      empty.computeHeartbeatFingerprint() === empty.computeHeartbeatFingerprint() &&
      Number.isInteger(empty.computeHeartbeatFingerprint()));
  }

  // ------------------------------------------ 3. java string hash semantics
  {
    // Java's String.hashCode over UTF-16 code units. These values were read off a
    // real JDK ("hello"/"world"/"Poland"/one astral char), so the port cannot drift
    // into JS number semantics and still pass.
    check('empty text hashes to 0', javaStringHash('') === 0);
    check('"hello" hashes like Java', javaStringHash('hello') === 99162322,
      String(javaStringHash('hello')));
    check('"world" hashes like Java', javaStringHash('world') === 113318802,
      String(javaStringHash('world')));
    check('a value that overflows 32 bits wraps NEGATIVE, like Java',
      javaStringHash('Poland') === -1898810230, String(javaStringHash('Poland')));
    check('a surrogate pair is hashed per code unit',
      javaStringHash('\uD83D\uDE00') === 1772899 &&
      javaStringHash('\uD83D\uDE00') === javaStringHash(String('😀')),
      String(javaStringHash('\uD83D\uDE00')));
    const fp = body().computeHeartbeatFingerprint();
    check('the fingerprint is a signed 32-bit int',
      fp >= -2147483648 && fp <= 2147483647 && Number.isInteger(fp), String(fp));
  }

  // --------------------------------------------- 4. without-sub clone
  {
    const hb = body();
    hb.heartbeatFingerprint = 777;
    const clone = hb.cloneWithoutSubscriptions();
    check('the clone carries withoutSub', clone.withoutSub === true);
    check('the clone keeps the client id and the fingerprint',
      clone.clientID === hb.clientID && clone.heartbeatFingerprint === 777);
    check('every subscription set is emptied',
      clone.consumerDataSet.length === 1 && clone.consumerDataSet[0].subscriptionDataSet.length === 0);
    check('the consumer identity fields survive',
      clone.consumerDataSet[0].groupName === 'GID_cons' &&
      clone.consumerDataSet[0].consumeType === ConsumeType.CONSUME_PASSIVELY &&
      clone.consumerDataSet[0].messageModel === MessageModel.CLUSTERING &&
      clone.consumerDataSet[0].consumeFromWhere === ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET &&
      clone.consumerDataSet[0].unitMode === false);
    check('producers survive', clone.producerDataSet.length === 1 &&
      clone.producerDataSet[0].groupName === 'GID_prod');
    check('the original is untouched', hb.consumerDataSet[0].subscriptionDataSet.length === 1 &&
      hb.withoutSub === false);
    check('wire encoding: withoutSub + fingerprint are the protocol key names', (() => {
      const d = clone.toDict();
      return d['withoutSub'] === true && d['heartbeatFingerprint'] === 777 &&
        Array.isArray(d['consumerDataSet']) &&
        (d['consumerDataSet'][0]['subscriptionDataSet'] as any[]).length === 0;
    })(), JSON.stringify(clone.toDict()));
    check('a V1 body keeps both fields at their zero value', (() => {
      const d = body().toDict();
      return d['heartbeatFingerprint'] === 0 && d['withoutSub'] === false;
    })());
    check('the wire shape round-trips', (() => {
      const back = HeartbeatData.decode(clone.encode());
      return back.withoutSub === true && back.heartbeatFingerprint === 777 &&
        back.consumerDataSet[0].groupName === 'GID_cons';
    })());
  }

  // ------------------------------------------------ 5. response flags
  {
    const mk = (ext?: Record<string, string>): RemotingCommand => {
      const r = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, '')!;
      if (ext) for (const [k, v] of Object.entries(ext)) r.addExtField(k, v);
      return r;
    };
    check('the ext keys are the MixAll spellings',
      heartbeatExtIsSupportV2 === 'IS_SUPPORT_HEART_BEAT_V2' &&
      heartbeatExtIsSubChange === 'IS_SUB_CHANGE');
    check('a V2-capable broker that saw new subscriptions answers both flags', (() => {
      const f = decodeHeartbeatV2Flags(mk({
        IS_SUPPORT_HEART_BEAT_V2: 'true', IS_SUB_CHANGE: 'true',
      }));
      return f.supportV2 === true && f.subChange === true;
    })());
    check('flags are TEXT booleans: "false" reads as false, not as "a key present"', (() => {
      const f = decodeHeartbeatV2Flags(mk({
        IS_SUPPORT_HEART_BEAT_V2: 'false', IS_SUB_CHANGE: 'false',
      }));
      return f.supportV2 === false && f.subChange === false;
    })());
    check('an old broker with no ext fields at all is NOT treated as V2-capable', (() => {
      const f = decodeHeartbeatV2Flags(mk());
      return f.supportV2 === false && f.subChange === false;
    })());
    check('a missing response is not V2-capable either', (() => {
      const f = decodeHeartbeatV2Flags(null as any);
      return f.supportV2 === false && f.subChange === false;
    })());
    check('the broker version is taken off the response header', (() => {
      const r = mk({ IS_SUPPORT_HEART_BEAT_V2: 'True' });
      r.version = 401;
      const f = decodeHeartbeatV2Flags(r);
      return f.version === 401 && f.supportV2 === true;
    })());
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
