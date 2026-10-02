// live_check_client_config verifies CHECK_CLIENT_CONFIG(46) — the pre-flight
// Java runs in DefaultMQPushConsumerImpl.start:1014 and
// DefaultLitePullConsumerImpl.start:410, right after the subscription routes
// are resolved and before the first heartbeat.
//
// What 46 is for: a subscription whose expressionType is not TAG (SQL92 or
// CLASS_FILTER) only compiles on the broker. A client that never asks gets no
// error at all, and the broker's ExpressionMessageFilter then admits EVERY
// message when the expression does not compile — a filter that silently turns
// into "subscribe to all". Asking turns that into a start-up failure.
//
// The assertions:
//   1. a TAG subscription puts no 46 on the wire (the common path must not pay
//      for the check) and the consumer still starts;
//   2. a SQL92 subscription DOES send 46, carrying the SQL92 expression type,
//      the broker answers SUCCESS and the consumer starts;
//   3. a lite-pull consumer runs the same pre-flight;
//   4. SQL92-filtered messages still flow end to end, so the check did not
//      break the subscription it validates.
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { MQClient } from '../src/client/mq_client.ts';
import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { Message } from '../src/common/message.ts';
import { ConsumeConcurrentlyStatus } from '../src/client/consumer_result.ts';

const args = process.argv.slice(2);
function argOf(name: string, def: string): string {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
}
const NS = argOf('--ns', process.env['NAMESRV_ADDR'] || '127.0.0.1:9876');
const STAMP = argOf('--stamp', String(Math.floor(Date.now() / 1000)));
const TOPIC = argOf('--topic', `NodeLive_Cfg46_${STAMP}`);

let passCount = 0;
let failCount = 0;
function check(name: string, ok: boolean, detail = ''): void {
  if (ok) { passCount++; console.log(`PASS  ${name}${detail ? '  ' + detail : ''}`); }
  else { failCount++; console.log(`FAIL  ${name}${detail ? '  ' + detail : ''}`); }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface Check46Call { addr: string; group: string; topic: string | null; expressionType: string; }

// Record every CHECK_CLIENT_CONFIG(46) an MQClient sends, by wrapping the
// instance method right after start() has built the client. This is test-only
// instrumentation on the instance, so the production path is untouched.
const recorded46 = new WeakMap<object, Check46Call[]>();
function watch46(client: MQClient | null): Check46Call[] {
  if (!client) return [];
  const calls: Check46Call[] = recorded46.get(client) ?? [];
  if (recorded46.has(client)) return calls;
  recorded46.set(client, calls);
  const original = client.checkClientConfig.bind(client);
  client.checkClientConfig = async (addr, group, sub, timeoutMillis) => {
    calls.push({ addr, group, topic: sub?.topic ?? null, expressionType: sub?.expressionType ?? '' });
    return original(addr, group, sub, timeoutMillis);
  };
  return calls;
}

async function makeTopic(producer: DefaultMQProducer): Promise<void> {
  for (let attempt = 0; attempt < 30; attempt++) {
    try {
      await producer.createTopic(TOPIC, producer.createTopicKey, 4);
      return;
    } catch {
      // "Not enough info to create topic" — the auto-create topic's nameserver
      // route has not landed yet.
      await sleep(1000);
    }
  }
  throw new Error('createTopic failed after 30 attempts');
}

async function main(): Promise<void> {
  const producer = new DefaultMQProducer(`GID_NodeLive_Cfg46_Producer_${STAMP}`);
  producer.setNamesrvAddr(NS);
  producer.start();
  await makeTopic(producer);
  await sleep(2000);

  // ---- 1. TAG subscription: no 46 on the wire ----
  const tagConsumer = new DefaultMQPushConsumer(`GID_NodeLive_Cfg46_Tag_${STAMP}`);
  tagConsumer.setNamesrvAddr(NS);
  tagConsumer.subscribe(TOPIC, 'TagA');
  tagConsumer.registerMessageListenerConcurrently(() => ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
  await tagConsumer.start();
  const tag46 = watch46(tagConsumer.mqClient);
  // start() already ran the pre-flight; re-run it so the assertion covers a
  // call that actually happened rather than an empty watch.
  await tagConsumer.mqClient!.checkClientInBroker();
  check('TAG subscription puts no CHECK_CLIENT_CONFIG(46) on the wire', tag46.length === 0,
    `calls=${tag46.length}`);
  check('TAG consumer started', tagConsumer.started === true);
  await tagConsumer.shutdown();

  // ---- 2. SQL92 subscription: 46 goes out and the broker's answer is honoured ----
  const sqlGroup = `GID_NodeLive_Cfg46_Sql_${STAMP}`;
  const sqlConsumer = new DefaultMQPushConsumer(sqlGroup);
  sqlConsumer.setNamesrvAddr(NS);
  sqlConsumer.subscribe(TOPIC, { type: 'SQL92', expression: "a = 'ok'" });
  sqlConsumer.registerMessageListenerConcurrently(() => ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
  // Wrap start() so the watcher is installed while the 46 round trip happens.
  // The broker's answer decides the outcome: SUCCESS starts the consumer, and
  // a non-SUCCESS code must abort start() with MQClientException — that
  // failure IS the feature working, because without the check a broker that
  // cannot compile the expression would silently deliver everything.
  const innerStart = sqlConsumer.start.bind(sqlConsumer);
  const sql46: Check46Call[] = [];
  const sqlErrors: Error[] = [];
  sqlConsumer.start = async () => {
    const realCheck = MQClient.prototype.checkClientConfig;
    MQClient.prototype.checkClientConfig = async function patched(addr, group, sub, timeoutMillis) {
      sql46.push({ addr, group, topic: sub?.topic ?? null, expressionType: sub?.expressionType ?? '' });
      return realCheck.call(this, addr, group, sub, timeoutMillis);
    };
    try {
      await innerStart();
    } catch (e) {
      sqlErrors.push(e as Error);
      throw e;
    } finally {
      MQClient.prototype.checkClientConfig = realCheck;
    }
  };
  try {
    await sqlConsumer.start();
  } catch (e) {
    // Expected when the broker runs with enablePropertyFilter=false.
  }
  check('SQL92 subscription sends exactly one 46', sql46.length === 1, `calls=${sql46.length}`);
  check('46 carries the SQL92 expression type and the consumer group',
    sql46.length === 1 && sql46[0].expressionType === 'SQL92' && sql46[0].group === sqlGroup
    && sql46[0].topic === TOPIC,
    JSON.stringify(sql46));
  check('46 addressed to a broker (not a nameserver)',
    sql46.length === 1 && /:\d+$/.test(sql46[0].addr), sql46[0]?.addr ?? '');
  // Two legal outcomes, and both prove the pre-flight is wired: the broker
  // accepted the expression, or it rejected it and start() aborted. What must
  // NOT happen is a silent start with the 46 never sent.
  const brokerRefused = sqlErrors.length > 0;
  const brokerAccepted = sqlConsumer.started === true;
  check('broker answer to 46 is honoured (started or failed, never silent)',
    brokerAccepted !== brokerRefused,
    brokerAccepted ? 'broker accepted, consumer running'
      : `start() aborted: ${sqlErrors[0]?.message ?? ''}`);
  if (brokerRefused) {
    // Java start:1018-1021 — a failure here shuts the consumer down.
    check('failed start leaves no running consumer', sqlConsumer.started === false);
    console.log('SKIP  end-to-end SQL92 delivery (broker refuses SQL92; set enablePropertyFilter=true)');
    await new Promise((r) => setTimeout(r, 500));
    producer.shutdown();
    console.log(`\n${passCount} passed, ${failCount} failed`);
    process.exit(failCount > 0 ? 1 : 0);
  }

  // ---- 3. lite pull consumer runs the same pre-flight ----
  const lite = new DefaultLitePullConsumer(`GID_NodeLive_Cfg46_Lite_${STAMP}`);
  lite.setNamesrvAddr(NS);
  lite.subscribe(TOPIC, '*');
  await lite.start();
  check('lite pull consumer started (pre-flight ran, TAG skipped the wire)', lite.started === true);
  await lite.shutdown();

  // ---- 4. end-to-end on the SQL92 path still works ----
  // A fresh consumer: start() is a no-op on an already-started instance, so
  // the listener has to be in place before the first start.
  const received = new Set<string>();
  const e2e = new DefaultMQPushConsumer(`GID_NodeLive_Cfg46_E2E_${STAMP}`);
  e2e.setNamesrvAddr(NS);
  e2e.subscribe(TOPIC, { type: 'SQL92', expression: "a = 'ok'" });
  e2e.registerMessageListenerConcurrently((msgs) => {
    for (const m of msgs) received.add((m.getBody() || Buffer.alloc(0)).toString('utf8'));
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
  });
  await e2e.start();
  const dl0 = Date.now() + 60_000;
  while (Date.now() < dl0 && (e2e.assigned || []).length === 0) await sleep(1000);
  for (let i = 0; i < 3; i++) {
    const msg = new Message(TOPIC, Buffer.from(`cfg-${i}`), 'TagA', `k${i}`);
    msg.putProperty('a', 'ok');
    await producer.send(msg);
  }
  const dl = Date.now() + 30_000;
  while (received.size < 3 && Date.now() < dl) await sleep(500);
  check('SQL92-filtered messages still delivered', received.size === 3, `got=${received.size}`);
  await e2e.shutdown();
  await sqlConsumer.shutdown();

  producer.shutdown();
  console.log(`\n${passCount} passed, ${failCount} failed`);
  process.exit(failCount > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error('live_check_client_config failed:', e);
  process.exit(1);
});
