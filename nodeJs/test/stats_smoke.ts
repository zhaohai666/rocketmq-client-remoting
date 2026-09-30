// Smoke: consumer_stats + consumer + mq_client + admin load + differential window
import { ConsumerStatsManager } from '../src/client/consumer_stats.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQAdminExt, string2Properties } from '../src/client/admin.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean) {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name}`); }
}

console.log('== consumer_stats differential window ==');
const mgr = new ConsumerStatsManager();
// TPS item: 10 msgs + 1 call
const tpsItem = mgr.topicAndGroupPullTPS.getAndCreate('MyTopic@MyGroup');
tpsItem.addValue(10, 1);
tpsItem.sample();
// Simulate a sample 10s ago: prepend a zero-cumulative point (test-only).
const chain: any[] = (tpsItem as any)._minute;
chain.unshift({ ts: Date.now() - 10000, value: 0, times: 0 });
const snap = tpsItem.getStatsDataInMinute();
check('sum = 10 (delta window)', snap.sum === 10);
check('tps = 1.0 (10 msgs / 10s)', Math.abs(snap.tps - 1.0) < 0.01);
check('times = 1', snap.times === 1);

// RT item: 3 calls, total 600ms → avgpt = 200
const rtItem = mgr.topicAndGroupConsumeRT.getAndCreate('MyTopic@MyGroup');
rtItem.addValue(100, 1); rtItem.addValue(200, 1); rtItem.addValue(300, 1);
rtItem.sample();
const rtChain: any[] = (rtItem as any)._minute;
rtChain.unshift({ ts: Date.now() - 10000, value: 0, times: 0 });
const rtSnap = rtItem.getStatsDataInMinute();
check('avgpt = 200 (avg latency)', Math.abs(rtSnap.avgpt - 200) < 1e-9);

// consumeStatus wiring
mgr.incConsumeFailedTPS('MyGroup', 'MyTopic', 5);
const failedItem = mgr.topicAndGroupConsumeFailedTPS.getAndCreate('MyTopic@MyGroup');
failedItem.sample();
failedItem.sampleHour();
(failedItem as any)._minute.unshift({ ts: Date.now() - 10000, value: 0, times: 0 });
(failedItem as any)._hour.unshift({ ts: Date.now() - 600000, value: 0, times: 0 });
const cs = mgr.consumeStatus('MyGroup', 'MyTopic');
check('pullTPS = 1.0', Math.abs(cs.pullTPS - 1.0) < 0.01);
check('consumeRT = 200', Math.abs(cs.consumeRT - 200) < 1e-9);
check('consumeFailedTPS = 0.5', Math.abs(cs.consumeFailedTPS - 0.5) < 0.01);
check('consumeFailedMsgs = 5 (hour window sum)', cs.consumeFailedMsgs === 5);

console.log('== manager lifecycle ==');
mgr.start();
mgr.start(); // idempotent
mgr.shutdown();
check('start/shutdown idempotent', true);

console.log('== consumer + admin construct ==');
const consumer = new DefaultMQPushConsumer('G');
check('statsManager null before start', consumer._statsManager == null);
const admin = new DefaultMQAdminExt();
check('admin default timeout 20000 (Java)', admin.timeoutMillis === 20000);
check('admin group admin_ext_group (Java)', admin.adminExtGroup === 'admin_ext_group');
check('admin kvNamespaceToDeleteList = [ORDER_TOPIC_CONFIG]',
  JSON.stringify(admin.kvNamespaceToDeleteList) === '["ORDER_TOPIC_CONFIG"]');
check('string2Properties comments+whitespace',
  JSON.stringify(string2Properties('a=1\n# c\nb = x y\nc:z\n')) === '{"a":"1","b":"x y","c":"z"}');

// 307 statusTable shape (pre-start → zeros per Java ConsumeStatus)
const st = (consumer as any)._buildStatusTable();
console.log('  statusTable(pre-start) =', JSON.stringify(st));

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
