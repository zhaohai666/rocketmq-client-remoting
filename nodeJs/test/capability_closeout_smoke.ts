// The capability gaps closed against the Java 5.x reference client.
//
// Each block pins one Java facade to this port and pairs the new accessor with the
// behavior it steers, so a refactor that keeps the setter but drops the wiring goes
// red here:
//
//   - ConsumeFromWhere: all six values of the Java enum. The three "deprecated" ones
//     are not dead -- Java folds them into the CONSUME_FROM_LAST_OFFSET branch of the
//     offset-init switch (RebalancePushImpl#computePullFromWhereWithException:171-173).
//   - LitePull connectBrokerByUser / defaultBrokerId: the only way to read from a slave
//     (PullAPIWrapper#recalculatePullFromWhichNode).
//   - Push awaitTerminationMillisWhenShutdown: the shutdown() drain budget.
//   - Admin adminExtGroup: default "admin_ext_group" plus the Java accessor pair.
import { ConsumeFromWhere } from '../src/remoting/heartbeat.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { MessageQueue } from '../src/common/message.ts';
import { PullAPI } from '../src/client/pull_api.ts';
import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQAdminExt, DefaultAdminExtGroup } from '../src/client/admin.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, extra = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${extra}`); }
}

const TOPIC = 'CloseoutTopic';
const BROKER = 'broker-a';

// The authoritative list is Java common/.../consumer/ConsumeFromWhere.java.
const JAVA_CONSUME_FROM_WHERE = [
  'CONSUME_FROM_LAST_OFFSET',
  'CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST',
  'CONSUME_FROM_MIN_OFFSET',
  'CONSUME_FROM_MAX_OFFSET',
  'CONSUME_FROM_FIRST_OFFSET',
  'CONSUME_FROM_TIMESTAMP',
];

function consumeFromWhere() {
  console.log('== ConsumeFromWhere: six Java values ==');
  for (const value of JAVA_CONSUME_FROM_WHERE) {
    check(`${value} exists and equals its wire string`,
      (ConsumeFromWhere as Record<string, string>)[value] === value,
      String((ConsumeFromWhere as Record<string, string>)[value]));
  }
  check('no extra values beyond the Java enum',
    Object.keys(ConsumeFromWhere).length === JAVA_CONSUME_FROM_WHERE.length,
    Object.keys(ConsumeFromWhere).join(','));
}

function litePullBrokerPin() {
  console.log('== LitePull connectBrokerByUser / defaultBrokerId ==');
  const stub = {} as any;
  const api = new PullAPI(stub, 'GID_Pin');
  const mq = new MessageQueue(TOPIC, BROKER, 0);
  check('pin defaults to off', api.connectBrokerByUser === false);
  check('defaultBrokerId defaults to MASTER_ID', api.defaultBrokerId === MixAll.MASTER_ID,
    String(api.defaultBrokerId));
  // The broker suggested a slave: unpinned rounds follow it.
  api.updatePullFromWhichNode(mq, 2);
  check('unpinned round follows the suggestion table',
    api.recalculatePullFromWhichNode(mq) === 2, String(api.recalculatePullFromWhichNode(mq)));
  api.connectBrokerByUser = true;
  api.defaultBrokerId = 1;
  check('pinned round ignores the suggestion table',
    api.recalculatePullFromWhichNode(mq) === 1, String(api.recalculatePullFromWhichNode(mq)));
  api.connectBrokerByUser = false;
  check('unpinning restores the table (no latch)',
    api.recalculatePullFromWhichNode(mq) === 2, String(api.recalculatePullFromWhichNode(mq)));

  // Facade: settable before start(), and mirrored onto the PullAPI once it exists.
  const c = new DefaultLitePullConsumer('GID_PinFacade');
  check('facade reports the Java defaults',
    c.isConnectBrokerByUser() === false && c.getDefaultBrokerId() === MixAll.MASTER_ID);
  c.setConnectBrokerByUser(true);
  c.setDefaultBrokerId(3);
  check('facade keeps the values before start()',
    c.isConnectBrokerByUser() === true && c.getDefaultBrokerId() === 3);
  (c as any).pullAPI = api;
  c.setDefaultBrokerId(4);
  check('setting after start() reaches the PullAPI', api.defaultBrokerId === 4,
    String(api.defaultBrokerId));
  c.setConnectBrokerByUser(false);
  check('clearing after start() reaches the PullAPI', api.connectBrokerByUser === false);
}

function terminationDrain() {
  console.log('== Push awaitTerminationMillisWhenShutdown ==');
  const c = new DefaultMQPushConsumer('GID_Await');
  // DEVIATION from Java's 0: this port drains by default (see the field's note).
  check('default drain budget is the historic 30s', c.getAwaitTerminationMillisWhenShutdown() === 30000,
    String(c.getAwaitTerminationMillisWhenShutdown()));
  c.setAwaitTerminationMillisWhenShutdown(0);
  check('0 is legal (do not wait)', c.getAwaitTerminationMillisWhenShutdown() === 0);
  c.setAwaitTerminationMillisWhenShutdown(1234);
  check('a positive budget round-trips', c.getAwaitTerminationMillisWhenShutdown() === 1234);
  c.setAwaitTerminationMillisWhenShutdown(-5);
  check('a negative budget clamps to 0', c.getAwaitTerminationMillisWhenShutdown() === 0);
}

function adminGroup() {
  console.log('== Admin adminExtGroup ==');
  check('default constant matches Java', DefaultAdminExtGroup === 'admin_ext_group', DefaultAdminExtGroup);
  const admin = new DefaultMQAdminExt();
  check('facade default', admin.getAdminExtGroup() === 'admin_ext_group', admin.getAdminExtGroup());
  admin.setAdminExtGroup('custom-admin-group');
  check('set/get round-trip', admin.getAdminExtGroup() === 'custom-admin-group');
  const ctorArg = new DefaultMQAdminExt(null, 20000, 'ctor-admin-group');
  check('constructor argument is honoured', ctorArg.getAdminExtGroup() === 'ctor-admin-group');
}

function main() {
  consumeFromWhere();
  litePullBrokerPin();
  terminationDrain();
  adminGroup();
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail > 0 ? 1 : 0);
}

main();
