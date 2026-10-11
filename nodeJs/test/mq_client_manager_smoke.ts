// Smoke: MQClientInstance 跨 facade 复用（org.apache.rocketmq.client.impl.MQClientManager
// + 引用计数关闭 + 实例级反向请求多播）。Run: node --experimental-strip-types test/mq_client_manager_smoke.ts
//
// 覆盖：
//   1. manager factoryTable：clientId 命中复用 / 未命中现建（created 标志、factory 只跑一次）。
//   2. 引用计数：register/unregister 平衡；**归零才真正 shutdown**（定时器停、实例不再 running）。
//   3. 非本 manager 缓存的实例（测试替身）归还时保持旧语义：直接关，不炸。
//   4. removeClientFactory：摘表不关停；identity 不符不误删顶替上来的新实例。
//   5. MQClient.start() 幂等：共享实例上第二个门面不会重复拉起后台定时器 / 重复挂 39。
//   6. 门面处理器多播：同 code 多个登记者扇出（39 广播），互不覆盖；最后一个摘除者卸掉传输层槽。
//   7. 真门面复用：两个 producer + 一个 admin 同 instanceName → 同一个 MQClient；
//      单个门面 shutdown 不杀共享实例；39 反向请求按组分发给正确的 producer，且广播给
//      其他登记者。
//   8. 独立 clientId 不复用：不同 instanceName 的 facade 各自一套实例与定时器，互不影响。
//
// 全程离线：假 name server 地址（心跳/路由节拍无 broker/无 publish info，不发网络），
// END_TRANSACTION 的 oneway 走被替换的 capture 函数 —— 不连真集群，无死端口断言。
import { MQClient } from '../src/client/mq_client.ts';
import { MQClientManager } from '../src/client/mq_client_manager.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RequestCode, ResponseCode } from '../src/remoting/codes.ts';
import { MessageExt } from '../src/common/message.ts';
import { MessageConst } from '../src/common/messageConst.ts';
import { encodeMessageExt } from '../src/common/messageDecoder.ts';
import { MixAll } from '../src/common/mixAll.ts';

let pass = 0, fail = 0;
function check(name: string, cond: boolean, detail = '') {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${detail}`); }
}
const mgr = MQClientManager.getInstance();
const FAKE_NS = '127.0.0.1:9876'; // 永不被拨：表空则节拍空转

async function tick(times = 2): Promise<void> {
  for (let i = 0; i < times; i++) await new Promise((r) => setTimeout(r, 0));
}

// ---------------------------------------------------------------- 1. factory table
console.log('== manager factoryTable ==');
{
  mgr.reset();
  let factoryCalls = 0;
  const r1 = mgr.getOrCreateMQClientInstance('cid_1A', () => {
    factoryCalls++;
    return new MQClient('cid_1A', FAKE_NS, null);
  });
  check('miss creates a new instance (created=true)', r1.created === true);
  const r2 = mgr.getOrCreateMQClientInstance('cid_1A', () => {
    factoryCalls++;
    return new MQClient('cid_1A', FAKE_NS, null);
  });
  check('hit reuses the cached instance (created=false)', r2.created === false && r2.instance === r1.instance);
  check('factory not run on a hit', factoryCalls === 1, `calls=${factoryCalls}`);
  const r3 = mgr.getOrCreateMQClientInstance('cid_1B', () => new MQClient('cid_1B', FAKE_NS, null));
  check('一个独立 clientId 不复用（不同实例）', r3.instance !== r1.instance && r3.created === true);
  check('getClientFactory 观测面', mgr.getClientFactory('cid_1A') === r1.instance);
  let threw = '';
  try { mgr.getOrCreateMQClientInstance('', () => new MQClient('x', FAKE_NS, null)); }
  catch (e) { threw = (e as Error).message; }
  check('空 clientId 直接报', threw.includes('clientId is empty'), threw);
  r1.instance.shutdown(); r3.instance.shutdown();
  mgr.reset();
}

// ---------------------------------------------------------------- 2. refcount
console.log('== 引用计数：归零才真正 shutdown ==');
{
  mgr.reset();
  const inst = new MQClient('cid_2', FAKE_NS, null);
  inst.start();
  check('实例已启动', inst.isRunning() === true);
  mgr.registerClient('cid_2', inst);
  mgr.registerClient('cid_2', inst); // 两个门面同一 clientId
  check('refcount 计到 2', mgr.refcountOf('cid_2') === 2, `got ${mgr.refcountOf('cid_2')}`);
  const last1 = mgr.unregisterClient('cid_2', inst);
  check('计数未归零：不关停，实例继续存活', last1 === false && inst.isRunning() === true);
  check('表项仍在', mgr.getClientFactory('cid_2') === inst);
  const last2 = mgr.unregisterClient('cid_2', inst);
  check('归零：真正 shutdown（停定时器/断连）', last2 === true && inst.isRunning() === false);
  check('归零后条目从表里摘掉', mgr.getClientFactory('cid_2') === null && mgr.refcountOf('cid_2') === 0);
}

// ---------------------------------------------------------------- 3. 非表实例归还
console.log('== 测试替身 / 非本 manager 缓存的实例 ==');
{
  mgr.reset();
  let shut = 0;
  const fake = { shutdown() { shut++; } } as any;
  check('表里没有该 clientId：按旧语义直接关', mgr.unregisterClient('cid_unknown', fake) === true && shut === 1);
  let threw = false;
  try { mgr.unregisterClient('cid_unknown', {} as any); } catch (e) { threw = true; }
  check('替身没有 shutdown 也不炸', threw === false);
  check('null 实例归还返回 false', mgr.unregisterClient('cid_unknown', null) === false);
  // 表里另有他人条目时不误伤：identity 不符只关调用方手里的实例
  const inst = new MQClient('cid_3', FAKE_NS, null);
  inst.start();
  mgr.registerClient('cid_3', inst);
  const stranger = { shutdown() { shut++; } } as any;
  mgr.unregisterClient('cid_3', stranger);
  check('identity 不符：不动表项，共享实例继续存活',
    mgr.refcountOf('cid_3') === 1 && inst.isRunning() === true && shut === 2);
  mgr.unregisterClient('cid_3', inst);
  mgr.reset();
}

// ---------------------------------------------------------------- 4. removeClientFactory
console.log('== removeClientFactory：摘表不关停 ==');
{
  mgr.reset();
  const inst = new MQClient('cid_4', FAKE_NS, null);
  inst.start();
  mgr.registerClient('cid_4', inst);
  const other = { shutdown() { throw new Error('must NOT be shut down'); } } as any;
  mgr.removeClientFactory('cid_4', other); // identity 不符：不误删
  check('identity 守卫：表项仍在', mgr.getClientFactory('cid_4') === inst);
  mgr.removeClientFactory('cid_4', inst);
  check('摘表后 getOrCreate 不会再毒化出半死实例', mgr.getClientFactory('cid_4') === null);
  check('removeClientFactory 不关停（实例仍在跑）', inst.isRunning() === true);
  inst.shutdown();
  mgr.reset();
}

// ---------------------------------------------------------------- 5. start() 幂等
console.log('== MQClient.start() 幂等（共享实例第二个门面不重复起后台任务） ==');
{
  const inst = new MQClient('cid_5', FAKE_NS, null);
  inst.start();
  const hb = (inst as any)._heartbeatTimer;
  const rt = (inst as any)._routeTimer;
  const cnt39 = inst.facadeProcessorCount(RequestCode.CHECK_TRANSACTION_STATE);
  inst.start(); // 复用门面再 start 一次
  check('心跳定时器只拉起一次', (inst as any)._heartbeatTimer === hb && hb != null);
  check('路由刷新定时器只拉起一次', (inst as any)._routeTimer === rt && rt != null);
  check('39 处理器不因重复 start 翻倍',
    inst.facadeProcessorCount(RequestCode.CHECK_TRANSACTION_STATE) === cnt39 && cnt39 === 1);
  inst.shutdown();
  check('shutdown 清掉后台定时器',
    (inst as any)._heartbeatTimer === null && (inst as any)._routeTimer === null);
}

// ---------------------------------------------------------------- 6. facade 多播
console.log('== 实例级反向请求多播（后注册不再覆盖先注册） ==');
{
  const inst = new MQClient('cid_6', FAKE_NS, null);
  inst.start();
  const rc = (inst as any).remotingClient;
  const hits: string[] = [];
  const h1 = () => { hits.push('h1'); return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, 'h1'); };
  const h2 = () => { hits.push('h2'); return null; };
  const code39 = RequestCode.CHECK_TRANSACTION_STATE;
  inst.registerFacadeProcessor(code39, h1);
  inst.registerFacadeProcessor(code39, h2);
  check('同 code 三个登记者（内置 + 两个门面）扇出而非覆盖',
    inst.facadeProcessorCount(code39) === 3, `got ${inst.facadeProcessorCount(code39)}`);
  const cmd39 = RemotingCommand.createRequestCommand(code39, null); // 空 body：内置解码失败仍应答
  const resp = inst.dispatchFacadeProcessor(cmd39, '127.0.0.1:10911');
  check('39 广播：h1 与 h2 都被调用', hits.includes('h1') && hits.includes('h2'), hits.join(','));
  check('应答取第一个非 null（内置处理器的 SYSTEM_ERROR）', resp != null && resp.code === ResponseCode.SYSTEM_ERROR);
  inst.unregisterFacadeProcessor(code39, h1);
  inst.unregisterFacadeProcessor(code39, h2);
  check('摘除后回到内置独占', inst.facadeProcessorCount(code39) === 1);
  // 传输层槽的装卸：首个登记者挂上分发器，最后一个摘除者卸槽（consumer 类 code 36）。
  const code36 = RequestCode.NOTIFY_CONSUMER_IDS_CHANGED;
  const ha = () => null;
  const hb2 = () => null;
  inst.registerFacadeProcessor(code36, ha);
  check('首个登记者把分发器挂上传输层槽', rc.processors.has(code36));
  inst.registerFacadeProcessor(code36, hb2);
  inst.unregisterFacadeProcessor(code36, () => null); // 摘不存在的身份：no-op
  check('identity 摘除只认登记的那份', inst.facadeProcessorCount(code36) === 2 && rc.processors.has(code36));
  inst.unregisterFacadeProcessor(code36, ha);
  check('还有登记者时不卸槽', rc.processors.has(code36) && inst.facadeProcessorCount(code36) === 1);
  inst.unregisterFacadeProcessor(code36, hb2);
  check('最后一个摘除者卸掉传输层槽', !rc.processors.has(code36) && inst.facadeProcessorCount(code36) === 0);
  // 广播 = 全扇出：后面的登记者产生应答，前面的照样被唤起；应答回给传输层的是第一个非 null。
  const seen: string[] = [];
  const m1 = () => { seen.push('m1'); return null; };
  const m2 = () => { seen.push('m2'); return RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, 'm2'); };
  inst.registerFacadeProcessor(code36, m1);
  inst.registerFacadeProcessor(code36, m2);
  const dresp = inst.dispatchFacadeProcessor(RemotingCommand.createRequestCommand(code36, null), '127.0.0.1:10911');
  check('广播：两个登记者都被唤起（不因先应答而短路）', seen.includes('m1') && seen.includes('m2'), seen.join(','));
  check('应答回给传输层的是第一个非 null', dresp != null && dresp.remark === 'm2');
  inst.unregisterFacadeProcessor(code36, m1);
  inst.unregisterFacadeProcessor(code36, m2);
  check('无人登记的 code 分派出 null',
    inst.dispatchFacadeProcessor(RemotingCommand.createRequestCommand(9999, null), 'x') === null);
  inst.shutdown();
}

// ---------------------------------------------------------------- 7. 真门面复用
console.log('== producer + producer + admin 同 instanceName → 共享一个 MQClient ==');
{
  mgr.reset();
  const sharedName = 'SMGR_shared';
  const expectedClientId = MixAll.clientIdFor(sharedName, null);
  const checkedFor: string[] = [];
  const listenerA = { checkLocalTransaction: (msg: MessageExt) => {
    checkedFor.push(String(msg.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP)));
    return 0; // COMMIT（LocalTransactionState.COMMIT_MESSAGE）
  } };
  const listenerB = { checkLocalTransaction: (msg: MessageExt) => {
    checkedFor.push('B:' + String(msg.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP)));
    return 2; // UNKNOW
  } };
  const pA = new DefaultMQProducer('GID_mgr_reuse_A');
  const pB = new DefaultMQProducer('GID_mgr_reuse_B');
  const admin = new DefaultMQAdminExt();
  pA.setInstanceName(sharedName); pB.setInstanceName(sharedName); admin.setInstanceName(sharedName);
  pA.setNamesrvAddr(FAKE_NS); pB.setNamesrvAddr(FAKE_NS); admin.setNamesrvAddr(FAKE_NS);
  (pA as any).transactionListener = listenerA;
  (pB as any).transactionListener = listenerB;
  await pA.start();
  await pB.start();
  admin.start();
  const shared = pA.client!;
  check('clientId 走 group 无关的 ip@instanceName 维度', shared.clientId === expectedClientId, shared.clientId);
  check('三个门面共享同一个 MQClient 实例', pB.client === shared && admin.client === shared);
  check('引用计数 = 门面数 3', mgr.refcountOf(shared.clientId) === 3, `got ${mgr.refcountOf(shared.clientId)}`);
  check('两个 producer 都在实例的 producerTable 上',
    shared.getRegisteredProducers().has('GID_mgr_reuse_A') && shared.getRegisteredProducers().has('GID_mgr_reuse_B'));
  // 一条 RPC 通道：同一个 RemotingClient 对象
  check('到 broker 的 RPC 通道只有一条（同一 RemotingClient）',
    (pB.client as any).remotingClient === (admin.client as any).remotingClient);

  // 反向请求：END_TRANSACTION 的 oneway 用 capture 替身（不外拨）。
  const ended: RemotingCommand[] = [];
  (shared.remotingClient as any).invokeOneway = (_addr: string, req: RemotingCommand) => {
    ended.push(req); return Promise.resolve();
  };
  // 额外的 39 登记者（广播观察）：共享实例上 39 扇出给所有登记者。
  const extra: number[] = [];
  const ex = () => { extra.push(1); return null; };
  shared.registerFacadeProcessor(RequestCode.CHECK_TRANSACTION_STATE, ex);

  const half = new MessageExt();
  half.setTopic('T_mgr');
  half.setBody(Buffer.from('x'));
  half.putProperty(MessageConst.PROPERTY_PRODUCER_GROUP, 'GID_mgr_reuse_A');
  half.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, 'UNIQ-A-1');
  const cmd = RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, null);
  cmd.body = encodeMessageExt(half, false);
  cmd.addExtField('tranStateTableOffset', '1');
  cmd.addExtField('commitLogOffset', '2');
  const resp = shared.dispatchFacadeProcessor(cmd, '127.0.0.1:10911');
  await tick(3);
  check('39 即时应答 SUCCESS（检查在 setImmediate 里跑，不在分发线程同步外拨）',
    resp != null && resp.code === ResponseCode.SUCCESS);
  check('39 按组分发：只有 A 组的 listener 被唤起',
    checkedFor.length === 1 && checkedFor[0] === 'GID_mgr_reuse_A', checkedFor.join(','));
  check('39 同时广播给其他登记者（多播，非覆盖）', extra.length === 1);
  check('A 组 COMMIT → END_TRANSACTION oneway 已发出',
    ended.length === 1 && ended[0].code === RequestCode.END_TRANSACTION);
  shared.unregisterFacadeProcessor(RequestCode.CHECK_TRANSACTION_STATE, ex);

  // 单个门面 shutdown 不杀共享实例
  pA.shutdown();
  check('A 退场：引用计数 3→2，共享实例继续存活',
    mgr.refcountOf(shared.clientId) === 2 && shared.isRunning() === true);
  check('A 从 producerTable 摘干净', !shared.getRegisteredProducers().has('GID_mgr_reuse_A'));
  check('B 仍在使用同一个实例（未受 A 退场影响）', pB.client === shared && shared.isRunning() === true);
  pB.shutdown();
  check('B 退场：还剩 admin 一份，实例不断连',
    mgr.refcountOf(shared.clientId) === 1 && shared.isRunning() === true);
  admin.shutdown();
  check('最后一任退场：归零，实例真正 shutdown 并摘表',
    shared.isRunning() === false && mgr.getClientFactory(shared.clientId) === null);
}

// ---------------------------------------------------------------- 8. 独立 clientId 不复用
console.log('== 独立 clientId 各自一套，互不影响 ==');
{
  mgr.reset();
  const pX = new DefaultMQProducer('GID_mgr_iso_X');
  const pY = new DefaultMQProducer('GID_mgr_iso_Y');
  pX.setInstanceName('SMGR_x'); pY.setInstanceName('SMGR_y');
  pX.setNamesrvAddr(FAKE_NS); pY.setNamesrvAddr(FAKE_NS);
  await pX.start();
  await pY.start();
  const xInst = pX.client!;
  const yInst = pY.client!;
  check('不同 instanceName → 不同实例', xInst !== yInst && xInst.clientId !== yInst.clientId);
  check('两个 clientId 各计各数', mgr.refcountOf(xInst.clientId) === 1
    && mgr.refcountOf(yInst.clientId) === 1);
  pX.shutdown();
  check('X 归零只关自己，Y 的实例与定时器不受影响',
    xInst.isRunning() === false && yInst.isRunning() === true
    && mgr.getClientFactory(yInst.clientId) === yInst);
  pY.shutdown();
  check('Y 也归零摘表', mgr.getClientFactory(yInst.clientId) === null);
  mgr.reset();
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
