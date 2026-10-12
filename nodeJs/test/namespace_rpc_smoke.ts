// -*- coding: utf-8 -*-
// NamespaceRpcHook 冒烟测试（运行：node --experimental-strip-types test/namespace_rpc_smoke.ts）。
//
// 离线行为验证：
//   1. 配置了 namespaceV2 时，钩子必须向请求 extFields 恰好添加 nsd="true" 和
//      ns=<value>（保留原有字段）。
//   2. namespace 为空/未定义时，请求 extFields 必须保持原样——钩子不能
//      物化出任何字段。
//   3. 钩子组合保持固定的注册顺序：Namespace -> Stream -> 用户（ACL）钩子。
//   4. namespaceV2 设置后 ACL 签名必须变化——namespace 钩子先运行，nsd/ns
//      字段因此被签名覆盖。
//   5. 每个门面（producer / push consumer / pull consumer / lite pull
//      consumer / admin）都暴露 setNamespaceV2/getNamespaceV2，且 producer 与
//      admin 的 start() 按约定顺序安装钩子链。
//   6. AsyncTraceDispatcher 会把 namespaceV2 传递给其内部的 trace producer。
import assert from 'node:assert';
import { Buffer } from 'node:buffer';

import { RemotingCommand } from '../src/remoting/remotingCommand.ts';
import { RemotingClient } from '../src/remoting/client.ts';
import { signAcl, AclRPCHook } from '../src/remoting/acl.ts';
import {
  NamespaceRpcHook, StreamTypeRPCHook, buildRpcHooks, registerRpcHooks,
} from '../src/remoting/rpc_hooks.ts';
import { MixAll } from '../src/common/mixAll.ts';
import { MQClient } from '../src/client/mq_client.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { AsyncTraceDispatcher } from '../src/client/trace_dispatcher.ts';

function section(name: string): void {
  console.log(`--- ${name} ---`);
}

// ---------------------------------------------------------------------------
// 1. namespaceV2 set -> exactly nsd=true + ns=<value>
// ---------------------------------------------------------------------------
function testAddsNamespacedFields(): void {
  section('NamespaceRpcHook adds nsd/ns');
  assert.strictEqual(MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD, 'nsd');
  assert.strictEqual(MixAll.RPC_REQUEST_HEADER_NAMESPACE_FIELD, 'ns');

  const cmd = RemotingCommand.createRequestCommand(310, null);
  cmd.extFields = { topic: 'MyTopic' };
  new NamespaceRpcHook('RMQ_INST_TEST').doBeforeRequest('127.0.0.1:10911', cmd);
  assert.strictEqual(cmd.extFields['nsd'], 'true', 'nsd must be the literal "true"');
  assert.strictEqual(cmd.extFields['ns'], 'RMQ_INST_TEST', 'ns must carry namespaceV2');
  assert.strictEqual(cmd.extFields['topic'], 'MyTopic', 'pre-existing extFields survive');
  assert.strictEqual(Object.keys(cmd.extFields).length, 3, 'EXACTLY the two namespace fields are added');

  // namespaceV2 是每次请求实时读取的——provider 形式必须看到当前值，
  // 而不是快照。
  let live: string | null = 'RMQ_INST_ONE';
  const hook = new NamespaceRpcHook(() => live);
  const cmd2 = RemotingCommand.createRequestCommand(310, null);
  hook.doBeforeRequest('127.0.0.1:10911', cmd2);
  assert.strictEqual(cmd2.extFields['ns'], 'RMQ_INST_ONE');
  live = 'RMQ_INST_TWO';
  const cmd3 = RemotingCommand.createRequestCommand(310, null);
  hook.doBeforeRequest('127.0.0.1:10911', cmd3);
  assert.strictEqual(cmd3.extFields['ns'], 'RMQ_INST_TWO', 'provider form must track updates');
  console.log('nsd/ns stamped OK');
}

// ---------------------------------------------------------------------------
// 2. empty / undefined namespace -> extFields UNTOUCHED
// ---------------------------------------------------------------------------
function testNoNamespaceLeavesExtFieldsUntouched(): void {
  section('Empty namespace is a no-op');
  for (const empty of [null, undefined, '']) {
    const cmd = RemotingCommand.createRequestCommand(310, null);
    cmd.extFields = { topic: 'MyTopic', b: 'MyTopic' };
    const before = JSON.stringify(cmd.extFields);
    new NamespaceRpcHook(empty as string | null).doBeforeRequest('127.0.0.1:10911', cmd);
    assert.deepStrictEqual(cmd.extFields, JSON.parse(before),
      `namespaceV2=${JSON.stringify(empty)}: extFields must stay exactly as they were`);
    assert.ok(!('nsd' in cmd.extFields) && !('ns' in cmd.extFields), 'no nsd/ns keys');
  }
  // A brand-new command AND a provider returning empty: nothing is materialized.
  const fresh = RemotingCommand.createRequestCommand(310, null);
  new NamespaceRpcHook(() => '').doBeforeRequest('127.0.0.1:10911', fresh);
  assert.deepStrictEqual(fresh.extFields, {}, 'empty namespace must not add any field');
  // doAfterResponse 为空实现。
  const hook = new NamespaceRpcHook('RMQ_INST_TEST');
  hook.doAfterResponse('127.0.0.1:10911', null, null);
  console.log('no-op parity OK');
}

// ---------------------------------------------------------------------------
// 3. composition order: Namespace -> Stream -> ACL
// ---------------------------------------------------------------------------
function testCompositionOrder(): void {
  section('钩子链注册顺序');
  const acl = new AclRPCHook('ak', 'sk');
  const hooks = buildRpcHooks({ namespaceV2: 'RMQ_INST_ORDER', enableStreamRequestType: true, userHook: acl });
  assert.strictEqual(hooks.length, 3, 'namespace + stream + user');
  assert.ok(hooks[0] instanceof NamespaceRpcHook, 'namespace hook must be FIRST');
  assert.ok(hooks[1] instanceof StreamTypeRPCHook, 'stream hook must be SECOND');
  assert.strictEqual(hooks[2], acl, 'the user (ACL) hook must run LAST');

  // 未启用 stream 时该钩子不存在（仅在 enableStreamRequestType 时注册），
  // namespace 钩子则始终无条件安装。
  const noStream = buildRpcHooks({ namespaceV2: null, userHook: acl });
  assert.strictEqual(noStream.length, 2);
  assert.ok(noStream[0] instanceof NamespaceRpcHook);

  // Through the RemotingClient itself: hooks are invoked in registration
  // order, so by the time the USER hook runs, nsd/ns/ReqT are on the command.
  const rc = new RemotingClient({});
  const seen: Record<string, string | undefined> = {};
  const recorder = {
    doBeforeRequest: (_addr: string, cmd: RemotingCommand) => {
      seen['nsd'] = cmd.extFields['nsd'];
      seen['ns'] = cmd.extFields['ns'];
      seen['ReqT'] = cmd.extFields['ReqT'];
      seen['Signature'] = cmd.extFields['Signature']; // must not exist yet
    },
    doAfterResponse: () => {},
  };
  registerRpcHooks(rc, {
    namespaceV2: 'RMQ_INST_ORDER', enableStreamRequestType: true, userHook: recorder,
  });
  const cmd = RemotingCommand.createRequestCommand(310, null);
  (rc as any)._applyBeforeRequestHooks('127.0.0.1:10911', cmd);
  assert.strictEqual(seen['nsd'], 'true', 'user hook must already see nsd');
  assert.strictEqual(seen['ns'], 'RMQ_INST_ORDER', 'user hook must already see ns');
  assert.strictEqual(seen['ReqT'], '0', 'stream ReqT = String.valueOf(RequestType.STREAM.getCode()) = "0"');
  assert.strictEqual(seen['Signature'], undefined, 'the user hook runs BEFORE the signature exists');
  // And the same chain against the real ACL hook leaves a signed command.
  const rc2 = new RemotingClient({});
  registerRpcHooks(rc2, {
    namespaceV2: 'RMQ_INST_ORDER', enableStreamRequestType: true, userHook: acl,
  });
  const cmd2 = RemotingCommand.createRequestCommand(310, null);
  cmd2.body = Buffer.from('order', 'utf8');
  (rc2 as any)._applyBeforeRequestHooks('127.0.0.1:10911', cmd2);
  assert.strictEqual(cmd2.extFields['nsd'], 'true');
  assert.strictEqual(cmd2.extFields['ReqT'], '0');
  assert.ok(cmd2.extFields['Signature'], 'ACL signature present after the chain');
  console.log('chain order OK');
}

// ---------------------------------------------------------------------------
// 4. the ACL signature changes when namespaceV2 is set
// ---------------------------------------------------------------------------
function testSignatureCoversNamespace(): void {
  section('ACL signature covers nsd/ns');
  const body = Buffer.from('hello-namespace', 'utf8');
  const mkCmd = () => {
    const cmd = RemotingCommand.createRequestCommand(310, null);
    cmd.extFields = { a: 'myGroup', b: 'myTopic' };
    cmd.body = body;
    return cmd;
  };

  // No namespace: baseline signature.
  const plain = mkCmd();
  new AclRPCHook('accessKey1', 'secretKey1').doBeforeRequest('127.0.0.1:10911', plain);
  const sigPlain = plain.extFields['Signature'];

  // 固定顺序（先 Namespace 再 ACL）：nsd/ns 的值进入签名内容
  // -> 签名不同。
  const first = mkCmd();
  for (const hook of buildRpcHooks({ namespaceV2: 'RMQ_INST_SIGN', userHook: new AclRPCHook('accessKey1', 'secretKey1') })) {
    hook.doBeforeRequest('127.0.0.1:10911', first);
  }
  const sigNamespaced = first.extFields['Signature'];
  assert.notStrictEqual(sigPlain, sigNamespaced,
    'setting namespaceV2 must change the ACL signature (nsd/ns are signed)');

  // The signed content is exactly signAcl over the extFields the chain built —
  // i.e. the broker can re-verify nsd/ns with the same key.
  const expectFields: Record<string, string> = { a: 'myGroup', b: 'myTopic' };
  new NamespaceRpcHook('RMQ_INST_SIGN').doBeforeRequest('127.0.0.1:10911', {
    extFields: expectFields,
    addExtField(k: string, v: string) { this.extFields[k] = v; },
  } as any);
  expectFields['AccessKey'] = 'accessKey1';
  assert.strictEqual(signAcl('accessKey1', 'secretKey1', expectFields, body), sigNamespaced,
    'signature must match HMAC over the namespace-stamped field set');

  // 反序（先 ACL 再 Namespace——错误的接线）会让 nsd/ns 不被签名：
  // 签名与基线完全相同——这正是固定的注册顺序所防止的那个 bug。
  const reversed = mkCmd();
  new AclRPCHook('accessKey1', 'secretKey1').doBeforeRequest('127.0.0.1:10911', reversed);
  new NamespaceRpcHook('RMQ_INST_SIGN').doBeforeRequest('127.0.0.1:10911', reversed);
  assert.strictEqual(reversed.extFields['Signature'], sigPlain,
    'namespace fields added AFTER signing must not alter the signature');
  assert.strictEqual(reversed.extFields['ns'], 'RMQ_INST_SIGN');
  console.log('signature parity OK');
}

// ---------------------------------------------------------------------------
// 5. config surface on every facade
// ---------------------------------------------------------------------------
function testFacadeConfigSurface(): void {
  section('setNamespaceV2 / getNamespaceV2 on the facades');
  const p = new DefaultMQProducer('nsSurfaceP');
  assert.strictEqual(p.getNamespaceV2(), null, 'defaults to unset');
  assert.strictEqual(p.setNamespaceV2('RMQ_INST_P'), p, 'fluent like the other knobs');
  assert.strictEqual(p.getNamespaceV2(), 'RMQ_INST_P');

  const push = new DefaultMQPushConsumer('nsSurfaceC');
  assert.strictEqual(push.getNamespaceV2(), null);
  assert.strictEqual(push.setNamespaceV2('RMQ_INST_C'), push);
  assert.strictEqual(push.getNamespaceV2(), 'RMQ_INST_C');

  const pull = new DefaultMQPullConsumer('nsSurfacePull');
  assert.strictEqual(pull.getNamespaceV2(), null);
  assert.strictEqual(pull.setNamespaceV2('RMQ_INST_PULL'), pull);
  assert.strictEqual(pull.getNamespaceV2(), 'RMQ_INST_PULL');

  const lite = new DefaultLitePullConsumer('nsSurfaceLite');
  assert.strictEqual(lite.getNamespaceV2(), null);
  assert.strictEqual(lite.setNamespaceV2('RMQ_INST_LITE'), lite);
  assert.strictEqual(lite.getNamespaceV2(), 'RMQ_INST_LITE');

  const admin = new DefaultMQAdminExt();
  assert.strictEqual(admin.getNamespaceV2(), null);
  admin.setNamespaceV2('RMQ_INST_ADMIN');
  assert.strictEqual(admin.getNamespaceV2(), 'RMQ_INST_ADMIN');
  console.log('facade surface OK');
}

// ---------------------------------------------------------------------------
// 5b. real registration sites (producer + admin start(), offline)
// ---------------------------------------------------------------------------
async function testRegistrationSites(): Promise<void> {
  section('start() installs the chain in the pinned order: Namespace -> Stream -> ACL');
  const acl = new AclRPCHook('ak', 'sk');
  const p = new DefaultMQProducer('nsRegisterP');
  p.setNamespaceV2('RMQ_INST_REG');
  p.rpcHook = acl;
  await p.start();
  const pHooks = (p.getMQClient()!.remotingClient as any).rpcHooks;
  assert.ok(pHooks[0] instanceof NamespaceRpcHook, 'producer: namespace hook FIRST');
  assert.strictEqual(pHooks[1], acl, 'producer: user (ACL) hook AFTER the namespace hook');
  p.shutdown();

  const admin = new DefaultMQAdminExt(acl);
  admin.setNamesrvAddr('127.0.0.1:9876');
  admin.setNamespaceV2('RMQ_INST_ADM');
  admin.enableStreamRequestType = true;
  admin.start();
  const aHooks = (admin.client!.remotingClient as any).rpcHooks;
  assert.ok(aHooks[0] instanceof NamespaceRpcHook, 'admin: namespace hook FIRST');
  assert.ok(aHooks[1] instanceof StreamTypeRPCHook, 'admin: stream hook SECOND');
  assert.strictEqual(aHooks[2], acl, 'admin: user (ACL) hook THIRD');
  admin.shutdown();

  // Consumers compose the chain on their instance-level client too. Their
  // full start() needs a live name server, so drive the same seam the
  // start() bodies use: a fresh MQClient + registerRpcHooks.
  for (const kind of ['push', 'pull', 'lite']) {
    const client = new MQClient(`nsRegister${kind}`, null, null);
    const ns: string | null = kind === 'push' ? 'RMQ_INST_PUSH' : kind === 'pull' ? 'RMQ_INST_PULL' : 'RMQ_INST_LITE';
    const hooks = registerRpcHooks(client.remotingClient, { namespaceV2: () => ns });
    assert.strictEqual(hooks.length, 1, `${kind} consumer chain: namespace only (no user hook on this port)`);
    const cmd = RemotingCommand.createRequestCommand(310, null);
    (client.remotingClient as any)._applyBeforeRequestHooks('127.0.0.1:10911', cmd);
    assert.strictEqual(cmd.extFields['ns'], ns, `${kind}: requests carry ns`);
    client.shutdown();
  }
  console.log('registration sites OK');
}

// ---------------------------------------------------------------------------
// 6. trace dispatcher propagates namespaceV2 to the internal producer
// ---------------------------------------------------------------------------
async function testTraceDispatcherPropagation(): Promise<void> {
  section('AsyncTraceDispatcher.namespaceV2 propagation');
  const d = new AsyncTraceDispatcher('nsTraceGroup', 'PRODUCER', 10);
  assert.strictEqual(d.getNamespaceV2(), null, 'defaults to unset');
  d.setNamespaceV2('RMQ_INST_TRACE');
  // start() 时应把 namespaceV2 传给内部的 trace producer。
  await d.start('127.0.0.1:9876');
  const inner = (d as any)._producer as DefaultMQProducer | null;
  assert.ok(inner != null, 'internal trace producer must be up (offline start)');
  assert.strictEqual(inner!.getNamespaceV2(), 'RMQ_INST_TRACE',
    'the trace producer must inherit namespaceV2 so its rpcs carry nsd/ns');
  await d.shutdown();
  console.log('trace propagation OK');
}

// ---------------------------------------------------------------------------
async function main(): Promise<void> {
  testAddsNamespacedFields();
  testNoNamespaceLeavesExtFieldsUntouched();
  testCompositionOrder();
  testSignatureCoversNamespace();
  testFacadeConfigSurface();
  await testRegistrationSites();
  await testTraceDispatcherPropagation();
  console.log('\nPASS');
}

main().catch((e) => { console.error(e); process.exit(1); });
