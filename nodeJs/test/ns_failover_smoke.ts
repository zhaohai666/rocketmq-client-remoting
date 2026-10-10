// -*- coding: utf-8 -*-
// NS failover + route-error transparency smoke (no network).
//
//   node --experimental-strip-types test/ns_failover_smoke.ts
//
// 1. 多 NS 挂一台：路由拉取轮换存活 NS，反复调用不再"撞死即失败"（旧实现纯随机
//    单挑，3 挂 1 时约 1/3 调用直接失败，新进程首拉路由死即表现为 no route info）。
// 2. 粘性：应答过的 NS 优先复用（Java namesrvAddrChoosed 语义），粘住的下一跳
//    恰好 1 次网络调用。
// 3. 粘住的死了：自动轮换到下一台并重新粘住。
// 4. 全部 NS 不可达：抛出最后那个真实错误（连接拒绝/TLS 握手…），不吞。
// 5. NS 可达但明确 TOPIC_NOT_EXIST：返回 false，不抛、不轮询后续 NS。
// 6. 生产者 No-route 报错带上路由失败原因（TLS/CA 痕迹），cause 链保留 ——
//    不再黑盒成 "No route info of this topic"。
import assert from 'node:assert';
import { Buffer } from 'node:buffer';

import { MQClient } from '../src/client/mq_client.ts';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { TopicRouteData, QueueData, BrokerData } from '../src/remoting/route.ts';
import { ResponseCode } from '../src/remoting/codes.ts';
import { Message } from '../src/common/message.ts';

let pass = 0;
let fail = 0;
function check(name: string, cond: boolean, detail = ''): void {
  if (cond) { pass++; console.log(`  PASS ${name}`); }
  else { fail++; console.log(`  FAIL ${name} ${detail}`); }
}

function routeBody(): Buffer {
  const trd = new TopicRouteData();
  trd.brokerDatas = [new BrokerData('broker-a', 'broker-a', { 0: '127.0.0.1:10911' })];
  trd.queueDatas = [new QueueData('broker-a', 4, 4, 6, 0)];
  return trd.encode();
}

type Handler = (addr: string) => Promise<any> | any;
function fakeRemoting(handlers: Record<string, Handler>): any {
  return {
    invokeSync: async (addr: string, _req: any, _timeout: number) => {
      const h = handlers[addr];
      if (h == null) throw new Error(`no handler for ${addr}`);
      return h(addr);
    },
  };
}

function okResponse(): any {
  return { code: ResponseCode.SUCCESS, body: routeBody(), remark: '' };
}

function deadErr(addr: string): Error {
  const e: any = new Error(`connect to ${addr} failed: ECONNREFUSED`);
  e.name = 'RemotingConnectException';
  return e;
}

async function testFailoverRotation(): Promise<void> {
  console.log('== dead NS rotation ==');
  let invocations = 0;
  const rc = fakeRemoting({
    'ns-a:9876': () => { invocations++; throw deadErr('ns-a:9876'); },
    'ns-b:9876': () => { invocations++; return okResponse(); },
    'ns-c:9876': () => { invocations++; return okResponse(); },
  });
  const c = new MQClient('cid-failover', 'ns-a:9876;ns-b:9876;ns-c:9876', rc as any);
  // 连续 30 次路由拉取：旧实现随机单挑，撞上 ns-a 即整个调用抛 —— 新实现必须全成功。
  for (let i = 0; i < 30; i++) {
    await c.updateTopicRouteInfoFromNameServer('FailoverTopic');
  }
  check('30 次路由拉取全部成功（3 台挂 1 台）', invocations >= 30);
  check('粘住的是存活 NS', c._namesrvChosen !== 'ns-a:9876' && c._namesrvChosen != null,
    `chosen=${c._namesrvChosen}`);

  // 粘性：chosen 应答过之后，下一次调用从它开始且立即成功 —— 恰好 1 次网络调用。
  const before = invocations;
  await c.updateTopicRouteInfoFromNameServer('FailoverTopic');
  check('粘住的 NS 直接命中（单次调用）', invocations === before + 1,
    `invocations ${before} -> ${invocations}`);

  // 粘住的死了：摘掉粘性、轮换到下一台。
  const prevChosen = c._namesrvChosen!;
  rc.invokeSync = async (addr: string) => {
    if (addr === prevChosen) throw deadErr(addr);
    return okResponse();
  };
  await c.updateTopicRouteInfoFromNameServer('FailoverTopic');
  check('粘住的 NS 死后轮换到下一台', c._namesrvChosen != null && c._namesrvChosen !== prevChosen,
    `chosen=${c._namesrvChosen}`);
}

async function testAllDead(): Promise<void> {
  console.log('== all NS dead ==');
  const attempted = new Set<string>();
  const rc = fakeRemoting({
    'ns-x:9876': () => { attempted.add('ns-x:9876'); throw deadErr('ns-x:9876'); },
    'ns-y:9876': () => { attempted.add('ns-y:9876'); throw deadErr('ns-y:9876'); },
  });
  const c = new MQClient('cid-dead', 'ns-x:9876;ns-y:9876', rc as any);
  // 起点是随机的：显式粘住 ns-x，断言顺序确定（x → y）
  c._namesrvChosen = 'ns-x:9876';
  let caught: Error | null = null;
  try {
    await c.updateTopicRouteInfoFromNameServer('SomeTopic');
  } catch (e) {
    caught = e as Error;
  }
  check('全部 NS 不可达时抛出', caught != null);
  check('抛的是最后尝试的 NS 的真实错误',
    caught != null && caught!.message.includes('ns-y:9876'),
    caught != null ? caught!.message : '');
  check('两个 NS 都被尝试过（按列表顺序轮换）',
    attempted.has('ns-x:9876') && attempted.has('ns-y:9876'),
    `attempted=${[...attempted].join(',')}`);
}

async function testTopicNotExist(): Promise<void> {
  console.log('== TOPIC_NOT_EXIST is a real answer ==');
  let calls = 0;
  const rc = fakeRemoting({
    'ns-1:9876': () => { calls++; return { code: ResponseCode.TOPIC_NOT_EXIST, body: Buffer.alloc(0), remark: 'topic not exist' }; },
    'ns-2:9876': () => { calls++; return okResponse(); },
  });
  const c = new MQClient('cid-tnr', 'ns-1:9876;ns-2:9876', rc as any);
  // 随机起点会先抽到 ns-2（成功路由）—— 显式粘住 ns-1，专测"可达 NS 的真实回答"
  c._namesrvChosen = 'ns-1:9876';
  const r = await c.updateTopicRouteInfoFromNameServer('MissingTopic');
  check('可达 NS 明确 TOPIC_NOT_EXIST → 返回 false', r === false);
  check('不再轮询后续 NS（NS 活着，这是真实回答）', calls === 1, `calls=${calls}`);
}

async function testRouteErrorTransparency(): Promise<void> {
  console.log('== producer No-route error carries the real cause ==');
  const p = new DefaultMQProducer('GID_RouteErr');
  (p as any).client = {
    getTopicPublishInfo: (_t: string) => null,
    updateTopicRouteInfoFromNameServer: async () => {
      const err: any = new Error('connect to ns failed: unable to verify the first certificate');
      err.name = 'RemotingConnectException';
      throw err;
    },
  };
  let caught: any = null;
  try {
    await p.send(new Message('NoRouteTopic', Buffer.from('hello')));
  } catch (e) {
    caught = e;
  }
  check('send 抛 No route info', caught != null && caught!.message.startsWith('No route info of this topic'),
    caught != null ? caught!.message : '');
  check('报错带路由失败原因（TLS 痕迹）',
    caught != null && caught!.message.includes('route fetch failed')
    && caught!.message.includes('unable to verify the first certificate'),
    caught != null ? caught!.message : '');
  check('cause 链保留原始异常', caught != null && caught!.cause != null
    && String(caught!.cause.message).includes('unable to verify'),
    caught != null && caught!.cause != null ? caught!.cause.message : '');
}

(async () => {
  await testFailoverRotation();
  await testAllDead();
  await testTopicNotExist();
  await testRouteErrorTransparency();
  console.log(`\n${fail === 0 ? `ALL GREEN (${pass} checks)` : `${fail} failure(s)`}`);
  process.exit(fail === 0 ? 0 : 1);
})();
