// -*- coding: utf-8 -*-
// NS failover + route-error transparency smoke (no network).
//
//   node --experimental-strip-types test/ns_failover_smoke.ts
//
// 1. One of several NS down: route fetching rotates over the live NS, and
//    repeated calls no longer "fail on the first dead pick" (the old
//    implementation picked purely at random, so with 1 of 3 down about 1/3 of
//    calls failed outright; in a new process the first route fetch dying showed
//    up as no route info).
// 2. Stickiness: an NS that answered is reused preferentially (sticky selection
//    semantics), so a sticky next hop costs exactly 1 network call.
// 3. The sticky one dies: automatically rotate to the next one and re-stick.
// 4. All NS unreachable: throw the last real error (connection refused / TLS
//    handshake, ...), do not swallow it.
// 5. NS reachable but definitively TOPIC_NOT_EXIST: return false, do not throw
//    and do not poll the remaining NS.
// 6. The producer's No-route error carries the route failure cause (TLS/CA
//    traces) and the cause chain is preserved — no longer a black-box
//    "No route info of this topic".
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
  // 30 consecutive route fetches: the old implementation picked one at random, so
  // hitting ns-a threw for the whole call — the new implementation must succeed
  // every time.
  for (let i = 0; i < 30; i++) {
    await c.updateTopicRouteInfoFromNameServer('FailoverTopic');
  }
  check('all 30 route fetches succeed (1 of 3 down)', invocations >= 30);
  check('the sticky NS is a live one', c._namesrvChosen !== 'ns-a:9876' && c._namesrvChosen != null,
    `chosen=${c._namesrvChosen}`);

  // Stickiness: once chosen has answered, the next call starts from it and
  // succeeds immediately — exactly 1 network call.
  const before = invocations;
  await c.updateTopicRouteInfoFromNameServer('FailoverTopic');
  check('the sticky NS is hit directly (single call)', invocations === before + 1,
    `invocations ${before} -> ${invocations}`);

  // The sticky one dies: drop the stickiness and rotate to the next one.
  const prevChosen = c._namesrvChosen!;
  rc.invokeSync = async (addr: string) => {
    if (addr === prevChosen) throw deadErr(addr);
    return okResponse();
  };
  await c.updateTopicRouteInfoFromNameServer('FailoverTopic');
  check('after the sticky NS dies it rotates to the next one', c._namesrvChosen != null && c._namesrvChosen !== prevChosen,
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
  // The starting point is random: pin ns-x explicitly so the assertion order is
  // deterministic (x -> y)
  c._namesrvChosen = 'ns-x:9876';
  let caught: Error | null = null;
  try {
    await c.updateTopicRouteInfoFromNameServer('SomeTopic');
  } catch (e) {
    caught = e as Error;
  }
  check('throws when every NS is unreachable', caught != null);
  check('the error thrown is the real error from the last NS tried',
    caught != null && caught!.message.includes('ns-y:9876'),
    caught != null ? caught!.message : '');
  check('both NS were tried (rotating in list order)',
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
  // A random start could draw ns-2 first (successful route) — pin ns-1 explicitly
  // to test purely "a real answer from a reachable NS"
  c._namesrvChosen = 'ns-1:9876';
  const r = await c.updateTopicRouteInfoFromNameServer('MissingTopic');
  check('the reachable NS clearly reports TOPIC_NOT_EXIST -> returns false', r === false);
  check('does not poll further NS (the NS is alive; this is a real answer)', calls === 1, `calls=${calls}`);
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
  check('send throws No route info', caught != null && caught!.message.startsWith('No route info of this topic'),
    caught != null ? caught!.message : '');
  check('the error carries the route-fetch failure reason (TLS traces)',
    caught != null && caught!.message.includes('route fetch failed')
    && caught!.message.includes('unable to verify the first certificate'),
    caught != null ? caught!.message : '');
  check('the cause chain preserves the original exception', caught != null && caught!.cause != null
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
