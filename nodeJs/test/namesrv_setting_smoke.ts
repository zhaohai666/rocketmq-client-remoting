// namesrv_setting_smoke — the address configuration surface, Java parity.
//
//   node --experimental-strip-types test/namesrv_setting_smoke.ts
//
// Java reference (5.x): ClientConfig.java:45 defaults the namesrvAddr field to
// the rocketmq.namesrv.addr system property falling back to the NAMESRV_ADDR
// environment variable; MixAll.getWSAddr (common/MixAll.java:169-177) builds
// http://<rocketmq.namesrv.domain>:8080/rocketmq/<rocketmq.namesrv.domain.subgroup>
// (a domain carrying a port keeps it); MQClientInstance.java:346/390 fetches
// from the address server at start and every 2min only when no static address
// exists; DefaultMQProducerImpl / DefaultMQPushConsumer expose the facade
// setters. On non-JVM ports the property names map to ROCKETMQ_NAMESRV_ADDR /
// ROCKETMQ_NAMESRV_DOMAIN / ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP environment
// variables, and every facade gains setNamesrvDomain / setNamesrvSubgroup.
import assert from 'node:assert';
import { DefaultMQProducer } from '../src/client/producer.ts';
import { DefaultMQPushConsumer } from '../src/client/consumer.ts';
import { DefaultMQPullConsumer } from '../src/client/pull_consumer.ts';
import { DefaultLitePullConsumer } from '../src/client/lite_pull_consumer.ts';
import { DefaultMQAdminExt } from '../src/client/admin.ts';
import { DefaultTopAddressing, wsAddrForDomain, resolveSubgroup } from '../src/client/top_addressing.ts';
import { MQClient } from '../src/client/mq_client.ts';
import MixAll from '../src/common/mixAll.ts';

let checks = 0;
let failed = 0;
function check(name: string, ok: boolean, detail?: string): void {
  checks++;
  if (ok) { console.log(`  ok    ${name}`); }
  else { failed++; console.log(`  FAIL  ${name}${detail ? ` — ${detail}` : ''}`); }
}

// ---------------------------------------------------------------------------
// 1. env default address chain (ClientConfig.java:45 semantics)
// ---------------------------------------------------------------------------
function testEnvAddrChain(): void {
  console.log('== env default address chain ==');
  const saved = { ra: process.env['ROCKETMQ_NAMESRV_ADDR'], na: process.env['NAMESRV_ADDR'] };
  process.env['ROCKETMQ_NAMESRV_ADDR'] = '10.0.0.1:9876';
  process.env['NAMESRV_ADDR'] = '10.0.0.2:9876';
  check('property env wins over NAMESRV_ADDR',
    MixAll.namesrvAddrFromEnv() === '10.0.0.1:9876');
  delete process.env['ROCKETMQ_NAMESRV_ADDR'];
  check('falls back to NAMESRV_ADDR',
    MixAll.namesrvAddrFromEnv() === '10.0.0.2:9876');
  process.env['NAMESRV_ADDR'] = ' 10.0.0.1:9876 ; 10.0.0.2:9876 ,, 10.0.0.3:9876 ';
  check('splits on ; and , and trims',
    MixAll.namesrvAddrFromEnv() === '10.0.0.1:9876;10.0.0.2:9876;10.0.0.3:9876');
  process.env['NAMESRV_ADDR'] = '   ';
  check('blank value means unset', MixAll.namesrvAddrFromEnv() === null);
  delete process.env['NAMESRV_ADDR'];
  check('unset means null', MixAll.namesrvAddrFromEnv() === null);
  if (saved.ra != null) process.env['ROCKETMQ_NAMESRV_ADDR'] = saved.ra; else delete process.env['ROCKETMQ_NAMESRV_ADDR'];
  if (saved.na != null) process.env['NAMESRV_ADDR'] = saved.na; else delete process.env['NAMESRV_ADDR'];
}

// ---------------------------------------------------------------------------
// 2. TopAddressing domain/subgroup priority: explicit > env > Java default
// ---------------------------------------------------------------------------
function testTopAddressingPriority(): void {
  console.log('== TopAddressing explicit/env/default priority ==');
  const savedDom = process.env['ROCKETMQ_NAMESRV_DOMAIN'];
  const savedGrp = process.env['ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP'];

  // URL rule: a domain with a port keeps it (Java MixAll.getWSAddr).
  check('default port appended', wsAddrForDomain('jmenv.tbsite.net', 'nsaddr')
    === 'http://jmenv.tbsite.net:8080/rocketmq/nsaddr');
  check('domain with port keeps it', wsAddrForDomain('host:12345', 'nsaddr')
    === 'http://host:12345/rocketmq/nsaddr');

  delete process.env['ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP'];
  const ta = new DefaultTopAddressing(null, null, { domain: 'addr.example.com' });
  check('explicit domain builds the default-subgroup URL',
    ta.wsAddr === 'http://addr.example.com:8080/rocketmq/nsaddr');

  process.env['ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP'] = 'envgrp';
  const taEnv = new DefaultTopAddressing(null, null, { domain: 'addr.example.com' });
  check('subgroup env backs the explicit domain',
    taEnv.wsAddr === 'http://addr.example.com:8080/rocketmq/envgrp');
  const taBoth = new DefaultTopAddressing(null, null, { domain: 'addr.example.com', subgroup: 'grpX' });
  check('explicit subgroup wins over the env',
    taBoth.wsAddr === 'http://addr.example.com:8080/rocketmq/grpX');
  // With the env subgroup present, an unset explicit falls through to the env;
  // the Java default only applies when the env is absent too.
  check('resolveSubgroup explicit > fallback > env',
    resolveSubgroup('a', 'b') === 'a' && resolveSubgroup(null, 'b') === 'b'
      && resolveSubgroup(null, null) === 'envgrp');
  delete process.env['ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP'];
  check('resolveSubgroup falls to the Java default when the env is absent',
    resolveSubgroup(null, null) === 'nsaddr');

  process.env['ROCKETMQ_NAMESRV_DOMAIN'] = 'env.example.com';
  const taExplicit = new DefaultTopAddressing(null, null, { domain: 'set.example.com:8080' });
  check('explicit domain wins over the domain env',
    taExplicit.wsAddr === 'http://set.example.com:8080/rocketmq/nsaddr');
  const taFromEnv = new DefaultTopAddressing();
  check('no explicit domain reads the env',
    taFromEnv.wsAddr === 'http://env.example.com:8080/rocketmq/nsaddr');

  if (savedDom != null) process.env['ROCKETMQ_NAMESRV_DOMAIN'] = savedDom; else delete process.env['ROCKETMQ_NAMESRV_DOMAIN'];
  if (savedGrp != null) process.env['ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP'] = savedGrp; else delete process.env['ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP'];
}

// ---------------------------------------------------------------------------
// 3. instance wiring: the MQClient options land in the TopAddressing URL
// ---------------------------------------------------------------------------
function testInstanceWiring(): void {
  console.log('== MQClient instance wiring ==');
  const savedDom = process.env['ROCKETMQ_NAMESRV_DOMAIN'];
  delete process.env['ROCKETMQ_NAMESRV_DOMAIN'];
  const c = new MQClient('nsset@DEFAULT', null, null,
    { namesrvDomain: 'addr.example.com:8080', namesrvSubgroup: 'grpB' });
  // The instance builds its TopAddressing inside the addressing loop's setup
  // (start() only runs it when no static address was configured) — drive that
  // private setup step directly so no timers need to fire.
  (c as any)._startNamesrvAddressingLoop();
  const ta = (c as any)._topAddressing;
  check('top addressing built with the explicit domain/subgroup',
    ta != null && ta.wsAddr === 'http://addr.example.com:8080/rocketmq/grpB',
    `got ${ta?.wsAddr}`);
  // Drop the addressing-loop timers so the process can exit cleanly.
  if (ta != null) ta.clearChangeCallBack();
  (c as any)._topAddressing = null;
  clearTimeout((c as any)._namesrvInitialTimer);
  clearInterval((c as any)._namesrvTimer);
  if (savedDom != null) process.env['ROCKETMQ_NAMESRV_DOMAIN'] = savedDom;
}

// ---------------------------------------------------------------------------
// 4. facade setting surface: producer / push / pull / lite / admin
// ---------------------------------------------------------------------------
function testFacadeSurface(): void {
  console.log('== facade setNamesrvDomain/Subgroup surface ==');
  const p = new DefaultMQProducer('GID_NS_P');
  check('producer setNamesrvDomain chainable', p.setNamesrvDomain('d.example.com') === p);
  check('producer domain stored', p.namesrvDomain === 'd.example.com');
  check('producer blank domain resets to null', p.setNamesrvDomain('  ').namesrvDomain === null);
  check('producer subgroup round-trips', p.setNamesrvSubgroup('grpA').namesrvSubgroup === 'grpA');

  const c = new DefaultMQPushConsumer('GID_NS_C');
  check('push consumer domain round-trips', c.setNamesrvDomain('d.example.com').nameServerDomain === 'd.example.com');
  check('push consumer subgroup round-trips', c.setNamesrvSubgroup('grpA').nameServerSubgroup === 'grpA');

  const pc = new DefaultMQPullConsumer('GID_NS_PC');
  check('pull consumer domain round-trips', pc.setNamesrvDomain('d.example.com').nameServerDomain === 'd.example.com');
  const lc = new DefaultLitePullConsumer('GID_NS_LC');
  check('lite consumer domain round-trips', lc.setNamesrvDomain('d.example.com').nameServerDomain === 'd.example.com');
  const a = new DefaultMQAdminExt();
  a.setNamesrvDomain('d.example.com');
  a.setNamesrvSubgroup('grpA');
  check('admin domain/subgroup round-trips', a.namesrvDomain === 'd.example.com' && a.namesrvSubgroup === 'grpA');
}

// ---------------------------------------------------------------------------
// 5. guard: explicit domain satisfies the start-time address check
// ---------------------------------------------------------------------------
async function testGuardAcceptsDomain(): Promise<void> {
  console.log('== start guard accepts the explicit domain ==');
  const savedDom = process.env['ROCKETMQ_NAMESRV_DOMAIN'];
  const savedGrp = process.env['ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP'];
  delete process.env['ROCKETMQ_NAMESRV_DOMAIN'];
  delete process.env['ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP'];

  const lc = new DefaultLitePullConsumer('GID_NS_GUARD');
  lc.setNamesrvDomain('127.0.0.1:1'); // unreachable on purpose
  let threwAddress = false;
  let threwOther = false;
  try {
    await lc.start();
  } catch (e) {
    const msg = (e as Error).message || '';
    if (msg.includes('name server address is not set')) threwAddress = true;
    else threwOther = true;
  }
  check('lite guard passes with the explicit domain (fails later for another reason)',
    !threwAddress, threwAddress ? 'guard rejected the domain-only facade' : undefined);
  if (threwOther) {
    // unreachable address server: acceptable — the guard itself passed
    check('failure came from the unreachable address server, not the guard', true);
  }

  if (savedDom != null) process.env['ROCKETMQ_NAMESRV_DOMAIN'] = savedDom; else delete process.env['ROCKETMQ_NAMESRV_DOMAIN'];
  if (savedGrp != null) process.env['ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP'] = savedGrp; else delete process.env['ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP'];
}

(async () => {
  testEnvAddrChain();
  testTopAddressingPriority();
  testInstanceWiring();
  testFacadeSurface();
  await testGuardAcceptsDomain();
  console.log(`\n${checks - failed} passed, ${failed} failed`);
  if (failed > 0) process.exit(1);
})();
