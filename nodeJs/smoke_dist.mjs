// smoke_dist guards the published package: it loads every module under dist/ and
// starts each facade, so a build that type-checks away but throws at runtime
// cannot ship. src/ runs under strip-types and never sees these failures.
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const distDir = path.join(here, 'dist');
// Nothing reachable: connects fail fast with ECONNREFUSED, so a broken start()
// still shows up as its own error instead of a network timeout.
const DEAD_NS = '127.0.0.1:1';
const asUrl = (p) => `file://${p}`;

let pass = 0, fail = 0;
function check(name, ok, detail = '') {
  if (ok) { pass++; console.log(`PASS  ${name}`); }
  else { fail++; console.log(`FAIL  ${name}  ${detail}`); }
}
// The failure class this smoke exists for: tsc --noCheck happily emits a module
// whose value import was dropped, so the first call throws
// "ReferenceError: X is not defined" / "TypeError: X is not a function".
function isWiringError(e) {
  const m = String((e && e.message) || e);
  return /is not defined/.test(m) || /is not a function/.test(m);
}
const withDeadline = (p, ms) => Promise.race([
  Promise.resolve(p).then((v) => ({ ok: true, v })),
  new Promise((res) => setTimeout(
    () => res({ ok: false, e: new Error(`deadline ${ms}ms exceeded`) }), ms)),
]);

async function main() {
  if (!fs.existsSync(distDir)) {
    console.log('FAIL  dist/ missing — run npm run build first');
    process.exit(1);
  }
  const files = [];
  (function walk(dir) {
    for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
      const p = path.join(dir, ent.name);
      if (ent.isDirectory()) walk(p);
      else if (ent.name.endsWith('.js')) files.push(p);
    }
  })(distDir);
  check(`dist/ has ${files.length} modules`, files.length > 0);

  let loadFails = [];
  for (const f of files) {
    try { await import(asUrl(f)); }
    catch (e) { loadFails.push(`${path.relative(distDir, f)}: ${e.message}`); }
  }
  check('every dist module imports', loadFails.length === 0, loadFails.join(' | '));

  const mod = await import(asUrl(path.join(distDir, 'index.js')));
  const { DefaultMQProducer, DefaultMQPullConsumer, DefaultLitePullConsumer,
    DefaultMQPushConsumer, Message, MessageModel } = mod;
  check('dist/index.js exports the facades',
    [DefaultMQProducer, DefaultMQPullConsumer, DefaultLitePullConsumer,
      DefaultMQPushConsumer, Message].every((x) => typeof x === 'function'),
    JSON.stringify(Object.keys(mod).slice(0, 8)));

  const started = [];
  async function startAndStop(name, build) {
    let inst = null;
    try {
      inst = build();
      const r = await withDeadline(inst.start(), 20000);
      check(name, r.ok, r.ok ? '' : String(r.e && r.e.message));
      if (r.ok) started.push(inst);
    } catch (e) {
      check(name, false, `${e.constructor ? e.constructor.name : 'Error'}: ${e.message}`);
      if (isWiringError(e)) {
        console.log('FAIL  dist wiring broken — the published package would crash');
      }
    }
  }

  await startAndStop('dist producer starts', () => {
    const p = new DefaultMQProducer('GID_SmokeDist_P'); p.setNamesrvAddr(DEAD_NS); return p;
  });
  await startAndStop('dist pull consumer starts without subscribe', () => {
    const c = new DefaultMQPullConsumer('GID_SmokeDist_Pull'); c.setNamesrvAddr(DEAD_NS); return c;
  });
  await startAndStop('dist pull consumer starts (BROADCASTING)', () => {
    const c = new DefaultMQPullConsumer('GID_SmokeDist_Broad');
    c.setNamesrvAddr(DEAD_NS); c.setMessageModel(MessageModel.BROADCASTING); return c;
  });
  await startAndStop('dist lite pull consumer starts', () => {
    const c = new DefaultLitePullConsumer('GID_SmokeDist_Lite');
    c.setNamesrvAddr(DEAD_NS); c.subscribe('SmokeDistTopic', '*'); return c;
  });
  await startAndStop('dist push consumer starts', () => {
    const c = new DefaultMQPushConsumer('GID_SmokeDist_Push');
    c.setNamesrvAddr(DEAD_NS);
    c.subscribe('SmokeDistTopic', '*');
    c.registerMessageListener(() => 0);
    return c;
  });

  // Offset bookkeeping touches the store implementations shipped in dist.
  try {
    const c = started.find((x) => x && typeof x.fetchMessageQueuesInBalance === 'function');
    if (c) {
      const q = new (mod.MessageQueue)('SmokeDistTopic', 'broker-a', 0);
      const r = await withDeadline(c.fetchConsumeOffset(q, true), 20000);
      check('dist pull consumer reads an unknown offset as -1', r.ok && r.v === -1,
        r.ok ? `got=${r.v}` : String(r.e && r.e.message));
    }
  } catch (e) {
    check('dist pull consumer reads an unknown offset as -1', false, e.message);
  }

  const rchild = timeoutCallbackChild(distDir);
  check('dist survives a timeout whose callback throws',
    rchild.status === 0 && (rchild.stdout || '').includes('SURVIVED'), childTail(rchild));

  for (const inst of started) {
    try { await withDeadline(inst.shutdown(), 10000); } catch (e) { /* best effort */ }
  }

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail > 0 ? 1 : 0);
}

// A throw from a callback invoked on Node's timer stack surfaces as an
// uncaughtException and ends the process, so this runs in a child: exit 0 plus
// SURVIVED is the only pass.
function timeoutCallbackChild(distDir) {
  const script = `
import net from 'node:net';
const { RemotingClient } = await import(${JSON.stringify(asUrl(path.join(distDir, 'remoting/client.js')))});
const { RemotingCommand } = await import(${JSON.stringify(asUrl(path.join(distDir, 'remoting/remotingCommand.js')))});
const sink = net.createServer(() => {});            // accepts, never answers
await new Promise((r) => sink.listen(0, '127.0.0.1', r));
const client = new RemotingClient({ connectTimeoutMillis: 2000, invokeTimeoutMillis: 500 });
client.invokeAsync('127.0.0.1:' + sink.address().port, RemotingCommand.createRequestCommand(325, null),
  () => { throw new Error('caller callback throws on purpose'); }, 500);
await new Promise((r) => setTimeout(r, 2500));
sink.close();
console.log('SURVIVED');
process.exit(0);
`;
  return spawnSync(process.execPath, ['--input-type=module', '-e', script],
    { encoding: 'utf8', timeout: 30000 });
}

function childTail(r) {
  const out = ((r.stdout || '') + ' ' + (r.stderr || '')).trim().replace(/\s+/g, ' ');
  return `exit=${r.status} ${out.slice(0, 240)}`;
}

main().then(null, (e) => { console.error(e); process.exit(1); });
