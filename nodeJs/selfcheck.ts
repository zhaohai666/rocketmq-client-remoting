// selfcheck — offline verification that every nodeJs module loads and the
// smoke suites pass, without a cluster. The live tools live in examples/.
//
//   node --experimental-strip-types selfcheck.ts
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const node = process.execPath;

// Every module in src/ must import cleanly (no console output expected).
const modules = [
  'src/remoting/remotingCommand.ts', 'src/remoting/serialize.ts', 'src/remoting/codes.ts',
  'src/remoting/headers.ts', 'src/remoting/bodies.ts', 'src/remoting/heartbeat.ts',
  'src/remoting/route.ts', 'src/remoting/namespace.ts', 'src/remoting/subscription.ts',
  'src/remoting/admin_body.ts', 'src/remoting/extra_info.ts', 'src/remoting/acl.ts',
  'src/remoting/rpc_hooks.ts',
  'src/remoting/client.ts', 'src/remoting/exception.ts',
  'src/common/message.ts', 'src/common/messageConst.ts', 'src/common/messageDecoder.ts',
  'src/common/messageClientIdSetter.ts', 'src/common/message_accessor.ts', 'src/common/messageType.ts',
  'src/common/mixAll.ts', 'src/common/sysflag.ts', 'src/common/utilAll.ts',
  'src/common/subscriptionData.ts', 'src/common/topic_config.ts', 'src/common/topic_validator.ts',
  'src/common/recall_message_handle.ts', 'src/common/boundary_type.ts', 'src/common/validators.ts',
  'src/common/compress.ts',
  'src/client/mq_client.ts', 'src/client/producer.ts', 'src/client/consumer.ts',
  'src/client/pull_consumer.ts', 'src/client/lite_pull_consumer.ts', 'src/client/admin.ts',
  'src/client/allocate.ts', 'src/client/process_queue.ts', 'src/client/offset_store.ts',
  'src/client/consumer_stats.ts', 'src/client/trace_context.ts', 'src/client/trace_dispatcher.ts',
  'src/client/trace_hook.ts', 'src/client/pull_api.ts', 'src/client/hook.ts',
  'src/client/latency.ts', 'src/client/backpressure.ts', 'src/client/produce_accumulator.ts',
  'src/client/traceparent.ts', 'src/client/request_reply.ts', 'src/client/top_addressing.ts', 'src/client/send_result.ts',
  'src/client/consumer_result.ts', 'src/client/exception.ts',
  'src/client/pop_process_queue.ts', 'src/client/pop_api.ts', 'src/client/pop_consumer.ts',
  'src/remoting/pop_bodies.ts',
];

let failures = 0;
console.log(`== module load check (${modules.length} modules) ==`);
for (const mod of modules) {
  const r = spawnSync(node, ['--experimental-strip-types', '--no-warnings', '-e',
    `import('./${mod}').then(() => console.log('OK'), (e) => { console.error(e.message); process.exit(1); })`],
    { cwd: here, encoding: 'utf8' });
  if (r.status === 0) console.log(`  OK   ${mod}`);
  else { failures++; console.log(`  FAIL ${mod}\n       ${(r.stderr || '').trim().split('\n')[0]}`); }
}

// The offline smoke suites.
const smokes = ['test/smoke.ts', 'test/producer_smoke.ts', 'test/consumer_smoke.ts', 'test/stats_smoke.ts',
  'test/java_gap_fill_smoke.ts', 'test/pop_smoke.ts', 'test/fixes2_smoke.ts', 'test/namespace_rpc_smoke.ts',
  'test/ns_failover_smoke.ts'];
console.log(`\n== smoke suites (${smokes.length}) ==`);
for (const smoke of smokes) {
  const r = spawnSync(node, ['--experimental-strip-types', '--no-warnings', path.join(here, smoke)],
    { cwd: here, encoding: 'utf8' });
  const tail = (r.stdout || '').trim().split('\n').slice(-3).join(' | ');
  if (r.status === 0) console.log(`  PASS ${smoke}  ${tail}`);
  else { failures++; console.log(`  FAIL ${smoke}  ${tail}\n${r.stdout || ''}${r.stderr || ''}`); }
}

console.log(`\n${failures === 0 ? 'ALL GREEN' : failures + ' failure(s)'}`);
process.exit(failures === 0 ? 0 : 1);
