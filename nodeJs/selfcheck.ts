// selfcheck — offline verification that every nodeJs module loads and the
// smoke suites pass, without a cluster. The live tools live in examples/.
//
//   node --experimental-strip-types selfcheck.ts
import { spawnSync } from 'node:child_process';
import { readdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const node = process.execPath;

// Every module in src/ must import cleanly (no console output expected). The set comes from the
// tree, so a module added without ever being loaded here cannot slip past this gate.
const modules = readdirSync(path.join(here, 'src'), { recursive: true })
  .map(String)
  .filter((f) => f.endsWith('.ts'))
  .sort()
  .map((f) => `src/${f}`);

let failures = 0;
console.log(`== module load check (${modules.length} modules) ==`);
for (const mod of modules) {
  const r = spawnSync(node, ['--experimental-strip-types', '--no-warnings', '-e',
    `import('./${mod}').then(() => console.log('OK'), (e) => { console.error(e.message); process.exit(1); })`],
    { cwd: here, encoding: 'utf8' });
  if (r.status === 0) console.log(`  OK   ${mod}`);
  else { failures++; console.log(`  FAIL ${mod}\n       ${(r.stderr || '').trim().split('\n')[0]}`); }
}

// The offline smoke suites. Every test/*_smoke.ts is a suite, so the directory — not a
// hand-kept list that silently falls behind — decides what gets run before a commit.
const smokes = readdirSync(path.join(here, 'test'))
  .filter((f) => f.endsWith('smoke.ts'))
  .sort()
  .map((f) => `test/${f}`);
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
