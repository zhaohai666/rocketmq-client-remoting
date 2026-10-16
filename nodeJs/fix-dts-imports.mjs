// Post-build step for the published dist/: TypeScript's
// rewriteRelativeImportExtensions rewrites relative `./x.ts` specifiers in the emitted
// **.js** files but leaves them untouched in the emitted **.d.ts** files. A .d.ts that
// imports `../common/message.ts` cannot be resolved by a consumer whose tsconfig does not
// allow .ts extensions, so every declaration file gets the same rewrite here.
//
// Scope is deliberately narrow: only relative specifiers ending in `.ts` (static
// `from '...'`, `export ... from '...'` and dynamic `import('...')`) are touched; `node:`
// built-ins and bare package specifiers never end in .ts.

import { readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const distDir = join(dirname(fileURLToPath(import.meta.url)), 'dist');

const FROM_RE = /(from\s*(['"]))(\.[^'"]*?)(\.ts)(\2)/g;
const DYNAMIC_RE = /(import\(\s*(['"]))(\.[^'"]*?)(\.ts)(\2)/g;

function walk(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) out.push(...walk(p));
    else if (p.endsWith('.d.ts')) out.push(p);
  }
  return out;
}

let fixed = 0;
for (const file of walk(distDir)) {
  const before = readFileSync(file, 'utf8');
  const after = before.replace(FROM_RE, '$1$3.js$5').replace(DYNAMIC_RE, '$1$3.js$5');
  if (after !== before) {
    writeFileSync(file, after);
    fixed++;
  }
}
console.log(`fix-dts-imports: rewrote .ts specifiers in ${fixed} declaration file(s)`);
if (fixed === 0) {
  // Nothing to fix is only OK when there are declaration files at all; an empty dist
  // means tsc did not run or emitted nothing, which must fail the build loudly.
  const total = walk(distDir).length;
  if (total === 0) {
    console.error('fix-dts-imports: no .d.ts files found under dist/ - did tsc run?');
    process.exit(1);
  }
}
