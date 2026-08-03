import { expect, test } from 'bun:test';
import { readFileSync, existsSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';

/**
 * Source-level guard: root entry modules must not import node:/bun:/cloudflare:
 * or reference Bun/Deno/process. (Built artifact checked by scripts/assert-root-runtime.mjs)
 */
const ROOT_FILES = [
  'src/index.ts',
  'src/logger.ts',
  'src/bootstrap.ts',
  'src/types.ts',
  'src/events.ts',
  'src/engine/batcher.ts',
  'src/engine/retry.ts',
  'src/engine/pool.ts',
  'src/reliability/memory.ts',
  'src/reliability/types.ts',
  'src/utils/uuidv7.ts',
  'src/utils/crypto.ts',
  'src/utils/namespace.ts',
  'src/utils/tenant.ts',
  'src/utils/json.ts',
  'src/utils/client-ip.ts',
  'src/plugins/hono.ts',
  'src/plugins/elysia.ts',
];

const FORBIDDEN = [
  /from\s+['"]node:/,
  /from\s+['"]bun:/,
  /from\s+['"]cloudflare:/,
  /\bBun\./,
  /\bDeno\./,
  // Node process global — not the English word "process" in comments
  /\bprocess\.(pid|env|cwd|exit|kill|uptime|versions)\b/,
];

test('root source graph has no forbidden runtime imports', () => {
  const hits: string[] = [];
  for (const f of ROOT_FILES) {
    if (!existsSync(f)) {
      hits.push(`missing ${f}`);
      continue;
    }
    // Strip line comments for word-process false positives
    const src = readFileSync(f, 'utf8')
      .split('\n')
      .map((l) => l.replace(/\/\/.*$/, ''))
      .join('\n');
    for (const re of FORBIDDEN) {
      if (re.test(src)) hits.push(`${f} matches ${re}`);
    }
  }
  expect(hits).toEqual([]);
});
