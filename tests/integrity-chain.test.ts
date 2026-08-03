import { makeFileReliability } from './helpers';
import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { AuditLogger } from '../src/logger';
import { BunSQLiteAdapter } from '../src/adapters/bun-sqlite';
import { INTEGRITY_GENESIS } from '../src/utils/crypto';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

test('integrityChain seals logs and verifyIntegrity detects tamper', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-int2-'));
  cleanupPaths.push(dataDir);

  const audit = new AuditLogger({
    namespace: 'int2',
    reliability: makeFileReliability('int2', dataDir),
    mode: 'durable',
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'audit.db') }),
    integrityChain: true,
    batching: { maxSize: 1, flushInterval: 50, maxQueueSize: 100, onQueueFull: 'dlq' },
  });
  await audit.ready;

  await audit.fireAsync('evt.a', {
    actorId: 'u1',
    tenantId: 't1',
    newValues: { n: 1 },
  });
  await audit.fireAsync('evt.b', {
    actorId: 'u1',
    tenantId: 't1',
    newValues: { n: 2 },
  });

  // Wait for flush
  await new Promise((r) => setTimeout(r, 200));

  const page = await audit.query({
    tenantId: 't1',
    pagination: { limit: 10 },
  });
  // newest first from SQLite
  const ordered = [...page.logs].reverse();
  expect(ordered.length).toBeGreaterThanOrEqual(2);
  const chain = ordered.slice(0, 2);
  // After reverse, chain[0] is oldest
  expect(chain[0]!.prevHash).toBe(INTEGRITY_GENESIS);
  expect(chain[0]!.contentHash).toBeTruthy();
  expect(chain[1]!.prevHash).toBe(chain[0]!.contentHash);

  const ok = await audit.verifyIntegrity(chain);
  expect(ok.ok).toBe(true);

  // Tamper payload
  const tampered = chain.map((l) => ({ ...l }));
  tampered[1] = { ...tampered[1]!, actorId: 'evil' };
  const bad = await audit.verifyIntegrity(tampered);
  expect(bad.ok).toBe(false);
  expect(bad.failedAt).toBe(1);

  await audit.shutdown();
});
