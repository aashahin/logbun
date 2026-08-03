import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { BunSQLiteAdapter } from '../src/adapters/bun-sqlite';
import type { LogbunLog } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function makeLog(
  id: string,
  createdAt: string,
  tenantId = 'tenant-1',
): LogbunLog {
  return {
    id,
    tenantId,
    actorId: 'actor-1',
    action: 'prune.test',
    createdAt,
  };
}

/**
 * F6: insert many old rows, prune, ensure deleted (batched DELETE with LIMIT loop).
 */
test('sqlite prune deletes old rows and keeps recent ones', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'logbun-sqlite-prune-'));
  cleanupPaths.push(dir);

  const adapter = new BunSQLiteAdapter({ path: join(dir, 'audit.db') });
  await adapter.init();

  const now = Date.now();
  const oldIso = new Date(now - 30 * 24 * 60 * 60 * 1000).toISOString(); // 30d ago
  const recentIso = new Date(now - 1 * 24 * 60 * 60 * 1000).toISOString(); // 1d ago

  // Many old rows to exercise batched DELETE LIMIT loop
  const oldLogs: LogbunLog[] = Array.from({ length: 250 }, (_, i) =>
    makeLog(`old-${String(i).padStart(4, '0')}`, oldIso),
  );
  const recentLogs: LogbunLog[] = [
    makeLog('new-0001', recentIso),
    makeLog('new-0002', recentIso),
  ];

  expect(await adapter.bulkInsert('tenant-1', oldLogs)).toBe(true);
  expect(await adapter.bulkInsert('tenant-1', recentLogs)).toBe(true);

  // Retention: 7 days — old (30d) should go, recent (1d) stay
  await adapter.prune(7);

  // Page through remaining logs
  const page = await adapter.query('tenant-1', {}, { limit: 500 });
  const remainingIds = page.logs.map((l) => l.id).sort();

  expect(remainingIds).toEqual(['new-0001', 'new-0002']);
  expect(page.logs.every((l) => !l.id.startsWith('old-'))).toBe(true);

  await adapter.close();
});

test('sqlite prune is a no-op when nothing is expired', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'logbun-sqlite-prune-noop-'));
  cleanupPaths.push(dir);

  const adapter = new BunSQLiteAdapter({ path: join(dir, 'audit.db') });
  await adapter.init();

  const recent = new Date().toISOString();
  await adapter.bulkInsert('tenant-1', [
    makeLog('keep-1', recent),
    makeLog('keep-2', recent),
  ]);

  // 365-day retention — nothing is old enough
  await adapter.prune(365);

  const page = await adapter.query('tenant-1', {}, { limit: 10 });
  expect(page.logs.map((l) => l.id).sort()).toEqual(['keep-1', 'keep-2']);

  await adapter.close();
});
