import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { BunSQLiteAdapter } from '../src/adapters/sqlite';
import { AuditLogger } from '../src/logger';
import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function makeLog(id: string, createdAt: string): LogbunLog {
  return {
    id,
    tenantId: 't1',
    actorId: 'actor-1',
    action: 'item.created',
    createdAt,
  };
}

test('maxQueryLimit clamps pagination limit via AuditLogger', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'logbun-query-limit-'));
  cleanupPaths.push(dir);

  const adapter = new BunSQLiteAdapter({ path: join(dir, 'audit.db') });
  await adapter.init();

  const logs: LogbunLog[] = [];
  for (let i = 1; i <= 10; i++) {
    const id = String(i).padStart(4, '0');
    logs.push(makeLog(id, `2026-01-${String(i).padStart(2, '0')}T00:00:00.000Z`));
  }
  expect(await adapter.bulkInsert('t1', logs)).toBe(true);

  const maxQueryLimit = 3;
  const audit = new AuditLogger({
    namespace: 'query-limit-ns',
    mode: 'volatile',
    adapter,
    dataDir: dir,
    maxQueryLimit,
  });

  await audit.ready;

  const result = await audit.query({
    tenantId: 't1',
    filters: {},
    pagination: { limit: 1_000 },
  });

  expect(result.logs.length).toBe(maxQueryLimit);

  const small = await audit.query({
    tenantId: 't1',
    filters: {},
    pagination: { limit: 1 },
  });
  expect(small.logs.length).toBe(1);

  await audit.shutdown();
});

test('default maxQueryLimit of 500 is passed to adapter.query', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'logbun-query-default-limit-'));
  cleanupPaths.push(dir);

  let seenLimit: number | undefined;
  const adapter: IAdapter = {
    async init() {},
    async bulkInsert() {
      return true;
    },
    async query(
      _tenantId: string | null,
      _filters: LogbunQueryFilters,
      pagination: { cursor?: string; limit: number },
    ): Promise<LogbunQueryResult> {
      seenLimit = pagination.limit;
      return { logs: [], nextCursor: null };
    },
    async prune() {},
    async close() {},
  };

  const audit = new AuditLogger({
    namespace: 'query-default-ns',
    mode: 'volatile',
    adapter,
    dataDir: dir,
  });

  await audit.ready;

  await audit.query({
    tenantId: 't1',
    pagination: { limit: 999_999 },
  });

  expect(seenLimit).toBe(500);

  await audit.shutdown();
});
