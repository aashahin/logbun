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

function createStubAdapter(): IAdapter {
  return {
    async init() {},
    async bulkInsert() {
      return true;
    },
    async query(
      _tenantId: string | null,
      _filters: LogbunQueryFilters,
      _pagination: { cursor?: string; limit: number },
    ): Promise<LogbunQueryResult> {
      return { logs: [], nextCursor: null };
    },
    async prune() {},
    async close() {},
  };
}

test('query without tenantId throws when requireTenantId is true', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'logbun-tenant-req-'));
  cleanupPaths.push(dir);

  const adapter = new BunSQLiteAdapter({ path: join(dir, 'audit.db') });
  const audit = new AuditLogger({
    namespace: 'tenant-req',
    mode: 'volatile',
    adapter,
    dataDir: dir,
    requireTenantId: true,
  });

  await audit.ready;

  await expect(audit.query({ filters: {} })).rejects.toThrow();
  await expect(audit.query({ pagination: { limit: 10 } })).rejects.toThrow();

  // With tenantId present, query should succeed
  const result = await audit.query({ tenantId: 't1', filters: {} });
  expect(result.logs).toEqual([]);

  await audit.shutdown();
});

test('database_per_tenant query without tenantId throws', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'logbun-dpt-'));
  cleanupPaths.push(dir);

  const baseAdapter = createStubAdapter();
  const audit = new AuditLogger({
    namespace: 'dpt-ns',
    mode: 'volatile',
    adapter: baseAdapter,
    dataDir: dir,
    tenancy: {
      mode: 'database_per_tenant',
      resolveConnection: async (tenantId: string) => ({
        path: join(dir, `${tenantId}.db`),
      }),
    },
    adapterFactory: async (config) => {
      const a = new BunSQLiteAdapter({ path: String(config['path'] ?? join(dir, 'fallback.db')) });
      await a.init();
      return a;
    },
  });

  await audit.ready;

  await expect(audit.query({ filters: {} })).rejects.toThrow();
  await expect(audit.query({ pagination: { limit: 5 } })).rejects.toThrow();

  await audit.shutdown();
});

test('requireTenantId false allows query without tenantId (single_database)', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'logbun-tenant-opt-'));
  cleanupPaths.push(dir);

  const adapter = new BunSQLiteAdapter({ path: join(dir, 'audit.db') });
  const audit = new AuditLogger({
    namespace: 'tenant-opt',
    mode: 'volatile',
    adapter,
    dataDir: dir,
    requireTenantId: false,
  });

  await audit.ready;

  const log: LogbunLog = {
    id: '0001',
    actorId: 'a1',
    action: 'x.created',
    createdAt: '2026-01-01T00:00:00.000Z',
  };
  await adapter.init();
  await adapter.bulkInsert(null, [log]);

  const result = await audit.query({ filters: {}, pagination: { limit: 10 } });
  expect(result.logs.map((l) => l.id)).toContain('0001');

  await audit.shutdown();
});
