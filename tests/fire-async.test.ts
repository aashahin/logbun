import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { BunSQLiteAdapter } from '../src/adapters/sqlite';
import { AuditLogger } from '../src/logger';
import type { IAdapter, LogbunEvent, LogbunQueryFilters, LogbunQueryResult } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function stubAdapter(onInsert?: () => void): IAdapter {
  return {
    async init() {},
    async bulkInsert() {
      onInsert?.();
      return true;
    },
    async query(
      _t: string | null,
      _f: LogbunQueryFilters,
      _p: { cursor?: string; limit: number },
    ): Promise<LogbunQueryResult> {
      return { logs: [], nextCursor: null };
    },
    async prune() {},
    async close() {},
  };
}

/**
 * F7: fireAsync awaits enqueue; getStats works.
 */
test('fireAsync awaits enqueue and getStats returns shape', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-fire-async-'));
  cleanupPaths.push(dataDir);

  const events: LogbunEvent[] = [];
  const audit = new AuditLogger({
    namespace: 'fire-async',
    mode: 'volatile',
    adapter: stubAdapter(),
    dataDir,
    batching: { maxSize: 100, flushInterval: 60_000, maxQueueSize: 100 },
    onEvent: (e) => events.push(e),
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1, initialDelayMs: 60_000 },
  });

  await audit.ready;

  await audit.fireAsync('async.act', {
    actorId: 'actor-1',
    tenantId: 't1',
    entityId: 'e1',
  });

  // After fireAsync resolves, enqueue path has completed
  expect(events.some((e) => e.type === 'enqueue')).toBe(true);

  const stats = audit.getStats();
  expect(typeof stats.queued).toBe('number');
  expect(typeof stats.tenants).toBe('number');
  expect(typeof stats.degraded).toBe('boolean');
  expect(typeof stats.recoveryBacklog).toBe('number');
  expect(stats.degraded).toBe(false);
  expect(stats.queued).toBeGreaterThanOrEqual(1);
  expect(stats.tenants).toBeGreaterThanOrEqual(1);

  await audit.shutdown();

  // After shutdown engine is gone — getStats should still be safe
  const after = audit.getStats();
  expect(after.queued).toBe(0);
  expect(typeof after.degraded).toBe('boolean');
});

test('fireAsync with durable mode awaits WAL path', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-fire-async-dur-'));
  cleanupPaths.push(dataDir);

  const events: LogbunEvent[] = [];
  const audit = new AuditLogger({
    namespace: 'fire-async-dur',
    mode: 'durable',
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
    dataDir,
    wal: { fsync: false },
    batching: { maxSize: 100, flushInterval: 60_000 },
    onEvent: (e) => events.push(e),
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1, initialDelayMs: 60_000 },
  });

  await audit.ready;

  await audit.fireAsync('dur.act', {
    actorId: 'a',
    tenantId: 't-dur',
    entityId: 'e-dur',
  });

  expect(events.some((e) => e.type === 'enqueue')).toBe(true);
  expect(audit.getStats().degraded).toBe(false);

  await audit.shutdown();
});

test('fireAsync_rejects_when_requireTenantId_and_tenant_missing_while_fire_does_not_throw', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-fire-async-throw-'));
  cleanupPaths.push(dataDir);

  const audit = new AuditLogger({
    namespace: 'fire-async-throw',
    mode: 'volatile',
    adapter: stubAdapter(),
    dataDir,
    requireTenantId: true,
    batching: { maxSize: 10, flushInterval: 60_000 },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1, initialDelayMs: 60_000 },
  });

  await audit.ready;

  expect(() => audit.fire('x.y', { actorId: 'a' })).not.toThrow();
  await expect(audit.fireAsync('x.y', { actorId: 'a' })).rejects.toThrow(
    /tenantId/,
  );

  await audit.shutdown();
});
