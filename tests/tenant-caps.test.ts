import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import { AuditLogger } from '../src/logger';
import { DLQStorage } from '../src/storage/dlq';
import type {
  IAdapter,
  LogbunEvent,
  LogbunLog,
  LogbunQueryFilters,
  LogbunQueryResult,
} from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function stubAdapter(): IAdapter {
  return {
    async init() {},
    async bulkInsert() {
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

function makeLog(id: string, tenantId: string): LogbunLog {
  return {
    id,
    tenantId,
    actorId: 'a1',
    action: 'cap.test',
    createdAt: new Date().toISOString(),
  };
}

/**
 * F5: maxActiveTenants drops / emits limit event for new tenant keys beyond cap.
 */
test('maxActiveTenants rejects or drops new tenant keys beyond cap', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-tenant-caps-'));
  cleanupPaths.push(dataDir);

  const events: LogbunEvent[] = [];
  const adapter = stubAdapter();
  const pool = new ConnectionPool(adapter, 5);
  const dlq = new DLQStorage('tenant-caps', dataDir);
  await dlq.init();

  const batcher = new Batcher({
    adapter,
    pool,
    wal: null,
    dlq,
    mode: 'volatile',
    batching: {
      maxSize: 100,
      flushInterval: 60_000,
      maxQueueSize: 100,
      onQueueFull: 'drop',
    },
    maxActiveTenants: 2,
    onEvent: (e) => events.push(e),
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  const ok1 = await batcher.enqueue(makeLog('l1', 'tenant-1'));
  const ok2 = await batcher.enqueue(makeLog('l2', 'tenant-2'));
  expect(ok1).toBe(true);
  expect(ok2).toBe(true);

  // Third distinct tenant key should hit the cap
  const ok3 = await batcher.enqueue(makeLog('l3', 'tenant-3'));
  expect(ok3).toBe(false);

  expect(
    events.some(
      (e) => e.type === 'drop' && e.detail === 'max_active_tenants',
    ),
  ).toBe(true);

  const stats = batcher.getStats();
  expect(stats.tenants).toBeLessThanOrEqual(2);
});

test('AuditLogger maxActiveTenants=1 drops second tenant key', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-tenant-caps-1-'));
  cleanupPaths.push(dataDir);

  const events: LogbunEvent[] = [];
  const audit = new AuditLogger({
    namespace: 'caps-one',
    mode: 'volatile',
    adapter: stubAdapter(),
    dataDir,
    maxActiveTenants: 1,
    batching: { maxSize: 50, flushInterval: 60_000 },
    onEvent: (e) => events.push(e),
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1, initialDelayMs: 60_000 },
  });
  await audit.ready;

  await audit.fireAsync('c.1', { actorId: 'a', tenantId: 'only' });
  // Second distinct tenant must be dropped by maxActiveTenants=1
  await audit.fireAsync('c.1', { actorId: 'a', tenantId: 'other' }).catch(() => {
    /* fireAsync may reject on drop */
  });

  expect(
    events.some(
      (e) =>
        e.type === 'drop' &&
        e.detail === 'max_active_tenants' &&
        e.tenantId === 'other',
    ),
  ).toBe(true);

  await audit.fireAsync('c.1', { actorId: 'a', tenantId: 'only' });

  const s = audit.getStats();
  expect(s.tenants).toBeLessThanOrEqual(1);

  await audit.shutdown();
});
