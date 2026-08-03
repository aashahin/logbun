import { makeFileReliability } from './helpers';
import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import { DLQStorage } from '../src/durability/filesystem';
import type {
  IAdapter,
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

function makeLog(id: string, tenantId: string): LogbunLog {
  return {
    id,
    tenantId,
    actorId: 'actor-1',
    action: 'recovery.cap',
    createdAt: new Date().toISOString(),
  };
}

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

/**
 * F2: injectRecovered with maxActiveTenants=1 and many tenants:
 * does not grow queues map beyond 1 active key; recoveryBacklog retains rest.
 */
test('injectRecovered respects maxActiveTenants=1 and retains rest in recoveryBacklog', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-recovery-caps-'));
  cleanupPaths.push(dataDir);

  const adapter = stubAdapter();
  const pool = new ConnectionPool(adapter, 5);
  const dlq = new DLQStorage('recovery-caps', dataDir);
  await dlq.init();
  const rel = makeFileReliability('rel-ns', dataDir);
  await rel.init();
  // use underlying if needed — prefer FileReliabilityAdapter alone


  const batcher = new Batcher({
    adapter,
    pool,
    reliability: rel,
    mode: 'volatile',
    maxActiveTenants: 1,
    // Large enough that all tenants fit in the first wave size-wise;
    // maxActiveTenants is the limiting factor.
    maxRecoveryBatch: 1_000,
    batching: {
      maxSize: 1_000,
      flushInterval: 60_000,
      maxQueueSize: 1_000,
      onQueueFull: 'dlq',
    },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  const tenantCount = 8;
  const recovered: LogbunLog[] = [];
  for (let i = 0; i < tenantCount; i++) {
    recovered.push(makeLog(`rec-${i}`, `tenant-${i}`));
  }

  batcher.injectRecovered(recovered);

  const stats = batcher.getStats();
  expect(stats.tenants).toBeLessThanOrEqual(1);
  expect(stats.tenants).toBe(1);
  // One tenant active (1 log), remaining 7 stay in recovery backlog
  expect(stats.queued).toBe(1);
  expect(stats.recoveryBacklog).toBe(tenantCount - 1);
  expect(stats.recoveryBacklog).toBeGreaterThan(0);
});

test('injectRecovered never grows queues beyond maxActiveTenants and backlogs overflow', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-recovery-caps-2-'));
  cleanupPaths.push(dataDir);

  const adapter = stubAdapter();
  const pool = new ConnectionPool(adapter, 5);
  const dlq = new DLQStorage('recovery-caps-2', dataDir);
  await dlq.init();

  const maxActive = 2;
    const rel = makeFileReliability('recovery-caps-2', dataDir);
    await rel.init();
  const batcher = new Batcher({
    adapter,
    pool,
    reliability: rel,
    mode: 'volatile',
    maxActiveTenants: maxActive,
    maxRecoveryBatch: 1_000,
    batching: {
      maxSize: 1_000,
      flushInterval: 60_000,
      maxQueueSize: 1_000,
      onQueueFull: 'dlq',
    },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  const recovered = Array.from({ length: 5 }, (_, i) =>
    makeLog(`r-${i}`, `t-${i}`),
  );

  batcher.injectRecovered(recovered);

  const stats = batcher.getStats();
  // Hard cap: active queue keys must never exceed maxActiveTenants
  expect(stats.tenants).toBeLessThanOrEqual(maxActive);
  expect(stats.tenants).toBeGreaterThanOrEqual(1);
  // Overflow tenants remain in recovery backlog (not dropped)
  expect(stats.recoveryBacklog).toBeGreaterThan(0);
  expect(stats.queued + stats.recoveryBacklog).toBe(recovered.length);
});

/**
 * P0: injectWave must respect per-tenant maxQueueSize.
 * Many recovered logs for one tenant must not all land in that queue;
 * overflow stays in recoveryBacklog.
 */
test('injectRecovered respects maxQueueSize per tenant and backlogs overflow', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-recovery-maxq-'));
  cleanupPaths.push(dataDir);

  const adapter = stubAdapter();
  const pool = new ConnectionPool(adapter, 5);
  const dlq = new DLQStorage('recovery-maxq', dataDir);
  await dlq.init();

  const maxQueueSize = 5;
  const recoveredCount = 20;
    const rel = makeFileReliability('recovery-maxq', dataDir);
    await rel.init();
  const batcher = new Batcher({
    adapter,
    pool,
    reliability: rel,
    mode: 'volatile',
    // Large wave so all logs hit injectWave in one shot
    maxRecoveryBatch: 1_000,
    maxTotalQueued: 50_000,
    batching: {
      maxSize: 1_000,
      flushInterval: 60_000,
      maxQueueSize,
      onQueueFull: 'dlq',
    },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  const recovered = Array.from({ length: recoveredCount }, (_, i) =>
    makeLog(`mq-${i}`, 'single-tenant'),
  );

  batcher.injectRecovered(recovered);

  const stats = batcher.getStats();
  // Per-key queue must not exceed maxQueueSize
  expect(stats.queued).toBeLessThanOrEqual(maxQueueSize);
  expect(stats.queued).toBe(maxQueueSize);
  expect(stats.tenants).toBe(1);
  // Overflow retained in recovery backlog (not dropped)
  expect(stats.recoveryBacklog).toBe(recoveredCount - maxQueueSize);
  expect(stats.queued + stats.recoveryBacklog).toBe(recoveredCount);
});
