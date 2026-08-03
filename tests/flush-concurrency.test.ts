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
    action: 'flush.concurrency',
    createdAt: new Date().toISOString(),
  };
}

/**
 * F1: maxFlushConcurrency=1 serializes bulkInsert under multi-tenant flush.
 * Track concurrent depth; assert max concurrent inserts never > 1.
 */
test('maxFlushConcurrency=1 serializes multi-tenant bulkInsert flushes', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-flush-conc-'));
  cleanupPaths.push(dataDir);

  let concurrent = 0;
  let maxConcurrent = 0;
  let insertCount = 0;

  const adapter: IAdapter = {
    async init() {},
    async bulkInsert(_tenantId, logs) {
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      insertCount++;
      // Hold the slot long enough that parallel flushes would overlap without a semaphore
      await new Promise((r) => setTimeout(r, 60));
      concurrent--;
      return logs.length >= 0;
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

  const pool = new ConnectionPool(adapter, 20);
  const dlq = new DLQStorage('flush-conc', dataDir);
  await dlq.init();
  const rel = makeFileReliability('rel-ns', dataDir);
  await rel.init();
  // use underlying if needed — prefer FileReliabilityAdapter alone


  const batcher = new Batcher({
    adapter,
    pool,
    reliability: rel,
    mode: 'volatile',
    maxFlushConcurrency: 1,
    batching: {
      maxSize: 100,
      flushInterval: 60_000,
      maxQueueSize: 100,
      onQueueFull: 'drop',
    },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  const tenants = ['t1', 't2', 't3', 't4', 't5'];
  for (const t of tenants) {
    const ok = await batcher.enqueue(makeLog(`log-${t}`, t));
    expect(ok).toBe(true);
  }

  // Multi-tenant flushAll would race bulkInsert without the global semaphore
  await batcher.flushAll();

  expect(insertCount).toBeGreaterThanOrEqual(tenants.length);
  expect(maxConcurrent).toBeLessThanOrEqual(1);
  expect(maxConcurrent).toBe(1);
});

test('maxFlushConcurrency default still admits concurrent flushes above 1', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-flush-conc-def-'));
  cleanupPaths.push(dataDir);

  let concurrent = 0;
  let maxConcurrent = 0;

  const adapter: IAdapter = {
    async init() {},
    async bulkInsert() {
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await new Promise((r) => setTimeout(r, 50));
      concurrent--;
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

  const pool = new ConnectionPool(adapter, 20);
  const dlq = new DLQStorage('flush-conc-def', dataDir);
  await dlq.init();

  // Explicit high concurrency so multi-tenant flushAll can overlap inserts
    const rel = makeFileReliability('flush-conc-def', dataDir);
    await rel.init();
  const batcher = new Batcher({
    adapter,
    pool,
    reliability: rel,
    mode: 'volatile',
    maxFlushConcurrency: 8,
    batching: {
      maxSize: 100,
      flushInterval: 60_000,
      maxQueueSize: 100,
      onQueueFull: 'drop',
    },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  for (const t of ['a', 'b', 'c', 'd', 'e', 'f']) {
    await batcher.enqueue(makeLog(`log-${t}`, t));
  }

  await batcher.flushAll();

  // With concurrency > 1, overlapping inserts should be observed
  expect(maxConcurrent).toBeGreaterThan(1);
});
