import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import { DLQStorage } from '../src/storage/dlq';
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

function makeLog(id: string, tenantId?: string): LogbunLog {
  return {
    id,
    tenantId,
    actorId: 'actor-1',
    action: 'flush.chunk',
    createdAt: new Date().toISOString(),
  };
}

function createRecordingAdapter(): IAdapter & {
  insertSizes: number[];
  totalInserted: number;
} {
  const insertSizes: number[] = [];
  let totalInserted = 0;
  return {
    get insertSizes() {
      return insertSizes;
    },
    get totalInserted() {
      return totalInserted;
    },
    async init() {},
    async bulkInsert(_tenantId, logs) {
      insertSizes.push(logs.length);
      totalInserted += logs.length;
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
 * Regression: flush must chunk to maxSize (not the entire queue).
 * Catches unbounded bulkInsert after recovery/backlog buildup.
 */
test('flush drains oversized queue in bulkInsert chunks of at most maxSize', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-flush-chunk-'));
  cleanupPaths.push(dataDir);

  const maxSize = 10;
  const total = 47;
  const adapter = createRecordingAdapter();
  const pool = new ConnectionPool(adapter, 5);
  const dlq = new DLQStorage('flush-chunk', dataDir);
  await dlq.init();

  const batcher = new Batcher({
    adapter,
    pool,
    wal: null,
    dlq,
    mode: 'volatile',
    batching: {
      maxSize,
      flushInterval: 60_000,
      maxQueueSize: 1_000,
      onQueueFull: 'drop',
    },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  for (let i = 0; i < total; i++) {
    const ok = await batcher.enqueue(makeLog(`log-${i}`, 'tenant-a'));
    expect(ok).toBe(true);
  }

  await batcher.flushAll();

  expect(adapter.totalInserted).toBe(total);
  expect(adapter.insertSizes.length).toBeGreaterThan(1);
  for (const size of adapter.insertSizes) {
    expect(size).toBeGreaterThan(0);
    expect(size).toBeLessThanOrEqual(maxSize);
  }
  expect(adapter.insertSizes.filter((s) => s === maxSize).length).toBe(
    Math.floor(total / maxSize),
  );
});

/**
 * Same chunk bound when size-triggered auto-flush runs (not only flushAll).
 */
test('size-triggered auto-flush still caps each bulkInsert at maxSize', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-flush-chunk-auto-'));
  cleanupPaths.push(dataDir);

  const maxSize = 5;
  const total = 23;
  const adapter = createRecordingAdapter();
  const pool = new ConnectionPool(adapter, 5);
  const dlq = new DLQStorage('flush-chunk-auto', dataDir);
  await dlq.init();

  const batcher = new Batcher({
    adapter,
    pool,
    wal: null,
    dlq,
    mode: 'volatile',
    batching: {
      maxSize,
      flushInterval: 60_000,
      maxQueueSize: 500,
      onQueueFull: 'drop',
    },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  for (let i = 0; i < total; i++) {
    await batcher.enqueue(makeLog(`auto-${i}`, 't1'));
  }

  // Drain any remainder below maxSize left on the timer path
  await batcher.flushAll();

  expect(adapter.totalInserted).toBe(total);
  for (const size of adapter.insertSizes) {
    expect(size).toBeLessThanOrEqual(maxSize);
  }
});
