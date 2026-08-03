import { makeFileReliability } from './helpers';
import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import { RetryEngine } from '../src/engine/retry';
import { DLQStorage } from '../src/durability/filesystem';
import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function makeLog(id: string): LogbunLog {
  return {
    id,
    actorId: 'a1',
    action: 'retry.test',
    createdAt: new Date().toISOString(),
  };
}

/**
 * F8: insertMaxRetries=2 means 2 total attempts (not 2 retries after first).
 * Engine: for (let attempt = 0; attempt < insertMaxRetries; attempt++)
 */
test('batcher insertMaxRetries=2 means exactly 2 bulkInsert attempts', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-retry-att-'));
  cleanupPaths.push(dataDir);

  let attempts = 0;
  const adapter: IAdapter = {
    async init() {},
    async bulkInsert() {
      attempts++;
      throw new Error(`fail #${attempts}`);
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

  const pool = new ConnectionPool(adapter, 5);
  const dlq = new DLQStorage('retry-att', dataDir);
  await dlq.init();
  const rel = makeFileReliability('rel-ns', dataDir);
  await rel.init();
  // use underlying if needed — prefer FileReliabilityAdapter alone


  const batcher = new Batcher({
    adapter,
    pool,
    reliability: rel,
    mode: 'volatile',
    batching: {
      maxSize: 1,
      flushInterval: 60_000,
      maxQueueSize: 100,
      onQueueFull: 'dlq',
    },
    retry: { insertMaxRetries: 2, insertBaseDelayMs: 1 },
  });

  await batcher.enqueue(makeLog('r1'));
  await batcher.flushAll();

  expect(attempts).toBe(2);
});

test('batcher insertMaxRetries=1 means single attempt then DLQ', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-retry-one-'));
  cleanupPaths.push(dataDir);

  let attempts = 0;
  const adapter: IAdapter = {
    async init() {},
    async bulkInsert() {
      attempts++;
      return false;
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

  const pool = new ConnectionPool(adapter, 5);
  const dlq = new DLQStorage('retry-one', dataDir);
  await dlq.init();

    const rel = makeFileReliability('retry-one', dataDir);
    await rel.init();
  const batcher = new Batcher({
    adapter,
    pool,
    reliability: rel,
    mode: 'volatile',
    batching: {
      maxSize: 1,
      flushInterval: 60_000,
      maxQueueSize: 50,
      onQueueFull: 'dlq',
    },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  await batcher.enqueue(makeLog('solo'));
  await batcher.flushAll();

  expect(attempts).toBe(1);
  expect((await dlq.listPendingPaths()).length).toBe(1);
});

test('retry engine insertMaxRetries=2 means 2 total attempts per scan', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-retry-eng-'));
  cleanupPaths.push(dataDir);

  let attempts = 0;
  const adapter: IAdapter = {
    async init() {},
    async bulkInsert() {
      attempts++;
      throw new Error('still down');
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

  const pool = new ConnectionPool(adapter, 5);
  const dlq = new DLQStorage('retry-eng', dataDir);
  await dlq.init();
  await dlq.write(null, [makeLog('dlq-1')]);

    const rel = makeFileReliability('retry-eng', dataDir);
    await rel.init();
  const engine = new RetryEngine({
    reliability: rel,
    adapter,
    pool,
    retry: {
      insertMaxRetries: 2,
      insertBaseDelayMs: 1,
      maxScanAttempts: 10,
    },
  });

  await engine.scan();
  expect(attempts).toBe(2);
  engine.stop();
});
