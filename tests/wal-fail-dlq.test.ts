import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import { DLQStorage, readBatch } from '../src/storage/dlq';
import { WALStorage } from '../src/storage/wal';
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

function makeLog(id: string, tenantId?: string): LogbunLog {
  return {
    id,
    tenantId,
    actorId: 'actor-1',
    action: 'wal.fail',
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
 * F2: durable mode + wal.append throw → must not leave the log only in RAM
 * with silent success. Either DLQ, drop event, or hard fail (fireAsync).
 */
test('durable wal.append failure does not silently succeed with RAM-only log', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-fail-'));
  cleanupPaths.push(dataDir);

  const events: LogbunEvent[] = [];
  const adapter = stubAdapter();
  const pool = new ConnectionPool(adapter, 5);

  const realWal = new WALStorage('wal-fail', dataDir, { fsync: false });
  await realWal.init();
  const wal: WALStorage = Object.create(realWal) as WALStorage;
  wal.append = async () => {
    throw new Error('wal disk full');
  };

  const dlq = new DLQStorage('wal-fail', dataDir);
  await dlq.init();

  const batcher = new Batcher({
    adapter,
    pool,
    wal,
    dlq,
    mode: 'durable',
    batching: {
      maxSize: 100,
      flushInterval: 60_000,
      maxQueueSize: 100,
      onQueueFull: 'dlq',
    },
    onEvent: (e) => events.push(e),
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  const log = makeLog('must-not-ram-only', 'tenant-w');
  let threw = false;
  try {
    await batcher.enqueue(log);
  } catch {
    threw = true;
  }

  const pending = await dlq.listPending();
  let inDlq = false;
  for (const p of pending) {
    const batch = await readBatch(p);
    if (batch.logs.some((l) => l.id === log.id)) inDlq = true;
  }

  const dropped = events.some((e) => e.type === 'drop');
  const walFail = events.some((e) => e.type === 'wal_fail');
  const enqueued = events.some((e) => e.type === 'enqueue');

  // Contract: not silent success without durability path.
  // Acceptable: DLQ copy, drop event, or throw. Not: enqueue-only with RAM.
  const durablePath = inDlq || dropped || threw;
  expect(walFail || durablePath).toBe(true);
  expect(durablePath).toBe(true);

  // If it claimed enqueue success, must also have DLQ (not RAM-only).
  if (enqueued) {
    expect(inDlq || dropped).toBe(true);
  }

  await realWal.close();
});

test('enqueue_returns_true_with_dlq_when_wal_fails_but_dlq_succeeds', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-fail-dlq-ok-'));
  cleanupPaths.push(dataDir);

  const events: LogbunEvent[] = [];
  const adapter = stubAdapter();
  const pool = new ConnectionPool(adapter, 5);

  const realWal = new WALStorage('wal-fail-fa', dataDir, { fsync: false });
  await realWal.init();
  const wal: WALStorage = Object.create(realWal) as WALStorage;
  wal.append = async () => {
    throw new Error('wal hard fail');
  };

  const dlq = new DLQStorage('wal-fail-fa', dataDir);
  await dlq.init();

  const batcher = new Batcher({
    adapter,
    pool,
    wal,
    dlq,
    mode: 'durable',
    batching: {
      maxSize: 100,
      flushInterval: 60_000,
      maxQueueSize: 100,
      onQueueFull: 'dlq',
    },
    onEvent: (e) => events.push(e),
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  const log = makeLog('async-wal-fail', 't-async');
  const result = await batcher.enqueue(log);

  // WAL fail + healthy DLQ → success via DLQ, no RAM enqueue
  expect(result).toBe(true);
  expect(events.some((e) => e.type === 'wal_fail')).toBe(true);
  expect(events.some((e) => e.type === 'dlq')).toBe(true);
  expect(events.some((e) => e.type === 'enqueue')).toBe(false);
  expect((await dlq.listPending()).length).toBe(1);

  await realWal.close();
});
