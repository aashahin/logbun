import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import { DLQStorage, readBatch } from '../src/storage/dlq';
import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function makeLog(id: string, tenantId = 't1'): LogbunLog {
  return {
    id,
    tenantId,
    actorId: 'actor-1',
    action: 'race.test',
    createdAt: new Date().toISOString(),
  };
}

function stubAdapter(onInsert?: (logs: LogbunLog[]) => void): IAdapter {
  return {
    async init() {},
    async bulkInsert(_t, logs) {
      onInsert?.(logs);
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
 * F1: concurrent enqueue while dumpQueueToDlq must not lose newly admitted logs.
 * dumpQueueToDlq must clear the queue synchronously after snapshot (before await dlq.write).
 */
test('concurrent enqueue during dumpQueueToDlq does not lose newly admitted logs', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dump-race-'));
  cleanupPaths.push(dataDir);

  const inserts: LogbunLog[] = [];
  const adapter = stubAdapter((logs) => inserts.push(...logs));
  const pool = new ConnectionPool(adapter, 5);

  const realDlq = new DLQStorage('dump-race', dataDir);
  await realDlq.init();

  let writeStarted!: () => void;
  const writeGate = new Promise<void>((r) => {
    writeStarted = r;
  });
  let releaseWrite!: () => void;
  const writeHold = new Promise<void>((r) => {
    releaseWrite = r;
  });
  let writeCalls = 0;

  const dlq: DLQStorage = Object.create(realDlq) as DLQStorage;
  dlq.write = async (tenantId, logs) => {
    writeCalls++;
    writeStarted();
    await writeHold;
    return realDlq.write(tenantId, logs);
  };

  const batcher = new Batcher({
    adapter,
    pool,
    wal: null,
    dlq,
    mode: 'volatile',
    batching: {
      maxSize: 100,
      flushInterval: 60_000,
      maxQueueSize: 2,
      onQueueFull: 'dlq',
    },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  // Fill queue to capacity (no flush)
  await batcher.enqueue(makeLog('fill-1'));
  await batcher.enqueue(makeLog('fill-2'));

  // Trigger dump via backpressure (queue full)
  const dumpPromise = batcher.enqueue(makeLog('trigger-dump'));

  // Wait until slow DLQ write is in flight
  await writeGate;

  // Admit a new log while dump is mid-write. With sync clear-after-snapshot,
  // the queue is empty so this admits cleanly. With clear-after-await, a
  // concurrent dump can wipe this log when the first write finishes.
  const lateId = 'late-admitted';
  const latePromise = batcher.enqueue(makeLog(lateId));

  // Unblock DLQ and finish concurrent enqueues
  releaseWrite();
  await Promise.all([dumpPromise, latePromise]);

  // Drain remaining RAM + ensure late log is somewhere durable/queued
  await batcher.flushAll();

  const recovered = new Set(inserts.map((l) => l.id));
  for (const p of await realDlq.listPending()) {
    const batch = await readBatch(p);
    for (const l of batch.logs) recovered.add(l.id);
  }

  expect(recovered.has(lateId)).toBe(true);
  // Fill logs must have gone to DLQ (backpressure path), not vanished
  expect(writeCalls).toBeGreaterThanOrEqual(1);
  expect(recovered.has('fill-1') || recovered.has('fill-2')).toBe(true);
});

test('dumpQueueToDlq race: many concurrent enqueues under tiny maxQueueSize preserve all admitted ids', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dump-race-n-'));
  cleanupPaths.push(dataDir);

  const inserts: LogbunLog[] = [];
  const adapter = stubAdapter((logs) => inserts.push(...logs));
  const pool = new ConnectionPool(adapter, 5);

  const realDlq = new DLQStorage('dump-race-n', dataDir);
  await realDlq.init();

  const dlq: DLQStorage = Object.create(realDlq) as DLQStorage;
  dlq.write = async (tenantId, logs) => {
    // Slow enough to create interleaving windows
    await new Promise((r) => setTimeout(r, 15));
    return realDlq.write(tenantId, logs);
  };

  const batcher = new Batcher({
    adapter,
    pool,
    wal: null,
    dlq,
    mode: 'volatile',
    batching: {
      maxSize: 50,
      flushInterval: 60_000,
      maxQueueSize: 3,
      onQueueFull: 'dlq',
    },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  const N = 20;
  const ids = Array.from({ length: N }, (_, i) => `race-${i}`);
  await Promise.all(ids.map((id) => batcher.enqueue(makeLog(id))));

  await batcher.flushAll();

  const recovered = new Set(inserts.map((l) => l.id));
  for (const p of await realDlq.listPending()) {
    const batch = await readBatch(p);
    for (const l of batch.logs) recovered.add(l.id);
  }
  // processing / dead also count as not lost
  for (const p of await realDlq.listProcessing()) {
    const batch = await readBatch(p);
    for (const l of batch.logs) recovered.add(l.id);
  }
  for (const p of await realDlq.listDead()) {
    const batch = await readBatch(p);
    for (const l of batch.logs) recovered.add(l.id);
  }

  for (const id of ids) {
    expect(recovered.has(id)).toBe(true);
  }
});
