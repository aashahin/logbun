import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import { DLQStorage } from '../src/storage/dlq';
import type { IAdapter, LogbunLog } from '../src/types';

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
    actorId: 'a',
    action: 'fair.test',
    createdAt: new Date().toISOString(),
  };
}

test('global cap dumps largest tenant queue (fair-share victim)', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-fair-'));
  cleanupPaths.push(dataDir);

  const dlqWrites: string[] = [];
  const adapter: IAdapter = {
    async init() {},
    async bulkInsert() {
      return true;
    },
    async query() {
      return { logs: [], nextCursor: null };
    },
    async prune() {},
    async close() {},
  };

  const realDlq = new DLQStorage('fair', dataDir, { maxFiles: 1000 });
  await realDlq.init();
  const dlq = Object.create(realDlq) as DLQStorage;
  dlq.write = async (tenantId, logs) => {
    dlqWrites.push(tenantId ?? '__global__');
    return realDlq.write(tenantId, logs);
  };

  const pool = new ConnectionPool(adapter, 5);
  const batcher = new Batcher({
    adapter,
    pool,
    wal: null,
    dlq,
    mode: 'volatile',
    maxTotalQueued: 5,
    maxActiveTenants: 100,
    batching: {
      maxSize: 100,
      flushInterval: 60_000,
      maxQueueSize: 100,
      onQueueFull: 'dlq',
    },
  });

  // Heavy tenant A fills most of the global budget
  for (let i = 0; i < 4; i++) {
    await batcher.enqueue(makeLog(`a-${i}`, 'tenant-A'));
  }
  // Small tenant B
  await batcher.enqueue(makeLog('b-0', 'tenant-B'));
  expect(batcher.getStats().queued).toBe(5);

  // Admit for B should dump largest (A) first under fair-share
  const ok = await batcher.enqueue(makeLog('b-1', 'tenant-B'));
  expect(ok).toBe(true);
  expect(dlqWrites.length).toBeGreaterThan(0);
  expect(dlqWrites[0]).toBe('tenant-A');
});
