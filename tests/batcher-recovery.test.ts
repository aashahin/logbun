import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import { DLQStorage } from '../src/storage/dlq';
import { WALStorage } from '../src/storage/wal';
import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function makeLog(partial: Partial<LogbunLog> & Pick<LogbunLog, 'id'>): LogbunLog {
  return {
    actorId: 'actor-1',
    action: 'recovered.action',
    createdAt: new Date().toISOString(),
    ...partial,
  };
}

function createRecordingAdapter(): IAdapter & {
  inserts: Array<{ tenantId: string | null; logs: LogbunLog[] }>;
} {
  const inserts: Array<{ tenantId: string | null; logs: LogbunLog[] }> = [];
  return {
    inserts,
    async init() {},
    async bulkInsert(tenantId, logs) {
      inserts.push({ tenantId, logs: [...logs] });
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

async function waitFor(
  predicate: () => boolean,
  timeoutMs = 2_000,
  intervalMs = 20,
): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

test('injectRecovered schedules timer flush without calling flushAll', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-batcher-recovery-'));
  cleanupPaths.push(dataDir);

  const adapter = createRecordingAdapter();
  const pool = new ConnectionPool(adapter, 10);
  const wal = new WALStorage('recovery-ns', dataDir);
  await wal.init();
  const dlq = new DLQStorage('recovery-ns', dataDir);
  await dlq.init();

  const batcher = new Batcher({
    adapter,
    pool,
    wal,
    dlq,
    mode: 'durable',
    batching: {
      maxSize: 100,
      flushInterval: 40,
      maxQueueSize: 1_000,
      onQueueFull: 'dlq',
    },
  });

  const recovered: LogbunLog[] = [
    makeLog({ id: 'rec-1', tenantId: 'tenant-a' }),
    makeLog({ id: 'rec-2', tenantId: 'tenant-a' }),
    makeLog({ id: 'rec-3', tenantId: 'tenant-b', action: 'other.action' }),
  ];

  batcher.injectRecovered(recovered);

  // Must drain via scheduled flush from injectRecovered — not flushAll
  await waitFor(() => {
    const ids = new Set(adapter.inserts.flatMap((i) => i.logs.map((l) => l.id)));
    return ids.has('rec-1') && ids.has('rec-2') && ids.has('rec-3');
  }, 1_500);

  const allIds = adapter.inserts.flatMap((i) => i.logs.map((l) => l.id)).sort();
  expect(allIds).toEqual(['rec-1', 'rec-2', 'rec-3']);

  for (const insert of adapter.inserts) {
    const tenants = new Set(insert.logs.map((l) => l.tenantId ?? null));
    expect(tenants.size).toBe(1);
  }

  await batcher.flushAll();
  await wal.close();
});

test('injectRecovered at maxSize flushes immediately', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-batcher-maxsize-'));
  cleanupPaths.push(dataDir);

  const adapter = createRecordingAdapter();
  const pool = new ConnectionPool(adapter, 10);
  const dlq = new DLQStorage('maxsize-ns', dataDir);
  await dlq.init();

  const batcher = new Batcher({
    adapter,
    pool,
    wal: null,
    dlq,
    mode: 'volatile',
    batching: {
      maxSize: 2,
      flushInterval: 60_000,
      maxQueueSize: 100,
      onQueueFull: 'dlq',
    },
  });

  batcher.injectRecovered([
    makeLog({ id: 'm1', tenantId: 't1' }),
    makeLog({ id: 'm2', tenantId: 't1' }),
  ]);

  await waitFor(() => adapter.inserts.length > 0, 500);

  const ids = adapter.inserts.flatMap((i) => i.logs.map((l) => l.id)).sort();
  expect(ids).toEqual(['m1', 'm2']);

  await batcher.flushAll();
});
