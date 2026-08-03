import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult, ReliabilityAdapter } from '../src/types';
import { makeFileReliability } from './helpers';

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

function wrapWriteDlq(
  base: ReliabilityAdapter,
  write: ReliabilityAdapter['writeDlq'],
): ReliabilityAdapter {
  return new Proxy(base, {
    get(target, prop, receiver) {
      if (prop === 'writeDlq') return write;
      const v = Reflect.get(target, prop, receiver);
      return typeof v === 'function' ? v.bind(target) : v;
    },
  });
}

test('concurrent enqueue during dumpQueueToDlq does not lose newly admitted logs', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dump-race-'));
  cleanupPaths.push(dataDir);

  const inserts: LogbunLog[] = [];
  const adapter = stubAdapter((logs) => inserts.push(...logs));
  const pool = new ConnectionPool(adapter, 5);

  const real = makeFileReliability('dump-race', dataDir);
  await real.init();

  let writeStarted!: () => void;
  const writeGate = new Promise<void>((r) => {
    writeStarted = r;
  });
  let releaseWrite!: () => void;
  const writeHold = new Promise<void>((r) => {
    releaseWrite = r;
  });
  let writeCalls = 0;

  const rel = wrapWriteDlq(real, async (tenantId, logs) => {
    writeCalls++;
    writeStarted();
    await writeHold;
    return real.writeDlq(tenantId, logs);
  });

  const batcher = new Batcher({
    adapter,
    pool,
    reliability: rel,
    mode: 'volatile',
    batching: {
      maxSize: 100,
      flushInterval: 60_000,
      maxQueueSize: 2,
      onQueueFull: 'dlq',
    },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  await batcher.enqueue(makeLog('fill-1'));
  await batcher.enqueue(makeLog('fill-2'));

  const overflow = batcher.enqueue(makeLog('overflow-trigger'));
  await writeGate;

  const mid = await batcher.enqueue(makeLog('during-dump'));
  expect(mid).toBe(true);

  releaseWrite();
  await overflow;

  expect(writeCalls).toBeGreaterThanOrEqual(1);

  await batcher.flushAll();

  const ids = new Set(inserts.map((l) => l.id));
  // during-dump must not be lost: either flushed or still in reliability DLQ
  const dlqEntries = await real.listDlq({ includePending: true, includeDead: true });
  for (const e of dlqEntries) {
    const batch = await real.readDlq(e.id);
    for (const l of batch?.logs ?? []) ids.add(l.id);
  }
  expect(ids.has('during-dump') || inserts.some((l) => l.id === 'during-dump')).toBe(true);

  await real.close();
});

test('dumpQueueToDlq race: many concurrent enqueues under tiny maxQueueSize preserve all admitted ids', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dump-race-n-'));
  cleanupPaths.push(dataDir);

  const inserts: LogbunLog[] = [];
  const adapter = stubAdapter((logs) => inserts.push(...logs));
  const pool = new ConnectionPool(adapter, 5);
  const rel = makeFileReliability('dump-race-n', dataDir);
  await rel.init();

  const batcher = new Batcher({
    adapter,
    pool,
    reliability: rel,
    mode: 'volatile',
    batching: {
      maxSize: 50,
      flushInterval: 60_000,
      maxQueueSize: 3,
      onQueueFull: 'dlq',
    },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
  });

  const n = 40;
  const results = await Promise.all(
    Array.from({ length: n }, (_, i) => batcher.enqueue(makeLog(`c-${i}`))),
  );
  expect(results.every(Boolean)).toBe(true);

  await batcher.flushAll();

  const ids = new Set(inserts.map((l) => l.id));
  const dlqEntries = await rel.listDlq({
    includePending: true,
    includeProcessing: true,
    includeDead: true,
  });
  for (const e of dlqEntries) {
    const batch = await rel.readDlq(e.id);
    for (const l of batch?.logs ?? []) ids.add(l.id);
  }

  for (let i = 0; i < n; i++) {
    expect(ids.has(`c-${i}`)).toBe(true);
  }

  await rel.close();
});
