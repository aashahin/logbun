import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import type {
  IAdapter,
  LogbunEvent,
  LogbunLog,
  LogbunQueryFilters,
  LogbunQueryResult,
  ReliabilityAdapter,
} from '../src/types';
import { makeFileReliability } from './helpers';

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

function wrapAppendFail(base: ReliabilityAdapter): ReliabilityAdapter {
  return new Proxy(base, {
    get(target, prop, receiver) {
      if (prop === 'appendJournal') {
        return async () => {
          throw new Error('wal disk full');
        };
      }
      const v = Reflect.get(target, prop, receiver);
      return typeof v === 'function' ? (v as Function).bind(target) : v;
    },
  });
}

/**
 * F2: durable mode + journal append throw → must not leave the log only in RAM
 * with silent success. Either DLQ, drop event, or hard fail (fireAsync).
 */
test('durable wal.append failure does not silently succeed with RAM-only log', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-fail-'));
  cleanupPaths.push(dataDir);

  const events: LogbunEvent[] = [];
  const adapter = stubAdapter();
  const pool = new ConnectionPool(adapter, 5);

  const real = makeFileReliability('wal-fail', dataDir);
  await real.init();
  const rel = wrapAppendFail(real);

  const batcher = new Batcher({
    adapter,
    pool,
    reliability: rel,
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

  const pending = await real.listDlq({ includePending: true });
  let inDlq = false;
  for (const e of pending) {
    const batch = await real.readDlq(e.id);
    if (batch?.logs.some((l) => l.id === log.id)) inDlq = true;
  }

  const dropped = events.some((e) => e.type === 'drop');
  const walFail = events.some((e) => e.type === 'wal_fail');
  const enqueued = events.some((e) => e.type === 'enqueue');

  const durablePath = inDlq || dropped || threw;
  expect(walFail || durablePath).toBe(true);
  expect(durablePath).toBe(true);

  if (enqueued) {
    expect(inDlq || dropped).toBe(true);
  }

  await real.close();
});

test('enqueue_returns_true_with_dlq_when_wal_fails_but_dlq_succeeds', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-fail-dlq-ok-'));
  cleanupPaths.push(dataDir);

  const events: LogbunEvent[] = [];
  const adapter = stubAdapter();
  const pool = new ConnectionPool(adapter, 5);

  const real = makeFileReliability('wal-fail-fa', dataDir);
  await real.init();
  const rel = wrapAppendFail(real);

  const batcher = new Batcher({
    adapter,
    pool,
    reliability: rel,
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

  expect(result).toBe(true);
  expect(events.some((e) => e.type === 'wal_fail')).toBe(true);
  expect(events.some((e) => e.type === 'dlq')).toBe(true);
  expect(events.some((e) => e.type === 'enqueue')).toBe(false);
  expect((await real.listDlq({ includePending: true })).length).toBe(1);

  await real.close();
});
