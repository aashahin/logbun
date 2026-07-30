import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { AuditLogger } from '../src/logger';
import { DLQStorage } from '../src/storage/dlq';
import type {
  IAdapter,
  LogbunQueryFilters,
  LogbunQueryResult,
} from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function failingAdapter(): IAdapter {
  return {
    async init() {},
    async bulkInsert() {
      throw new Error('adapter intentionally down');
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

function okAdapter(): IAdapter {
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

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 2_000,
  intervalMs = 20,
): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

/**
 * F4: getStats has inflightFlushes; getStatsDetailed fills dlq counts after a dlq write.
 */
test('getStats includes inflightFlushes', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-stats-inflight-'));
  cleanupPaths.push(dataDir);

  const audit = new AuditLogger({
    namespace: 'stats-inflight',
    mode: 'volatile',
    adapter: okAdapter(),
    dataDir,
    batching: { maxSize: 100, flushInterval: 60_000 },
    retry: {
      insertMaxRetries: 1,
      insertBaseDelayMs: 1,
      initialDelayMs: 60_000,
      scanIntervalMs: 60_000,
    },
  });

  await audit.ready;

  const stats = audit.getStats();
  expect(typeof stats.queued).toBe('number');
  expect(typeof stats.tenants).toBe('number');
  expect(typeof stats.degraded).toBe('boolean');
  expect(typeof stats.recoveryBacklog).toBe('number');
  expect(typeof stats.inflightFlushes).toBe('number');
  expect(stats.inflightFlushes).toBeGreaterThanOrEqual(0);
  expect(stats.degraded).toBe(false);

  await audit.shutdown();
});

test('getStatsDetailed fills dlqPending after a DLQ write', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-stats-detailed-'));
  cleanupPaths.push(dataDir);

  const ns = 'stats-detailed';
  const audit = new AuditLogger({
    namespace: ns,
    mode: 'volatile',
    adapter: failingAdapter(),
    dataDir,
    // Flush immediately on first log so bulkInsert fail routes to DLQ
    batching: {
      maxSize: 1,
      flushInterval: 60_000,
      maxQueueSize: 100,
      onQueueFull: 'dlq',
    },
    retry: {
      insertMaxRetries: 1,
      insertBaseDelayMs: 1,
      initialDelayMs: 60_000,
      scanIntervalMs: 60_000,
    },
  });

  await audit.ready;

  await audit.fireAsync('stats.dlq', {
    actorId: 'a1',
    tenantId: 't-stats',
    entityId: 'e1',
  });

  // Wait until the failed flush lands in DLQ
  await waitFor(async () => {
    const pending = await audit.listDlq({ includePending: true });
    return pending.length >= 1;
  });

  const detailed = await audit.getStatsDetailed();
  expect(typeof detailed.inflightFlushes).toBe('number');
  expect(detailed.dlqPending).toBeGreaterThanOrEqual(1);
  expect(typeof detailed.dlqProcessing).toBe('number');
  expect(typeof detailed.dlqDead).toBe('number');
  // volatile mode: no WAL
  expect(detailed.walApproxBytes).toBe(0);

  // Cross-check with direct DLQ storage under the same namespace/dataDir
  const dlq = new DLQStorage(ns, dataDir);
  await dlq.init();
  const pendingFiles = await dlq.listPending();
  expect(pendingFiles.length).toBeGreaterThanOrEqual(1);
  expect(detailed.dlqPending).toBe(pendingFiles.length);

  await audit.shutdown();
});

test('getStatsDetailed before any DLQ activity reports zero counts', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-stats-zero-'));
  cleanupPaths.push(dataDir);

  const audit = new AuditLogger({
    namespace: 'stats-zero',
    mode: 'volatile',
    adapter: okAdapter(),
    dataDir,
    batching: { maxSize: 50, flushInterval: 60_000 },
    retry: {
      insertMaxRetries: 1,
      insertBaseDelayMs: 1,
      initialDelayMs: 60_000,
      scanIntervalMs: 60_000,
    },
  });

  await audit.ready;

  const detailed = await audit.getStatsDetailed();
  expect(detailed.dlqPending).toBe(0);
  expect(detailed.dlqProcessing).toBe(0);
  expect(detailed.dlqDead).toBe(0);
  expect(typeof detailed.inflightFlushes).toBe('number');

  await audit.shutdown();
});
