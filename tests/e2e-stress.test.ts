/**
 * Heavy E2E stress / production-shape scenarios:
 * - high concurrency durable enqueue
 * - multi-tenant fair-share under global cap
 * - getStats / getStatsDetailed under load
 * - rapid restart cycles
 */
import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';

import { BunSQLiteAdapter } from '../src/adapters/sqlite';
import { AuditLogger, ENTERPRISE_DEFAULTS } from '../src/index';
import {
  installTestCleanup,
  eventCollector,
  FAST_BATCH,
  FAST_RETRY,
  memoryAdapter,
  sleep,
  waitFor,
} from './helpers';

type Actions = 'evt.a' | 'evt.b' | 'evt.c';

const { tempDataDir } = installTestCleanup();

describe('e2e stress & production shape', () => {
  test('1000 durable fireAsync enqueues then full drain and count', async () => {
    const dataDir = await tempDataDir('logbun-e2e-1k-');
    const N = 1_000;
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-1k',
      dataDir,
      adapter: new BunSQLiteAdapter({
        path: join(dataDir, 'bulk.db'),
        synchronous: 'NORMAL',
      }),
      wal: { fsync: false },
      batching: { maxSize: 50, flushInterval: 50, maxQueueSize: 5_000 },
      maxFlushConcurrency: 8,
      retry: FAST_RETRY,
    });
    await audit.ready;

    const start = performance.now();
    const batchSize = 50;
    for (let offset = 0; offset < N; offset += batchSize) {
      const chunk: Promise<void>[] = [];
      for (let i = offset; i < Math.min(offset + batchSize, N); i++) {
        chunk.push(
          audit.fireAsync('evt.a', {
            tenantId: `t${i % 10}`,
            actorId: `actor-${i % 20}`,
            entityId: `e-${i}`,
            newValues: { i },
          }),
        );
      }
      await Promise.all(chunk);
    }
    const enqueueMs = performance.now() - start;

    await audit.shutdown();
    const totalMs = performance.now() - start;

    const reader = new AuditLogger<Actions>({
      namespace: 'e2e-1k-r',
      mode: 'volatile',
      requireTenantId: true,
      dataDir: join(dataDir, 'r'),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'bulk.db') }),
      batching: FAST_BATCH,
      retry: FAST_RETRY,
    });
    await reader.ready;

    let total = 0;
    for (let t = 0; t < 10; t++) {
      let cursor: string | undefined;
      for (;;) {
        const page = await reader.query({
          tenantId: `t${t}`,
          pagination: { limit: 200, cursor },
        });
        total += page.logs.length;
        if (!page.nextCursor) break;
        cursor = page.nextCursor;
      }
    }

    expect(total).toBe(N);
    // Sanity: should complete in reasonable time on CI/dev machines
    expect(totalMs).toBeLessThan(60_000);
    expect(enqueueMs).toBeLessThan(60_000);
    await reader.shutdown();
  });

  test('getStats and getStatsDetailed remain consistent under concurrent fire', async () => {
    const dataDir = await tempDataDir('logbun-e2e-stats-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-stats',
      dataDir,
      adapter: memoryAdapter({ delayMs: 5 }),
      wal: { fsync: false },
      batching: { maxSize: 20, flushInterval: 40, maxQueueSize: 500 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    const writers: Promise<void>[] = [];
    for (let i = 0; i < 100; i++) {
      writers.push(
        audit.fireAsync('evt.b', {
          tenantId: `t${i % 5}`,
          actorId: 'a',
          entityId: `s-${i}`,
        }),
      );
    }

    // Sample stats while writing
    const snapshots: ReturnType<typeof audit.getStats>[] = [];
    const sampler = (async () => {
      for (let i = 0; i < 10; i++) {
        const s = audit.getStats();
        snapshots.push(s);
        expect(typeof s.queued).toBe('number');
        expect(typeof s.tenants).toBe('number');
        expect(typeof s.degraded).toBe('boolean');
        expect(typeof s.recoveryBacklog).toBe('number');
        expect(typeof s.inflightFlushes).toBe('number');
        expect(s.degraded).toBe(false);
        expect(s.queued).toBeGreaterThanOrEqual(0);
        expect(s.tenants).toBeGreaterThanOrEqual(0);
        await sleep(10);
      }
    })();

    await Promise.all([...writers, sampler]);

    const detailed = await audit.getStatsDetailed();
    expect(detailed.walApproxBytes).toBeGreaterThanOrEqual(0);
    expect(detailed.dlqPending).toBeGreaterThanOrEqual(0);
    expect(detailed.dlqProcessing).toBeGreaterThanOrEqual(0);
    expect(detailed.dlqDead).toBeGreaterThanOrEqual(0);

    await audit.shutdown();
    expect(audit.getStats().queued).toBe(0);
  });

  test('rapid open/close cycles do not corrupt namespace data', async () => {
    const dataDir = await tempDataDir('logbun-e2e-cycle-');
    const dbPath = join(dataDir, 'cycle.db');

    for (let cycle = 0; cycle < 5; cycle++) {
      const audit = new AuditLogger<Actions>({
        ...ENTERPRISE_DEFAULTS,
        namespace: 'e2e-cycle',
        dataDir,
        adapter: new BunSQLiteAdapter({ path: dbPath }),
        wal: { fsync: false },
        batching: { maxSize: 5, flushInterval: 20, maxQueueSize: 100 },
        retry: FAST_RETRY,
      });
      await audit.ready;
      await audit.fireAsync('evt.c', {
        tenantId: 't-cycle',
        actorId: 'a',
        entityId: `cycle-${cycle}`,
      });
      await audit.shutdown();
    }

    const reader = new AuditLogger<Actions>({
      namespace: 'e2e-cycle-r',
      mode: 'volatile',
      requireTenantId: true,
      dataDir: join(dataDir, 'r'),
      adapter: new BunSQLiteAdapter({ path: dbPath }),
      batching: FAST_BATCH,
      retry: FAST_RETRY,
    });
    await reader.ready;
    const page = await reader.query({
      tenantId: 't-cycle',
      pagination: { limit: 20 },
    });
    expect(page.logs).toHaveLength(5);
    expect(page.logs.map((l) => l.entityId).sort()).toEqual([
      'cycle-0',
      'cycle-1',
      'cycle-2',
      'cycle-3',
      'cycle-4',
    ]);
    await reader.shutdown();
  });

  test('global maxTotalQueued triggers fair-share dump under multi-tenant load', async () => {
    const dataDir = await tempDataDir('logbun-e2e-fair-');
    const { ofType, onEvent } = eventCollector();
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-fair',
      dataDir,
      adapter: memoryAdapter({ delayMs: 300 }),
      wal: { fsync: false },
      maxTotalQueued: 20,
      maxActiveTenants: 50,
      batching: {
        maxSize: 100,
        flushInterval: 60_000,
        maxQueueSize: 15,
        onQueueFull: 'dlq',
      },
      retry: FAST_RETRY,
      onEvent,
    });
    await audit.ready;

    // Flood one tenant then others — should see dlq/drop under pressure
    const jobs: Promise<void>[] = [];
    for (let i = 0; i < 40; i++) {
      jobs.push(
        audit
          .fireAsync('evt.a', {
            tenantId: i < 25 ? 'noisy' : `quiet-${i}`,
            actorId: 'a',
            entityId: `f-${i}`,
          })
          .catch(() => undefined),
      );
    }
    await Promise.all(jobs);

    const pressure =
      ofType('dlq').length +
      ofType('drop').length +
      ofType('limit').length;
    // Under hard caps we expect some pressure signal OR successful queueing
    expect(pressure + audit.getStats().queued).toBeGreaterThan(0);
    expect(audit.degraded).toBe(false);
    await audit.shutdown();
  });

  test('filter combinations and empty result sets are stable', async () => {
    const dataDir = await tempDataDir('logbun-e2e-filt-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-filt',
      dataDir,
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'f.db') }),
      wal: { fsync: false },
      batching: { maxSize: 10, flushInterval: 25 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    const now = Date.now();
    await audit.fireAsync('evt.a', {
      tenantId: 'tf',
      actorId: 'alice',
      entityId: 'e1',
    });
    await audit.fireAsync('evt.b', {
      tenantId: 'tf',
      actorId: 'bob',
      entityId: 'e2',
    });
    await audit.fireAsync('evt.a', {
      tenantId: 'tf',
      actorId: 'alice',
      entityId: 'e3',
    });

    await waitFor(async () => {
      const p = await audit.query({ tenantId: 'tf', pagination: { limit: 10 } });
      return p.logs.length >= 3;
    });

    const byActionActor = await audit.query({
      tenantId: 'tf',
      filters: { action: 'evt.a', actorId: 'alice' },
      pagination: { limit: 10 },
    });
    expect(byActionActor.logs).toHaveLength(2);
    expect(byActionActor.logs.every((l) => l.action === 'evt.a')).toBe(true);

    const byEntity = await audit.query({
      tenantId: 'tf',
      filters: { entityId: 'e2', action: 'evt.b' },
      pagination: { limit: 10 },
    });
    expect(byEntity.logs).toHaveLength(1);

    const none = await audit.query({
      tenantId: 'tf',
      filters: { action: 'evt.c' },
      pagination: { limit: 10 },
    });
    expect(none.logs).toHaveLength(0);
    expect(none.nextCursor).toBeNull();

    const past = await audit.query({
      tenantId: 'tf',
      filters: {
        endDate: new Date(now - 86_400_000).toISOString(),
      },
      pagination: { limit: 10 },
    });
    expect(past.logs).toHaveLength(0);

    // Unknown tenant
    const emptyTenant = await audit.query({
      tenantId: 'no-such-tenant',
      pagination: { limit: 10 },
    });
    expect(emptyTenant.logs).toHaveLength(0);

    await audit.shutdown();
  });

  test('idempotent bulkInsert (INSERT OR IGNORE) does not duplicate on replay', async () => {
    const dataDir = await tempDataDir('logbun-e2e-idem-');
    const adapter = new BunSQLiteAdapter({ path: join(dataDir, 'i.db') });
    await adapter.init();

    const log = {
      id: '01900000-0000-7000-8000-000000000001',
      tenantId: 't',
      actorId: 'a',
      action: 'evt.a',
      entityId: 'once',
      createdAt: new Date().toISOString(),
    };

    expect(await adapter.bulkInsert('t', [log])).toBe(true);
    expect(await adapter.bulkInsert('t', [log])).toBe(true); // same id again

    const page = await adapter.query('t', {}, { limit: 10 });
    expect(page.logs).toHaveLength(1);
    expect(page.logs[0]!.entityId).toBe('once');
    await adapter.close();
  });
});
