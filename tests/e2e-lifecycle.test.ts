/**
 * Heavy E2E: durable lifecycle — fire, flush, query, pagination, crash recovery, shutdown.
 */
import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';

import { BunSQLiteAdapter } from '../src/adapters/bun-sqlite';
import { AuditLogger, ENTERPRISE_DEFAULTS } from '../src/index';
import {
  installTestCleanup,
  eventCollector,
  FAST_BATCH,
  FAST_RETRY,
  sleep,
  waitFor,
  makeFileReliability,
} from './helpers';

type Actions =
  | 'course.created'
  | 'course.updated'
  | 'course.deleted'
  | 'user.login'
  | 'billing.charged';

const { tempDataDir } = installTestCleanup();

describe('e2e durable lifecycle', () => {
  test('fireAsync → flush → query all fields and multi-filter', async () => {
    const dataDir = await tempDataDir('logbun-e2e-life-');
    const { events, onEvent, has } = eventCollector();

    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-life',
      reliability: makeFileReliability('e2e-life', dataDir),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'audit.db') }),
      batching: { ...FAST_BATCH, maxSize: 5, flushInterval: 30 },
      retry: FAST_RETRY,
      onEvent,
      redactPaths: ['password', 'token'],
    });

    await audit.ready;
    expect(audit.degraded).toBe(false);

    await audit.fireAsync('course.created', {
      tenantId: 'tenant_a',
      actorId: 'user_1',
      entityId: 'course_1',
      newValues: { title: 'TypeScript 101', password: 'secret' },
      metadata: { source: 'api', token: 'tok_xyz' },
    });

    await audit.fireAsync('course.updated', {
      tenantId: 'tenant_a',
      actorId: 'user_1',
      entityId: 'course_1',
      oldValues: { title: 'TypeScript 101' },
      newValues: { title: 'Advanced TypeScript' },
    });

    await audit.fireAsync('user.login', {
      tenantId: 'tenant_a',
      actorId: 'user_2',
      entityId: 'user_2',
      metadata: { method: 'oauth' },
    });

    await waitFor(() => has('flush_ok'), 5_000);

    const all = await audit.query({
      tenantId: 'tenant_a',
      pagination: { limit: 50 },
    });
    expect(all.logs.length).toBe(3);

    // Redaction applied before persist
    const created = all.logs.find((l) => l.action === 'course.created');
    expect(created).toBeDefined();
    expect(created!.newValues).toBeDefined();
    expect(created!.newValues!['password']).toBeUndefined();
    expect(created!.metadata!['token']).toBeUndefined();
    expect(created!.metadata!['source']).toBe('api');
    expect(created!.newValues!['title']).toBe('TypeScript 101');

    // Filter by action
    const onlyLogin = await audit.query({
      tenantId: 'tenant_a',
      filters: { action: 'user.login' },
      pagination: { limit: 10 },
    });
    expect(onlyLogin.logs).toHaveLength(1);
    expect(onlyLogin.logs[0]!.actorId).toBe('user_2');

    // Filter by actor
    const byActor = await audit.query({
      tenantId: 'tenant_a',
      filters: { actorId: 'user_1' },
      pagination: { limit: 10 },
    });
    expect(byActor.logs).toHaveLength(2);
    expect(byActor.logs.every((l) => l.actorId === 'user_1')).toBe(true);

    // Filter by entity
    const byEntity = await audit.query({
      tenantId: 'tenant_a',
      filters: { entityId: 'course_1' },
      pagination: { limit: 10 },
    });
    expect(byEntity.logs).toHaveLength(2);

    // Date range (all recent)
    const recent = await audit.query({
      tenantId: 'tenant_a',
      filters: {
        startDate: new Date(Date.now() - 60_000).toISOString(),
        endDate: new Date(Date.now() + 60_000).toISOString(),
      },
      pagination: { limit: 10 },
    });
    expect(recent.logs.length).toBe(3);

    // Future window returns empty
    const future = await audit.query({
      tenantId: 'tenant_a',
      filters: {
        startDate: new Date(Date.now() + 3600_000).toISOString(),
      },
      pagination: { limit: 10 },
    });
    expect(future.logs).toHaveLength(0);

    // Ids look like UUIDv7 (time-ordered hex)
    for (const log of all.logs) {
      expect(log.id).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
      );
      expect(typeof log.createdAt).toBe('string');
      expect(Number.isNaN(Date.parse(log.createdAt))).toBe(false);
    }

    expect(events.some((e) => e.type === 'enqueue')).toBe(true);

    await audit.shutdown();
  });

  test('cursor pagination walks all pages without duplicates', async () => {
    const dataDir = await tempDataDir('logbun-e2e-page-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-page',
      reliability: makeFileReliability('e2e-page', dataDir),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'audit.db') }),
      batching: { maxSize: 20, flushInterval: 20, maxQueueSize: 200 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    const N = 35;
    for (let i = 0; i < N; i++) {
      await audit.fireAsync('course.created', {
        tenantId: 't-page',
        actorId: `actor_${i % 5}`,
        entityId: `entity_${i}`,
        newValues: { i },
      });
    }

    // Force drain
    await audit.shutdown();

    // Re-open for query (shutdown closed adapter)
    const audit2 = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-page-q',
      reliability: makeFileReliability('e2e-page-q', dataDir),
      dataDir: join(dataDir, 'q'),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'audit.db') }),
      mode: 'volatile',
      requireTenantId: true,
      batching: FAST_BATCH,
      retry: FAST_RETRY,
    });
    await audit2.ready;

    const seen = new Set<string>();
    let cursor: string | undefined;
    let pages = 0;
    for (;;) {
      const page = await audit2.query({
        tenantId: 't-page',
        pagination: { limit: 10, cursor },
      });
      pages++;
      for (const log of page.logs) {
        expect(seen.has(log.id)).toBe(false);
        seen.add(log.id);
      }
      if (!page.nextCursor) break;
      cursor = page.nextCursor;
      expect(pages).toBeLessThan(20);
    }

    expect(seen.size).toBe(N);
    expect(pages).toBeGreaterThanOrEqual(4);
    await audit2.shutdown();
  });

  test('shutdown flushes queued durable logs so they are queryable', async () => {
    const dataDir = await tempDataDir('logbun-e2e-shut-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-shut',
      reliability: makeFileReliability('e2e-shut', dataDir),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'audit.db') }),
      // Large batch so timer alone would not flush
      batching: { maxSize: 10_000, flushInterval: 60_000, maxQueueSize: 10_000 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    for (let i = 0; i < 12; i++) {
      await audit.fireAsync('billing.charged', {
        tenantId: 't-bill',
        actorId: 'billing-svc',
        entityId: `inv_${i}`,
        newValues: { cents: 100 * (i + 1) },
      });
    }

    expect(audit.getStats().queued).toBeGreaterThan(0);
    await audit.shutdown();
    expect(audit.getStats().queued).toBe(0);

    // Query via fresh reader on same SQLite file
    const reader = new AuditLogger<Actions>({
      namespace: 'e2e-shut-read',
      mode: 'volatile',
      requireTenantId: true,
      dataDir: join(dataDir, 'read'),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'audit.db') }),
      batching: FAST_BATCH,
      retry: FAST_RETRY,
    });
    await reader.ready;
    const page = await reader.query({
      tenantId: 't-bill',
      pagination: { limit: 50 },
    });
    expect(page.logs).toHaveLength(12);
    await reader.shutdown();
  });

  test('crash recovery: WAL survives process restart without flush', async () => {
    const dataDir = await tempDataDir('logbun-e2e-crash-');
    const dbPath = join(dataDir, 'audit.db');

    // Instance 1: durable enqueue with failing destination so journal stays unacked
    const failAdapter: import('../src/types').IAdapter = {
      async init() {},
      async bulkInsert() {
        throw new Error('remote down');
      },
      async query() {
        return { logs: [], nextCursor: null };
      },
      async prune() {},
      async close() {},
    };

    const aFail = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-crash-wal',
      reliability: makeFileReliability('e2e-crash-wal', dataDir),
      adapter: failAdapter,
      batching: {
        maxSize: 1,
        flushInterval: 20,
        maxQueueSize: 100,
        onQueueFull: 'dlq',
      },
      retry: { ...FAST_RETRY, maxScanAttempts: 1 },
    });
    await aFail.ready;

    for (const entityId of ['e1', 'e2', 'e3']) {
      await aFail.fireAsync('course.created', {
        tenantId: 't-crash',
        actorId: 'user_x',
        entityId,
      });
    }

    await waitFor(async () => {
      const s = await aFail.getStatsDetailed();
      return (s.walApproxBytes ?? 0) > 0 || (s.dlqPending ?? 0) > 0;
    }, 5_000);

    // Hard close reliability without successful delivery (simulate crash after journal)
    await aFail.shutdown();

    // Instance 2: healthy adapter recovers journal / DLQ
    const a2 = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-crash-wal',
      reliability: makeFileReliability('e2e-crash-wal', dataDir),
      adapter: new BunSQLiteAdapter({ path: dbPath }),
      batching: { ...FAST_BATCH, maxSize: 5, flushInterval: 30 },
      retry: FAST_RETRY,
    });
    await a2.ready;
    expect(a2.degraded).toBe(false);

    await a2.runMaintenance();
    await waitFor(async () => {
      const q = await a2.query({
        tenantId: 't-crash',
        pagination: { limit: 50 },
      });
      return q.logs.length >= 3;
    }, 8_000);

    const q = await a2.query({
      tenantId: 't-crash',
      pagination: { limit: 50 },
    });
    const entities = new Set(q.logs.map((l) => l.entityId));
    expect(entities.has('e1')).toBe(true);
    expect(entities.has('e2')).toBe(true);
    expect(entities.has('e3')).toBe(true);

    await a2.shutdown();
  });

  test('fire before ready buffers then drains after bootstrap', async () => {
    const dataDir = await tempDataDir('logbun-e2e-preready-');
    const { has, onEvent } = eventCollector();

    // Slow adapter init to widen pre-ready window
    let resolveInit!: () => void;
    const initGate = new Promise<void>((r) => {
      resolveInit = r;
    });
    const adapter = new BunSQLiteAdapter({ path: join(dataDir, 'a.db') });
    const origInit = adapter.init.bind(adapter);
    adapter.init = async () => {
      await initGate;
      await origInit();
    };

    const audit = new AuditLogger<Actions>({
      namespace: 'e2e-preready',
      mode: 'volatile',
      requireTenantId: true,
      adapter,
      batching: { maxSize: 5, flushInterval: 30 },
      retry: FAST_RETRY,
      onEvent,
    });

    // fire() before ready — never throws
    audit.fire('course.created', {
      tenantId: 't1',
      actorId: 'a1',
      entityId: 'buffered-1',
    });
    audit.fire('course.created', {
      tenantId: 't1',
      actorId: 'a1',
      entityId: 'buffered-2',
    });

    expect(audit.getStats().queued).toBe(2);
    resolveInit();
    await audit.ready;

    await waitFor(() => has('flush_ok'), 5_000);

    const page = await audit.query({
      tenantId: 't1',
      pagination: { limit: 10 },
    });
    expect(page.logs.map((l) => l.entityId).sort()).toEqual([
      'buffered-1',
      'buffered-2',
    ]);

    await audit.shutdown();
  });

  test('request context (ip / userAgent) persists via fireAsync', async () => {
    const dataDir = await tempDataDir('logbun-e2e-ctx-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-ctx',
      reliability: makeFileReliability('e2e-ctx', dataDir),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      batching: { maxSize: 1, flushInterval: 20 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    await audit.fireAsync(
      'user.login',
      {
        tenantId: 't-ctx',
        actorId: 'u1',
        entityId: 'session-1',
      },
      { ipAddress: '203.0.113.10', userAgent: 'LogbunE2E/1.0' },
    );

    await waitFor(async () => {
      const page = await audit.query({
        tenantId: 't-ctx',
        pagination: { limit: 5 },
      });
      return page.logs.length >= 1;
    });

    const page = await audit.query({
      tenantId: 't-ctx',
      pagination: { limit: 5 },
    });
    expect(page.logs[0]!.ipAddress).toBe('203.0.113.10');
    expect(page.logs[0]!.userAgent).toBe('LogbunE2E/1.0');
    await audit.shutdown();
  });

  test('double shutdown is safe; fire after shutdown drops without throw', async () => {
    const dataDir = await tempDataDir('logbun-e2e-dbl-');
    const { has, onEvent } = eventCollector();
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-dbl',
      reliability: makeFileReliability('e2e-dbl', dataDir),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      batching: FAST_BATCH,
      retry: FAST_RETRY,
      onEvent,
    });
    await audit.ready;
    await audit.shutdown();
    await audit.shutdown(); // no-op

    expect(() =>
      audit.fire('course.created', {
        tenantId: 't1',
        actorId: 'a1',
      }),
    ).not.toThrow();

    await sleep(20);
    expect(has('drop', 'shutdown')).toBe(true);

    await expect(
      audit.fireAsync('course.created', {
        tenantId: 't1',
        actorId: 'a1',
      }),
    ).rejects.toThrow();
  });
});
