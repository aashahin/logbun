/**
 * Heavy E2E: multi-tenant isolation, concurrent load, database_per_tenant.
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
  waitFor,
} from './helpers';

type Actions = 'resource.created' | 'resource.deleted' | 'resource.read';

const { tempDataDir } = installTestCleanup();

describe('e2e multi-tenant', () => {
  test('single_database: tenants never see each others logs', async () => {
    const dataDir = await tempDataDir('logbun-e2e-mt-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-mt',
      dataDir,
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'shared.db') }),
      wal: { fsync: false },
      batching: { maxSize: 10, flushInterval: 30, maxQueueSize: 500 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    const tenants = ['acme', 'globex', 'initech'];
    for (const t of tenants) {
      for (let i = 0; i < 8; i++) {
        await audit.fireAsync('resource.created', {
          tenantId: t,
          actorId: `${t}-user`,
          entityId: `${t}-res-${i}`,
          newValues: { tenant: t, i },
        });
      }
    }

    await waitFor(async () => {
      let total = 0;
      for (const t of tenants) {
        const page = await audit.query({
          tenantId: t,
          pagination: { limit: 50 },
        });
        total += page.logs.length;
      }
      return total >= 24;
    }, 5_000);

    for (const t of tenants) {
      const page = await audit.query({
        tenantId: t,
        pagination: { limit: 50 },
      });
      expect(page.logs).toHaveLength(8);
      expect(page.logs.every((l) => l.tenantId === t)).toBe(true);
      expect(page.logs.every((l) => l.entityId?.startsWith(`${t}-`))).toBe(
        true,
      );
      // No leakage of other tenant ids in entityId
      for (const other of tenants.filter((x) => x !== t)) {
        expect(page.logs.some((l) => l.entityId?.includes(other))).toBe(false);
      }
    }

    await audit.shutdown();
  });

  test('concurrent fireAsync across many tenants preserves all events', async () => {
    const dataDir = await tempDataDir('logbun-e2e-conc-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-conc',
      dataDir,
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'c.db') }),
      wal: { fsync: false },
      batching: { maxSize: 25, flushInterval: 40, maxQueueSize: 5_000 },
      maxFlushConcurrency: 8,
      retry: FAST_RETRY,
    });
    await audit.ready;

    const TENANTS = 20;
    const PER_TENANT = 15;
    const tasks: Promise<void>[] = [];

    for (let t = 0; t < TENANTS; t++) {
      const tenantId = `tenant_${t}`;
      for (let i = 0; i < PER_TENANT; i++) {
        tasks.push(
          audit.fireAsync('resource.created', {
            tenantId,
            actorId: `actor_${t}`,
            entityId: `e_${t}_${i}`,
            newValues: { t, i },
          }),
        );
      }
    }

    await Promise.all(tasks);
    await audit.shutdown();

    const reader = new AuditLogger<Actions>({
      namespace: 'e2e-conc-r',
      mode: 'volatile',
      requireTenantId: true,
      dataDir: join(dataDir, 'r'),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'c.db') }),
      batching: FAST_BATCH,
      retry: FAST_RETRY,
    });
    await reader.ready;

    let total = 0;
    for (let t = 0; t < TENANTS; t++) {
      const tenantId = `tenant_${t}`;
      const seen = new Set<string>();
      let cursor: string | undefined;
      for (;;) {
        const page = await reader.query({
          tenantId,
          pagination: { limit: 50, cursor },
        });
        for (const log of page.logs) {
          seen.add(log.entityId!);
          expect(log.tenantId).toBe(tenantId);
        }
        if (!page.nextCursor) break;
        cursor = page.nextCursor;
      }
      expect(seen.size).toBe(PER_TENANT);
      total += seen.size;
    }
    expect(total).toBe(TENANTS * PER_TENANT);
    await reader.shutdown();
  });

  test('database_per_tenant isolates SQLite files per tenant', async () => {
    const dataDir = await tempDataDir('logbun-e2e-dpt-');
    const tenantDbs = new Map<string, string>();

    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-dpt',
      dataDir,
      // Base adapter unused for tenant writes when DPT resolves
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'base.db') }),
      tenancy: {
        mode: 'database_per_tenant',
        resolveConnection: async (tenantId) => {
          const path = join(dataDir, `tenant-${tenantId}.db`);
          tenantDbs.set(tenantId, path);
          return { path };
        },
      },
      adapterFactory: async (config) =>
        new BunSQLiteAdapter({ path: String(config['path']) }),
      wal: { fsync: false },
      batching: { maxSize: 5, flushInterval: 25, maxQueueSize: 200 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    await audit.fireAsync('resource.created', {
      tenantId: 'alpha',
      actorId: 'a1',
      entityId: 'alpha-only',
    });
    await audit.fireAsync('resource.created', {
      tenantId: 'beta',
      actorId: 'b1',
      entityId: 'beta-only',
    });

    await waitFor(async () => {
      const a = await audit.query({
        tenantId: 'alpha',
        pagination: { limit: 10 },
      });
      const b = await audit.query({
        tenantId: 'beta',
        pagination: { limit: 10 },
      });
      return a.logs.length >= 1 && b.logs.length >= 1;
    }, 5_000);

    const alpha = await audit.query({
      tenantId: 'alpha',
      pagination: { limit: 10 },
    });
    const beta = await audit.query({
      tenantId: 'beta',
      pagination: { limit: 10 },
    });

    expect(alpha.logs.map((l) => l.entityId)).toEqual(['alpha-only']);
    expect(beta.logs.map((l) => l.entityId)).toEqual(['beta-only']);

    // Physical files exist and are separate
    expect(tenantDbs.has('alpha')).toBe(true);
    expect(tenantDbs.has('beta')).toBe(true);
    expect(tenantDbs.get('alpha')).not.toBe(tenantDbs.get('beta'));

    // Cross-query: alpha DB must not contain beta entity
    const alphaFile = tenantDbs.get('alpha')!;
    const direct = new BunSQLiteAdapter({ path: alphaFile });
    await direct.init();
    const directPage = await direct.query('alpha', {}, { limit: 50 });
    expect(directPage.logs.every((l) => l.entityId !== 'beta-only')).toBe(
      true,
    );
    await direct.close();

    // Missing tenantId rejected (requireTenant forced by DPT)
    await expect(audit.query({ pagination: { limit: 5 } })).rejects.toThrow(
      /tenantId/,
    );
    await expect(
      audit.fireAsync('resource.read', { actorId: 'x' }),
    ).rejects.toThrow(/tenantId/);

    await audit.shutdown();
  });

  test('requireTenantId blocks cross-tenant query mistakes', async () => {
    const dataDir = await tempDataDir('logbun-e2e-reqt-');
    const { has, onEvent } = eventCollector();
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-reqt',
      dataDir,
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      wal: { fsync: false },
      batching: FAST_BATCH,
      retry: FAST_RETRY,
      onEvent,
    });
    await audit.ready;

    expect(() =>
      audit.fire('resource.created', { actorId: 'x' }),
    ).not.toThrow();
    expect(has('drop', 'require_tenant_id')).toBe(true);

    await expect(
      audit.fireAsync('resource.created', { actorId: 'x' }),
    ).rejects.toThrow(/tenantId/);

    await expect(audit.query({ filters: {} })).rejects.toThrow(/tenantId/);

    await audit.shutdown();
  });

  test('high-volume mixed fire and fireAsync under flush pressure', async () => {
    const dataDir = await tempDataDir('logbun-e2e-mix-');
    const { ofType, onEvent } = eventCollector();
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-mix',
      dataDir,
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'mix.db') }),
      wal: { fsync: false },
      batching: { maxSize: 15, flushInterval: 25, maxQueueSize: 2_000 },
      maxFlushConcurrency: 4,
      retry: FAST_RETRY,
      onEvent,
    });
    await audit.ready;

    const asyncN = 40;
    const fireN = 60;
    const asyncs: Promise<void>[] = [];

    for (let i = 0; i < asyncN; i++) {
      asyncs.push(
        audit.fireAsync('resource.created', {
          tenantId: `t${i % 4}`,
          actorId: 'async-actor',
          entityId: `async-${i}`,
        }),
      );
    }
    for (let i = 0; i < fireN; i++) {
      audit.fire('resource.deleted', {
        tenantId: `t${i % 4}`,
        actorId: 'fire-actor',
        entityId: `fire-${i}`,
      });
    }

    await Promise.all(asyncs);
    // Let fire() drain
    await waitFor(() => ofType('flush_ok').length >= 1, 5_000);
    await audit.shutdown();

    const reader = new AuditLogger<Actions>({
      namespace: 'e2e-mix-r',
      mode: 'volatile',
      requireTenantId: true,
      dataDir: join(dataDir, 'r'),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'mix.db') }),
      batching: FAST_BATCH,
      retry: FAST_RETRY,
    });
    await reader.ready;

    let total = 0;
    for (let t = 0; t < 4; t++) {
      let cursor: string | undefined;
      for (;;) {
        const page = await reader.query({
          tenantId: `t${t}`,
          pagination: { limit: 100, cursor },
        });
        total += page.logs.length;
        if (!page.nextCursor) break;
        cursor = page.nextCursor;
      }
    }
    expect(total).toBe(asyncN + fireN);
    expect(ofType('flush_ok').length).toBeGreaterThan(0);
    await reader.shutdown();
  });
});
