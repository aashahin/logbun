import { makeFileReliability } from './helpers';
import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { AuditLogger } from '../src/logger';
import { ENTERPRISE_DEFAULTS } from '../src/types';
import type {
  IAdapter,
  LogbunEvent,
  LogbunQueryFilters,
  LogbunQueryResult,
} from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function okAdapter(delayMs = 0): IAdapter {
  return {
    async init() {
      if (delayMs > 0) {
        await new Promise((r) => setTimeout(r, delayMs));
      }
    },
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

test('onEvent receives limit/unsafe_default_volatile when mode omitted', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-unsafe-vol-'));
  cleanupPaths.push(dataDir);

  const events: LogbunEvent[] = [];
  const audit = new AuditLogger({
    namespace: 'unsafe-vol',
    // mode omitted → volatile default
    adapter: okAdapter(),
    requireTenantId: true,
    onEvent: (e) => events.push(e),
    batching: { maxSize: 100, flushInterval: 60_000 },
    retry: {
      insertMaxRetries: 1,
      insertBaseDelayMs: 1,
    },
  });

  await audit.ready;

  const volatile = events.filter(
    (e) => e.type === 'limit' && e.detail === 'unsafe_default_volatile',
  );
  expect(volatile.length).toBe(1);

  const requireTenant = events.filter(
    (e) => e.type === 'limit' && e.detail === 'unsafe_default_require_tenant',
  );
  expect(requireTenant.length).toBe(0);

  await audit.shutdown();
});

test('onEvent receives limit/unsafe_default_require_tenant when requireTenantId omitted', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-unsafe-tenant-'));
  cleanupPaths.push(dataDir);

  const events: LogbunEvent[] = [];
  const audit = new AuditLogger({
    namespace: 'unsafe-tenant',
    reliability: makeFileReliability('unsafe-tenant', dataDir),
    mode: 'durable',
    adapter: okAdapter(),
    // requireTenantId omitted
    onEvent: (e) => events.push(e),
    batching: { maxSize: 100, flushInterval: 60_000 },
    retry: {
      insertMaxRetries: 1,
      insertBaseDelayMs: 1,
    },
  });

  await audit.ready;

  const requireTenant = events.filter(
    (e) => e.type === 'limit' && e.detail === 'unsafe_default_require_tenant',
  );
  expect(requireTenant.length).toBe(1);

  const volatile = events.filter(
    (e) => e.type === 'limit' && e.detail === 'unsafe_default_volatile',
  );
  expect(volatile.length).toBe(0);

  await audit.shutdown();
});

test('enterprise defaults suppress both unsafe limit events', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-enterprise-def-'));
  cleanupPaths.push(dataDir);

  const events: LogbunEvent[] = [];
  const audit = new AuditLogger({
    namespace: 'enterprise-def',
    reliability: makeFileReliability('enterprise-def', dataDir),
    ...ENTERPRISE_DEFAULTS,
    adapter: okAdapter(),
    onEvent: (e) => events.push(e),
    batching: { maxSize: 100, flushInterval: 60_000 },
    retry: {
      insertMaxRetries: 1,
      insertBaseDelayMs: 1,
    },
  });

  await audit.ready;

  expect(
    events.some(
      (e) =>
        e.type === 'limit' &&
        (e.detail === 'unsafe_default_volatile' ||
          e.detail === 'unsafe_default_require_tenant'),
    ),
  ).toBe(false);

  await audit.shutdown();
});

test('database_per_tenant does not emit unsafe_default_require_tenant', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-unsafe-dpt-'));
  cleanupPaths.push(dataDir);

  const events: LogbunEvent[] = [];
  const audit = new AuditLogger({
    namespace: 'unsafe-dpt',
    mode: 'volatile',
    adapter: okAdapter(),
    tenancy: {
      mode: 'database_per_tenant',
      resolveConnection: async (tenantId: string) => ({
        path: join(dataDir, `${tenantId}.db`),
      }),
    },
    adapterFactory: async () => okAdapter(),
    onEvent: (e) => events.push(e),
    batching: { maxSize: 100, flushInterval: 60_000 },
    retry: {
      insertMaxRetries: 1,
      insertBaseDelayMs: 1,
    },
  });

  await audit.ready;

  expect(
    events.some(
      (e) => e.type === 'limit' && e.detail === 'unsafe_default_require_tenant',
    ),
  ).toBe(false);
  // volatile still warns
  expect(
    events.some(
      (e) => e.type === 'limit' && e.detail === 'unsafe_default_volatile',
    ),
  ).toBe(true);

  await audit.shutdown();
});

test('getStats before ready includes preReadyBuffer in queued', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-preready-stats-'));
  cleanupPaths.push(dataDir);

  // Slow bootstrap so fire() lands in the pre-ready buffer.
  const audit = new AuditLogger({
    namespace: 'preready-stats',
    mode: 'volatile',
    adapter: okAdapter(200),
    requireTenantId: true,
    batching: { maxSize: 100, flushInterval: 60_000 },
    retry: {
      insertMaxRetries: 1,
      insertBaseDelayMs: 1,
    },
  });

  audit.fire('pre.ready', {
    actorId: 'a1',
    tenantId: 't1',
    entityId: 'e1',
  });
  audit.fire('pre.ready', {
    actorId: 'a2',
    tenantId: 't1',
    entityId: 'e2',
  });

  const stats = audit.getStats();
  expect(stats.degraded).toBe(false);
  expect(stats.queued).toBeGreaterThan(0);
  expect(stats.queued).toBe(2);
  expect(stats.tenants).toBe(1);
  expect(stats.recoveryBacklog).toBe(0);
  expect(stats.inflightFlushes).toBe(0);

  await audit.ready;
  await audit.shutdown();
});
