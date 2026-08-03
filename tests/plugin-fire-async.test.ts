import { makeFileReliability } from './helpers';
import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { AuditLogger } from '../src/logger';
import {
  createAuditMiddleware,
  type HonoAuditLog,
} from '../src/plugins/hono';
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

function stubAdapter(onInsert?: () => void): IAdapter {
  return {
    async init() {},
    async bulkInsert() {
      onInsert?.();
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
 * Minimal Hono-like context for unit-testing createAuditMiddleware without
 * spinning up a real server.
 */
function mockHonoContext(headers: Record<string, string> = {}) {
  const store = new Map<string, unknown>();
  return {
    store,
    set(key: string, value: unknown) {
      store.set(key, value);
    },
    get(key: string) {
      return store.get(key);
    },
    req: {
      header(name: string) {
        return headers[name.toLowerCase()] ?? headers[name] ?? undefined;
      },
    },
  };
}

/**
 * F5: Hono auditLog exposes fireAsync (shape + basic behavior via mocked context).
 */
test('createAuditMiddleware sets auditLog.fire and auditLog.fireAsync', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-plugin-fa-'));
  cleanupPaths.push(dataDir);

  const events: LogbunEvent[] = [];
  const audit = new AuditLogger({
    namespace: 'plugin-fa',
    mode: 'volatile',
    adapter: stubAdapter(),
    batching: { maxSize: 100, flushInterval: 60_000 },
    onEvent: (e) => events.push(e),
    retry: {
      insertMaxRetries: 1,
      insertBaseDelayMs: 1,
    },
  });
  await audit.ready;

  const mw = createAuditMiddleware(audit, {
    getTenantId: () => 'tenant-from-plugin',
  });

  const c = mockHonoContext({ 'user-agent': 'plugin-test/1.0' });
  let nextCalled = false;
  await mw(c as never, async () => {
    nextCalled = true;
  });

  expect(nextCalled).toBe(true);

  const auditLog = c.get('auditLog') as HonoAuditLog;
  expect(auditLog).toBeDefined();
  expect(typeof auditLog.fire).toBe('function');
  expect(typeof auditLog.fireAsync).toBe('function');

  // fireAsync should await enqueue (may reject only on hard fail)
  await auditLog.fireAsync('plugin.act', { actorId: 'actor-p' });

  expect(events.some((e) => e.type === 'enqueue')).toBe(true);
  // getTenantId filled tenantId when omitted
  expect(
    events.some(
      (e) => e.type === 'enqueue' && e.tenantId === 'tenant-from-plugin',
    ),
  ).toBe(true);

  await audit.shutdown();
});

test('auditLog.fire is never-throws; fireAsync rejects when requireTenantId and tenant missing', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-plugin-fa-req-'));
  cleanupPaths.push(dataDir);

  const audit = new AuditLogger({
    namespace: 'plugin-fa-req',
    mode: 'volatile',
    adapter: stubAdapter(),
    requireTenantId: true,
    batching: { maxSize: 50, flushInterval: 60_000 },
    retry: {
      insertMaxRetries: 1,
      insertBaseDelayMs: 1,
    },
  });
  await audit.ready;

  // No getTenantId — tenant must come from caller
  const mw = createAuditMiddleware(audit);
  const c = mockHonoContext();
  await mw(c as never, async () => {});

  const auditLog = c.get('auditLog') as HonoAuditLog;

  expect(() => auditLog.fire('x.y', { actorId: 'a' })).not.toThrow();
  await expect(auditLog.fireAsync('x.y', { actorId: 'a' })).rejects.toThrow(
    /tenantId/,
  );

  // Explicit tenantId works
  await auditLog.fireAsync('x.y', { actorId: 'a', tenantId: 'ok-tenant' });

  await audit.shutdown();
});
