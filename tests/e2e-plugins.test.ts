/**
 * Heavy E2E: Hono + Elysia plugins over real HTTP handlers.
 */
import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';
import { Elysia } from 'elysia';
import { Hono } from 'hono';

import { BunSQLiteAdapter } from '../src/adapters/sqlite';
import { AuditLogger, ENTERPRISE_DEFAULTS } from '../src/index';
import {
  auditPlugin,
  type AuditPluginOptions,
} from '../src/plugins/elysia';
import {
  createAuditMiddleware,
  type LogbunHonoVariables,
} from '../src/plugins/hono';
import {
  installTestCleanup,
  FAST_BATCH,
  FAST_RETRY,
  waitFor,
} from './helpers';

type Actions =
  | 'http.created'
  | 'http.updated'
  | 'http.deleted'
  | 'http.viewed';

async function waitFlushed(
  audit: AuditLogger<Actions>,
  tenantId: string,
  min: number,
  timeoutMs = 5_000,
): Promise<void> {
  await waitFor(async () => {
    const page = await audit.query({
      tenantId,
      pagination: { limit: 100 },
    });
    return page.logs.length >= min;
  }, timeoutMs);
}

const { tempDataDir } = installTestCleanup();

describe('e2e Hono plugin', () => {
  test('middleware injects fire/fireAsync with UA, XFF IP, and getTenantId', async () => {
    const dataDir = await tempDataDir('logbun-e2e-hono-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-hono',
      dataDir,
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'hono.db') }),
      wal: { fsync: false },
      batching: { maxSize: 5, flushInterval: 25 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    const app = new Hono<{ Variables: LogbunHonoVariables<Actions> }>();
    app.use(
      '*',
      createAuditMiddleware(audit, {
        trustedProxyCount: 1,
        getTenantId: (c) => c.req.header('x-tenant-id') ?? undefined,
      }),
    );

    app.post('/resources', async (c) => {
      const body = await c.req.json<{ id: string; name: string }>();
      await c.get('auditLog').fireAsync('http.created', {
        actorId: c.req.header('x-actor-id') ?? 'anon',
        entityId: body.id,
        newValues: { name: body.name },
      });
      return c.json({ ok: true, id: body.id }, 201);
    });

    app.get('/resources/:id', (c) => {
      c.get('auditLog').fire('http.viewed', {
        actorId: c.req.header('x-actor-id') ?? 'anon',
        entityId: c.req.param('id'),
      });
      return c.json({ id: c.req.param('id') });
    });

    // Create via fireAsync
    const createRes = await app.request('/resources', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-tenant-id': 'tenant-hono',
        'x-actor-id': 'user-42',
        'user-agent': 'HonoE2E/2.0',
        'x-forwarded-for': '203.0.113.50, 10.0.0.1',
      },
      body: JSON.stringify({ id: 'res-1', name: 'Widget' }),
    });
    expect(createRes.status).toBe(201);

    // View via fire
    const viewRes = await app.request('/resources/res-1', {
      headers: {
        'x-tenant-id': 'tenant-hono',
        'x-actor-id': 'user-42',
        'user-agent': 'HonoE2E/2.0',
        'x-forwarded-for': '203.0.113.50, 10.0.0.1',
      },
    });
    expect(viewRes.status).toBe(200);

    await waitFlushed(audit, 'tenant-hono', 2);

    const page = await audit.query({
      tenantId: 'tenant-hono',
      pagination: { limit: 20 },
    });
    expect(page.logs.length).toBeGreaterThanOrEqual(2);

    const created = page.logs.find((l) => l.action === 'http.created');
    expect(created).toBeDefined();
    expect(created!.actorId).toBe('user-42');
    expect(created!.entityId).toBe('res-1');
    expect(created!.newValues?.['name']).toBe('Widget');
    expect(created!.userAgent).toBe('HonoE2E/2.0');
    // trustedProxyCount=1 → client IP from XFF
    expect(created!.ipAddress).toBe('203.0.113.50');

    const viewed = page.logs.find((l) => l.action === 'http.viewed');
    expect(viewed).toBeDefined();
    expect(viewed!.entityId).toBe('res-1');

    await audit.shutdown();
  });

  test('fireAsync rejects without tenant when requireTenantId and no getTenantId', async () => {
    const dataDir = await tempDataDir('logbun-e2e-hono-t-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-hono-t',
      dataDir,
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      wal: { fsync: false },
      batching: FAST_BATCH,
      retry: FAST_RETRY,
    });
    await audit.ready;

    const app = new Hono<{ Variables: LogbunHonoVariables<Actions> }>();
    app.use('*', createAuditMiddleware(audit));
    app.post('/x', async (c) => {
      try {
        await c.get('auditLog').fireAsync('http.updated', {
          actorId: 'u',
          entityId: 'e',
        });
        return c.json({ ok: true });
      } catch (err) {
        return c.json(
          { ok: false, error: err instanceof Error ? err.message : String(err) },
          400,
        );
      }
    });

    const res = await app.request('/x', { method: 'POST' });
    expect(res.status).toBe(400);
    const body = (await res.json()) as { ok: boolean; error: string };
    expect(body.ok).toBe(false);
    expect(body.error).toMatch(/tenantId/);

    await audit.shutdown();
  });

  test('explicit tenantId in input wins over getTenantId header', async () => {
    const dataDir = await tempDataDir('logbun-e2e-hono-ov-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-hono-ov',
      dataDir,
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      wal: { fsync: false },
      batching: { maxSize: 1, flushInterval: 20 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    const app = new Hono<{ Variables: LogbunHonoVariables<Actions> }>();
    app.use(
      '*',
      createAuditMiddleware(audit, {
        getTenantId: () => 'from-header',
      }),
    );
    app.post('/x', async (c) => {
      await c.get('auditLog').fireAsync('http.created', {
        tenantId: 'from-body',
        actorId: 'u',
        entityId: 'e1',
      });
      return c.json({ ok: true });
    });

    await app.request('/x', { method: 'POST' });
    await waitFlushed(audit, 'from-body', 1);

    const page = await audit.query({
      tenantId: 'from-body',
      pagination: { limit: 5 },
    });
    expect(page.logs).toHaveLength(1);

    const wrong = await audit.query({
      tenantId: 'from-header',
      pagination: { limit: 5 },
    });
    expect(wrong.logs).toHaveLength(0);

    await audit.shutdown();
  });
});

describe('e2e Elysia plugin', () => {
  test('derive plugin fire/fireAsync with trusted proxy and tenant header', async () => {
    const dataDir = await tempDataDir('logbun-e2e-ely-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-ely',
      dataDir,
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'ely.db') }),
      wal: { fsync: false },
      batching: { maxSize: 5, flushInterval: 25 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    const opts: AuditPluginOptions = {
      trustedProxyCount: 1,
      getTenantId: ({ request }) =>
        request.headers.get('x-tenant-id') ?? undefined,
    };

    const app = new Elysia()
      .use(auditPlugin(audit, opts))
      .post('/items', async ({ auditLog, request, body }) => {
        const actorId = request.headers.get('x-actor-id') ?? 'anon';
        const b = body as { id: string; title: string };
        await auditLog.fireAsync('http.created', {
          actorId,
          entityId: b.id,
          newValues: { title: b.title },
        });
        return { ok: true };
      })
      .delete('/items/:id', ({ auditLog, request, params }) => {
        auditLog.fire('http.deleted', {
          actorId: request.headers.get('x-actor-id') ?? 'anon',
          entityId: params.id,
        });
        return { ok: true };
      });

    const createRes = await app.handle(
      new Request('http://localhost/items', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-tenant-id': 'tenant-ely',
          'x-actor-id': 'ely-user',
          'user-agent': 'ElysiaE2E/1.0',
          'x-forwarded-for': '198.51.100.20, 192.168.1.1',
        },
        body: JSON.stringify({ id: 'item-9', title: 'Notebook' }),
      }),
    );
    expect(createRes.status).toBe(200);

    const delRes = await app.handle(
      new Request('http://localhost/items/item-9', {
        method: 'DELETE',
        headers: {
          'x-tenant-id': 'tenant-ely',
          'x-actor-id': 'ely-user',
          'user-agent': 'ElysiaE2E/1.0',
          'x-forwarded-for': '198.51.100.20, 192.168.1.1',
        },
      }),
    );
    expect(delRes.status).toBe(200);

    await waitFlushed(audit, 'tenant-ely', 2);

    const page = await audit.query({
      tenantId: 'tenant-ely',
      pagination: { limit: 20 },
    });
    expect(page.logs.length).toBeGreaterThanOrEqual(2);

    const created = page.logs.find((l) => l.action === 'http.created');
    expect(created!.entityId).toBe('item-9');
    expect(created!.newValues?.['title']).toBe('Notebook');
    expect(created!.userAgent).toBe('ElysiaE2E/1.0');
    expect(created!.ipAddress).toBe('198.51.100.20');
    expect(created!.actorId).toBe('ely-user');

    const deleted = page.logs.find((l) => l.action === 'http.deleted');
    expect(deleted!.entityId).toBe('item-9');

    await audit.shutdown();
  });

  test('XFF is ignored when trustedProxyCount is 0', async () => {
    const dataDir = await tempDataDir('logbun-e2e-ely-xff-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-ely-xff',
      dataDir,
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      wal: { fsync: false },
      batching: { maxSize: 1, flushInterval: 20 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    const app = new Elysia()
      .use(
        auditPlugin(audit, {
          trustedProxyCount: 0,
          getTenantId: () => 't-xff',
        }),
      )
      .post('/ping', async ({ auditLog }) => {
        await auditLog.fireAsync('http.viewed', {
          actorId: 'u',
          entityId: 'ping',
        });
        return { ok: true };
      });

    await app.handle(
      new Request('http://localhost/ping', {
        method: 'POST',
        headers: {
          'x-forwarded-for': '8.8.8.8',
          'user-agent': 'xff-test',
        },
      }),
    );

    await waitFlushed(audit, 't-xff', 1);
    const log = (
      await audit.query({ tenantId: 't-xff', pagination: { limit: 5 } })
    ).logs[0]!;
    expect(log.ipAddress).toBeUndefined();
    expect(log.userAgent).toBe('xff-test');

    await audit.shutdown();
  });
});
