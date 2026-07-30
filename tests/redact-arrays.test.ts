import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { BunSQLiteAdapter } from '../src/adapters/sqlite';
import { AuditLogger } from '../src/logger';
import type { LogbunLog } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 3_000,
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
 * F6: redactPaths removes password inside array of objects.
 */
test('redactPaths removes password inside array of objects in newValues', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-redact-arr-'));
  cleanupPaths.push(dataDir);

  const dbPath = join(dataDir, 'audit.db');
  const audit = new AuditLogger({
    namespace: 'redact-arr',
    mode: 'volatile',
    adapter: new BunSQLiteAdapter({ path: dbPath }),
    dataDir,
    redactPaths: ['password'],
    batching: { maxSize: 1, flushInterval: 20 },
  });
  await audit.ready;

  audit.fire('user.bulk', {
    actorId: 'admin',
    tenantId: 't1',
    entityId: 'batch-1',
    newValues: {
      users: [
        { name: 'alice', password: 'secret-a' },
        { name: 'bob', password: 'secret-b' },
      ],
    },
  });

  await waitFor(async () => {
    const page = await audit.query({
      tenantId: 't1',
      filters: { action: 'user.bulk' },
      pagination: { limit: 10 },
    });
    return page.logs.length > 0;
  });

  const page = await audit.query({
    tenantId: 't1',
    filters: { action: 'user.bulk' },
    pagination: { limit: 10 },
  });
  expect(page.logs.length).toBeGreaterThan(0);

  const log = page.logs[0]! as LogbunLog;
  const users = (log.newValues as { users: Array<Record<string, unknown>> }).users;
  expect(Array.isArray(users)).toBe(true);
  expect(users.length).toBe(2);
  for (const u of users) {
    expect(u['password']).toBeUndefined();
    expect(u['name']).toBeDefined();
  }

  await audit.shutdown();
});

test('redactPaths removes password in nested array under metadata', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-redact-meta-'));
  cleanupPaths.push(dataDir);

  const dbPath = join(dataDir, 'audit.db');
  const audit = new AuditLogger({
    namespace: 'redact-meta',
    mode: 'volatile',
    adapter: new BunSQLiteAdapter({ path: dbPath }),
    dataDir,
    redactPaths: ['password'],
    batching: { maxSize: 1, flushInterval: 20 },
  });
  await audit.ready;

  audit.fire('meta.arr', {
    actorId: 'a',
    tenantId: 't2',
    metadata: {
      items: [{ id: 1, password: 'p1' }, { id: 2, password: 'p2' }],
    },
  });

  await waitFor(async () => {
    const page = await audit.query({
      tenantId: 't2',
      filters: { action: 'meta.arr' },
      pagination: { limit: 5 },
    });
    return page.logs.length > 0;
  });

  const page = await audit.query({
    tenantId: 't2',
    filters: { action: 'meta.arr' },
    pagination: { limit: 5 },
  });
  const items = (page.logs[0]!.metadata as { items: Array<Record<string, unknown>> })
    .items;
  for (const item of items) {
    expect(item['password']).toBeUndefined();
    expect(item['id']).toBeDefined();
  }

  await audit.shutdown();
});

test('redactPaths still redacts top-level bag password object key', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-redact-obj-'));
  cleanupPaths.push(dataDir);

  const dbPath = join(dataDir, 'audit.db');
  const audit = new AuditLogger({
    namespace: 'redact-obj',
    mode: 'volatile',
    adapter: new BunSQLiteAdapter({ path: dbPath }),
    dataDir,
    redactPaths: ['password'],
    batching: { maxSize: 1, flushInterval: 20 },
  });
  await audit.ready;

  audit.fire('obj.pwd', {
    actorId: 'a',
    tenantId: 't3',
    newValues: { password: 'top', email: 'a@b.c' },
  });

  await waitFor(async () => {
    const page = await audit.query({
      tenantId: 't3',
      filters: { action: 'obj.pwd' },
      pagination: { limit: 5 },
    });
    return page.logs.length > 0;
  });

  const page = await audit.query({
    tenantId: 't3',
    filters: { action: 'obj.pwd' },
    pagination: { limit: 5 },
  });
  const nv = page.logs[0]!.newValues as Record<string, unknown>;
  expect(nv['password']).toBeUndefined();
  expect(nv['email']).toBe('a@b.c');

  await audit.shutdown();
});
