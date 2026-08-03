import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

import { BunSQLiteAdapter } from '../src/adapters/bun-sqlite';
import type { LogbunLog } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

test('query returns newest logs first and paginates toward older logs', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'logbun-query-order-'));
  cleanupPaths.push(dir);

  const adapter = new BunSQLiteAdapter({ path: join(dir, 'audit.db') });
  await adapter.init();

  const logs: LogbunLog[] = [
    {
      id: '0001',
      tenantId: 'tenant-1',
      actorId: 'actor-1',
      action: 'course.created',
      createdAt: '2026-01-01T00:00:00.000Z',
    },
    {
      id: '0002',
      tenantId: 'tenant-1',
      actorId: 'actor-1',
      action: 'course.updated',
      createdAt: '2026-01-02T00:00:00.000Z',
    },
    {
      id: '0003',
      tenantId: 'tenant-1',
      actorId: 'actor-1',
      action: 'course.deleted',
      createdAt: '2026-01-03T00:00:00.000Z',
    },
  ];

  expect(await adapter.bulkInsert('tenant-1', logs)).toBe(true);

  const firstPage = await adapter.query('tenant-1', {}, { limit: 2 });

  expect(firstPage.logs.map((log) => log.id)).toEqual(['0003', '0002']);
  expect(firstPage.nextCursor).toBe('0002');

  const secondPage = await adapter.query('tenant-1', {}, { limit: 2, cursor: firstPage.nextCursor ?? undefined });

  expect(secondPage.logs.map((log) => log.id)).toEqual(['0001']);
  expect(secondPage.nextCursor).toBeNull();

  await adapter.close();
});
