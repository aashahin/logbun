import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { BunSQLiteAdapter } from '../src/adapters/bun-sqlite';
import { ConnectionPool } from '../src/engine/pool';
import type { IAdapter, LogbunQueryFilters, LogbunQueryResult } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

type TrackedAdapter = IAdapter & { closed: boolean; id: string };

function trackedAdapter(id: string): TrackedAdapter {
  return {
    id,
    closed: false,
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
    async close() {
      this.closed = true;
    },
  };
}

/**
 * F4: pin / withAdapter prevents close of in-use adapter under eviction pressure.
 */
test('pin prevents close of in-use adapter during eviction pressure', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-pool-pin-'));
  cleanupPaths.push(dataDir);

  const created = new Map<string, TrackedAdapter>();

  const base = trackedAdapter('base');
  const pool = new ConnectionPool(
    base,
    2, // tiny pool → eviction pressure
    {
      mode: 'database_per_tenant',
      resolveConnection: async (tenantId) => ({
        path: join(dataDir, `${tenantId}.db`),
        tenantId,
      }),
    },
    async (config) => {
      const tid = String(config['tenantId'] ?? config['path']);
      const a = trackedAdapter(tid);
      created.set(String(config['tenantId'] ?? tid), a);
      return a;
    },
  );

  const a = await pool.get('tenant-a');
  expect(a).toBeDefined();

  await pool.pin('tenant-a');

  // Fill/evict: get b then c — without pin, a would be LRU-evicted and closed
  await pool.get('tenant-b');
  await pool.get('tenant-c');

  const pinned = created.get('tenant-a');
  expect(pinned).toBeDefined();
  expect(pinned!.closed).toBe(false);

  // get still works for pinned tenant
  const again = await pool.get('tenant-a');
  expect(again).toBe(pinned);

  pool.unpin('tenant-a');

  // After unpin, further pressure may close it
  await pool.get('tenant-d');
  await pool.get('tenant-e');
  // Not asserting closed=true (LRU order depends on lastUsed), just unpin works

  await pool.closeAll();
});

test('withAdapter holds refCount for duration of callback', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-pool-with-'));
  cleanupPaths.push(dataDir);

  const created = new Map<string, TrackedAdapter>();
  const base = trackedAdapter('base');

  // maxSize=2: one pinned "held" + one free slot so eviction can reclaim
  // unpinned entries without pool_exhausted.
  const pool = new ConnectionPool(
    base,
    2,
    {
      mode: 'database_per_tenant',
      resolveConnection: async (tenantId) => ({
        path: join(dataDir, `${tenantId}.db`),
        tenantId,
      }),
    },
    async (config) => {
      const tid = String(config['tenantId']);
      const a = trackedAdapter(tid);
      created.set(tid, a);
      return a;
    },
  );

  await pool.withAdapter('held', async (adapter) => {
    expect((adapter as TrackedAdapter).closed).toBe(false);
    // Fill free slot, then force eviction of the unpinned entry
    await pool.get('other-1');
    await pool.get('other-2');
    expect((adapter as TrackedAdapter).closed).toBe(false);
    expect(created.get('held')!.closed).toBe(false);
  });

  expect(created.get('held')!.closed).toBe(false);
  await pool.closeAll();
});

test('pool get still works under pin for concurrent tenants', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-pool-get-'));
  cleanupPaths.push(dataDir);

  const pool = new ConnectionPool(
    trackedAdapter('base'),
    5,
    {
      mode: 'database_per_tenant',
      resolveConnection: async (tenantId) => ({
        path: join(dataDir, `${tenantId}.db`),
        tenantId,
      }),
    },
    async (config) => new BunSQLiteAdapter({ path: String(config['path']) }),
  );

  const a1 = await pool.get('t1');
  await pool.pin('t1');
  const a2 = await pool.get('t1');
  expect(a1).toBe(a2);
  pool.unpin('t1');
  await pool.closeAll();
});

test('pool.close(tenantId) throws pool_close_in_use while pinned', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-pool-close-pin-'));
  cleanupPaths.push(dataDir);

  const created = new Map<string, TrackedAdapter>();
  const pool = new ConnectionPool(
    trackedAdapter('base'),
    5,
    {
      mode: 'database_per_tenant',
      resolveConnection: async (tenantId) => ({
        path: join(dataDir, `${tenantId}.db`),
        tenantId,
      }),
    },
    async (config) => {
      const tid = String(config['tenantId']);
      const a = trackedAdapter(tid);
      created.set(tid, a);
      return a;
    },
  );

  await pool.get('tenant-x');
  await pool.pin('tenant-x');

  await expect(pool.close('tenant-x')).rejects.toThrow('pool_close_in_use');
  expect(created.get('tenant-x')!.closed).toBe(false);
  expect(pool.size).toBe(1);

  pool.unpin('tenant-x');
  await pool.close('tenant-x');
  expect(created.get('tenant-x')!.closed).toBe(true);
  expect(pool.size).toBe(0);

  await pool.closeAll();
});
