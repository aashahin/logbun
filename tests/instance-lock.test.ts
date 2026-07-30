import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { InstanceLock, InstanceLockError } from '../src/storage/instance-lock';
import { AuditLogger } from '../src/logger';
import { BunSQLiteAdapter } from '../src/adapters/sqlite';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

test('InstanceLock exclusive: second acquire fails while first holds', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-'));
  cleanupPaths.push(dataDir);

  const a = new InstanceLock('ns-a', dataDir);
  const b = new InstanceLock('ns-a', dataDir);
  await a.acquire();
  await expect(b.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  await a.release();
  await b.acquire();
  await b.release();
});

test('durable mode acquires instance lock by default; second logger degrades', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-aud-'));
  cleanupPaths.push(dataDir);

  const audit1 = new AuditLogger({
    namespace: 'same-ns',
    mode: 'durable',
    dataDir,
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a1.db') }),
    wal: { fsync: false },
    dlqFsync: false,
  });
  await audit1.ready;
  expect(audit1.degraded).toBe(false);

  const audit2 = new AuditLogger({
    namespace: 'same-ns',
    mode: 'durable',
    dataDir,
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a2.db') }),
    wal: { fsync: false },
    dlqFsync: false,
  });
  await audit2.ready;
  expect(audit2.degraded).toBe(true);

  await audit1.shutdown();
  await audit2.shutdown();
});

test('instanceLock: false allows two durable loggers on same namespace', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-off-'));
  cleanupPaths.push(dataDir);

  const audit1 = new AuditLogger({
    namespace: 'shared',
    mode: 'durable',
    dataDir,
    instanceLock: false,
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'b1.db') }),
    wal: { fsync: false },
    dlqFsync: false,
  });
  const audit2 = new AuditLogger({
    namespace: 'shared',
    mode: 'durable',
    dataDir,
    instanceLock: false,
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'b2.db') }),
    wal: { fsync: false },
    dlqFsync: false,
  });
  await Promise.all([audit1.ready, audit2.ready]);
  expect(audit1.degraded).toBe(false);
  expect(audit2.degraded).toBe(false);
  await audit1.shutdown();
  await audit2.shutdown();
});
