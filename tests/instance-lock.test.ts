import { makeFileReliability } from './helpers';
import { afterEach, expect, test } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { InstanceLock, InstanceLockError } from '../src/durability/filesystem';
import { AuditLogger } from '../src/logger';
import { BunSQLiteAdapter } from '../src/adapters/bun-sqlite';

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

test('releasing a failed same-process contender cannot remove the owner lock', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-release-owner-'));
  cleanupPaths.push(dataDir);
  const owner = new InstanceLock('same-process', dataDir);
  const failed = new InstanceLock('same-process', dataDir);
  const third = new InstanceLock('same-process', dataDir);
  await owner.acquire();
  await expect(failed.acquire()).rejects.toBeInstanceOf(InstanceLockError);

  await failed.release();
  await expect(third.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  await owner.release();
  await third.acquire();
  await third.release();
});

test.each([
  { name: 'EPERM', error: Object.assign(new Error('not permitted'), { code: 'EPERM' }) },
  { name: 'EACCES', error: Object.assign(new Error('access denied'), { code: 'EACCES' }) },
  { name: 'Deno NotCapable', error: Object.assign(new Error('not capable'), { name: 'NotCapable' }) },
  { name: 'unknown', error: new Error('unknown liveness failure') },
])('InstanceLock treats $name liveness failures as potentially alive', async ({ error }) => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-fail-closed-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'fail-closed');
  const lockPath = join(namespaceDir, '.instance.lock');
  await mkdir(namespaceDir);
  await writeFile(lockPath, '2147483646\n0\n');

  const lock = new InstanceLock('fail-closed', dataDir, {
    killProcess: () => {
      throw error;
    },
  });
  await expect(lock.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(await readFile(lockPath, 'utf8')).toBe('2147483646\n0\n');
});

test('InstanceLock fails closed when an existing lock owner cannot be parsed', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-invalid-owner-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'invalid-owner');
  const lockPath = join(namespaceDir, '.instance.lock');
  await mkdir(namespaceDir);
  await writeFile(lockPath, 'not-a-pid\n');

  const lock = new InstanceLock('invalid-owner', dataDir, {
    killProcess: () => {
      throw new Error('probe must not run for malformed owner metadata');
    },
  });
  await expect(lock.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(await readFile(lockPath, 'utf8')).toBe('not-a-pid\n');
});

test('InstanceLock recovers a stale lock only when the owner probe returns ESRCH', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-esrch-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'known-stale');
  const lockPath = join(namespaceDir, '.instance.lock');
  await mkdir(namespaceDir);
  await writeFile(lockPath, '2147483646\n0\n');

  const lock = new InstanceLock('known-stale', dataDir, {
    killProcess: () => {
      const error = new Error('process does not exist') as NodeJS.ErrnoException;
      error.code = 'ESRCH';
      throw error;
    },
  });
  await lock.acquire();
  expect(await readFile(lockPath, 'utf8')).toStartWith(`${process.pid}\n`);
  await lock.release();
});

test('durable mode acquires instance lock by default; second logger degrades', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-aud-'));
  cleanupPaths.push(dataDir);

  const audit1 = new AuditLogger({
    namespace: 'same-ns',
    reliability: makeFileReliability('same-ns', dataDir, {
      instanceLock: true,
    }),
    mode: 'durable',
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a1.db') }),
  });
  await audit1.ready;
  expect(audit1.degraded).toBe(false);

  const audit2 = new AuditLogger({
    namespace: 'same-ns',
    reliability: makeFileReliability('same-ns', dataDir, {
      instanceLock: true,
    }),
    mode: 'durable',
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a2.db') }),
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
    reliability: makeFileReliability('shared', dataDir),
    mode: 'durable',
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'b1.db') }),
  });
  const audit2 = new AuditLogger({
    namespace: 'shared',
    reliability: makeFileReliability('shared', dataDir),
    mode: 'durable',
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'b2.db') }),
  });
  await Promise.all([audit1.ready, audit2.ready]);
  expect(audit1.degraded).toBe(false);
  expect(audit2.degraded).toBe(false);
  await audit1.shutdown();
  await audit2.shutdown();
});
