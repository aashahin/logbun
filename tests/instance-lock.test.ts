import { makeFileReliability } from './helpers';
import { afterEach, expect, test } from 'bun:test';
import { mkdir, mkdtemp, readFile, rm, unlink, writeFile } from 'node:fs/promises';
import type { FileHandle } from 'node:fs/promises';
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

test('metadata write failure closes and removes only the lock file it created', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-write-failure-'));
  cleanupPaths.push(dataDir);
  let createdHandle: FileHandle | undefined;
  const failed = new InstanceLock('write-failure', dataDir, {
    writeMetadata: async (handle) => {
      createdHandle = handle;
      const error = new Error('simulated metadata write failure') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });

  await expect(failed.acquire()).rejects.toThrow(/metadata write failure/);
  await expect(createdHandle!.stat()).rejects.toThrow();

  const retry = new InstanceLock('write-failure', dataDir);
  await expect(retry.acquire()).resolves.toBeUndefined();
  await retry.release();
});

test('metadata sync failure after writing still closes, cleans up, and permits retry', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-sync-failure-'));
  cleanupPaths.push(dataDir);
  let createdHandle: FileHandle | undefined;
  const failed = new InstanceLock('sync-failure', dataDir, {
    writeMetadata: async (handle, metadata) => {
      createdHandle = handle;
      await handle.writeFile(metadata, 'utf8');
      const error = new Error('simulated metadata sync failure') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });

  await expect(failed.acquire()).rejects.toThrow(/metadata sync failure/);
  await expect(createdHandle!.stat()).rejects.toThrow();

  const retry = new InstanceLock('sync-failure', dataDir);
  await expect(retry.acquire()).resolves.toBeUndefined();
  await retry.release();
});

test('metadata write failure never unlinks a replacement lock', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-write-replaced-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'write-replaced');
  const lockPath = join(namespaceDir, '.instance.lock');
  let createdHandle: FileHandle | undefined;
  const failed = new InstanceLock('write-replaced', dataDir, {
    writeMetadata: async (handle, metadata) => {
      createdHandle = handle;
      await handle.writeFile(metadata, 'utf8');
      await handle.sync();
      await unlink(lockPath);
      await writeFile(lockPath, '2147483646\n0\n');
      const error = new Error('simulated failure after replacement') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });

  await expect(failed.acquire()).rejects.toThrow(/failure after replacement/);
  await expect(createdHandle!.stat()).rejects.toThrow();
  expect(await readFile(lockPath, 'utf8')).toBe('2147483646\n0\n');

  const contender = new InstanceLock('write-replaced', dataDir, {
    killProcess: () => undefined,
  });
  await expect(contender.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(await readFile(lockPath, 'utf8')).toBe('2147483646\n0\n');
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

test('concurrent stale recovery never unlinks the owner installed after a shared probe', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-stale-race-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'stale-race');
  const lockPath = join(namespaceDir, '.instance.lock');
  await mkdir(namespaceDir);
  await writeFile(lockPath, '2147483646\n0\n');

  let observedCount = 0;
  let resolveBothObserved!: () => void;
  const bothObserved = new Promise<void>((resolve) => {
    resolveBothObserved = resolve;
  });
  let releaseFirst!: () => void;
  const firstMayRecover = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  let releaseSecond!: () => void;
  const secondMayRecover = new Promise<void>((resolve) => {
    releaseSecond = resolve;
  });
  const staleProbe = () => {
    const error = new Error('process does not exist') as NodeJS.ErrnoException;
    error.code = 'ESRCH';
    throw error;
  };
  const afterProbe = (release: Promise<void>) => async () => {
    observedCount++;
    if (observedCount === 2) resolveBothObserved();
    await release;
  };
  const first = new InstanceLock('stale-race', dataDir, {
    killProcess: staleProbe,
    afterStaleProbe: afterProbe(firstMayRecover),
  });
  const second = new InstanceLock('stale-race', dataDir, {
    killProcess: staleProbe,
    afterStaleProbe: afterProbe(secondMayRecover),
  });

  const firstAcquire = first.acquire();
  const secondAcquire = second.acquire();
  const reachedBarrier = await Promise.race([
    bothObserved.then(() => true),
    new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 250)),
  ]);
  if (!reachedBarrier) {
    releaseFirst();
    releaseSecond();
    await Promise.allSettled([firstAcquire, secondAcquire]);
  }
  expect(reachedBarrier).toBe(true);

  releaseFirst();
  await expect(firstAcquire).resolves.toBeUndefined();
  releaseSecond();
  await expect(secondAcquire).rejects.toBeInstanceOf(InstanceLockError);

  await second.release();
  const third = new InstanceLock('stale-race', dataDir);
  await expect(third.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(await readFile(lockPath, 'utf8')).toStartWith(`${process.pid}\n`);
  await first.release();
  await third.acquire();
  await third.release();
});

test('stale recovery fails closed when the owner is replaced after identity check', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-stale-post-check-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'stale-post-check');
  const lockPath = join(namespaceDir, '.instance.lock');
  await mkdir(namespaceDir);
  await writeFile(lockPath, '2147483646\n0\n');
  let replaced = false;
  const lock = new InstanceLock('stale-post-check', dataDir, {
    killProcess: () => {
      const error = new Error('process does not exist') as NodeJS.ErrnoException;
      error.code = 'ESRCH';
      throw error;
    },
    beforeOwnedUnlink: async () => {
      if (replaced) return;
      replaced = true;
      await unlink(lockPath);
      await writeFile(lockPath, `${process.pid}\n0\n`);
    },
  });

  await expect(lock.acquire()).rejects.toThrow(/owner changed during stale recovery/);
  expect(await readFile(lockPath, 'utf8')).toBe(`${process.pid}\n0\n`);
  await lock.release();

  const contender = new InstanceLock('stale-post-check', dataDir);
  await expect(contender.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(await readFile(lockPath, 'utf8')).toBe(`${process.pid}\n0\n`);
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
