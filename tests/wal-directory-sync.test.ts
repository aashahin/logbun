import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { WALStorage } from '../src/durability/filesystem';
import type { LogbunLog } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function log(id: string, padding = 0): LogbunLog {
  return {
    id,
    actorId: 'directory-sync',
    action: 'wal.directory-sync',
    createdAt: new Date().toISOString(),
    metadata: padding > 0 ? { padding: 'x'.repeat(padding) } : undefined,
  };
}

test('WAL initialization syncs newly created file entries before becoming ready', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-dir-sync-init-'));
  cleanupPaths.push(dataDir);
  const walDir = join(dataDir, 'initialize', 'wal');
  const syncReasons: string[] = [];
  const wal = new WALStorage('initialize', dataDir, {
    fsync: true,
    directorySync: async (_directory, reason) => {
      syncReasons.push(reason);
      if (reason === 'initialize') {
        const entries = await readdir(walDir);
        expect(entries).toContain('current.aof');
        expect(entries).toContain('acked.ids');
      }
    },
  });

  await wal.init();
  expect(syncReasons).toContain('initialize');
  await wal.close();
});

test('fresh WAL initialization publishes its hierarchy child before parent', async () => {
  const rootParent = await mkdtemp(join(tmpdir(), 'logbun-wal-hierarchy-order-'));
  cleanupPaths.push(rootParent);
  const dataDir = join(rootParent, 'fresh-data');
  const namespaceDir = join(dataDir, 'hierarchy-order');
  const walDir = join(namespaceDir, 'wal');
  const syncs: Array<{ directory: string; reason: string }> = [];
  const wal = new WALStorage('hierarchy-order', dataDir, {
    fsync: true,
    directorySync: (directory, reason) => {
      syncs.push({ directory, reason });
    },
  });

  await wal.init();
  expect(syncs).toEqual([
    { directory: walDir, reason: 'initialize' },
    { directory: namespaceDir, reason: 'initialize-hierarchy' },
    { directory: dataDir, reason: 'initialize-hierarchy' },
    { directory: rootParent, reason: 'initialize-hierarchy' },
  ]);
  await wal.close();
});

test('fresh WAL initialization publishes every recursively created dataDir ancestor', async () => {
  const rootParent = await mkdtemp(join(tmpdir(), 'logbun-wal-hierarchy-nested-'));
  cleanupPaths.push(rootParent);
  const firstMissing = join(rootParent, 'missing-a');
  const dataDir = join(firstMissing, 'missing-b');
  const namespaceDir = join(dataDir, 'hierarchy-nested');
  const walDir = join(namespaceDir, 'wal');
  const syncs: Array<{ directory: string; reason: string }> = [];
  const wal = new WALStorage('hierarchy-nested', dataDir, {
    fsync: true,
    directorySync: (directory, reason) => {
      syncs.push({ directory, reason });
    },
  });

  await wal.init();
  expect(syncs).toEqual([
    { directory: walDir, reason: 'initialize' },
    { directory: namespaceDir, reason: 'initialize-hierarchy' },
    { directory: dataDir, reason: 'initialize-hierarchy' },
    { directory: firstMissing, reason: 'initialize-hierarchy' },
    { directory: rootParent, reason: 'initialize-hierarchy' },
  ]);
  await wal.close();
});

test.each(['namespace', 'dataDir', 'parent'] as const)(
  'WAL retains and retries unexpected %s hierarchy sync debt before admission',
  async (level) => {
    const rootParent = await mkdtemp(join(tmpdir(), `logbun-wal-hierarchy-${level}-`));
    cleanupPaths.push(rootParent);
    const dataDir = join(rootParent, 'fresh-data');
    const namespaceDir = join(dataDir, `hierarchy-${level}`);
    const targets = {
      namespace: namespaceDir,
      dataDir,
      parent: rootParent,
    };
    const target = targets[level];
    const syncs: Array<{ directory: string; reason: string }> = [];
    let injectedFailure = false;
    const wal = new WALStorage(`hierarchy-${level}`, dataDir, {
      fsync: true,
      directorySync: (directory, reason) => {
        syncs.push({ directory, reason });
        if (
          !injectedFailure &&
          directory === target &&
          reason === 'initialize-hierarchy'
        ) {
          injectedFailure = true;
          const error = new Error(`simulated ${level} hierarchy sync failure`) as NodeJS.ErrnoException;
          error.code = 'EIO';
          throw error;
        }
      },
    });

    await expect(wal.init()).rejects.toThrow(new RegExp(`${level} hierarchy sync failure`));
    await expect(wal.append(log(`blocked-${level}`))).rejects.toThrow(/not initialized/);
    const retryStart = syncs.length;
    await expect(wal.init()).resolves.toBeUndefined();
    expect(syncs[retryStart]).toEqual({
      directory: target,
      reason: 'initialize-hierarchy',
    });
    await expect(wal.append(log(`accepted-${level}`))).resolves.toBeUndefined();
    await wal.close();
  },
);

test.each(['namespace', 'dataDir', 'firstMissing', 'parent'] as const)(
  'replacement WAL conservatively republishes every ancestor after discarded %s hierarchy debt',
  async (level) => {
    const rootParent = await mkdtemp(join(tmpdir(), `logbun-wal-replacement-${level}-`));
    cleanupPaths.push(rootParent);
    const firstMissing = join(rootParent, 'missing-a');
    const dataDir = join(firstMissing, 'missing-b');
    const namespace = `replacement-${level}`;
    const namespaceDir = join(dataDir, namespace);
    const targets = {
      namespace: namespaceDir,
      dataDir,
      firstMissing,
      parent: rootParent,
    };
    const failedTarget = targets[level];
    const discarded = new WALStorage(namespace, dataDir, {
      fsync: true,
      directorySync: (directory, reason) => {
        if (directory !== failedTarget || reason !== 'initialize-hierarchy') return;
        const error = new Error(`discarded ${level} hierarchy debt`) as NodeJS.ErrnoException;
        error.code = 'EIO';
        throw error;
      },
    });

    await expect(discarded.init()).rejects.toThrow(new RegExp(`discarded ${level}`));

    const replacementSyncs: string[] = [];
    const replacement = new WALStorage(namespace, dataDir, {
      fsync: true,
      directorySync: (directory, reason) => {
        if (reason === 'initialize-hierarchy') replacementSyncs.push(directory);
      },
    });
    await replacement.init();
    expect(replacementSyncs.slice(0, 4)).toEqual([
      namespaceDir,
      dataDir,
      firstMissing,
      rootParent,
    ]);
    await expect(replacement.append(log(`accepted-after-${level}-replacement`))).resolves.toBeUndefined();
    await replacement.close();
  },
);

test('WAL treats a Deno capability denial at the dataDir parent as best-effort', async () => {
  const rootParent = await mkdtemp(join(tmpdir(), 'logbun-wal-hierarchy-deno-'));
  cleanupPaths.push(rootParent);
  const dataDir = join(rootParent, 'fresh-data');
  let parentAttempted = false;
  const wal = new WALStorage('hierarchy-deno', dataDir, {
    fsync: true,
    directorySync: (directory, reason) => {
      if (directory !== rootParent || reason !== 'initialize-hierarchy') return;
      parentAttempted = true;
      const error = new Error('parent is outside the Deno grant') as NodeJS.ErrnoException;
      error.code = 'ERR_DENO_NOT_CAPABLE';
      error.name = 'NotCapable';
      throw error;
    },
  });

  await expect(wal.init()).resolves.toBeUndefined();
  expect(parentAttempted).toBe(true);
  await expect(wal.append(log('deno-capability-boundary'))).resolves.toBeUndefined();
  await wal.close();
});

test('WAL initialization retries unexpected directory-sync failure before becoming ready', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-dir-sync-init-retry-'));
  cleanupPaths.push(dataDir);
  let syncAttempts = 0;
  const wal = new WALStorage('initialize-retry', dataDir, {
    fsync: true,
    directorySync: async (_directory, reason) => {
      if (reason !== 'initialize') return;
      syncAttempts++;
      if (syncAttempts === 1) {
        const error = new Error('simulated initialization sync failure') as NodeJS.ErrnoException;
        error.code = 'EIO';
        throw error;
      }
    },
  });

  await expect(wal.init()).rejects.toThrow(/initialization sync failure/);
  await expect(wal.append(log('not-ready'))).rejects.toThrow(/not initialized/);
  await expect(wal.init()).resolves.toBeUndefined();
  expect(syncAttempts).toBe(2);
  await wal.close();
});

test.each(['EINVAL', 'ENOTSUP', 'EOPNOTSUPP', 'EPERM'])(
  'WAL treats unsupported directory fsync code %s as best-effort',
  async (code) => {
    const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-dir-sync-unsupported-'));
    cleanupPaths.push(dataDir);
    const wal = new WALStorage(`unsupported-${code.toLowerCase()}`, dataDir, {
      fsync: true,
      directorySync: async () => {
        const error = new Error(`unsupported directory sync: ${code}`) as NodeJS.ErrnoException;
        error.code = code;
        throw error;
      },
    });

    await expect(wal.init()).resolves.toBeUndefined();
    await expect(wal.append(log(`accepted-${code}`))).resolves.toBeUndefined();
    await wal.close();
  },
);

test('WAL rotation syncs its directory after publishing the segment and new current file', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-dir-sync-rotate-'));
  cleanupPaths.push(dataDir);
  const walDir = join(dataDir, 'rotate', 'wal');
  const syncReasons: string[] = [];
  const wal = new WALStorage('rotate', dataDir, {
    fsync: true,
    hardMaxBytes: false,
    segmentBytes: 256,
    directorySync: async (_directory, reason) => {
      syncReasons.push(reason);
      if (reason === 'rotate') {
        const entries = await readdir(walDir);
        expect(entries).toContain('current.aof');
        expect(entries.some((entry) => /^seg-\d+\.aof$/.test(entry))).toBe(true);
      }
    },
  });
  await wal.init();
  await wal.append(log('sealed', 512));
  await wal.append(log('current'));

  expect(syncReasons).toContain('rotate');
  await wal.close();
});

test('WAL retries failed rotation directory sync before a later append can succeed', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-dir-sync-rotate-retry-'));
  cleanupPaths.push(dataDir);
  let rotationSyncAttempts = 0;
  const wal = new WALStorage('rotate-retry', dataDir, {
    fsync: true,
    hardMaxBytes: false,
    segmentBytes: 256,
    directorySync: async (_directory, reason) => {
      if (reason !== 'rotate') return;
      rotationSyncAttempts++;
      if (rotationSyncAttempts === 1) {
        const error = new Error('simulated rotation sync failure') as NodeJS.ErrnoException;
        error.code = 'EIO';
        throw error;
      }
    },
  });
  await wal.init();
  await wal.append(log('sealed', 512));

  await expect(wal.append(log('rejected'))).rejects.toThrow(/rotation sync failure/);
  expect(rotationSyncAttempts).toBe(1);
  await expect(wal.append(log('accepted-after-retry'))).resolves.toBeUndefined();
  expect(rotationSyncAttempts).toBe(2);
  expect((await wal.readAll()).map((entry) => entry.id)).toEqual([
    'sealed',
    'accepted-after-retry',
  ]);
  await wal.close();
});

test('WAL compaction syncs a sealed-segment deletion before clearing ack state', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-dir-sync-delete-'));
  cleanupPaths.push(dataDir);
  const walDir = join(dataDir, 'delete-segment', 'wal');
  const ackPath = join(walDir, 'acked.ids');
  let sawDeleteSync = false;
  const wal = new WALStorage('delete-segment', dataDir, {
    fsync: true,
    hardMaxBytes: false,
    segmentBytes: 256,
    compactAckThreshold: 1,
    directorySync: async (_directory, reason) => {
      if (reason !== 'delete-segment') return;
      sawDeleteSync = true;
      const entries = await readdir(walDir);
      expect(entries.some((entry) => /^seg-\d+\.aof$/.test(entry))).toBe(false);
      expect(await readFile(ackPath, 'utf8')).toContain('sealed');
      const error = new Error('simulated directory fsync failure') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });
  await wal.init();
  await wal.append(log('sealed', 512));
  await wal.append(log('current'));

  await expect(wal.acknowledge(['sealed'])).rejects.toThrow(/directory fsync failure/);
  expect(sawDeleteSync).toBe(true);
  expect(await readFile(ackPath, 'utf8')).toContain('sealed');
  await wal.close();
});

test('WAL compaction syncs a rewritten current file before clearing ack state', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-dir-sync-rewrite-'));
  cleanupPaths.push(dataDir);
  const walDir = join(dataDir, 'rewrite-current', 'wal');
  const currentPath = join(walDir, 'current.aof');
  const ackPath = join(walDir, 'acked.ids');
  let sawRewriteSync = false;
  const wal = new WALStorage('rewrite-current', dataDir, {
    fsync: true,
    compactAckThreshold: 1,
    directorySync: async (_directory, reason) => {
      if (reason !== 'rewrite' || sawRewriteSync) return;
      sawRewriteSync = true;
      expect(await readFile(currentPath, 'utf8')).toBe('');
      expect(await readFile(ackPath, 'utf8')).toContain('rewrite-me');
      const error = new Error('simulated rewrite directory fsync failure') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });
  await wal.init();
  await wal.append(log('rewrite-me'));

  await expect(wal.acknowledge(['rewrite-me'])).rejects.toThrow(
    /rewrite directory fsync failure/,
  );
  expect(sawRewriteSync).toBe(true);
  expect(await readFile(ackPath, 'utf8')).toContain('rewrite-me');
  await wal.close();
});

test('WAL truncate syncs sealed-segment deletion before resetting its ack sidecar', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-dir-sync-truncate-'));
  cleanupPaths.push(dataDir);
  const walDir = join(dataDir, 'truncate-segments', 'wal');
  const ackPath = join(walDir, 'acked.ids');
  let rejectDeleteSync = false;
  const wal = new WALStorage('truncate-segments', dataDir, {
    fsync: true,
    hardMaxBytes: false,
    segmentBytes: 256,
    compactAckThreshold: 100,
    directorySync: async (_directory, reason) => {
      if (!rejectDeleteSync || reason !== 'delete-segment') return;
      expect(await readFile(ackPath, 'utf8')).toContain('sealed');
      const error = new Error('simulated truncate directory fsync failure') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });
  await wal.init();
  await wal.append(log('sealed', 512));
  await wal.append(log('current'));
  await wal.acknowledge(['sealed', 'current']);

  rejectDeleteSync = true;
  await expect(wal.truncate()).rejects.toThrow(/truncate directory fsync failure/);
  expect(await readFile(ackPath, 'utf8')).toContain('sealed');
  await wal.close();
});
