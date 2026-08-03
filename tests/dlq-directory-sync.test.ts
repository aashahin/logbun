import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  DLQStorage,
  FileReliabilityAdapter,
} from '../src/durability/filesystem';
import type { LogbunLog } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function log(id: string): LogbunLog {
  return {
    id,
    actorId: 'directory-sync',
    action: 'dlq.directory-sync',
    createdAt: new Date().toISOString(),
  };
}

test('fresh DLQ initialization publishes every recursively created directory child before parent', async () => {
  const rootParent = await mkdtemp(join(tmpdir(), 'logbun-dlq-hierarchy-order-'));
  cleanupPaths.push(rootParent);
  const firstMissing = join(rootParent, 'missing-a');
  const dataDir = join(firstMissing, 'missing-b');
  const namespaceDir = join(dataDir, 'hierarchy-order');
  const syncs: Array<{ directory: string; reason: string }> = [];
  const dlq = new DLQStorage('hierarchy-order', dataDir, {
    fsync: true,
    directorySync: (directory, reason) => {
      syncs.push({ directory, reason });
    },
  });

  await dlq.init();
  expect(syncs).toEqual([
    { directory: namespaceDir, reason: 'initialize-hierarchy' },
    { directory: dataDir, reason: 'initialize-hierarchy' },
    { directory: firstMissing, reason: 'initialize-hierarchy' },
    { directory: rootParent, reason: 'initialize-hierarchy' },
  ]);
});

test.each(['namespace', 'dataDir', 'parent'] as const)(
  'DLQ retains and retries unexpected %s hierarchy sync debt before admission',
  async (level) => {
    const rootParent = await mkdtemp(join(tmpdir(), `logbun-dlq-hierarchy-${level}-`));
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
    const dlq = new DLQStorage(`hierarchy-${level}`, dataDir, {
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

    await expect(dlq.init()).rejects.toThrow(new RegExp(`${level} hierarchy sync failure`));
    await expect(dlq.write('tenant-a', [log(`blocked-${level}`)])).rejects.toThrow(
      /not initialized/,
    );
    const retryStart = syncs.length;
    await expect(dlq.init()).resolves.toBeUndefined();
    expect(syncs[retryStart]).toEqual({
      directory: target,
      reason: 'initialize-hierarchy',
    });
    await expect(dlq.write('tenant-a', [log(`accepted-${level}`)])).resolves.toBeString();
  },
);

test('DLQ treats a Deno capability denial at the outer created parent as best-effort', async () => {
  const rootParent = await mkdtemp(join(tmpdir(), 'logbun-dlq-hierarchy-deno-'));
  cleanupPaths.push(rootParent);
  const dataDir = join(rootParent, 'fresh-data');
  let parentAttempted = false;
  const dlq = new DLQStorage('hierarchy-deno', dataDir, {
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

  await expect(dlq.init()).resolves.toBeUndefined();
  expect(parentAttempted).toBe(true);
  await expect(dlq.write('tenant-a', [log('deno-capability-boundary')])).resolves.toBeString();
});

test('adapter first-run sync order publishes WAL hierarchy before the later DLQ directory', async () => {
  const rootParent = await mkdtemp(join(tmpdir(), 'logbun-adapter-hierarchy-order-'));
  cleanupPaths.push(rootParent);
  const firstMissing = join(rootParent, 'missing-a');
  const dataDir = join(firstMissing, 'missing-b');
  const namespaceDir = join(dataDir, 'adapter-order');
  const walDir = join(namespaceDir, 'wal');
  const syncs: Array<{ owner: 'wal' | 'dlq'; directory: string; reason: string }> = [];
  const adapter = new FileReliabilityAdapter(
    {
      namespace: 'adapter-order',
      dataDir,
      wal: {
        fsync: true,
      },
      dlq: {
        fsync: true,
      },
    },
    {
      walDirectorySync: (directory, reason) => {
        syncs.push({ owner: 'wal', directory, reason });
      },
      dlqDirectorySync: (directory, reason) => {
        syncs.push({ owner: 'dlq', directory, reason });
      },
    },
  );

  await adapter.init();
  expect(syncs).toEqual([
    { owner: 'wal', directory: walDir, reason: 'initialize' },
    { owner: 'wal', directory: namespaceDir, reason: 'initialize-hierarchy' },
    { owner: 'wal', directory: dataDir, reason: 'initialize-hierarchy' },
    { owner: 'wal', directory: firstMissing, reason: 'initialize-hierarchy' },
    { owner: 'wal', directory: rootParent, reason: 'initialize-hierarchy' },
    { owner: 'dlq', directory: namespaceDir, reason: 'initialize-hierarchy' },
    { owner: 'dlq', directory: dataDir, reason: 'initialize-hierarchy' },
    { owner: 'dlq', directory: firstMissing, reason: 'initialize-hierarchy' },
    { owner: 'dlq', directory: rootParent, reason: 'initialize-hierarchy' },
  ]);
  await adapter.close();
});

test('adapter DLQ publishes the full first-run hierarchy when WAL fsync is disabled', async () => {
  const rootParent = await mkdtemp(join(tmpdir(), 'logbun-adapter-dlq-only-sync-'));
  cleanupPaths.push(rootParent);
  const firstMissing = join(rootParent, 'missing-a');
  const dataDir = join(firstMissing, 'missing-b');
  const namespaceDir = join(dataDir, 'adapter-dlq-only');
  const syncs: Array<{ directory: string; reason: string }> = [];
  const adapter = new FileReliabilityAdapter(
    {
      namespace: 'adapter-dlq-only',
      dataDir,
      wal: { fsync: false },
      dlq: {
        fsync: true,
      },
    },
    {
      dlqDirectorySync: (directory, reason) => {
        syncs.push({ directory, reason });
      },
    },
  );

  await adapter.init();
  expect(syncs).toEqual([
    { directory: namespaceDir, reason: 'initialize-hierarchy' },
    { directory: dataDir, reason: 'initialize-hierarchy' },
    { directory: firstMissing, reason: 'initialize-hierarchy' },
    { directory: rootParent, reason: 'initialize-hierarchy' },
  ]);
  await adapter.close();
});
