import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { FileReliabilityAdapter } from '../src/durability/filesystem';
import { setFileReliabilityAdapterTestHooks } from '../src/durability/filesystem/adapter-test-hooks';
import type { LogbunLog } from '../src/types';

const cleanupPaths: string[] = [];

function log(id: string): LogbunLog {
  return {
    id,
    actorId: 'adapter-lifecycle',
    action: 'adapter.lifecycle',
    createdAt: new Date().toISOString(),
  };
}

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

test('failed initialization releases local resources and the same adapter can retry', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-adapter-init-retry-'));
  cleanupPaths.push(dataDir);
  const namespace = 'adapter-init-retry';
  let failOnce = true;
  const options = { namespace, dataDir };
  setFileReliabilityAdapterTestHooks(options, {
    dlqDirectorySync: (_directory, reason) => {
      if (!failOnce || reason !== 'initialize-hierarchy') return;
      failOnce = false;
      const error = new Error('simulated adapter DLQ initialization failure') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });
  const adapter = new FileReliabilityAdapter(options);

  await expect(adapter.init()).rejects.toThrow(/DLQ initialization failure/);
  expect(adapter.walStorage).toBeNull();
  expect(adapter.dlqStorage).toBeNull();
  await expect(adapter.init()).resolves.toBeUndefined();
  await adapter.close();
});

test('retry after discarded WAL hierarchy debt republishes every ancestor before admission', async () => {
  const rootParent = await mkdtemp(join(tmpdir(), 'logbun-adapter-wal-debt-'));
  cleanupPaths.push(rootParent);
  const firstMissing = join(rootParent, 'missing-a');
  const dataDir = join(firstMissing, 'missing-b');
  const namespace = 'adapter-wal-debt';
  let failedOnce = false;
  const replacementHierarchySyncs: string[] = [];
  const options = {
    namespace,
    dataDir,
    dlq: { fsync: false },
  };
  setFileReliabilityAdapterTestHooks(options, {
    walDirectorySync: (directory, reason) => {
      if (reason !== 'initialize-hierarchy') return;
      if (!failedOnce && directory === rootParent) {
        failedOnce = true;
        const error = new Error('discarded adapter WAL hierarchy debt') as NodeJS.ErrnoException;
        error.code = 'EIO';
        throw error;
      }
      if (failedOnce) replacementHierarchySyncs.push(directory);
    },
  });
  const adapter = new FileReliabilityAdapter(options);

  await expect(adapter.init()).rejects.toThrow(/discarded adapter WAL hierarchy debt/);
  replacementHierarchySyncs.length = 0;
  await expect(adapter.init()).resolves.toBeUndefined();
  expect(replacementHierarchySyncs.slice(0, 4)).toEqual([
    join(dataDir, namespace),
    dataDir,
    firstMissing,
    rootParent,
  ]);
  await expect(adapter.appendJournal(log('accepted-after-adapter-retry'))).resolves.toBeUndefined();
  await adapter.close();
});

test('concurrent init calls share one initialization flight and one instance lock', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-adapter-init-single-flight-'));
  cleanupPaths.push(dataDir);
  const namespace = 'adapter-init-single-flight';
  let enteredResolve!: () => void;
  const entered = new Promise<void>((resolve) => { enteredResolve = resolve; });
  let continueResolve!: () => void;
  const mayContinue = new Promise<void>((resolve) => { continueResolve = resolve; });
  let lockAcquires = 0;
  const options = { namespace, dataDir };
  setFileReliabilityAdapterTestHooks(options, {
    afterLockAcquire: async () => {
      lockAcquires++;
      enteredResolve();
      await mayContinue;
    },
  });
  const adapter = new FileReliabilityAdapter(options);

  const first = adapter.init();
  await entered;
  const second = adapter.init();
  expect(second).toBe(first);
  continueResolve();
  await expect(Promise.all([first, second])).resolves.toEqual([undefined, undefined]);
  expect(lockAcquires).toBe(1);
  await adapter.close();
});

test('close waits for in-flight initialization, releases it, and permits a successor', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-adapter-close-init-'));
  cleanupPaths.push(dataDir);
  const namespace = 'adapter-close-init';
  let enteredResolve!: () => void;
  const entered = new Promise<void>((resolve) => { enteredResolve = resolve; });
  let continueResolve!: () => void;
  const mayContinue = new Promise<void>((resolve) => { continueResolve = resolve; });
  const options = { namespace, dataDir };
  setFileReliabilityAdapterTestHooks(options, {
    afterLockAcquire: async () => {
      enteredResolve();
      await mayContinue;
    },
  });
  const adapter = new FileReliabilityAdapter(options);

  const initializing = adapter.init();
  await entered;
  const closing = adapter.close();
  continueResolve();
  await expect(Promise.all([initializing, closing])).resolves.toEqual([undefined, undefined]);
  expect(adapter.walStorage).toBeNull();
  expect(adapter.dlqStorage).toBeNull();

  const successor = new FileReliabilityAdapter({ namespace, dataDir });
  await expect(successor.init()).resolves.toBeUndefined();
  await successor.close();
});

test('close retains ownership until an admitted DLQ write finishes', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-adapter-close-write-'));
  cleanupPaths.push(dataDir);
  const namespace = 'adapter-close-write';
  let mutationReachedResolve!: () => void;
  const mutationReached = new Promise<void>((resolve) => { mutationReachedResolve = resolve; });
  let mutationMayFinishResolve!: () => void;
  const mutationMayFinish = new Promise<void>((resolve) => { mutationMayFinishResolve = resolve; });
  let blockMutation = false;
  let mutationFinished = false;
  const options = { namespace, dataDir };
  setFileReliabilityAdapterTestHooks(options, {
    dlqDirectorySync: async (_directory, reason) => {
      if (!blockMutation || reason !== 'mutation') return;
      blockMutation = false;
      mutationReachedResolve();
      await mutationMayFinish;
      mutationFinished = true;
    },
    beforeStorageClose: () => {
      if (!mutationFinished) throw new Error('storage close reached before admitted write settled');
    },
  });
  const adapter = new FileReliabilityAdapter(options);
  await adapter.init();

  blockMutation = true;
  const writing = adapter.writeDlq('tenant-a', [log('blocked-write')]);
  await mutationReached;
  const closing = adapter.close().then(
    () => null,
    (error: unknown) => error,
  );
  const lateWriteResult = adapter.writeDlq('tenant-a', [log('late-write')]).then(
    () => null,
    (error: unknown) => error,
  );

  const successor = new FileReliabilityAdapter({ namespace, dataDir });
  const earlySuccessorError = await successor.init().then(
    () => null,
    (error: unknown) => error,
  );
  if (earlySuccessorError === null) await successor.close();
  expect(String(earlySuccessorError)).toContain('instance_lock_held');

  mutationMayFinishResolve();
  await expect(writing).resolves.toBeString();
  const lateWriteError = await lateWriteResult;
  const closeError = await closing;
  if (closeError !== null) await adapter.close();
  expect(lateWriteError).not.toBeNull();
  expect(closeError).toBeNull();
  await expect(successor.init()).resolves.toBeUndefined();
  await successor.close();
});

test('close retains ownership until an admitted DLQ claim finishes', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-adapter-close-claim-'));
  cleanupPaths.push(dataDir);
  const namespace = 'adapter-close-claim';
  let mutationReachedResolve!: () => void;
  const mutationReached = new Promise<void>((resolve) => { mutationReachedResolve = resolve; });
  let mutationMayFinishResolve!: () => void;
  const mutationMayFinish = new Promise<void>((resolve) => { mutationMayFinishResolve = resolve; });
  let blockMutation = false;
  let mutationFinished = false;
  const options = { namespace, dataDir };
  setFileReliabilityAdapterTestHooks(options, {
    dlqDirectorySync: async (_directory, reason) => {
      if (!blockMutation || reason !== 'mutation') return;
      blockMutation = false;
      mutationReachedResolve();
      await mutationMayFinish;
      mutationFinished = true;
    },
    beforeStorageClose: () => {
      if (!mutationFinished) throw new Error('storage close reached before admitted claim settled');
    },
  });
  const adapter = new FileReliabilityAdapter(options);
  await adapter.init();
  const id = await adapter.writeDlq('tenant-a', [log('blocked-claim')]);

  blockMutation = true;
  const claiming = adapter.claimDlq(id);
  await mutationReached;
  const closing = adapter.close();
  const lateClaimResult = adapter.claimDlq(id).then(
    () => null,
    (error: unknown) => error,
  );
  const successor = new FileReliabilityAdapter({ namespace, dataDir });
  const earlySuccessorError = await successor.init().then(
    () => null,
    (error: unknown) => error,
  );
  if (earlySuccessorError === null) await successor.close();
  expect(String(earlySuccessorError)).toContain('instance_lock_held');

  mutationMayFinishResolve();
  await expect(claiming).resolves.toMatchObject({ id });
  expect(String(await lateClaimResult)).toContain('closing');
  await expect(closing).resolves.toBeUndefined();
  await expect(successor.init()).resolves.toBeUndefined();
  await successor.close();
});

test('close retains ownership through both mutations of an admitted failed settlement', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-adapter-close-settle-'));
  cleanupPaths.push(dataDir);
  const namespace = 'adapter-close-settle';
  let mutationReachedResolve!: () => void;
  const mutationReached = new Promise<void>((resolve) => { mutationReachedResolve = resolve; });
  let mutationMayFinishResolve!: () => void;
  const mutationMayFinish = new Promise<void>((resolve) => { mutationMayFinishResolve = resolve; });
  let blockMutation = false;
  let mutationFinished = false;
  const options = { namespace, dataDir };
  setFileReliabilityAdapterTestHooks(options, {
    dlqDirectorySync: async (_directory, reason) => {
      if (!blockMutation || reason !== 'mutation') return;
      blockMutation = false;
      mutationReachedResolve();
      await mutationMayFinish;
      mutationFinished = true;
    },
    beforeStorageClose: () => {
      if (!mutationFinished) throw new Error('storage close reached before admitted settlement settled');
    },
  });
  const adapter = new FileReliabilityAdapter(options);
  await adapter.init();
  const id = await adapter.writeDlq('tenant-a', [log('blocked-settlement')]);
  await adapter.claimDlq(id);

  blockMutation = true;
  const settling = adapter.settleDlqFailure(id, 1);
  await mutationReached;
  const closing = adapter.close();
  const lateSettlementResult = adapter.settleDlqFailure(id, 2).then(
    () => null,
    (error: unknown) => error,
  );
  const successor = new FileReliabilityAdapter({ namespace, dataDir });
  const earlySuccessorError = await successor.init().then(
    () => null,
    (error: unknown) => error,
  );
  if (earlySuccessorError === null) await successor.close();
  expect(String(earlySuccessorError)).toContain('instance_lock_held');

  mutationMayFinishResolve();
  await expect(settling).resolves.toBeUndefined();
  expect(String(await lateSettlementResult)).toContain('closing');
  await expect(closing).resolves.toBeUndefined();
  await expect(successor.init()).resolves.toBeUndefined();
  await successor.close();
});

test('close releases the namespace lock even when WAL close fails', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-adapter-close-error-'));
  cleanupPaths.push(dataDir);
  const namespace = 'adapter-close-error';
  const adapter = new FileReliabilityAdapter({ namespace, dataDir });
  await adapter.init();
  const wal = adapter.walStorage;
  if (!wal) throw new Error('expected initialized WAL');
  wal.close = async () => {
    throw new Error('simulated WAL close failure');
  };

  await expect(adapter.close()).rejects.toThrow(/simulated WAL close failure/);
  const successor = new FileReliabilityAdapter({ namespace, dataDir });
  await expect(successor.init()).resolves.toBeUndefined();
  await successor.close();
});

test('close retains a failed lock release for retry before permitting a successor', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-adapter-release-retry-'));
  cleanupPaths.push(dataDir);
  const namespace = 'adapter-release-retry';
  let failRelease = true;
  const options = { namespace, dataDir };
  setFileReliabilityAdapterTestHooks(options, {
    instanceLockOptions: {
      afterOwnedUnlinkCheck: () => {
        if (!failRelease) return;
        const error = new Error('simulated adapter lock release failure') as NodeJS.ErrnoException;
        error.code = 'EIO';
        throw error;
      },
    },
  });
  const adapter = new FileReliabilityAdapter(options);
  await adapter.init();

  await expect(adapter.close()).rejects.toThrow(/adapter lock release failure/);
  const successor = new FileReliabilityAdapter({ namespace, dataDir });
  await expect(successor.init()).rejects.toThrow(/instance_lock_held/);

  failRelease = false;
  await expect(adapter.close()).resolves.toBeUndefined();
  await expect(successor.init()).resolves.toBeUndefined();
  await successor.close();
});
