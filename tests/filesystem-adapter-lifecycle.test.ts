import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { FileReliabilityAdapter } from '../src/durability/filesystem';

interface AdapterTestHooks {
  walDirectorySync?: (directory: string, reason: string) => void | Promise<void>;
  dlqDirectorySync?: (directory: string, reason: string) => void | Promise<void>;
  afterLockAcquire?: () => void | Promise<void>;
}

const hookSymbol = Symbol.for('logbun.FileReliabilityAdapter.testHooks');
const cleanupPaths: string[] = [];
const hookedNamespaces = new Set<string>();

function hookRegistry(): Map<string, AdapterTestHooks> {
  const globalRecord = globalThis as unknown as Record<PropertyKey, unknown>;
  const current = globalRecord[hookSymbol];
  if (current instanceof Map) return current as Map<string, AdapterTestHooks>;
  const registry = new Map<string, AdapterTestHooks>();
  globalRecord[hookSymbol] = registry;
  return registry;
}

function setHooks(namespace: string, hooks: AdapterTestHooks): void {
  hookedNamespaces.add(namespace);
  hookRegistry().set(namespace, hooks);
}

afterEach(async () => {
  const registry = hookRegistry();
  for (const namespace of hookedNamespaces) registry.delete(namespace);
  hookedNamespaces.clear();
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

test('failed initialization releases local resources and the same adapter can retry', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-adapter-init-retry-'));
  cleanupPaths.push(dataDir);
  const namespace = 'adapter-init-retry';
  let failOnce = true;
  setHooks(namespace, {
    dlqDirectorySync: (_directory, reason) => {
      if (!failOnce || reason !== 'initialize-hierarchy') return;
      failOnce = false;
      const error = new Error('simulated adapter DLQ initialization failure') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });
  const adapter = new FileReliabilityAdapter({ namespace, dataDir });

  await expect(adapter.init()).rejects.toThrow(/DLQ initialization failure/);
  expect(adapter.walStorage).toBeNull();
  expect(adapter.dlqStorage).toBeNull();
  await expect(adapter.init()).resolves.toBeUndefined();
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
  setHooks(namespace, {
    afterLockAcquire: async () => {
      lockAcquires++;
      enteredResolve();
      await mayContinue;
    },
  });
  const adapter = new FileReliabilityAdapter({ namespace, dataDir });

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
  setHooks(namespace, {
    afterLockAcquire: async () => {
      enteredResolve();
      await mayContinue;
    },
  });
  const adapter = new FileReliabilityAdapter({ namespace, dataDir });

  const initializing = adapter.init();
  await entered;
  const closing = adapter.close();
  continueResolve();
  await expect(Promise.all([initializing, closing])).resolves.toEqual([undefined, undefined]);
  expect(adapter.walStorage).toBeNull();
  expect(adapter.dlqStorage).toBeNull();

  hookRegistry().delete(namespace);
  const successor = new FileReliabilityAdapter({ namespace, dataDir });
  await expect(successor.init()).resolves.toBeUndefined();
  await successor.close();
});
