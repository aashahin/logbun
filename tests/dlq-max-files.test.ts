import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { DLQStorage } from '../src/durability/filesystem';
import type { LogbunLog } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function makeLog(id: string, tenantId?: string): LogbunLog {
  return {
    id,
    tenantId,
    actorId: 'actor-1',
    action: 'dlq.max-files',
    createdAt: new Date().toISOString(),
  };
}

/**
 * F3: when maxFiles=1, second write throws/fails with dlq_full.
 */
test('maxFiles=1: second write throws with dlq_full', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-max-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('dlq-max', dataDir, { maxFiles: 1 });
  await dlq.init();

  expect(await dlq.canWrite()).toBe(true);

  await dlq.write('tenant-a', [makeLog('a1', 'tenant-a')]);

  expect(await dlq.canWrite()).toBe(false);

  const pending = await dlq.listPendingPaths();
  expect(pending.length).toBe(1);

  let threw: unknown;
  try {
    await dlq.write('tenant-b', [makeLog('b1', 'tenant-b')]);
  } catch (err) {
    threw = err;
  }

  expect(threw).toBeInstanceOf(Error);
  expect((threw as Error).message).toMatch(/dlq_full/);

  // Still only one pending file
  expect((await dlq.listPendingPaths()).length).toBe(1);
});

test('countByKind / countFiles reports pending after write under maxFiles', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-count-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('dlq-count', dataDir, { maxFiles: 10 });
  await dlq.init();

  await dlq.write('t1', [makeLog('c1', 't1')]);
  await dlq.write('t2', [makeLog('c2', 't2')]);

  // Prefer contract name countFiles(); fall back to countByKind()
  const dlqAny = dlq as DLQStorage & {
    countFiles?: () => Promise<{
      pending: number;
      processing: number;
      dead: number;
    }>;
    countByKind: () => Promise<{
      pending: number;
      processing: number;
      dead: number;
    }>;
  };

  const counts =
    typeof dlqAny.countFiles === 'function'
      ? await dlqAny.countFiles()
      : await dlqAny.countByKind();

  expect(counts.pending).toBe(2);
  expect(counts.processing).toBe(0);
  expect(await dlq.canWrite()).toBe(true);
});

test('dead files do not consume maxFiles write budget', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-dead-budget-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('dlq-dead-budget', dataDir, { maxFiles: 1 });
  await dlq.init();

  await dlq.write('t1', [makeLog('d1', 't1')]);
  const [file] = await dlq.listPendingPaths();
  expect(file).toBeDefined();

  const processing = await dlq.markProcessing(file!);
  await dlq.markPoisoned(processing);

  // pending+processing == 0; dead does not count
  expect(await dlq.canWrite()).toBe(true);
  await dlq.write('t2', [makeLog('d2', 't2')]);
  expect((await dlq.listPendingPaths()).length).toBe(1);
});

/**
 * requeueDead must respect maxFiles: when pending already at cap,
 * requeue throws dlq_full and leaves the .dead file in place.
 */
test('requeueDead under maxFiles=1 throws dlq_full when pending at cap', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-requeue-cap-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('dlq-requeue-cap', dataDir, { maxFiles: 1 });
  await dlq.init();

  // Create a .dead file first (poison path), then fill the write budget
  await dlq.write('dead-tenant', [makeLog('dead1', 'dead-tenant')]);
  const [toPoison] = await dlq.listPendingPaths();
  const processing = await dlq.markProcessing(toPoison!);
  await dlq.markPoisoned(processing);

  const dead = (await dlq.listDead())[0];
  expect(dead).toBeDefined();
  expect((await dlq.listPendingPaths()).length).toBe(0);

  // Cap is 1: write one pending so requeue has no room
  await dlq.write('pending-tenant', [makeLog('p1', 'pending-tenant')]);
  expect((await dlq.listPendingPaths()).length).toBe(1);
  expect(await dlq.canWrite()).toBe(false);

  let threw: unknown;
  try {
    await dlq.requeueDead(dead!);
  } catch (err) {
    threw = err;
  }

  expect(threw).toBeInstanceOf(Error);
  expect((threw as Error).message).toMatch(/dlq_full/);

  // .dead must still exist; pending unchanged
  expect((await dlq.listDead()).length).toBe(1);
  expect((await dlq.listPendingPaths()).length).toBe(1);
});
