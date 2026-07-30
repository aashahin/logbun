import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm, writeFile, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { DLQStorage } from '../src/storage/dlq';
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
    action: 'dlq.confine',
    createdAt: new Date().toISOString(),
  };
}

/**
 * F3: requeueDead / deleteDead must reject paths outside the DLQ directory.
 */
test('requeueDead with path outside dlq dir throws', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-confine-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('confine', dataDir);
  await dlq.init();

  // Outside: absolute path under tmp that is NOT the dlq directory
  const outsideDir = await mkdtemp(join(tmpdir(), 'logbun-outside-'));
  cleanupPaths.push(outsideDir);
  const outsideDead = join(outsideDir, 'evil.batch.dead');
  await writeFile(
    outsideDead,
    JSON.stringify({
      v: 1,
      tenantId: 't1',
      attempts: 3,
      logs: [makeLog('x', 't1')],
    }),
  );

  await expect(dlq.requeueDead(outsideDead)).rejects.toThrow();
});

test('deleteDead with path outside dlq dir throws', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-del-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('confine-del', dataDir);
  await dlq.init();

  const outsideDir = await mkdtemp(join(tmpdir(), 'logbun-outside-del-'));
  cleanupPaths.push(outsideDir);
  const outsideDead = join(outsideDir, 'poison.batch.dead');
  await writeFile(
    outsideDead,
    JSON.stringify({
      v: 1,
      tenantId: 't1',
      attempts: 9,
      logs: [makeLog('y', 't1')],
    }),
  );

  await expect(dlq.deleteDead(outsideDead)).rejects.toThrow();

  // File must still exist (not unlinked)
  const stillThere = await Bun.file(outsideDead).exists();
  expect(stillThere).toBe(true);
});

test('requeueDead/deleteDead reject path traversal into sibling dirs', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-trav-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('confine-trav', dataDir);
  await dlq.init();

  // Sibling dir under same dataDir root (namespace/other), not under dlq/
  const sibling = join(dataDir, 'confine-trav', 'not-dlq');
  await mkdir(sibling, { recursive: true });
  const sneaky = join(sibling, 'sneaky.batch.dead');
  await writeFile(
    sneaky,
    JSON.stringify({
      v: 1,
      tenantId: null,
      attempts: 1,
      logs: [makeLog('z')],
    }),
  );

  // Also try relative-looking traversal name that resolves outside
  const traversal = join(dlq.directory, '..', 'not-dlq', 'sneaky.batch.dead');

  await expect(dlq.requeueDead(traversal)).rejects.toThrow();
  await expect(dlq.deleteDead(traversal)).rejects.toThrow();
  await expect(dlq.requeueDead(sneaky)).rejects.toThrow();
  await expect(dlq.deleteDead(sneaky)).rejects.toThrow();
});

test('requeueDead/deleteDead still work for in-dir dead files', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-ok-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('confine-ok', dataDir);
  await dlq.init();

  await dlq.write('t-ok', [makeLog('ok-1', 't-ok')]);
  const [pending] = await dlq.listPending();
  const processing = await dlq.markProcessing(pending!);
  await dlq.markPoisoned(processing);

  const [dead] = await dlq.listDead();
  expect(dead).toBeDefined();

  const requeued = await dlq.requeueDead(dead!);
  expect(requeued.startsWith(dlq.directory)).toBe(true);
  expect(await dlq.listDead()).toHaveLength(0);
  expect(await dlq.listPending()).toHaveLength(1);

  // Poison again and delete
  const [p2] = await dlq.listPending();
  const proc2 = await dlq.markProcessing(p2!);
  await dlq.markPoisoned(proc2);
  const [dead2] = await dlq.listDead();
  await dlq.deleteDead(dead2!);
  expect(await dlq.listDead()).toHaveLength(0);
});
