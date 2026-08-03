import { afterEach, expect, test } from 'bun:test';
import { lstat, mkdtemp, readdir, rm, symlink, writeFile, mkdir } from 'node:fs/promises';
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
  const [pending] = await dlq.listPendingPaths();
  const processing = await dlq.markProcessing(pending!);
  await dlq.markPoisoned(processing);

  const [dead] = await dlq.listDead();
  expect(dead).toBeDefined();

  const requeued = await dlq.requeueDead(dead!);
  // requeue preserves opaque id (not a path)
  expect(requeued).toBe(dead);
  expect(await dlq.listDead()).toHaveLength(0);
  expect(await dlq.listPendingPaths()).toHaveLength(1);

  // Poison again and delete
  const [p2] = await dlq.listPendingPaths();
  const proc2 = await dlq.markProcessing(p2!);
  await dlq.markPoisoned(proc2);
  const [dead2] = await dlq.listDead();
  await dlq.deleteDead(dead2!);
  expect(await dlq.listDead()).toHaveLength(0);
});

test('diagnostic DLQ metadata path is not accepted as requeue authority', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-id-only-'));
  cleanupPaths.push(dataDir);
  const dlq = new DLQStorage('id-only', dataDir);
  await dlq.init();

  const id = await dlq.write('tenant', [makeLog('id-only', 'tenant')]);
  const claimed = await dlq.claim(id);
  expect(claimed).not.toBeNull();
  await dlq.markPoisoned(id);
  const [dead] = await dlq.listAll({ includePending: false, includeDead: true });
  const diagnosticPath = dead?.metadata?.path;
  expect(typeof diagnosticPath).toBe('string');
  await expect(dlq.requeueDead(String(diagnosticPath))).rejects.toThrow(/id/);
  expect(await dlq.requeueDead(dead!.id)).toBe(id);
});

test('symlinked batch entries cannot be read, overwritten, or deleted outside the DLQ', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-symlink-'));
  const outsideDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-symlink-outside-'));
  cleanupPaths.push(dataDir, outsideDir);
  const dlq = new DLQStorage('symlink', dataDir);
  await dlq.init();

  const external = join(outsideDir, 'external.batch');
  const original = JSON.stringify({
    v: 2,
    id: 'linked-processing',
    tenantId: 'outside',
    attempts: 0,
    logs: [makeLog('outside-log', 'outside')],
  });
  await writeFile(external, original);

  const processingLink = join(dlq.directory, 'linked-processing.batch.processing');
  await symlink(external, processingLink);
  await expect(dlq.setAttempts('linked-processing', 4)).rejects.toThrow(/symbolic link/);
  expect(await Bun.file(external).text()).toBe(original);

  const pendingLink = join(dlq.directory, 'linked-pending.batch');
  await symlink(external, pendingLink);
  await expect(dlq.claim('linked-pending')).rejects.toThrow(/symbolic link/);
  expect(await Bun.file(external).text()).toBe(original);

  const deadLink = join(dlq.directory, 'linked-dead.batch.dead');
  await symlink(external, deadLink);
  await expect(dlq.deleteDead('linked-dead')).rejects.toThrow(/symbolic link/);
  expect((await lstat(deadLink)).isSymbolicLink()).toBe(true);
  expect(await Bun.file(external).text()).toBe(original);
});

test('a symlinked DLQ directory cannot redirect a new write', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-dir-link-'));
  const outsideDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-dir-link-outside-'));
  cleanupPaths.push(dataDir, outsideDir);
  const dlq = new DLQStorage('dir-link', dataDir);
  await dlq.init();

  await rm(dlq.directory, { recursive: true, force: true });
  await symlink(outsideDir, dlq.directory);
  await expect(dlq.write('tenant', [makeLog('must-stay-confined', 'tenant')])).rejects.toThrow(
    /symbolic link/,
  );
  expect(await readdir(outsideDir)).toEqual([]);
});

test('a symlinked configured data directory is rejected before initialization writes', async () => {
  const parentDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-data-link-'));
  const outsideDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-data-link-outside-'));
  cleanupPaths.push(parentDir, outsideDir);
  const linkedDataDir = join(parentDir, 'data');
  await symlink(outsideDir, linkedDataDir);

  const dlq = new DLQStorage('data-link', linkedDataDir);
  await expect(dlq.init()).rejects.toThrow(/symbolic link/);
  expect(await readdir(outsideDir)).toEqual([]);
});

test('a symlink in a configured data-directory segment is rejected', async () => {
  const parentDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-data-segment-'));
  const outsideDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-data-segment-outside-'));
  cleanupPaths.push(parentDir, outsideDir);
  const safeDir = join(parentDir, 'safe');
  const linkedSegment = join(safeDir, 'link');
  const configuredDataDir = join(linkedSegment, 'nested');
  await mkdir(safeDir);
  await mkdir(join(outsideDir, 'nested'));
  await symlink(outsideDir, linkedSegment);

  const dlq = new DLQStorage('nested-link', configuredDataDir);
  await expect(dlq.init()).rejects.toThrow(/symbolic link/);
  expect(await readdir(join(outsideDir, 'nested'))).toEqual([]);
});

test('a symlinked ancestor is rejected before a missing data directory is created', async () => {
  const parentDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-missing-segment-'));
  const outsideDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-missing-segment-outside-'));
  cleanupPaths.push(parentDir, outsideDir);
  const safeDir = join(parentDir, 'safe');
  const linkedSegment = join(safeDir, 'link');
  await mkdir(safeDir);
  await symlink(outsideDir, linkedSegment);

  const dlq = new DLQStorage('missing-link', join(linkedSegment, 'missing'));
  await expect(dlq.init()).rejects.toThrow(/symbolic link/);
  expect(await readdir(outsideDir)).toEqual([]);
});
