import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  DLQStorage,
  parseBatch,
  readBatch,
} from '../src/durability/filesystem';
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
    action: 'dlq.test',
    createdAt: new Date().toISOString(),
  };
}

test('write stores v2 envelope with attempts:0 and logs', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-env-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('dlq-env', dataDir);
  await dlq.init();

  const logs = [makeLog('e1', 'tenant-1'), makeLog('e2', 'tenant-1')];
  await dlq.write('tenant-1', logs);

  const pending = await dlq.listPendingPaths();
  expect(pending.length).toBe(1);

  const raw = await readFile(pending[0]!, 'utf8');
  const parsed = JSON.parse(raw) as Record<string, unknown>;

  // New envelope format (not a bare array)
  expect(Array.isArray(parsed)).toBe(false);
  expect(parsed['v']).toBe(2);
  expect(parsed['attempts']).toBe(0);
  expect(parsed['tenantId']).toBe('tenant-1');
  expect(Array.isArray(parsed['logs'])).toBe(true);
  expect((parsed['logs'] as LogbunLog[]).map((l) => l.id)).toEqual(['e1', 'e2']);
});

test('parseBatch reads envelope and legacy array formats', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-parse-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('dlq-parse', dataDir);
  await dlq.init();

  // Envelope path
  await dlq.write('t-env', [makeLog('env-1', 't-env')]);
  const pending = await dlq.listPendingPaths();
  expect(pending.length).toBeGreaterThan(0);

  const envelopeContent = await readFile(pending[0]!, 'utf8');
  const envelope = parseBatch(envelopeContent);
  expect(envelope.attempts).toBe(0);
  expect(envelope.tenantId).toBe('t-env');
  expect(envelope.logs.map((l) => l.id)).toEqual(['env-1']);

  // Legacy array path (backward compat)
  const legacyLogs = [makeLog('legacy-1'), makeLog('legacy-2')];
  const legacy = parseBatch(JSON.stringify(legacyLogs));
  expect(legacy.attempts).toBe(0);
  expect(legacy.logs.map((l) => l.id)).toEqual(['legacy-1', 'legacy-2']);
  expect(legacy.tenantId).toBeNull();
});

test('incrementAttempts rewrites envelope and poison path moves to .dead', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-poison-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('dlq-poison', dataDir);
  await dlq.init();

  await dlq.write('tenant-p', [makeLog('p1', 'tenant-p')]);
  const [filePath] = await dlq.listPendingPaths();
  expect(filePath).toBeDefined();

  const processingPath = await dlq.markProcessing(filePath!);

  const before = await readBatch(processingPath);
  expect(before.attempts).toBe(0);
  expect(before.logs.map((l) => l.id)).toEqual(['p1']);

  await dlq.incrementAttempts(processingPath, before.attempts);
  const after = await readBatch(processingPath);
  expect(after.attempts).toBe(1);
  expect(after.logs.map((l) => l.id)).toEqual(['p1']);

  // Poison path: markPoisoned renames to .dead
  await dlq.markPoisoned(processingPath);

  const pendingAfter = await dlq.listPendingPaths();
  expect(pendingAfter).toEqual([]);

  // .dead file lives under resolveLogbunDir(namespace, dataDir)/dlq
  const dlqDir = join(dataDir, 'dlq-poison', 'dlq');
  const entries = await readdir(dlqDir);
  expect(entries.some((e) => e.endsWith('.dead'))).toBe(true);
});

test('write with null tenantId uses global key', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-global-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('dlq-global', dataDir);
  await dlq.init();

  await dlq.write(null, [makeLog('g1')]);
  const pending = await dlq.listPendingPaths();
  expect(pending.length).toBe(1);

  const content = await readFile(pending[0]!, 'utf8');
  const batch = parseBatch(content);
  expect(batch.logs.map((l) => l.id)).toEqual(['g1']);
  // envelope stores null tenantId for global writes
  expect(batch.tenantId).toBeNull();
});

test('failed retry metadata replacement leaves the processing batch valid', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-atomic-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('dlq-atomic', dataDir, {
    fsync: true,
    beforeAtomicRename: async (targetPath) => {
      if (targetPath.endsWith('.batch.processing')) {
        throw new Error('simulated crash before rename');
      }
    },
  });
  await dlq.init();
  await dlq.write('tenant-a', [makeLog('atomic-1', 'tenant-a')]);
  const [pending] = await dlq.listPendingPaths();
  const processing = await dlq.markProcessing(pending!);

  await expect(dlq.incrementAttempts(processing, 0)).rejects.toThrow(
    /simulated crash before rename/,
  );
  const preserved = await readBatch(processing);
  expect(preserved.attempts).toBe(0);
  expect(preserved.logs.map((log) => log.id)).toEqual(['atomic-1']);
  expect((await readdir(dlq.directory)).some((name) => name.endsWith('.tmp'))).toBe(false);
});

test('failed requeue state rename leaves one valid reset dead entry and can retry', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-requeue-atomic-'));
  cleanupPaths.push(dataDir);
  let failRequeueRename = false;
  const dlq = new DLQStorage('dlq-requeue-atomic', dataDir, {
    fsync: true,
    beforeAtomicRename: async (targetPath) => {
      if (failRequeueRename && targetPath.endsWith('.batch')) {
        throw new Error('simulated crash before requeue rename');
      }
    },
  });
  await dlq.init();
  const id = await dlq.write('tenant-a', [makeLog('requeue-1', 'tenant-a')]);
  await dlq.claim(id);
  await dlq.setAttempts(id, 3);
  await dlq.markPoisoned(id);

  failRequeueRename = true;
  await expect(dlq.requeueDead(id)).rejects.toThrow(/simulated crash/);
  expect(await dlq.listPending()).toEqual([]);
  expect(await dlq.listDead()).toEqual([id]);
  const stillDead = await dlq.resolvePath(id);
  expect(stillDead?.state).toBe('dead');
  expect((await dlq.readBatchFile(stillDead!.path)).attempts).toBe(0);

  failRequeueRename = false;
  expect(await dlq.requeueDead(id)).toBe(id);
  expect(await dlq.listDead()).toEqual([]);
  expect(await dlq.listPending()).toEqual([id]);
  const pending = await dlq.resolvePath(id);
  expect((await dlq.readBatchFile(pending!.path)).attempts).toBe(0);
});

test('requeueDead rejects a duplicate pending state without overwriting either entry', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-requeue-collision-'));
  cleanupPaths.push(dataDir);
  const dlq = new DLQStorage('dlq-requeue-collision', dataDir);
  await dlq.init();
  const id = await dlq.write('dead-tenant', [makeLog('dead-log', 'dead-tenant')]);
  await dlq.claim(id);
  await dlq.markPoisoned(id);
  const deadPath = join(dlq.directory, `${id}.batch.dead`);
  const pendingPath = join(dlq.directory, `${id}.batch`);
  const collision = JSON.stringify({
    v: 2,
    id,
    tenantId: 'pending-tenant',
    attempts: 0,
    logs: [makeLog('pending-log', 'pending-tenant')],
  });
  await writeFile(pendingPath, collision);

  await expect(dlq.requeueDead(id)).rejects.toThrow(/dlq_state_collision/);
  expect((await readBatch(deadPath)).logs.map((item) => item.id)).toEqual(['dead-log']);
  expect((await readBatch(pendingPath)).logs.map((item) => item.id)).toEqual(['pending-log']);
});

test('requeueDead rejects a pending collision introduced at the state-transition hook', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-requeue-race-'));
  cleanupPaths.push(dataDir);
  let injectCollision = false;
  let collision = '';
  const dlq = new DLQStorage('dlq-requeue-race', dataDir, {
    beforeAtomicRename: async (targetPath) => {
      if (injectCollision && targetPath.endsWith('.batch')) {
        await writeFile(targetPath, collision, { flag: 'wx' });
      }
    },
  });
  await dlq.init();
  const id = await dlq.write('dead-tenant', [makeLog('dead-log', 'dead-tenant')]);
  await dlq.claim(id);
  await dlq.markPoisoned(id);
  const deadPath = join(dlq.directory, `${id}.batch.dead`);
  const pendingPath = join(dlq.directory, `${id}.batch`);
  collision = JSON.stringify({
    v: 2,
    id,
    tenantId: 'racing-tenant',
    attempts: 0,
    logs: [makeLog('racing-log', 'racing-tenant')],
  });

  injectCollision = true;
  await expect(dlq.requeueDead(id)).rejects.toThrow(/dlq_state_collision/);
  expect((await readBatch(deadPath)).logs.map((item) => item.id)).toEqual(['dead-log']);
  expect((await readBatch(pendingPath)).logs.map((item) => item.id)).toEqual(['racing-log']);
});
