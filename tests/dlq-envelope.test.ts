import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, readdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  DLQStorage,
  parseBatch,
  readBatch,
} from '../src/storage/dlq';
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

test('write stores v1 envelope with attempts:0 and logs', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-dlq-env-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('dlq-env', dataDir);
  await dlq.init();

  const logs = [makeLog('e1', 'tenant-1'), makeLog('e2', 'tenant-1')];
  await dlq.write('tenant-1', logs);

  const pending = await dlq.listPending();
  expect(pending.length).toBe(1);

  const raw = await readFile(pending[0]!, 'utf8');
  const parsed = JSON.parse(raw) as Record<string, unknown>;

  // New envelope format (not a bare array)
  expect(Array.isArray(parsed)).toBe(false);
  expect(parsed['v']).toBe(1);
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
  const pending = await dlq.listPending();
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
  const [filePath] = await dlq.listPending();
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

  const pendingAfter = await dlq.listPending();
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
  const pending = await dlq.listPending();
  expect(pending.length).toBe(1);

  const content = await readFile(pending[0]!, 'utf8');
  const batch = parseBatch(content);
  expect(batch.logs.map((l) => l.id)).toEqual(['g1']);
  // envelope stores null tenantId for global writes
  expect(batch.tenantId).toBeNull();
});
