import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { bootstrap } from '../src/bootstrap';
import { AuditLogger } from '../src/logger';
import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from '../src/types';
import { WALStorage } from '../src/durability/filesystem';
import { makeFileReliability } from './helpers';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function makeLog(id: string): LogbunLog {
  return {
    id,
    actorId: 'actor-1',
    action: 'recover.me',
    createdAt: new Date().toISOString(),
  };
}

test('bootstrap durable recovery keeps WAL until flush acknowledges', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-bootstrap-'));
  cleanupPaths.push(dataDir);

  // Seed WAL as if process crashed mid-flight
  const seedWal = new WALStorage('boot-ns', dataDir, { fsync: false });
  await seedWal.init();
  await seedWal.append(makeLog('boot-1'));
  await seedWal.append(makeLog('boot-2'));
  await seedWal.close();

  const inserts: LogbunLog[] = [];
  const adapter: IAdapter = {
    async init() {},
    async bulkInsert(_tenantId, logs) {
      inserts.push(...logs);
      return true;
    },
    async query(
      _t: string | null,
      _f: LogbunQueryFilters,
      _p: { cursor?: string; limit: number },
    ): Promise<LogbunQueryResult> {
      return { logs: [], nextCursor: null };
    },
    async prune() {},
    async close() {},
  };

  const reliability = makeFileReliability('boot-ns', dataDir);
  const engine = await bootstrap({
    namespace: 'boot-ns',
    reliability,
    mode: 'durable',
    adapter,
    batching: { maxSize: 100, flushInterval: 30, maxQueueSize: 1000, onQueueFull: 'dlq' },
  });

  // Immediately after bootstrap, journal must still contain recovered ids
  const afterBoot = await engine.reliability.recoverJournal();
  expect(afterBoot.logs.map((l) => l.id).sort()).toEqual(['boot-1', 'boot-2']);

  // Drain via scheduled recovery flush
  const start = Date.now();
  while (inserts.length < 2 && Date.now() - start < 2_000) {
    await new Promise((r) => setTimeout(r, 20));
  }
  expect(inserts.map((l) => l.id).sort()).toEqual(['boot-1', 'boot-2']);

  // After successful flush + acknowledge, journal should no longer hold those ids
  const afterFlush = await engine.reliability.recoverJournal();
  expect(afterFlush.logs.map((l) => l.id)).not.toContain('boot-1');
  expect(afterFlush.logs.map((l) => l.id)).not.toContain('boot-2');

  engine.retryEngine.stop();
  await engine.batcher.flushAll();
  await engine.pool.closeAll();
  await engine.reliability.close();
});

test('maintenance drains every bounded filesystem recovery wave without duplicates', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-recovery-waves-'));
  cleanupPaths.push(dataDir);
  const seedWal = new WALStorage('waves-ns', dataDir, { fsync: false });
  await seedWal.init();
  const ids = Array.from({ length: 6 }, (_, index) => `wave-${index}`);
  for (const [index, id] of ids.entries()) {
    await seedWal.append({ ...makeLog(id), tenantId: `tenant-${index % 3}` });
  }
  await seedWal.close();

  const inserts: string[] = [];
  const adapter: IAdapter = {
    async init() {},
    async bulkInsert(_tenantId, logs) {
      inserts.push(...logs.map((log) => log.id));
      return true;
    },
    async query(
      _t: string | null,
      _f: LogbunQueryFilters,
      _p: { cursor?: string; limit: number },
    ): Promise<LogbunQueryResult> {
      return { logs: [], nextCursor: null };
    },
    async prune() {},
    async close() {},
  };

  const engine = await bootstrap({
    namespace: 'waves-ns',
    reliability: makeFileReliability('waves-ns', dataDir),
    mode: 'durable',
    adapter,
    maxRecoveryBatch: 2,
    batching: { maxSize: 10, flushInterval: 60_000, maxQueueSize: 20, onQueueFull: 'dlq' },
  });

  await engine.batcher.flushAll();
  expect(inserts.sort()).toEqual(ids);
  expect(new Set(inserts).size).toBe(ids.length);
  expect((await engine.reliability.recoverJournal()).logs).toEqual([]);

  engine.retryEngine.stop();
  await engine.pool.closeAll();
  await engine.reliability.close();
});

test('AuditLogger.runMaintenance drains every filesystem recovery wave', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-logger-waves-'));
  cleanupPaths.push(dataDir);
  const seedWal = new WALStorage('logger-waves', dataDir, { fsync: false });
  await seedWal.init();
  const ids = Array.from({ length: 5 }, (_, index) => `logger-wave-${index}`);
  for (const [index, id] of ids.entries()) {
    await seedWal.append({ ...makeLog(id), tenantId: `tenant-${index % 2}` });
  }
  await seedWal.close();

  const inserts: string[] = [];
  const adapter: IAdapter = {
    async init() {},
    async bulkInsert(_tenantId, logs) {
      inserts.push(...logs.map((log) => log.id));
      return true;
    },
    async query(
      _t: string | null,
      _f: LogbunQueryFilters,
      _p: { cursor?: string; limit: number },
    ): Promise<LogbunQueryResult> {
      return { logs: [], nextCursor: null };
    },
    async prune() {},
    async close() {},
  };

  const audit = new AuditLogger({
    namespace: 'logger-waves',
    reliability: makeFileReliability('logger-waves', dataDir),
    mode: 'durable',
    adapter,
    maxRecoveryBatch: 2,
    batching: { maxSize: 10, flushInterval: 60_000, maxQueueSize: 20, onQueueFull: 'dlq' },
  });
  await audit.ready;
  await audit.runMaintenance();
  expect(inserts.sort()).toEqual(ids);
  expect(new Set(inserts).size).toBe(ids.length);
  await audit.shutdown();

  const verify = makeFileReliability('logger-waves', dataDir);
  await verify.init();
  expect((await verify.recoverJournal()).logs).toEqual([]);
  await verify.close();
});
