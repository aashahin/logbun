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

test('maintenance retries a transient recovery-wave failure without duplicate delivery', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-recovery-retry-'));
  cleanupPaths.push(dataDir);
  const seedWal = new WALStorage('retry-waves', dataDir, { fsync: false });
  await seedWal.init();
  const ids = ['retry-wave-1', 'retry-wave-2'];
  for (const id of ids) await seedWal.append(makeLog(id));
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

  const reliability = makeFileReliability('retry-waves', dataDir);
  const recoverJournal = reliability.recoverJournal.bind(reliability);
  let recoveryCalls = 0;
  reliability.recoverJournal = async (options) => {
    recoveryCalls++;
    if (recoveryCalls === 2) throw new Error('transient recovery read');
    return recoverJournal(options);
  };

  const audit = new AuditLogger({
    namespace: 'retry-waves',
    reliability,
    mode: 'durable',
    adapter,
    maxRecoveryBatch: 1,
    batching: { maxSize: 10, flushInterval: 60_000, maxQueueSize: 20, onQueueFull: 'dlq' },
  });
  await audit.ready;

  await audit.runMaintenance();
  expect(recoveryCalls).toBe(2);
  expect(inserts).toEqual(['retry-wave-1']);

  await audit.runMaintenance();
  expect(recoveryCalls).toBe(3);
  expect(inserts.sort()).toEqual(ids);
  expect(new Set(inserts).size).toBe(ids.length);
  await audit.shutdown();
});

test('truncated recovery and a delayed live append deliver every journal id exactly once', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-reservation-'));
  cleanupPaths.push(dataDir);
  const seedWal = new WALStorage('reservation-waves', dataDir, { fsync: false });
  await seedWal.init();
  const recoveredIds = ['reservation-wave-1', 'reservation-wave-2'];
  for (const id of recoveredIds) await seedWal.append(makeLog(id));
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

  const reliability = makeFileReliability('reservation-waves', dataDir);
  const appendJournal = reliability.appendJournal.bind(reliability);
  let liveId = '';
  let signalAppendStarted!: () => void;
  const appendStarted = new Promise<void>((resolve) => {
    signalAppendStarted = resolve;
  });
  let releaseAppend!: () => void;
  const appendGate = new Promise<void>((resolve) => {
    releaseAppend = resolve;
  });
  reliability.appendJournal = async (log) => {
    await appendJournal(log);
    if (log.action === 'live.reservation') {
      liveId = log.id;
      signalAppendStarted();
      await appendGate;
    }
  };

  const audit = new AuditLogger({
    namespace: 'reservation-waves',
    reliability,
    mode: 'durable',
    adapter,
    maxRecoveryBatch: 1,
    batching: { maxSize: 10, flushInterval: 60_000, maxQueueSize: 20, onQueueFull: 'dlq' },
  });
  await audit.ready;

  const liveEnqueue = audit.fireAsync('live.reservation', { actorId: 'actor-live' });
  await appendStarted;
  await audit.runMaintenance();

  releaseAppend();
  await liveEnqueue;
  await audit.runMaintenance();

  const expectedIds = [...recoveredIds, liveId].sort();
  expect(liveId).not.toBe('');
  expect(inserts.sort()).toEqual(expectedIds);
  expect(new Set(inserts).size).toBe(expectedIds.length);
  await audit.shutdown();
});

test('a reservation completed during an awaited recovery read prevents duplicate injection', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-mid-read-reservation-'));
  cleanupPaths.push(dataDir);
  const namespace = 'mid-read-reservation';
  const seedWal = new WALStorage(namespace, dataDir, { fsync: false });
  await seedWal.init();
  const recoveredIds = ['mid-read-wave-1', 'mid-read-wave-2'];
  for (const id of recoveredIds) await seedWal.append(makeLog(id));
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

  const reliability = makeFileReliability(namespace, dataDir);
  const recoverJournal = reliability.recoverJournal.bind(reliability);
  const appendJournal = reliability.appendJournal.bind(reliability);
  let liveLog: LogbunLog | null = null;
  let recoveryCalls = 0;
  let signalRecoveryStarted!: () => void;
  const recoveryStarted = new Promise<void>((resolve) => {
    signalRecoveryStarted = resolve;
  });
  let releaseRecovery!: () => void;
  const recoveryGate = new Promise<void>((resolve) => {
    releaseRecovery = resolve;
  });
  reliability.recoverJournal = async (options) => {
    recoveryCalls++;
    if (recoveryCalls === 2) {
      signalRecoveryStarted();
      await recoveryGate;
      return { logs: [liveLog!], truncated: false, approxBytes: 0 };
    }
    return recoverJournal(options);
  };

  let signalAppendStarted!: () => void;
  const appendStarted = new Promise<void>((resolve) => {
    signalAppendStarted = resolve;
  });
  reliability.appendJournal = async (log) => {
    await appendJournal(log);
    if (log.action === 'live.mid-read') {
      liveLog = log;
      signalAppendStarted();
    }
  };

  const audit = new AuditLogger({
    namespace,
    reliability,
    mode: 'durable',
    adapter,
    maxRecoveryBatch: 1,
    batching: { maxSize: 10, flushInterval: 60_000, maxQueueSize: 20, onQueueFull: 'dlq' },
  });
  await audit.ready;

  const maintenance = audit.runMaintenance();
  await recoveryStarted;
  const liveEnqueue = audit.fireAsync('live.mid-read', { actorId: 'actor-live' });
  await appendStarted;
  await liveEnqueue;
  releaseRecovery();
  await maintenance;

  await audit.runMaintenance();

  const expectedIds = [...recoveredIds, liveLog!.id].sort();
  expect(inserts.sort()).toEqual(expectedIds);
  expect(new Set(inserts).size).toBe(expectedIds.length);
  await audit.shutdown();
});
