import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { AuditLogger } from '../src/logger';
import { FileReliabilityAdapter, WALStorage } from '../src/durability/filesystem';
import { makeLog, memoryAdapter } from './helpers';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function observeMaintenanceRequests(reliability: FileReliabilityAdapter): {
  calls: () => number;
  scheduled: () => number;
} {
  let calls = 0;
  let scheduled = 0;
  Object.assign(reliability, {
    async requestMaintenance() {
      calls++;
      if ((await reliability.getStats()).hasPendingWork) scheduled++;
    },
  });
  return { calls: () => calls, scheduled: () => scheduled };
}

test('maintenance requests another wake-up after destination and DLQ failures are swallowed', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-maintenance-durable-failure-'));
  cleanupPaths.push(dataDir);
  const reliability = new FileReliabilityAdapter({
    namespace: 'maintenance-durable-failure',
    dataDir,
    wal: { fsync: false },
    dlq: { fsync: false },
  });
  const requests = observeMaintenanceRequests(reliability);
  reliability.writeDlq = async () => {
    throw new Error('simulated maintenance DLQ failure');
  };
  const audit = new AuditLogger({
    namespace: 'maintenance-durable-failure',
    mode: 'durable',
    reliability,
    adapter: memoryAdapter({ failInsert: true }),
    batching: { maxSize: 10, flushInterval: 60_000 },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
  });
  await audit.ready;
  await audit.fireAsync('maintenance.pending', { actorId: 'actor' });

  await expect(audit.runMaintenance()).resolves.toBeUndefined();
  expect(requests.calls()).toBe(1);
  expect(requests.scheduled()).toBe(1);
  expect((await reliability.recoverJournal()).logs).toHaveLength(1);
  await audit.shutdown();
});

test('maintenance requests another wake-up after a swallowed recovery-wave read failure', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-maintenance-recovery-wave-'));
  cleanupPaths.push(dataDir);
  const namespace = 'maintenance-recovery-wave';
  const seed = new WALStorage(namespace, dataDir, { fsync: false });
  await seed.init();
  await seed.append(makeLog('recovery-wake-a'));
  await seed.append(makeLog('recovery-wake-b'));
  await seed.close();

  const reliability = new FileReliabilityAdapter({
    namespace,
    dataDir,
    wal: { fsync: false },
    dlq: { fsync: false },
  });
  const requests = observeMaintenanceRequests(reliability);
  const recoverJournal = reliability.recoverJournal.bind(reliability);
  let recoveryCalls = 0;
  reliability.recoverJournal = async (options) => {
    recoveryCalls++;
    if (recoveryCalls === 2) throw new Error('simulated deferred recovery read');
    return recoverJournal(options);
  };
  const destination = memoryAdapter();
  const audit = new AuditLogger({
    namespace,
    mode: 'durable',
    reliability,
    adapter: destination,
    maxRecoveryBatch: 1,
    batching: { maxSize: 10, flushInterval: 60_000 },
  });
  await audit.ready;

  await expect(audit.runMaintenance()).resolves.toBeUndefined();
  expect(destination.inserted.map((entry) => entry.id)).toEqual(['recovery-wake-a']);
  expect(requests.calls()).toBe(1);
  expect(requests.scheduled()).toBe(1);

  await expect(audit.runMaintenance()).resolves.toBeUndefined();
  expect(destination.inserted.map((entry) => entry.id).sort()).toEqual([
    'recovery-wake-a',
    'recovery-wake-b',
  ]);
  expect(requests.calls()).toBe(2);
  await audit.shutdown();
});

test('maintenance rearms after settlement storage failure and the next pass delivers once', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-maintenance-settlement-'));
  cleanupPaths.push(dataDir);
  const reliability = new FileReliabilityAdapter({
    namespace: 'maintenance-settlement',
    dataDir,
    wal: { fsync: false },
    dlq: { fsync: false },
  });
  const requests = observeMaintenanceRequests(reliability);
  await reliability.init();
  await reliability.writeDlq(null, [makeLog('maintenance-settlement')]);
  await reliability.close();

  const settleDlqFailure = reliability.settleDlqFailure.bind(reliability);
  let failSettlement = true;
  reliability.settleDlqFailure = async (id, attempts) => {
    if (failSettlement) {
      failSettlement = false;
      throw new Error('simulated transient settlement failure');
    }
    return settleDlqFailure(id, attempts);
  };
  let destinationAttempt = 0;
  const destination = memoryAdapter({ failInsert: () => ++destinationAttempt === 1 });
  const audit = new AuditLogger({
    namespace: 'maintenance-settlement',
    mode: 'durable',
    reliability,
    adapter: destination,
    batching: { maxSize: 10, flushInterval: 60_000 },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
  });
  await audit.ready;

  await expect(audit.runMaintenance()).rejects.toThrow(/transient settlement failure/);
  expect(requests.calls()).toBe(1);
  expect(requests.scheduled()).toBe(1);

  await expect(audit.runMaintenance()).resolves.toBeUndefined();
  expect(destination.inserted.map((entry) => entry.id)).toEqual(['maintenance-settlement']);
  expect(requests.calls()).toBe(2);
  await audit.shutdown();
});

test('maintenance preserves a committed scheduling error and later drains its single DLQ copy', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-maintenance-committed-schedule-'));
  cleanupPaths.push(dataDir);
  const reliability = new FileReliabilityAdapter({
    namespace: 'maintenance-committed-schedule',
    dataDir,
    wal: { fsync: false },
    dlq: { fsync: false },
  });
  const requests = observeMaintenanceRequests(reliability);
  const writeDlq = reliability.writeDlq.bind(reliability);
  const committedError = Object.assign(new Error('cross-bundle setAlarm EIO'), {
    name: 'DurableAdmissionSchedulingError',
    durableAdmissionCommitted: true as const,
  });
  let failScheduling = true;
  reliability.writeDlq = async (tenantId, entries) => {
    const id = await writeDlq(tenantId, entries);
    if (failScheduling) {
      failScheduling = false;
      throw committedError;
    }
    return id;
  };
  let destinationDown = true;
  const destination = memoryAdapter({ failInsert: () => destinationDown });
  const audit = new AuditLogger({
    namespace: 'maintenance-committed-schedule',
    mode: 'durable',
    reliability,
    adapter: destination,
    batching: { maxSize: 10, flushInterval: 60_000 },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
  });
  await audit.ready;
  await audit.fireAsync('maintenance.committed-schedule', { actorId: 'actor' });

  const observed = await audit.runMaintenance().then(
    () => null,
    (error: unknown) => error,
  );
  expect(observed).toBe(committedError);
  expect((await reliability.recoverJournal()).logs).toEqual([]);
  expect(await reliability.listDlq()).toEqual([
    expect.objectContaining({ state: 'pending', logCount: 1 }),
  ]);
  expect(requests.calls()).toBe(1);

  destinationDown = false;
  await audit.runMaintenance();
  expect(destination.inserted).toHaveLength(1);
  expect(await reliability.listDlq()).toEqual([]);
  expect(requests.calls()).toBe(2);
  await audit.shutdown();
});

test.each(['absent', 'failing'] as const)(
  'maintenance preserves a committed scheduling error when its rearm hook is %s',
  async (rearmMode) => {
    const dataDir = await mkdtemp(join(tmpdir(), `logbun-maintenance-${rearmMode}-rearm-`));
    cleanupPaths.push(dataDir);
    const namespace = `maintenance-${rearmMode}-rearm`;
    const reliability = new FileReliabilityAdapter({
      namespace,
      dataDir,
      wal: { fsync: false },
      dlq: { fsync: false },
    });
    if (rearmMode === 'failing') {
      Object.assign(reliability, {
        async requestMaintenance() {
          throw new Error('simulated rearm failure');
        },
      });
    }
    const writeDlq = reliability.writeDlq.bind(reliability);
    const committedError = Object.assign(
      new Error(`cross-bundle ${rearmMode} rearm scheduling failure`),
      {
        name: 'DurableAdmissionSchedulingError',
        durableAdmissionCommitted: true as const,
      },
    );
    reliability.writeDlq = async (tenantId, entries) => {
      const id = await writeDlq(tenantId, entries);
      throw committedError;
    };
    const audit = new AuditLogger({
      namespace,
      mode: 'durable',
      reliability,
      adapter: memoryAdapter({ failInsert: true }),
      batching: { maxSize: 10, flushInterval: 60_000 },
      retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
    });
    await audit.ready;
    await audit.fireAsync('maintenance.committed-schedule', {
      actorId: 'actor',
    });

    const observed = await audit.runMaintenance().then(
      () => null,
      (error: unknown) => error,
    );
    expect(observed).toBe(committedError);
    expect((await reliability.recoverJournal()).logs).toEqual([]);
    expect(await reliability.listDlq()).toEqual([
      expect.objectContaining({ state: 'pending', logCount: 1 }),
    ]);
    await audit.shutdown();
  },
);
