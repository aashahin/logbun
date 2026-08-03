import { expect, test } from 'bun:test';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import { RetryEngine } from '../src/engine/retry';
import { MemoryReliabilityAdapter } from '../src/reliability/memory';
import { DurableAdmissionSchedulingError } from '../src/reliability/scheduling-error';
import type { LogbunLog, ReliabilityAdapter } from '../src/types';
import { memoryAdapter } from './helpers';

type MutationKind = 'appendJournal' | 'writeDlq';

function log(id: string, tenantId?: string): LogbunLog {
  return {
    id,
    tenantId,
    actorId: 'committed-scheduling',
    action: 'committed.scheduling',
    createdAt: new Date().toISOString(),
  };
}

function faultingReliability(): {
  reliability: ReliabilityAdapter;
  arm: (kind: MutationKind, error: unknown) => void;
  failAppendBeforeCommit: (error: unknown) => void;
  failDlqBeforeCommit: (error: unknown) => void;
  holdAppend: (id: string) => {
    reached: Promise<void>;
    release: () => void;
  };
} {
  const memory = new MemoryReliabilityAdapter({ enableJournal: true });
  let armed: { kind: MutationKind; error: unknown } | null = null;
  let appendBeforeCommitError: unknown;
  let dlqBeforeCommitError: unknown;
  let appendHold: {
    id: string;
    reached: () => void;
    wait: Promise<void>;
  } | null = null;
  const appendJournal = memory.appendJournal.bind(memory);
  const writeDlq = memory.writeDlq.bind(memory);
  const reliability = new Proxy(memory, {
    get(target, property, receiver) {
      if (property === 'persistent') return true;
      if (property === 'appendJournal') {
        return async (entry: LogbunLog) => {
          if (appendBeforeCommitError !== undefined) {
            const error = appendBeforeCommitError;
            appendBeforeCommitError = undefined;
            throw error;
          }
          await appendJournal(entry);
          if (appendHold?.id === entry.id) {
            const hold = appendHold;
            appendHold = null;
            hold.reached();
            await hold.wait;
          }
          if (armed?.kind === 'appendJournal') {
            const error = armed.error;
            armed = null;
            throw error;
          }
        };
      }
      if (property === 'writeDlq') {
        return async (tenantId: string | null, entries: LogbunLog[]) => {
          if (dlqBeforeCommitError !== undefined) {
            const error = dlqBeforeCommitError;
            dlqBeforeCommitError = undefined;
            throw error;
          }
          const id = await writeDlq(tenantId, entries);
          if (armed?.kind === 'writeDlq') {
            const error = armed.error;
            armed = null;
            throw error;
          }
          return id;
        };
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  }) as ReliabilityAdapter;
  return {
    reliability,
    arm: (kind, error) => {
      armed = { kind, error };
    },
    failAppendBeforeCommit: (error) => {
      appendBeforeCommitError = error;
    },
    failDlqBeforeCommit: (error) => {
      dlqBeforeCommitError = error;
    },
    holdAppend: (id) => {
      let reachedResolve!: () => void;
      const reached = new Promise<void>((resolve) => {
        reachedResolve = resolve;
      });
      let release!: () => void;
      const wait = new Promise<void>((resolve) => {
        release = resolve;
      });
      appendHold = { id, reached: reachedResolve, wait };
      return { reached, release };
    },
  };
}

function batcherFor(
  reliability: ReliabilityAdapter,
  destination = memoryAdapter(),
  pool = new ConnectionPool(destination, 5),
  maxQueueSize = 100,
): Batcher {
  return new Batcher({
    reliability,
    adapter: destination,
    pool,
    mode: 'durable',
    batching: {
      maxSize: 10,
      flushInterval: 60_000,
      maxQueueSize,
      onQueueFull: 'dlq',
    },
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
  });
}

const committedSchedulingCases = [
  {
    name: 'flush fallback acknowledges WAL and retains one DLQ copy',
    run: async () => {
      const { reliability, arm } = faultingReliability();
      await reliability.init();
      const failingDestination = memoryAdapter({ failInsert: true });
      const batcher = batcherFor(reliability, failingDestination);
      await batcher.enqueue(log('flush-dlq-commit'));
      const committedError = new DurableAdmissionSchedulingError(
        new Error('setAlarm EIO'),
      );
      arm('writeDlq', committedError);

      const observed = await batcher.flushAll().then(
        () => null,
        (error: unknown) => error,
      );
      expect(observed).toBe(committedError);
      expect((await reliability.recoverJournal()).logs).toEqual([]);
      expect(await reliability.listDlq()).toEqual([
        expect.objectContaining({ state: 'pending', logCount: 1 }),
      ]);
      expect(batcher.getStats().queued).toBe(0);

      const recoveredDestination = memoryAdapter();
      const retry = new RetryEngine({
        reliability,
        adapter: recoveredDestination,
        pool: new ConnectionPool(recoveredDestination, 5),
        retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
      });
      await retry.scan();
      expect(recoveredDestination.inserted.map((entry) => entry.id)).toEqual([
        'flush-dlq-commit',
      ]);
      expect(await reliability.listDlq()).toEqual([]);
    },
  },

  {
    name: 'pre-commit WAL failure retains one committed DLQ fallback copy across bundles',
    run: async () => {
      const { reliability, arm, failAppendBeforeCommit } =
        faultingReliability();
      await reliability.init();
      const destination = memoryAdapter();
      const batcher = batcherFor(reliability, destination);
      const committedError = Object.assign(
        new Error('cross-bundle getAlarm EIO'),
        {
          name: 'DurableAdmissionSchedulingError',
          durableAdmissionCommitted: true as const,
        },
      );
      failAppendBeforeCommit(new Error('WAL device failure'));
      arm('writeDlq', committedError);

      const observed = await batcher.enqueue(log('fallback-dlq-commit')).then(
        () => null,
        (error: unknown) => error,
      );
      expect(observed).toBe(committedError);
      expect((await reliability.recoverJournal()).logs).toEqual([]);
      expect(await reliability.listDlq()).toEqual([
        expect.objectContaining({ state: 'pending', logCount: 1 }),
      ]);
      expect(batcher.getStats().queued).toBe(0);

      const retry = new RetryEngine({
        reliability,
        adapter: destination,
        pool: new ConnectionPool(destination, 5),
        retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
      });
      await retry.scan();
      expect(destination.inserted.map((entry) => entry.id)).toEqual([
        'fallback-dlq-commit',
      ]);
      expect(await reliability.listDlq()).toEqual([]);
    },
  },

  {
    name: 'shutdown journal admission does not escalate its committed WAL copy',
    run: async () => {
      const { reliability, arm } = faultingReliability();
      await reliability.init();
      const destination = memoryAdapter();
      const batcher = batcherFor(reliability, destination);
      batcher.beginShutdown();
      const committedError = new DurableAdmissionSchedulingError(
        new Error('shutdown alarm EIO'),
      );
      arm('appendJournal', committedError);

      const observed = await batcher
        .enqueue(log('shutdown-journal-commit'))
        .then(
          () => null,
          (error: unknown) => error,
        );
      expect(observed).toBe(committedError);
      expect(
        (await reliability.recoverJournal()).logs.map((entry) => entry.id),
      ).toEqual(['shutdown-journal-commit']);
      expect(await reliability.listDlq()).toEqual([]);
      expect(batcher.getStats().queued).toBe(0);

      const recoveryBatcher = batcherFor(reliability, destination);
      recoveryBatcher.injectRecovered(
        (await reliability.recoverJournal()).logs as LogbunLog[],
      );
      await recoveryBatcher.flushAll();
      expect(destination.inserted.map((entry) => entry.id)).toEqual([
        'shutdown-journal-commit',
      ]);
      expect((await reliability.recoverJournal()).logs).toEqual([]);
    },
  },

  {
    name: 'shutdown DLQ admission acknowledges its WAL copy',
    run: async () => {
      const { reliability, arm } = faultingReliability();
      await reliability.init();
      const destination = memoryAdapter();
      const batcher = batcherFor(reliability, destination);
      batcher.beginShutdown();
      const committedError = new DurableAdmissionSchedulingError(
        new Error('shutdown DLQ alarm EIO'),
      );
      arm('writeDlq', committedError);

      const observed = await batcher.enqueue(log('shutdown-dlq-commit')).then(
        () => null,
        (error: unknown) => error,
      );
      expect(observed).toBe(committedError);
      expect((await reliability.recoverJournal()).logs).toEqual([]);
      expect(await reliability.listDlq()).toEqual([
        expect.objectContaining({ state: 'pending', logCount: 1 }),
      ]);
      expect(batcher.getStats().queued).toBe(0);

      const retry = new RetryEngine({
        reliability,
        adapter: destination,
        pool: new ConnectionPool(destination, 5),
        retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
      });
      await retry.scan();
      expect(destination.inserted.map((entry) => entry.id)).toEqual([
        'shutdown-dlq-commit',
      ]);
      expect(await reliability.listDlq()).toEqual([]);
    },
  },

  {
    name: 'tenant-adapter fallback acknowledges WAL and retains one DLQ copy',
    run: async () => {
      const { reliability, arm } = faultingReliability();
      await reliability.init();
      const destination = memoryAdapter();
      const pool = new ConnectionPool(
        destination,
        5,
        {
          mode: 'database_per_tenant',
          resolveConnection: async () => ({ database: 'unavailable' }),
        },
        async () => {
          throw new Error('tenant adapter unavailable');
        },
      );
      const batcher = batcherFor(reliability, destination, pool);
      await batcher.enqueue(log('tenant-fallback-commit', 'tenant-a'));
      const committedError = new DurableAdmissionSchedulingError(
        new Error('tenant fallback alarm EIO'),
      );
      arm('writeDlq', committedError);

      const observed = await batcher.flushAll().then(
        () => null,
        (error: unknown) => error,
      );
      expect(observed).toBe(committedError);
      expect((await reliability.recoverJournal()).logs).toEqual([]);
      expect(await reliability.listDlq()).toEqual([
        expect.objectContaining({
          state: 'pending',
          tenantId: 'tenant-a',
          logCount: 1,
        }),
      ]);
      expect(batcher.getStats().queued).toBe(0);

      const retry = new RetryEngine({
        reliability,
        adapter: destination,
        pool: new ConnectionPool(destination, 5),
        retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
      });
      await retry.scan();
      expect(destination.inserted.map((entry) => entry.id)).toEqual([
        'tenant-fallback-commit',
      ]);
      expect(await reliability.listDlq()).toEqual([]);
    },
  },

  {
    name: 'backpressure dump does not restore its committed DLQ batch to RAM or WAL',
    run: async () => {
      const { reliability, arm } = faultingReliability();
      await reliability.init();
      const destination = memoryAdapter();
      const batcher = batcherFor(
        reliability,
        destination,
        new ConnectionPool(destination, 5),
        1,
      );
      await batcher.enqueue(log('backpressure-committed'));
      const committedError = new DurableAdmissionSchedulingError(
        new Error('backpressure alarm EIO'),
      );
      arm('writeDlq', committedError);

      const observed = await batcher.enqueue(log('not-yet-admitted')).then(
        () => null,
        (error: unknown) => error,
      );
      expect(observed).toBe(committedError);
      expect((await reliability.recoverJournal()).logs).toEqual([]);
      expect(await reliability.listDlq()).toEqual([
        expect.objectContaining({ state: 'pending', logCount: 1 }),
      ]);
      expect(batcher.getStats().queued).toBe(0);

      const retry = new RetryEngine({
        reliability,
        adapter: destination,
        pool: new ConnectionPool(destination, 5),
        retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
      });
      await retry.scan();
      expect(destination.inserted.map((entry) => entry.id)).toEqual([
        'backpressure-committed',
      ]);
      expect(await reliability.listDlq()).toEqual([]);
    },
  },

  {
    name: 'queue-room fallback keeps the target only in DLQ while preserving unrelated RAM work',
    run: async () => {
      const { reliability, arm, failDlqBeforeCommit, holdAppend } =
        faultingReliability();
      await reliability.init();
      const destination = memoryAdapter();
      const batcher = batcherFor(
        reliability,
        destination,
        new ConnectionPool(destination, 5),
        1,
      );
      const hold = holdAppend('queue-room-target');
      const targetAdmission = batcher.enqueue(log('queue-room-target'));
      await hold.reached;
      await batcher.enqueue(log('queue-room-filler'));
      failDlqBeforeCommit(new Error('first queue dump failed before commit'));
      const committedError = new DurableAdmissionSchedulingError(
        new Error('queue-room alarm EIO'),
      );
      arm('writeDlq', committedError);
      hold.release();

      const observed = await targetAdmission.then(
        () => null,
        (error: unknown) => error,
      );
      expect(observed).toBe(committedError);
      expect(
        (await reliability.recoverJournal()).logs.map((entry) => entry.id),
      ).toEqual(['queue-room-filler']);
      expect(await reliability.listDlq()).toEqual([
        expect.objectContaining({ state: 'pending', logCount: 1 }),
      ]);
      expect(batcher.getStats().queued).toBe(1);

      await batcher.flushAll();
      const retry = new RetryEngine({
        reliability,
        adapter: destination,
        pool: new ConnectionPool(destination, 5),
        retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
      });
      await retry.scan();
      expect(destination.inserted.map((entry) => entry.id).sort()).toEqual([
        'queue-room-filler',
        'queue-room-target',
      ]);
      expect(new Set(destination.inserted.map((entry) => entry.id)).size).toBe(
        2,
      );
      expect((await reliability.recoverJournal()).logs).toEqual([]);
      expect(await reliability.listDlq()).toEqual([]);
    },
  },
] as const;

test.each(committedSchedulingCases)(
  'committed scheduling error: $name',
  async ({ run }) => {
    await run();
  },
);

test.each([
  {
    name: 'size-triggered',
    batching: { maxSize: 1, flushInterval: 60_000 },
  },
  {
    name: 'timer-triggered',
    batching: { maxSize: 10, flushInterval: 1 },
  },
] as const)(
  'committed scheduling error: $name background flush retains exact failure for maintenance',
  async ({ name, batching }) => {
    const { reliability, arm } = faultingReliability();
    await reliability.init();
    const destination = memoryAdapter({ failInsert: true });
    let observedBackgroundFailure!: () => void;
    const backgroundFailure = new Promise<void>((resolve) => {
      observedBackgroundFailure = resolve;
    });
    const batcher = new Batcher({
      reliability,
      adapter: destination,
      pool: new ConnectionPool(destination, 5),
      mode: 'durable',
      batching: {
        ...batching,
        maxQueueSize: 100,
        onQueueFull: 'dlq',
      },
      retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
      onEvent: (event) => {
        if (event.detail === 'background_committed_scheduling') {
          observedBackgroundFailure();
        }
      },
    });
    const committedError = new DurableAdmissionSchedulingError(
      new Error(`${name} alarm EIO`),
    );
    arm('writeDlq', committedError);

    await expect(batcher.enqueue(log(`${name}-background`))).resolves.toBe(
      true,
    );
    await backgroundFailure;
    const observed = await batcher.flushAll().then(
      () => null,
      (error: unknown) => error,
    );
    expect(observed).toBe(committedError);
    expect((await reliability.recoverJournal()).logs).toEqual([]);
    expect(await reliability.listDlq()).toEqual([
      expect.objectContaining({ state: 'pending', logCount: 1 }),
    ]);
    expect(batcher.getStats().queued).toBe(0);

    const recoveredDestination = memoryAdapter();
    const retry = new RetryEngine({
      reliability,
      adapter: recoveredDestination,
      pool: new ConnectionPool(recoveredDestination, 5),
      retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
    });
    await retry.scan();
    expect(recoveredDestination.inserted.map((entry) => entry.id)).toEqual([
      `${name}-background`,
    ]);
  },
);
