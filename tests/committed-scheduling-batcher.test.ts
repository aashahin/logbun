import { expect, test } from 'bun:test';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import { RetryEngine } from '../src/engine/retry';
import { AuditLogger } from '../src/logger';
import { MemoryReliabilityAdapter } from '../src/reliability/memory';
import {
  DurableAdmissionSchedulingError,
  isDurableAdmissionSchedulingError,
} from '../src/reliability/scheduling-error';
import type { LogbunEvent, LogbunLog, ReliabilityAdapter } from '../src/types';
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
  failAppendBeforeCommit: (error: unknown, action?: string) => void;
  failDlqBeforeCommit: (error: unknown, action?: string) => void;
  appendAttempts: () => readonly LogbunLog[];
  holdAppend: (id: string) => {
    reached: Promise<void>;
    release: () => void;
  };
} {
  const memory = new MemoryReliabilityAdapter({ enableJournal: true });
  const armed: Record<MutationKind, unknown[]> = {
    appendJournal: [],
    writeDlq: [],
  };
  const attemptedAppends: LogbunLog[] = [];
  let appendBeforeCommitFailure:
    | { error: unknown; action?: string }
    | undefined;
  let dlqBeforeCommitFailure: { error: unknown; action?: string } | undefined;
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
          attemptedAppends.push(entry);
          if (
            appendBeforeCommitFailure !== undefined &&
            (appendBeforeCommitFailure.action === undefined ||
              appendBeforeCommitFailure.action === entry.action)
          ) {
            const { error } = appendBeforeCommitFailure;
            appendBeforeCommitFailure = undefined;
            throw error;
          }
          await appendJournal(entry);
          if (appendHold?.id === entry.id) {
            const hold = appendHold;
            appendHold = null;
            hold.reached();
            await hold.wait;
          }
          if (armed.appendJournal.length > 0) {
            const error = armed.appendJournal.shift();
            throw error;
          }
        };
      }
      if (property === 'writeDlq') {
        return async (tenantId: string | null, entries: LogbunLog[]) => {
          if (
            dlqBeforeCommitFailure !== undefined &&
            (dlqBeforeCommitFailure.action === undefined ||
              entries.some(
                (entry) => entry.action === dlqBeforeCommitFailure?.action,
              ))
          ) {
            const { error } = dlqBeforeCommitFailure;
            dlqBeforeCommitFailure = undefined;
            throw error;
          }
          const id = await writeDlq(tenantId, entries);
          if (armed.writeDlq.length > 0) {
            const error = armed.writeDlq.shift();
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
      armed[kind].push(error);
    },
    failAppendBeforeCommit: (error, action) => {
      appendBeforeCommitFailure = { error, action };
    },
    failDlqBeforeCommit: (error, action) => {
      dlqBeforeCommitFailure = { error, action };
    },
    appendAttempts: () => attemptedAppends,
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

const preAdmissionAttributionCases = (
  [
    {
      pressure: 'backpressure',
      oldTenant: 'tenant-a',
      currentTenant: 'tenant-a',
      maxQueueSize: 1,
      maxTotalQueued: 100,
    },
    {
      pressure: 'global-cap',
      oldTenant: 'old-tenant',
      currentTenant: 'current-tenant',
      maxQueueSize: 100,
      maxTotalQueued: 1,
    },
  ] as const
).flatMap((pressureCase) =>
  (
    ['succeeds', 'fails-precommit', 'commits-then-scheduling-fails'] as const
  ).map((outcome) => ({ ...pressureCase, outcome })),
);

test.each(preAdmissionAttributionCases)(
  'pre-admission $pressure spill debt keeps $outcome attribution on the current event',
  async ({
    pressure,
    outcome,
    oldTenant,
    currentTenant,
    maxQueueSize,
    maxTotalQueued,
  }) => {
    const {
      reliability,
      arm,
      failAppendBeforeCommit,
      failDlqBeforeCommit,
      appendAttempts,
    } = faultingReliability();
    let rearmCalls = 0;
    Object.assign(reliability, {
      async requestMaintenance() {
        rearmCalls++;
      },
    });
    const destination = memoryAdapter();
    const events: LogbunEvent[] = [];
    const audit = new AuditLogger({
      namespace: `attribution-${pressure}-${outcome}`,
      mode: 'durable',
      reliability,
      adapter: destination,
      batching: {
        maxSize: 10,
        flushInterval: 60_000,
        maxQueueSize,
        onQueueFull: 'dlq',
      },
      maxTotalQueued,
      retry: { insertMaxRetries: 1, insertBaseDelayMs: 0 },
      onEvent: (event) => events.push(event),
    });
    await audit.ready;
    const oldAction = `older.${pressure}.${outcome}`;
    const currentAction = `current.${pressure}.${outcome}`;
    await audit.fireAsync(oldAction, {
      actorId: 'older',
      tenantId: oldTenant,
    });
    const oldId = appendAttempts().at(-1)!.id;
    const priorError = new DurableAdmissionSchedulingError(
      new Error(`older ${pressure} alarm EIO`),
    );
    const currentCommittedError = new DurableAdmissionSchedulingError(
      new Error(`current ${pressure} alarm EIO`),
    );
    arm('writeDlq', priorError);
    if (outcome === 'fails-precommit') {
      failAppendBeforeCommit(
        new Error('current WAL failed before commit'),
        currentAction,
      );
      failDlqBeforeCommit(
        new Error('current DLQ fallback failed before commit'),
        currentAction,
      );
    } else if (outcome === 'commits-then-scheduling-fails') {
      arm('appendJournal', currentCommittedError);
    }

    const observedAdmission = await audit
      .fireAsync(currentAction, {
        actorId: 'current',
        tenantId: currentTenant,
      })
      .then(
        () => null,
        (error: unknown) => error,
      );
    const firstCurrentId = appendAttempts().at(-1)!.id;
    const shouldRetryCurrent =
      observedAdmission !== null &&
      !isDurableAdmissionSchedulingError(observedAdmission);
    expect(shouldRetryCurrent).toBe(outcome === 'fails-precommit');
    if (outcome === 'succeeds') {
      expect(observedAdmission).toBeNull();
    } else if (outcome === 'fails-precommit') {
      expect(observedAdmission).toBeInstanceOf(Error);
      expect(observedAdmission).not.toBe(priorError);
      expect(isDurableAdmissionSchedulingError(observedAdmission)).toBe(false);
    } else {
      expect(observedAdmission).toBe(currentCommittedError);
    }

    const journalBeforeMaintenance = (await reliability.recoverJournal()).logs;
    expect(journalBeforeMaintenance.map((entry) => entry.id)).toEqual(
      outcome === 'fails-precommit' ? [] : [firstCurrentId],
    );
    const dlqBeforeMaintenance = await reliability.listDlq();
    expect(dlqBeforeMaintenance).toEqual([
      expect.objectContaining({
        state: 'pending',
        tenantId: oldTenant,
        logCount: 1,
      }),
    ]);
    const oldBatch = await reliability.readDlq(dlqBeforeMaintenance[0]!.id);
    expect(oldBatch?.logs.map((entry) => entry.id)).toEqual([oldId]);
    expect(oldBatch?.logs.map((entry) => entry.action)).toEqual([oldAction]);
    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'flush_fail',
        tenantId: oldTenant,
        detail: `pre_admission_${pressure.replace('-', '_')}`,
      }),
    );
    expect(audit.getStats().queued).toBe(outcome === 'succeeds' ? 1 : 0);

    const observedMaintenance = await audit.runMaintenance().then(
      () => null,
      (error: unknown) => error,
    );
    expect(observedMaintenance).toBe(priorError);
    expect(rearmCalls).toBe(1);
    expect(destination.inserted.map((entry) => entry.id)).toEqual([oldId]);

    let deliverableCurrentId = firstCurrentId;
    if (outcome === 'fails-precommit') {
      await expect(
        audit.fireAsync(currentAction, {
          actorId: 'current-retry',
          tenantId: currentTenant,
        }),
      ).resolves.toBeUndefined();
      deliverableCurrentId = appendAttempts().at(-1)!.id;
      expect(deliverableCurrentId).not.toBe(firstCurrentId);
    }

    await expect(audit.runMaintenance()).resolves.toBeUndefined();
    expect(rearmCalls).toBe(2);
    if (outcome === 'commits-then-scheduling-fails') {
      const recoveryBatcher = batcherFor(reliability, destination);
      recoveryBatcher.injectRecovered(
        (await reliability.recoverJournal()).logs as LogbunLog[],
      );
      await recoveryBatcher.flushAll();
    }
    expect(destination.inserted.map((entry) => entry.id).sort()).toEqual(
      [oldId, deliverableCurrentId].sort(),
    );
    expect(new Set(destination.inserted.map((entry) => entry.id)).size).toBe(2);
    if (outcome === 'fails-precommit') {
      expect(destination.inserted.map((entry) => entry.id)).not.toContain(
        firstCurrentId,
      );
    }
    expect((await reliability.recoverJournal()).logs).toEqual([]);
    expect(await reliability.listDlq()).toEqual([]);
    await audit.shutdown();
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
