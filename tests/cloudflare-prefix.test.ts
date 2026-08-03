import { expect, test } from 'bun:test';
import { AuditLogger } from '../src/logger';
import {
  CloudflareReliabilityAdapter,
  type DurableObjectSqlStorage,
} from '../src/durability/cloudflare/adapter';
import type { IAdapter } from '../src/types';
import { isDurableAdmissionSchedulingError } from '../src/reliability/scheduling-error';

const state = {
  storage: {
    sql: {
      exec() {
        return {};
      },
    },
  },
};

test('CloudflareReliabilityAdapter rejects unsafe SQL table prefixes', () => {
  expect(() =>
    new CloudflareReliabilityAdapter({
      state,
      tablePrefix: 'audit; DROP TABLE journal',
    }),
  ).toThrow(/tablePrefix/);
});

test('durable scheduling errors are recognized across separately bundled entrypoints', () => {
  expect(isDurableAdmissionSchedulingError({
    durableAdmissionCommitted: true,
  })).toBe(true);
});

test('failed Cloudflare maintenance rearms the consumed alarm and propagates', async () => {
  const alarmTimes: number[] = [];
  const fakeState = {
    storage: {
      sql: {
        exec(query: string) {
          if (/SELECT COUNT\(\*\).*journal.*acked = 0/s.test(query)) {
            return { one: () => ({ c: 1 }) };
          }
          if (/SELECT COUNT\(\*\).*dlq/s.test(query)) {
            return { one: () => ({ c: 0 }) };
          }
          if (/SELECT id, payload.*journal/s.test(query)) {
            return { toArray: () => [] };
          }
          return {};
        },
      },
      setAlarm: async (scheduledTime: number | Date) => {
        alarmTimes.push(Number(scheduledTime));
      },
      getAlarm: async () => null,
      transactionSync: <T>(closure: () => T) => closure(),
    },
  };
  const reliability = new CloudflareReliabilityAdapter({
    state: fakeState,
    alarmDelayMs: 25,
  });
  const destination: IAdapter = {
    async init() {},
    async bulkInsert() { return true; },
    async query() { return { logs: [], nextCursor: null }; },
    async prune() {},
    async close() {},
  };
  const audit = new AuditLogger({
    namespace: 'cloudflare-maintenance-rearm',
    mode: 'durable',
    reliability,
    adapter: destination,
    batching: { maxSize: 10, flushInterval: 60_000 },
  });
  await audit.ready;
  alarmTimes.length = 0;
  reliability.listDlq = async () => {
    throw new Error('simulated consumed-alarm scan failure');
  };

  await expect(audit.runMaintenance()).rejects.toThrow(/consumed-alarm scan failure/);
  expect(alarmTimes).toHaveLength(1);
  expect(alarmTimes[0]!).toBeGreaterThan(Date.now());
  await audit.shutdown();
});

class StatefulJournalSql implements DurableObjectSqlStorage {
  readonly journal = new Map<string, { payload: string; createdMs: number }>();

  exec(query: string, ...bindings: unknown[]) {
    if (/INSERT INTO .*_journal/.test(query)) {
      this.journal.set(String(bindings[0]), {
        payload: String(bindings[1]),
        createdMs: Number(bindings[2]),
      });
      return {};
    }
    if (/SELECT COUNT\(\*\) AS c FROM .*_journal.*acked = 0/s.test(query)) {
      return { one: () => ({ c: this.journal.size }) };
    }
    if (/SELECT COUNT\(\*\) AS c FROM .*_dlq/s.test(query)) {
      return { one: () => ({ c: 0 }) };
    }
    if (/SELECT id, payload FROM .*_journal/s.test(query)) {
      const rows = [...this.journal.entries()]
        .sort((a, b) => a[1].createdMs - b[1].createdMs)
        .map(([id, row]) => ({ id, payload: row.payload }));
      return { toArray: () => rows };
    }
    if (/UPDATE .*_journal SET acked = 1/.test(query)) {
      this.journal.delete(String(bindings[0]));
      return {};
    }
    return {};
  }
}

test.each(['getAlarm', 'setAlarm'] as const)(
  'Cloudflare %s failure rejects fireAsync after journal commit and later rearm succeeds',
  async (failedOperation) => {
    const sql = new StatefulJournalSql();
    const alarmTimes: number[] = [];
    let failScheduling = false;
    const fakeState = {
      storage: {
        sql,
        getAlarm: async () => {
          if (failScheduling && failedOperation === 'getAlarm') {
            throw new Error('simulated getAlarm failure');
          }
          return null;
        },
        setAlarm: async (scheduledTime: number | Date) => {
          if (failScheduling && failedOperation === 'setAlarm') {
            throw new Error('simulated setAlarm failure');
          }
          alarmTimes.push(Number(scheduledTime));
        },
        transactionSync: <T>(closure: () => T) => closure(),
      },
    };
    const reliability = new CloudflareReliabilityAdapter({
      state: fakeState,
      alarmDelayMs: 25,
    });
    const destination: IAdapter = {
      async init() {},
      async bulkInsert() { return true; },
      async query() { return { logs: [], nextCursor: null }; },
      async prune() {},
      async close() {},
    };
    const audit = new AuditLogger({
      namespace: `cloudflare-${failedOperation}`,
      mode: 'durable',
      reliability,
      adapter: destination,
      batching: { maxSize: 10, flushInterval: 60_000 },
    });
    await audit.ready;

    failScheduling = true;
    await expect(
      audit.fireAsync('cloudflare.alarm-failure', { actorId: 'actor' }),
    ).rejects.toThrow(new RegExp(`simulated ${failedOperation} failure`));
    const recovered = await reliability.recoverJournal();
    expect(recovered.logs).toHaveLength(1);
    expect(recovered.logs[0]?.action).toBe('cloudflare.alarm-failure');

    failScheduling = false;
    await expect(reliability.rearmMaintenance()).resolves.toBeUndefined();
    expect(alarmTimes).toHaveLength(1);
    await audit.shutdown();
  },
);
