import { expect, test } from 'bun:test';
import { AuditLogger } from '../src/logger';
import { CloudflareReliabilityAdapter } from '../src/durability/cloudflare/adapter';
import type { IAdapter } from '../src/types';

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
