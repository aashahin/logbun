import { expect, test } from 'bun:test';
import { AuditLogger } from '../src/logger';
import type { IAdapter } from '../src/types';

const adapter: IAdapter = {
  async init() {},
  async bulkInsert() {
    return true;
  },
  async query() {
    return { logs: [], nextCursor: null };
  },
  async prune() {},
  async close() {},
};

test('fire registers waitUntil task and never throws when waitUntil throws', async () => {
  const seen: Promise<unknown>[] = [];
  const audit = new AuditLogger({
    namespace: 'wu',
    mode: 'volatile',
    adapter,
    batching: { maxSize: 100, flushInterval: 60_000 },
  });
  await audit.ready;

  expect(() => {
    audit.fire(
      'wu.event',
      { actorId: 'a1' },
      {
        waitUntil: (p) => {
          seen.push(p);
          throw new Error('host waitUntil boom');
        },
      },
    );
  }).not.toThrow();

  expect(seen.length).toBe(1);
  await seen[0];
  await audit.shutdown();
});

test('flush and runMaintenance are single-flight safe', async () => {
  const audit = new AuditLogger({
    namespace: 'maint',
    mode: 'volatile',
    adapter,
    batching: { maxSize: 100, flushInterval: 60_000 },
    retention: { days: 30 },
  });
  await audit.ready;
  await audit.fireAsync('m.1', { actorId: 'a' });

  const [a, b] = await Promise.all([
    audit.runMaintenance(),
    audit.runMaintenance(),
  ]);
  expect(a).toBeUndefined();
  expect(b).toBeUndefined();
  await audit.flush();
  await audit.shutdown();
});
