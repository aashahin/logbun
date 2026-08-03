import { expect, test } from 'bun:test';
import { AuditLogger } from '../src/logger';
import { MemoryReliabilityAdapter } from '../src/reliability/memory';
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

test('durable mode without reliability throws synchronously', () => {
  expect(() => {
    new AuditLogger({
      namespace: 'no-rel',
      mode: 'durable',
      adapter,
    });
  }).toThrow(/persistent adapter/);
});

test('durable mode with MemoryReliabilityAdapter throws synchronously', () => {
  expect(() => {
    new AuditLogger({
      namespace: 'mem-rel',
      mode: 'durable',
      reliability: new MemoryReliabilityAdapter(),
      adapter,
    });
  }).toThrow(/persistent/);
});
