import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import type { IAdapter, LogbunLog, ReliabilityAdapter } from '../src/types';
import { makeFileReliability } from './helpers';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function makeLog(id: string, tenantId: string): LogbunLog {
  return {
    id,
    tenantId,
    actorId: 'actor-1',
    action: 'fair.test',
    createdAt: new Date().toISOString(),
  };
}

test('global cap dumps largest tenant queue (fair-share victim)', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-fair-'));
  cleanupPaths.push(dataDir);

  const dlqWrites: string[] = [];
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

  const realRel = makeFileReliability('fair', dataDir, { maxDlqEntries: 1000 });
  await realRel.init();
  const rel: ReliabilityAdapter = new Proxy(realRel, {
    get(target, prop, receiver) {
      if (prop === 'writeDlq') {
        return async (tenantId: string | null, logs: LogbunLog[]) => {
          dlqWrites.push(tenantId ?? '__global__');
          return realRel.writeDlq(tenantId, logs);
        };
      }
      const v = Reflect.get(target, prop, receiver);
      return typeof v === 'function' ? (v as Function).bind(target) : v;
    },
  });

  const pool = new ConnectionPool(adapter, 5);
  const batcher = new Batcher({
    adapter,
    pool,
    reliability: rel,
    mode: 'volatile',
    maxTotalQueued: 5,
    maxActiveTenants: 100,
    batching: {
      maxSize: 100,
      flushInterval: 60_000,
      maxQueueSize: 100,
      onQueueFull: 'dlq',
    },
  });

  // Heavy tenant A fills most of the global cap
  for (let i = 0; i < 5; i++) {
    await batcher.enqueue(makeLog(`a-${i}`, 'tenant-a'));
  }

  // Tenant B triggers global cap → fair-share should dump A first
  const ok = await batcher.enqueue(makeLog('b-0', 'tenant-b'));
  expect(ok).toBe(true);
  expect(dlqWrites.length).toBeGreaterThanOrEqual(1);
  expect(dlqWrites[0]).toBe('tenant-a');

  await realRel.close();
});
