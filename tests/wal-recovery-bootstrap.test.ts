import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { bootstrap } from '../src/bootstrap';
import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from '../src/types';
import { WALStorage } from '../src/storage/wal';

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
  const seedWal = new WALStorage('boot-ns', dataDir);
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

  const engine = await bootstrap({
    namespace: 'boot-ns',
    mode: 'durable',
    dataDir,
    adapter,
    batching: { maxSize: 100, flushInterval: 30, maxQueueSize: 1000, onQueueFull: 'dlq' },
  });

  // Immediately after bootstrap, WAL must still contain recovered ids
  // (truncating here would risk data loss if process dies before flush)
  const afterBoot = await engine.wal!.readAll();
  expect(afterBoot.map((l) => l.id).sort()).toEqual(['boot-1', 'boot-2']);

  // Drain via scheduled recovery flush
  const start = Date.now();
  while (inserts.length < 2 && Date.now() - start < 2_000) {
    await new Promise((r) => setTimeout(r, 20));
  }
  expect(inserts.map((l) => l.id).sort()).toEqual(['boot-1', 'boot-2']);

  // After successful flush + acknowledge, WAL should no longer hold those ids
  const afterFlush = await engine.wal!.readAll();
  expect(afterFlush.map((l) => l.id)).not.toContain('boot-1');
  expect(afterFlush.map((l) => l.id)).not.toContain('boot-2');

  engine.retryEngine.stop();
  await engine.batcher.flushAll();
  await engine.wal?.close();
  await engine.pool.closeAll();
  // Release instance lock handle before afterEach deletes dataDir (avoids GC fd errors).
  if (engine.instanceLock) {
    await engine.instanceLock.release();
  }
});
