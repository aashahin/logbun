import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Batcher } from '../src/engine/batcher';
import { ConnectionPool } from '../src/engine/pool';
import { AuditLogger } from '../src/logger';
import { BunSQLiteAdapter } from '../src/adapters/sqlite';
import { DLQStorage, readBatch } from '../src/storage/dlq';
import { WALStorage } from '../src/storage/wal';
import { sanitizeTenantKey } from '../src/utils/tenant';
import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function makeLog(id: string, tenantId?: string): LogbunLog {
  return {
    id,
    tenantId,
    actorId: 'a1',
    action: 'p0.test',
    createdAt: new Date().toISOString(),
  };
}

function stubAdapter(opts?: {
  failInsert?: boolean;
  onInsert?: (logs: LogbunLog[]) => void;
}): IAdapter {
  return {
    async init() {},
    async bulkInsert(_t, logs) {
      if (opts?.failInsert) throw new Error('adapter down');
      opts?.onInsert?.(logs);
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
}

async function waitFor(
  predicate: () => boolean,
  timeoutMs = 2_000,
  intervalMs = 15,
): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

// ─── P0: durability / security regressions ───────────────────────────────────

test('flushAll_keeps_unacked_wal_when_adapter_and_dlq_both_fail', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-p0-truncate-'));
  cleanupPaths.push(dataDir);

  const adapter = stubAdapter({ failInsert: true });
  const pool = new ConnectionPool(adapter, 5);
  const wal = new WALStorage('p0-trunc', dataDir, {
    fsync: false,
    compactAckThreshold: 10_000,
  });
  await wal.init();

  const realDlq = new DLQStorage('p0-trunc', dataDir);
  await realDlq.init();
  const dlq: DLQStorage = Object.create(realDlq) as DLQStorage;
  dlq.write = async () => {
    throw new Error('disk full');
  };

  const batcher = new Batcher({
    adapter,
    pool,
    wal,
    dlq,
    mode: 'durable',
    batching: {
      maxSize: 10,
      flushInterval: 60_000,
      maxQueueSize: 100,
      onQueueFull: 'dlq',
    },
    retry: { insertMaxRetries: 0, insertBaseDelayMs: 1 },
  });

  await batcher.enqueue(makeLog('keep-1'));
  await batcher.enqueue(makeLog('keep-2'));
  expect((await wal.readAll()).map((l) => l.id).sort()).toEqual(['keep-1', 'keep-2']);

  await batcher.flushAll();

  // Unacked logs must remain recoverable after shutdown drain
  expect((await wal.readAll()).map((l) => l.id).sort()).toEqual(['keep-1', 'keep-2']);
  await wal.close();
});

test('dlq_write_with_path_traversal_tenant_stays_inside_dlq_dir', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-p0-path-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('p0-path', dataDir);
  await dlq.init();

  const evil = '../../etc/passwd';
  await dlq.write(evil, [makeLog('evil-1', evil)]);

  const pending = await dlq.listPending();
  expect(pending).toHaveLength(1);
  expect(pending[0]!.startsWith(dlq.directory + '/')).toBe(true);
  expect(pending[0]!.includes('..')).toBe(false);

  const batch = await readBatch(pending[0]!);
  expect(batch.tenantId).toBe(evil);
  expect(batch.logs.map((l) => l.id)).toEqual(['evil-1']);

  const base = pending[0]!.split('/').pop()!;
  expect(base.startsWith(sanitizeTenantKey(evil))).toBe(true);
});

test.each([
  { input: '../../etc/passwd', reject: ['/', '..'] as const },
  { input: 'normal-tenant', expectExact: 'normal-tenant' },
  { input: null as string | null, expectExact: '__global__' },
])('sanitizeTenantKey($input) is path-safe', ({ input, reject, expectExact }) => {
  const key = sanitizeTenantKey(input);
  if (expectExact !== undefined) {
    expect(key).toBe(expectExact);
  }
  if (reject) {
    for (const ch of reject) {
      expect(key).not.toContain(ch);
    }
  }
});

test('wal_truncate_with_unacked_entries_preserves_them', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-p0-wal-trunc-'));
  cleanupPaths.push(dataDir);

  const wal = new WALStorage('p0-wal', dataDir, { fsync: false });
  await wal.init();
  await wal.append(makeLog('u1'));

  await wal.truncate();
  expect((await wal.readAll()).map((l) => l.id)).toEqual(['u1']);

  await wal.acknowledge(['u1']);
  await wal.compact();
  expect(await wal.readAll()).toEqual([]);
  await wal.truncate();
  expect(await wal.readAll()).toEqual([]);
  await wal.close();
});

test('pool_get_same_tenant_concurrently_creates_one_adapter', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-p0-pool-'));
  cleanupPaths.push(dataDir);

  let creates = 0;
  const pool = new ConnectionPool(
    stubAdapter(),
    10,
    {
      mode: 'database_per_tenant',
      resolveConnection: async (tenantId) => ({ path: join(dataDir, `${tenantId}.db`) }),
    },
    async (config) => {
      creates++;
      await new Promise((r) => setTimeout(r, 30));
      return new BunSQLiteAdapter({ path: String(config['path']) });
    },
  );

  const [a1, a2, a3] = await Promise.all([
    pool.get('tenant-x'),
    pool.get('tenant-x'),
    pool.get('tenant-x'),
  ]);

  expect(a1).toBe(a2);
  expect(a2).toBe(a3);
  expect(creates).toBe(1);
  expect(pool.size).toBe(1);
  await pool.closeAll();
});

test('requireTenantId_drops_fire_and_rejects_query_without_tenant', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-p0-tenant-'));
  cleanupPaths.push(dataDir);

  const events: string[] = [];
  const audit = new AuditLogger({
    namespace: 'p0-tenant',
    mode: 'volatile',
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
    dataDir,
    requireTenantId: true,
    batching: { maxSize: 1, flushInterval: 20 },
    onEvent: (e) => events.push(`${e.type}:${e.detail ?? ''}`),
  });
  await audit.ready;

  audit.fire('x.y', { actorId: 'a' });
  expect(events.some((e) => e.includes('require_tenant_id'))).toBe(true);
  await expect(audit.query({ filters: {} })).rejects.toThrow(/tenantId/);

  audit.fire('x.y', { actorId: 'a', tenantId: 't1', entityId: 'e1' });
  await waitFor(() => events.includes('flush_ok:'));
  const page = await audit.query({ tenantId: 't1', pagination: { limit: 10 } });
  expect(page.logs.some((l) => l.entityId === 'e1')).toBe(true);

  await audit.shutdown();
});

test('oversized_payload_emits_truncated_not_drop', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-p0-trunc-ev-'));
  cleanupPaths.push(dataDir);

  const events: string[] = [];
  const audit = new AuditLogger({
    namespace: 'p0-tev',
    mode: 'volatile',
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
    dataDir,
    maxPayloadBytes: 50,
    onEvent: (e) => events.push(e.type),
  });
  await audit.ready;

  audit.fire('big.payload', {
    actorId: 'a',
    newValues: { blob: 'x'.repeat(500) },
  });

  expect(events).toContain('truncated');
  expect(events).not.toContain('drop');
  await audit.shutdown();
});

test('pre_ready_buffer_drops_when_over_maxPreReadyBuffer', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-p0-preready-'));
  cleanupPaths.push(dataDir);

  let resolveInit!: () => void;
  const gate = new Promise<void>((r) => {
    resolveInit = r;
  });

  const adapter: IAdapter = {
    async init() {
      await gate;
    },
    async bulkInsert() {
      return true;
    },
    async query() {
      return { logs: [], nextCursor: null };
    },
    async prune() {},
    async close() {},
  };

  const drops: string[] = [];
  const audit = new AuditLogger({
    namespace: 'p0-pre',
    mode: 'volatile',
    adapter,
    dataDir,
    maxPreReadyBuffer: 3,
    onEvent: (e) => {
      if (e.type === 'drop') drops.push(e.detail ?? '');
    },
  });

  for (let i = 0; i < 10; i++) {
    audit.fire('pre.ready', { actorId: 'a', entityId: String(i) });
  }

  // 10 fires, buffer holds 3 → 7 drops
  expect(drops.filter((d) => d === 'pre_ready_buffer_full')).toHaveLength(7);

  resolveInit();
  await audit.ready;
  await audit.shutdown();
});

test('dlq_requeueDead_moves_poison_back_to_pending', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-p0-dlqops-'));
  cleanupPaths.push(dataDir);

  const dlq = new DLQStorage('p0-ops', dataDir);
  await dlq.init();
  await dlq.write('t1', [makeLog('d1', 't1')]);
  const [pending] = await dlq.listPending();
  const processing = await dlq.markProcessing(pending!);
  await dlq.markPoisoned(processing);

  expect(await dlq.listDead()).toHaveLength(1);
  const requeued = await dlq.requeueDead((await dlq.listDead())[0]!);
  expect(requeued.endsWith('.batch')).toBe(true);
  expect(await dlq.listDead()).toHaveLength(0);
  expect(await dlq.listPending()).toHaveLength(1);

  const batch = await readBatch((await dlq.listPending())[0]!);
  expect(batch.attempts).toBe(0);
  expect(batch.logs.map((l) => l.id)).toEqual(['d1']);
});

test('successful_enqueue_emits_enqueue_event', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-p0-enq-'));
  cleanupPaths.push(dataDir);

  const events: string[] = [];
  const audit = new AuditLogger({
    namespace: 'p0-enq',
    mode: 'volatile',
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
    dataDir,
    batching: { flushInterval: 60_000, maxSize: 100 },
    onEvent: (e) => events.push(e.type),
  });
  await audit.ready;
  audit.fire('e.q', { actorId: 'a' });
  await waitFor(() => events.includes('enqueue'));
  await audit.shutdown();
});

test('injectRecovered_respects_maxRecoveryBatch_then_drains_all', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-p0-recbatch-'));
  cleanupPaths.push(dataDir);

  const inserts: LogbunLog[] = [];
  const adapter = stubAdapter({
    onInsert: (logs) => inserts.push(...logs),
  });
  const pool = new ConnectionPool(adapter, 5);
  const dlq = new DLQStorage('p0-rec', dataDir);
  await dlq.init();

  const batcher = new Batcher({
    adapter,
    pool,
    wal: null,
    dlq,
    mode: 'volatile',
    batching: {
      maxSize: 100,
      flushInterval: 20,
      maxQueueSize: 50,
      onQueueFull: 'dlq',
    },
    maxRecoveryBatch: 2,
  });

  batcher.injectRecovered(Array.from({ length: 5 }, (_, i) => makeLog(`r${i}`)));
  await batcher.flushAll();

  expect(inserts.map((l) => l.id).sort()).toEqual(['r0', 'r1', 'r2', 'r3', 'r4']);
});

test('fire_during_shutdown_durable_still_reaches_dlq', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-p0-shut-'));
  cleanupPaths.push(dataDir);

  const audit = new AuditLogger({
    namespace: 'p0-shut',
    mode: 'durable',
    adapter: stubAdapter({ failInsert: true }),
    dataDir,
    wal: { fsync: false },
    batching: { maxSize: 100, flushInterval: 60_000, onQueueFull: 'dlq' },
    retry: { insertMaxRetries: 0, insertBaseDelayMs: 1, initialDelayMs: 60_000 },
  });
  await audit.ready;

  // Start shutdown without awaiting — race fire into shutdown path
  const shutting = audit.shutdown();
  audit.fire('during.shutdown', { actorId: 'a', entityId: 'late' });
  await shutting;

  // After full shutdown engine is null; re-open DLQ for the namespace
  const dlq = new DLQStorage('p0-shut', dataDir);
  await dlq.init();
  const pending = await dlq.listPending();
  // Either landed in DLQ during shutdown, or was dropped only if volatile —
  // durable path should leave at least WAL or DLQ evidence. Prefer DLQ.
  const wal = new WALStorage('p0-shut', dataDir, { fsync: false });
  await wal.init();
  const walLeft = await wal.readAll();
  await wal.close();

  const inDlq =
    pending.length > 0 &&
    (await Promise.all(pending.map((p) => readBatch(p)))).some((b) =>
      b.logs.some((l) => l.entityId === 'late'),
    );
  const inWal = walLeft.some((l) => l.entityId === 'late');
  expect(inDlq || inWal).toBe(true);
});
