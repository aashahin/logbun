import type { LogbunConfig } from './types';
import { WALStorage } from './storage/wal';
import { DLQStorage } from './storage/dlq';
import { ConnectionPool } from './engine/pool';
import { Batcher } from './engine/batcher';
import { RetryEngine } from './engine/retry';

export interface BootstrapResult {
  wal: WALStorage | null;
  dlq: DLQStorage;
  pool: ConnectionPool;
  batcher: Batcher;
  retryEngine: RetryEngine;
  retentionCron: { stop: () => void } | null;
}

/**
 * Bootstrap & Recovery Sequence
 *
 * Runs automatically inside AuditLogger initialization.
 * Must complete before the main engine starts accepting logs.
 *
 * Sequence:
 *   1. WAL RECOVERY (durable mode only)
 *      - Read current.aof → parse NDJSON lines
 *      - Filter out malformed partial writes
 *      - Inject recovered logs at front of in-memory queues
 *      - Truncate current.aof
 *
 *   2. DLQ ORPHAN CLEANUP
 *      - Scan dlq/ for *.processing files
 *      - Rename back to *.batch (crash recovery)
 *
 *   3. RETRY ENGINE INIT
 *      - Schedule first retry scan after 10s delay
 *
 *   4. MAIN ENGINE START
 *      - Initialize Batcher timers
 *      - Start Bun.cron for retention pruning (if configured)
 */
export async function bootstrap<T extends string>(
  config: LogbunConfig<T>
): Promise<BootstrapResult> {
  const mode = config.mode ?? 'volatile';
  const namespace = config.namespace;

  // ─── Initialize Storage ────────────────────────────────────────────
  let wal: WALStorage | null = null;
  if (mode === 'durable') {
    wal = new WALStorage(namespace);
    await wal.init();
  }

  const dlq = new DLQStorage(namespace);
  await dlq.init();

  // ─── Initialize Connection Pool ────────────────────────────────────
  const pool = new ConnectionPool(
    config.adapter,
    config.pool?.maxActiveConnections ?? 50,
    config.tenancy
  );

  // ─── Initialize Adapter ────────────────────────────────────────────
  await config.adapter.init();

  // ─── Initialize Batcher ────────────────────────────────────────────
  const batcher = new Batcher(
    config.adapter,
    pool,
    wal,
    dlq,
    mode,
    config.batching
  );

  // ─── Step 1: WAL Recovery (durable mode only) ──────────────────────
  if (wal) {
    const recovered = await wal.readAll();
    if (recovered.length > 0) {
      batcher.injectRecovered(recovered);
      await wal.truncate();
    }
  }

  // ─── Step 2: DLQ Orphan Cleanup ────────────────────────────────────
  await dlq.recoverOrphans();

  // ─── Step 3: Retry Engine Init ─────────────────────────────────────
  const retryEngine = new RetryEngine(dlq, config.adapter, pool);
  retryEngine.start(10_000); // 10s initial delay

  // ─── Step 4: Retention Cron ────────────────────────────────────────
  let retentionCron: { stop: () => void } | null = null;

  if (config.retention) {
    const cronExpression = config.retention.cronExpression ?? '0 0 * * *';
    const retentionDays = config.retention.days;
    const adapter = config.adapter;

    // Use native Bun.cron for scheduled retention pruning
    const job = Bun.cron(cronExpression, async () => {
      try {
        await adapter.prune(retentionDays);
      } catch {
        // Prune failure is non-fatal — TTL in ClickHouse acts as safety net
      }
    });

    retentionCron = { stop: () => job.stop() };
  }

  return {
    wal,
    dlq,
    pool,
    batcher,
    retryEngine,
    retentionCron,
  };
}
