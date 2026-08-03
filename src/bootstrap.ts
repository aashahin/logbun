import type { IAdapter, LogbunConfig, ReliabilityAdapter } from './types';
import { MemoryReliabilityAdapter } from './reliability/memory';
import { ConnectionPool } from './engine/pool';
import { Batcher, type BatcherDeps } from './engine/batcher';
import { RetryEngine } from './engine/retry';
import { sanitizeNamespace } from './utils/namespace';
import { safeEmit, type LogbunEventHandler } from './events';

/** Default max concurrent bulkInsert flushes (shared contract). */
const DEFAULT_MAX_FLUSH_CONCURRENCY = 16;

export interface BootstrapResult {
  reliability: ReliabilityAdapter;
  pool: ConnectionPool;
  batcher: Batcher;
  retryEngine: RetryEngine;
  /** @deprecated Retention is host-scheduled via runMaintenance; always null. */
  retentionCron: { stop: () => void } | null;
}

/**
 * Bootstrap & Recovery Sequence
 *
 * Runs automatically inside AuditLogger initialization.
 *
 * Sequence:
 *   1. Init reliability (journal + DLQ + ownership)
 *   2. Journal recovery — inject recovered logs in bounded waves
 *   3. DLQ orphan cleanup (processing → pending)
 *   4. Retry engine ready (no auto timers — hosts call runMaintenance)
 */
export async function bootstrap<T extends string>(
  config: LogbunConfig<T>
): Promise<BootstrapResult> {
  sanitizeNamespace(config.namespace);
  const mode = config.mode ?? 'volatile';
  const onEvent = config.onEvent;

  const maxFlushConcurrency =
    config.maxFlushConcurrency ?? DEFAULT_MAX_FLUSH_CONCURRENCY;

  const reliability =
    config.reliability ?? new MemoryReliabilityAdapter();

  try {
    await reliability.init();

    const pool = new ConnectionPool(
      config.adapter,
      config.pool?.maxActiveConnections ?? 50,
      config.tenancy,
      config.adapterFactory
    );

    await config.adapter.init();

    const maxQueueSize = config.batching?.maxQueueSize ?? 1_000;
    const batcherDeps: BatcherDeps = {
      adapter: config.adapter,
      pool,
      reliability,
      mode,
      batching: config.batching,
      onEvent,
      retry: config.retry,
      maxRecoveryBatch: config.maxRecoveryBatch ?? maxQueueSize,
      maxActiveTenants: config.maxActiveTenants ?? 10_000,
      maxTotalQueued: config.maxTotalQueued ?? 50_000,
      flushTimeoutMs: config.flushTimeoutMs ?? 30_000,
      maxFlushConcurrency,
    };
    const batcher = new Batcher(batcherDeps);

    // Keep recovered entries durable until flush acknowledges them.
    if (mode === 'durable' && reliability.persistent) {
      await recoverJournal(reliability, batcher, {
        maxLogs: config.maxRecoveryBatch ?? maxQueueSize,
        onEvent,
      });
    }

    await reliability.recoverOrphans();

    const retryEngine = new RetryEngine({
      reliability,
      adapter: config.adapter,
      pool,
      retry: config.retry,
      onEvent,
    });

    return {
      reliability,
      pool,
      batcher,
      retryEngine,
      retentionCron: null,
    };
  } catch (err) {
    try {
      await reliability.close();
    } catch {
      /* ignore */
    }
    throw err;
  }
}

/**
 * Journal recovery wave.
 * Truncated recovery emits `limit`/`recovery` and injects only what was read —
 * unread lines stay unacked for later waves / next process.
 */
async function recoverJournal(
  reliability: ReliabilityAdapter,
  batcher: Batcher,
  opts: {
    maxLogs: number;
    onEvent: LogbunEventHandler | undefined;
  }
): Promise<void> {
  const { maxLogs, onEvent } = opts;

  const result = await reliability.recoverJournal({ maxLogs });

  if (result.truncated) {
    safeEmit(onEvent, {
      type: 'limit',
      detail: 'recovery',
      count: result.logs.length,
    });
  }

  if (result.logs.length > 0) {
    batcher.injectRecovered(result.logs as import("./types").LogbunLog[]);
  }
}

export async function runRetentionPrune(opts: {
  tenancyMode: string | undefined;
  knownTenantIds?: () => string[] | Promise<string[]>;
  pool: ConnectionPool;
  baseAdapter: IAdapter;
  retentionDays: number;
  onEvent: LogbunEventHandler | undefined;
}): Promise<void> {
  const {
    tenancyMode,
    knownTenantIds,
    pool,
    baseAdapter,
    retentionDays,
    onEvent,
  } = opts;

  if (tenancyMode === 'database_per_tenant') {
    const tenantSet = new Set<string>(pool.listActiveTenantIds());

    if (knownTenantIds) {
      try {
        const known = await knownTenantIds();
        for (const id of known) {
          if (id) tenantSet.add(id);
        }
      } catch (err) {
        safeEmit(onEvent, {
          type: 'prune_fail',
          error: err instanceof Error ? err.message : String(err),
          detail: 'knownTenantIds',
        });
      }
    }

    for (const tenantId of tenantSet) {
      try {
        await pool.withAdapter(tenantId, (adapter) =>
          adapter.prune(retentionDays)
        );
      } catch (err) {
        safeEmit(onEvent, {
          type: 'prune_fail',
          tenantId,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    try {
      await baseAdapter.prune(retentionDays);
    } catch (err) {
      safeEmit(onEvent, {
        type: 'prune_fail',
        error: err instanceof Error ? err.message : String(err),
        detail: 'base',
      });
    }
    return;
  }

  try {
    await baseAdapter.prune(retentionDays);
  } catch (err) {
    safeEmit(onEvent, {
      type: 'prune_fail',
      error: err instanceof Error ? err.message : String(err),
    });
  }
}
