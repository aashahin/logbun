import type { IAdapter, LogbunConfig, LogbunLog } from './types';
import {
  WALStorage,
  WAL_SIZE_SOFT_LIMIT_BYTES,
  WAL_SEGMENT_BYTES_DEFAULT,
} from './storage/wal';
import { DLQStorage } from './storage/dlq';
import { InstanceLock } from './storage/instance-lock';
import { ConnectionPool } from './engine/pool';
import { Batcher, type BatcherDeps } from './engine/batcher';
import { RetryEngine } from './engine/retry';
import { sanitizeNamespace } from './utils/path';
import { safeEmit, type LogbunEventHandler } from './events';
import { normalizeEncryptionKey } from './utils/crypto';

/** Default max concurrent bulkInsert flushes (shared contract). */
const DEFAULT_MAX_FLUSH_CONCURRENCY = 16;
/** Default max DLQ pending+processing files before refusing new writes. */
const DEFAULT_MAX_DLQ_FILES = 10_000;

/**
 * Scale/ops knobs from the shared API contract.
 * Declared here so bootstrap compiles even if types lag a parallel stream.
 */
interface ScaleOpsConfig {
  maxFlushConcurrency?: number;
  maxWalBytes?: number;
  maxDlqFiles?: number;
  /** Optional alias for maxWalBytes. */
  walSoftLimitBytes?: number;
  encryptionKey?: string | Uint8Array;
  integrityChain?: boolean;
  instanceLock?: boolean;
}

export interface BootstrapResult {
  wal: WALStorage | null;
  dlq: DLQStorage;
  pool: ConnectionPool;
  batcher: Batcher;
  retryEngine: RetryEngine;
  retentionCron: { stop: () => void } | null;
  instanceLock: InstanceLock | null;
}

/**
 * Bootstrap & Recovery Sequence
 *
 * Runs automatically inside AuditLogger initialization.
 *
 * Sequence:
 *   0. Instance lock (durable default) — exclusive namespace data dir
 *   1. WAL recovery — inject recovered logs in bounded waves
 *   2. DLQ orphan cleanup (.processing → .batch)
 *   3. Retry engine start
 *   4. Retention cron (if configured)
 */
export async function bootstrap<T extends string>(
  config: LogbunConfig<T>
): Promise<BootstrapResult> {
  const namespace = sanitizeNamespace(config.namespace);
  const mode = config.mode ?? 'volatile';
  const onEvent = config.onEvent;
  const scale = config as LogbunConfig<T> & ScaleOpsConfig;

  const maxWalBytes =
    scale.maxWalBytes ??
    scale.walSoftLimitBytes ??
    WAL_SIZE_SOFT_LIMIT_BYTES;
  const maxFlushConcurrency =
    scale.maxFlushConcurrency ?? DEFAULT_MAX_FLUSH_CONCURRENCY;
  const maxDlqFiles = scale.maxDlqFiles ?? DEFAULT_MAX_DLQ_FILES;

  // Instance lock: default on for durable (multi-process safety)
  const wantLock =
    scale.instanceLock !== undefined
      ? scale.instanceLock
      : mode === 'durable';
  let instanceLock: InstanceLock | null = null;

  try {
    if (wantLock) {
      instanceLock = new InstanceLock(namespace, config.dataDir);
      await instanceLock.acquire();
    }

    let encryptionKeyBytes: Uint8Array | undefined;
    if (scale.encryptionKey != null) {
      encryptionKeyBytes = await normalizeEncryptionKey(scale.encryptionKey);
    }

    let wal: WALStorage | null = null;
    if (mode === 'durable') {
      const walOptions = {
        fsync: config.wal?.fsync ?? true,
        compactAckThreshold: config.wal?.compactAckThreshold ?? 256,
        maxBytes: maxWalBytes,
        maxWalBytes,
        hardMaxBytes: config.wal?.hardMaxBytes !== false,
        segmentBytes: config.wal?.segmentBytes ?? WAL_SEGMENT_BYTES_DEFAULT,
        encryptionKey: encryptionKeyBytes,
      };
      wal = new WALStorage(namespace, config.dataDir, walOptions);
      await wal.init();
    }

    // durable default true unless explicitly false; volatile only if explicitly true
    const dlqFsync =
      mode === 'durable' ? config.dlqFsync !== false : !!config.dlqFsync;

    const dlqOptions = {
      fsync: dlqFsync,
      maxFiles: maxDlqFiles,
      maxDlqFiles,
      encryptionKey: encryptionKeyBytes,
    };
    const dlq = new DLQStorage(namespace, config.dataDir, dlqOptions);
    await dlq.init();

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
      wal,
      dlq,
      mode,
      batching: config.batching,
      onEvent,
      retry: config.retry,
      maxRecoveryBatch: config.maxRecoveryBatch ?? maxQueueSize,
      maxActiveTenants: config.maxActiveTenants ?? 10_000,
      maxTotalQueued: config.maxTotalQueued ?? 50_000,
      flushTimeoutMs: config.flushTimeoutMs ?? 30_000,
      maxFlushConcurrency,
      maxWalBytes,
    };
    const batcher = new Batcher(batcherDeps);

    // Keep recovered entries on disk until flush acknowledges them.
    if (wal) {
      await recoverWal(wal, batcher, {
        maxWalBytes,
        maxLogs: config.maxRecoveryBatch ?? maxQueueSize,
        onEvent,
      });
    }

    await dlq.recoverOrphans();

    const retryEngine = new RetryEngine({
      dlq,
      adapter: config.adapter,
      pool,
      retry: config.retry,
      onEvent,
    });
    retryEngine.start(config.retry?.initialDelayMs ?? 10_000);

    let retentionCron: { stop: () => void } | null = null;

    if (config.retention) {
      const cronExpression = config.retention.cronExpression ?? '0 0 * * *';
      const retentionDays = config.retention.days;
      const baseAdapter = config.adapter;
      const tenancy = config.tenancy;

      const job = Bun.cron(cronExpression, async () => {
        await runRetentionPrune({
          tenancyMode: tenancy?.mode,
          knownTenantIds: tenancy?.knownTenantIds,
          pool,
          baseAdapter,
          retentionDays,
          onEvent,
        });
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
      instanceLock,
    };
  } catch (err) {
    if (instanceLock) {
      try {
        await instanceLock.release();
      } catch {
        /* ignore */
      }
    }
    throw err;
  }
}

/**
 * WAL recovery wave.
 * Prefers `readAllBounded` (size/count guard) when present; otherwise
 * approximates size for ops alerts and uses `readAll` (unacked only).
 * Truncated recovery emits `limit`/`recovery` and injects only what was read —
 * unread lines stay unacked on disk for later waves.
 */
async function recoverWal(
  wal: WALStorage,
  batcher: Batcher,
  opts: {
    maxWalBytes: number;
    maxLogs: number;
    onEvent: LogbunEventHandler | undefined;
  }
): Promise<void> {
  const { maxWalBytes, maxLogs, onEvent } = opts;

  type BoundedResult =
    | LogbunLog[]
    | {
        logs: LogbunLog[];
        truncated?: boolean;
        bytesRead?: number;
        approxBytes?: number;
      };

  const walExt = wal as WALStorage & {
    readAllBounded?: (opts?: {
      maxBytes?: number;
      maxLogs?: number;
    }) => Promise<BoundedResult>;
  };

  let recovered: LogbunLog[] = [];
  let truncated = false;

  if (typeof walExt.readAllBounded === 'function') {
    const result = await walExt.readAllBounded({
      maxBytes: maxWalBytes,
      maxLogs,
    });
    if (Array.isArray(result)) {
      recovered = result;
      if (recovered.length >= maxLogs) {
        truncated = true;
      }
    } else if (result && typeof result === 'object' && Array.isArray(result.logs)) {
      recovered = result.logs;
      truncated = Boolean(result.truncated);
      const rec = result as {
        approxBytes?: number;
        bytesRead?: number;
      };
      const approx =
        typeof rec.approxBytes === 'number'
          ? rec.approxBytes
          : typeof rec.bytesRead === 'number'
            ? rec.bytesRead
            : undefined;
      if (typeof approx === 'number' && approx > maxWalBytes) {
        safeEmit(onEvent, {
          type: 'limit',
          detail: 'wal_size',
          count: approx,
        });
      }
    }
  } else {
    try {
      const size = await wal.approximateSize();
      if (size > maxWalBytes) {
        safeEmit(onEvent, {
          type: 'limit',
          detail: 'wal_size',
          count: size,
        });
      }
    } catch {
      /* best-effort size probe */
    }
    recovered = await wal.readAll();
  }

  if (truncated) {
    safeEmit(onEvent, {
      type: 'limit',
      detail: 'recovery',
      count: recovered.length,
    });
  }

  if (recovered.length > 0) {
    batcher.injectRecovered(recovered);
  }
}

async function runRetentionPrune(opts: {
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
