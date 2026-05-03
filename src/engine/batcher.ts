import type { LogbunLog, BatchingConfig, DurabilityMode, IAdapter } from '../types';
import type { WALStorage } from '../storage/wal';
import type { DLQStorage } from '../storage/dlq';
import type { ConnectionPool } from './pool';

const DEFAULT_BATCHING: BatchingConfig = {
  maxSize: 100,
  flushInterval: 5_000,
  maxQueueSize: 1_000,
  onQueueFull: 'dlq',
};

/**
 * Batching Engine — per-tenant in-memory queues with backpressure.
 *
 * Responsibilities:
 *   - Buffer logs in per-tenant queues
 *   - Flush on size threshold (maxSize) or time interval (flushInterval)
 *   - Backpressure: dump to DLQ when queue exceeds maxQueueSize
 *   - Exponential backoff on flush failure before DLQ escalation
 *
 * The Batcher owns data queues. The Connection Pool owns sockets.
 * These concerns are strictly separated.
 */
export class Batcher {
  /** One queue per tenant — data is never mixed across tenants */
  private readonly queues: Map<string, LogbunLog[]> = new Map();
  private readonly timers: Map<string, Timer> = new Map();

  private readonly config: BatchingConfig;
  private readonly mode: DurabilityMode;
  private readonly wal: WALStorage | null;
  private readonly dlq: DLQStorage;
  private readonly pool: ConnectionPool;
  private readonly adapter: IAdapter;

  constructor(
    adapter: IAdapter,
    pool: ConnectionPool,
    wal: WALStorage | null,
    dlq: DLQStorage,
    mode: DurabilityMode,
    batchingConfig?: Partial<BatchingConfig>
  ) {
    this.adapter = adapter;
    this.pool = pool;
    this.wal = wal;
    this.dlq = dlq;
    this.mode = mode;
    this.config = { ...DEFAULT_BATCHING, ...batchingConfig };

    // Validate: durable mode + drop is a configuration error
    if (this.mode === 'durable' && this.config.onQueueFull === 'drop') {
      throw new Error(
        'Configuration error: onQueueFull="drop" is not valid with mode="durable". ' +
        'Use "dlq" to prevent data loss, or switch to mode="volatile" for drop behavior.'
      );
    }
  }

  /**
   * Enqueue a log entry.
   *
   * Flow (per guide §6.2 + §6.3):
   *   1. Determine queue key (tenantId ?? '__global__')
   *   2. If durable mode: WAL append (slight I/O overhead)
   *   3. Backpressure check — if >= maxQueueSize BEFORE push:
   *      - 'dlq': dump queue to DLQ, clear queue, then push new log
   *      - 'drop': silently discard the incoming log (return early)
   *   4. Push to in-memory queue
   *   5. If >= maxSize: immediate flush, else start flush timer
   */
  async enqueue(log: LogbunLog): Promise<void> {
    const key = log.tenantId ?? '__global__';

    // Get or create queue
    let queue = this.queues.get(key);
    if (!queue) {
      queue = [];
      this.queues.set(key, queue);
    }

    // Step 2: Durable mode — WAL append BEFORE any queue manipulation
    // Ensures every log is persisted even if backpressure triggers.
    // If WAL write fails (disk full, permissions), we still enqueue in
    // memory so the log isn't lost — it just won't survive a crash.
    if (this.mode === 'durable' && this.wal) {
      try {
        await this.wal.append(log);
      } catch {
        // WAL write failed — log will still be queued in-memory.
        // Not crash-safe, but better than losing the log entirely.
      }
    }

    // Step 3: Backpressure check — BEFORE pushing the new log
    if (queue.length >= this.config.maxQueueSize) {
      if (this.config.onQueueFull === 'drop') {
        // volatile + drop: silently discard the incoming log
        return;
      } else {
        // dlq: serialize entire queue to DLQ, then clear
        try {
          await this.dlq.write(log.tenantId ?? null, queue);
        } catch {
          // DLQ write failed — queue stays in memory, will retry on next backpressure
          // Don't clear the queue if we couldn't persist it
          return;
        }
        queue.length = 0;
        this.clearTimer(key);
      }
    }

    // Step 4: Push to in-memory queue
    queue.push(log);

    // Step 5: Check flush threshold
    if (queue.length >= this.config.maxSize) {
      // Immediate flush — don't await to keep fire() non-blocking
      void this.flush(key);
    } else if (!this.timers.has(key)) {
      // Start interval-based flush timer
      const timer = setTimeout(() => {
        this.timers.delete(key);
        void this.flush(key);
      }, this.config.flushInterval);
      this.timers.set(key, timer);
    }
  }

  /**
   * Flush a tenant's queue to the adapter.
   *
   * Flow:
   *   1. Snapshot current queue → local array, clear in-memory queue
   *   2. Attempt bulkInsert via adapter (using connection pool)
   *   3. On failure: exponential backoff (1s, 2s, 4s) → DLQ escalation
   *
   * NOTE: WAL is NOT truncated here — individual flushes only drain one
   * tenant's queue, but the WAL contains interleaved logs from ALL tenants.
   * WAL truncation happens only in flushAll() after ALL queues are drained.
   *
   * Entire method is wrapped in try/catch because flush() is called via
   * `void this.flush()` — unhandled rejections would crash the process.
   */
  async flush(key: string): Promise<void> {
    const queue = this.queues.get(key);
    if (!queue || queue.length === 0) return;

    // Snapshot and clear
    const snapshot = [...queue];
    queue.length = 0;
    this.clearTimer(key);

    const tenantId = key === '__global__' ? null : key;

    try {
      // Get the right adapter: use pool only for real tenant IDs
      let targetAdapter: IAdapter;
      if (tenantId) {
        try {
          targetAdapter = await this.pool.get(tenantId);
        } catch {
          targetAdapter = this.adapter;
        }
      } else {
        // __global__ key — always use the base adapter
        targetAdapter = this.adapter;
      }

      // Attempt insert with exponential backoff
      const maxRetries = 3;
      const baseDelay = 1_000;

      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        if (attempt > 0) {
          const delay = baseDelay * Math.pow(2, attempt - 1);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }

        const success = await targetAdapter.bulkInsert(tenantId, snapshot);
        if (success) return;
      }

      // All retries exhausted — escalate to DLQ
      await this.dlq.write(tenantId, snapshot);
    } catch {
      // Last resort: try to DLQ the snapshot so data isn't lost
      try {
        await this.dlq.write(tenantId, snapshot);
      } catch {
        // Both adapter and DLQ failed — data is lost.
        // This should be extremely rare (disk full, permissions).
      }
    }
  }

  /**
   * Flush all tenant queues — called during graceful shutdown.
   * After all queues are drained, truncates the WAL (safe because
   * all data has been flushed or DLQ'd).
   */
  async flushAll(): Promise<void> {
    const keys = [...this.queues.keys()];
    await Promise.allSettled(keys.map((key) => this.flush(key)));

    // Truncate WAL only after ALL queues have been flushed.
    // If truncation fails (e.g. disk full), duplicates on next recovery
    // are handled safely by INSERT OR IGNORE in the adapters.
    if (this.mode === 'durable' && this.wal) {
      try {
        await this.wal.truncate();
      } catch {
        // WAL truncation failed — stale entries remain but are safe
        // due to idempotent INSERT OR IGNORE in adapters.
      }
    }
  }

  /**
   * Inject recovered logs at the front of their respective queues.
   * Called by bootstrap after WAL recovery.
   */
  injectRecovered(logs: LogbunLog[]): void {
    for (const log of logs) {
      const key = log.tenantId ?? '__global__';
      let queue = this.queues.get(key);
      if (!queue) {
        queue = [];
        this.queues.set(key, queue);
      }
      // Prepend — recovered logs go to the front
      queue.unshift(log);
    }
  }

  /** Clear the flush timer for a tenant */
  private clearTimer(key: string): void {
    const timer = this.timers.get(key);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(key);
    }
  }
}
