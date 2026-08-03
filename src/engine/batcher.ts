import type {
  LogbunLog,
  BatchingConfig,
  DurabilityMode,
  IAdapter,
  LogbunEvent,
  RetryConfig,
  ReliabilityAdapter,
} from '../types';
import type { ConnectionPool } from './pool';
import { safeEmit, type LogbunEventHandler } from '../events';

const DEFAULT_BATCHING: BatchingConfig = {
  maxSize: 100,
  flushInterval: 5_000,
  maxQueueSize: 1_000,
  onQueueFull: 'dlq',
};

/** insertMaxRetries = total bulkInsert attempts (default 3). */
const DEFAULT_INSERT_MAX_RETRIES = 3;
const DEFAULT_INSERT_BASE_DELAY_MS = 1_000;
const DEFAULT_MAX_ACTIVE_TENANTS = 10_000;
const DEFAULT_MAX_TOTAL_QUEUED = 50_000;
const DEFAULT_FLUSH_TIMEOUT_MS = 30_000;
/** Max concurrent bulkInsert flushes globally. */
const DEFAULT_MAX_FLUSH_CONCURRENCY = 16;

/** Constructor deps for {@link Batcher} — avoids long positional arg lists. */
export interface BatcherDeps {
  adapter: IAdapter;
  pool: ConnectionPool;
  reliability: ReliabilityAdapter;
  mode: DurabilityMode;
  batching?: Partial<BatchingConfig>;
  onEvent?: LogbunEventHandler;
  retry?: RetryConfig;
  /** Max logs injected from journal recovery per wave. @default maxQueueSize */
  maxRecoveryBatch?: number;
  /**
   * Max concurrent queue keys (tenant map size).
   * @default 10_000
   */
  maxActiveTenants?: number;
  /**
   * Global cap: sum of all queue lengths + reservations.
   * @default 50_000
   */
  maxTotalQueued?: number;
  /**
   * Max wait per bulkInsert flush path in flushAll (Promise.race).
   * @default 30_000
   */
  flushTimeoutMs?: number;
  /**
   * Max concurrent bulkInsert flushes globally (semaphore).
   * Queue snapshot/clear is not blocked — only the insert path.
   * @default 16
   */
  maxFlushConcurrency?: number;
}

export interface BatcherStats {
  queued: number;
  tenants: number;
  recoveryBacklog: number;
  inflightFlushes: number;
}

/** Source for the next durable recovery wave after the current queues drain. */
export type RecoveryWaveLoader = () => Promise<{
  logs: LogbunLog[];
  truncated: boolean;
}>;

/**
 * Batching Engine — per-tenant in-memory queues with backpressure.
 *
 * Owns data queues only. The connection pool owns sockets.
 *
 * {@link enqueue} returns `true` when the log was queued or safely DLQ'd,
 * `false` when dropped. Callers such as fireAsync may throw on `false` in
 * durable mode.
 */
export class Batcher {
  private readonly queues: Map<string, LogbunLog[]> = new Map();
  private readonly timers: Map<string, ReturnType<typeof setTimeout>> =
    new Map();
  /** Reserved slots awaiting WAL append (sync admit, before any await). */
  private readonly inflightReservations: Map<string, number> = new Map();
  /** Per-key flush serialization chain (belt + suspenders). */
  private readonly flushChains: Map<string, Promise<void>> = new Map();
  /** Remaining WAL recovery logs not yet injected (bounded inject). */
  private recoveryBacklog: LogbunLog[] = [];
  private recoveryLoader: RecoveryWaveLoader | null = null;
  private recoverySourceMayHaveMore = false;
  private recoveryLoad: Promise<boolean> | null = null;
  private recoveryPumpQueued = false;

  private readonly config: BatchingConfig;
  private readonly mode: DurabilityMode;
  private readonly reliability: ReliabilityAdapter;
  private readonly pool: ConnectionPool;
  private readonly adapter: IAdapter;
  private readonly onEvent?: LogbunEventHandler;
  private readonly insertMaxRetries: number;
  private readonly insertBaseDelayMs: number;
  private readonly maxRecoveryBatch: number;
  private readonly maxActiveTenants: number;
  private readonly maxTotalQueued: number;
  private readonly flushTimeoutMs: number;
  private readonly maxFlushConcurrency: number;

  private shuttingDown = false;
  private inflightFlushes = 0;
  /** Slots held by in-flight bulkInsert paths (global semaphore). */
  private flushSlots = 0;
  private flushWaiters: Array<() => void> = [];

  constructor(deps: BatcherDeps) {
    this.adapter = deps.adapter;
    this.pool = deps.pool;
    this.reliability = deps.reliability;
    this.mode = deps.mode;
    this.config = { ...DEFAULT_BATCHING, ...deps.batching };
    this.onEvent = deps.onEvent;
    this.insertMaxRetries =
      deps.retry?.insertMaxRetries ?? DEFAULT_INSERT_MAX_RETRIES;
    this.insertBaseDelayMs =
      deps.retry?.insertBaseDelayMs ?? DEFAULT_INSERT_BASE_DELAY_MS;
    this.maxRecoveryBatch =
      deps.maxRecoveryBatch ?? this.config.maxQueueSize;
    this.maxActiveTenants =
      deps.maxActiveTenants ?? DEFAULT_MAX_ACTIVE_TENANTS;
    this.maxTotalQueued = deps.maxTotalQueued ?? DEFAULT_MAX_TOTAL_QUEUED;
    this.flushTimeoutMs = deps.flushTimeoutMs ?? DEFAULT_FLUSH_TIMEOUT_MS;
    this.maxFlushConcurrency = Math.max(
      1,
      deps.maxFlushConcurrency ?? DEFAULT_MAX_FLUSH_CONCURRENCY
    );
    if (this.mode === 'durable' && this.config.onQueueFull === 'drop') {
      throw new Error(
        'Configuration error: onQueueFull="drop" is not valid with mode="durable". ' +
          'Use "dlq" to prevent data loss, or switch to mode="volatile" for drop behavior.'
      );
    }
  }

  private emit(e: LogbunEvent): void {
    safeEmit(this.onEvent, e);
  }

  /** True when a DLQ write error indicates the file-count cap was hit. */
  private isDlqFullError(err: unknown): boolean {
    const msg = err instanceof Error ? err.message : String(err);
    return msg.includes('dlq_full');
  }

  /**
   * Emit drop (or reuse caller type path) when DLQ write fails.
   * Uses detail `dlq_full` when the error message indicates the DLQ cap.
   */
  private emitDlqWriteFail(
    tenantId: string | null,
    count: number,
    err: unknown,
    fallbackDetail: string
  ): void {
    const msg = err instanceof Error ? err.message : String(err);
    this.emit({
      type: 'drop',
      tenantId,
      count,
      detail: this.isDlqFullError(err) ? 'dlq_full' : fallbackDetail,
      error: msg,
    });
  }

  beginShutdown(): void {
    this.shuttingDown = true;
  }

  /**
   * Snapshot of queue pressure for ops / AuditLogger.getStats.
   */
  getStats(): BatcherStats {
    let queued = 0;
    for (const q of this.queues.values()) {
      queued += q.length;
    }
    return {
      queued,
      tenants: this.queues.size,
      recoveryBacklog: this.recoveryBacklog.length,
      inflightFlushes: this.inflightFlushes,
    };
  }

  /**
   * Admit a log into the per-tenant queue (or durable DLQ fallbacks).
   * @returns true if queued or safely DLQ'd; false if dropped
   */
  async enqueue(log: LogbunLog): Promise<boolean> {
    const key = log.tenantId ?? '__global__';
    const tenantId = log.tenantId ?? null;

    if (this.shuttingDown) {
      return this.enqueueDuringShutdown(log, tenantId);
    }

    // Global tenant-map cap — check before creating a new queue key
    if (
      this.queues.size >= this.maxActiveTenants &&
      !this.queues.has(key)
    ) {
      this.emit({
        type: 'drop',
        tenantId,
        count: 1,
        detail: 'max_active_tenants',
      });
      return false;
    }

    const queue = this.getOrCreateQueue(key);

    if (!(await this.admitUnderBackpressure(key, queue, tenantId))) {
      this.maybePruneQueue(key);
      return false;
    }

    if (!(await this.admitGlobalCap(key, tenantId))) {
      this.maybePruneQueue(key);
      return false;
    }

    this.reserve(key);
    try {
      const durable = await this.appendDurable(log, tenantId);
      if (durable === 'dlq') {
        // WAL failed but DLQ holds the log — do not also queue
        return true;
      }
      if (durable === 'fail') {
        return false;
      }

      if (!(await this.ensureQueueRoom(key, queue, tenantId))) {
        // WAL already holds the log in durable mode — escalate to DLQ so
        // the live process can retry (do not leave it stranded until restart).
        return this.persistSingleAfterRoomFail(log, tenantId);
      }

      queue.push(log);
      this.emit({ type: 'enqueue', tenantId, count: 1 });
      this.armFlush(key, queue);
      return true;
    } finally {
      this.releaseReserve(key);
      this.maybePruneQueue(key);
    }
  }

  /**
   * Flush one queue key. Concurrent calls for the same key serialize.
   */
  async flush(key: string): Promise<void> {
    const prev = this.flushChains.get(key) ?? Promise.resolve();
    const run = prev
      .catch(() => {
        /* prior flush errors must not block the chain */
      })
      .then(() => this.flushKey(key));
    this.flushChains.set(key, run);
    try {
      await run;
    } finally {
      if (this.flushChains.get(key) === run) {
        this.flushChains.delete(key);
      }
    }
  }

  private async flushKey(key: string): Promise<void> {
    const queue = this.queues.get(key);
    if (!queue || queue.length === 0) return;

    this.inflightFlushes++;
    try {
      // Sync splice ≤ maxSize before any await (no double-take; no huge inserts).
      const snapshot = queue.splice(0, this.flushChunkSize());
      this.clearTimer(key);
      const tenantId = key === '__global__' ? null : key;

      await this.acquireFlushSlot();
      try {
        try {
          const insertResult = await this.insertSnapshot(tenantId, snapshot);
          if (insertResult === 'adapter_fail') {
            this.rearmAfterChunk(key);
            this.drainRecoveryBacklog();
            return;
          }

          if (insertResult.ok) {
            await this.acknowledgeJournal(snapshot);
            this.emit({ type: 'flush_ok', tenantId, count: snapshot.length });
            this.rearmAfterChunk(key);
            this.drainRecoveryBacklog();
            return;
          }

          await this.persistSnapshotToDlq(
            tenantId,
            snapshot,
            'flush_retries_exhausted'
          );
          this.emit({
            type: 'flush_fail',
            tenantId,
            count: snapshot.length,
            detail: 'retries_exhausted',
            error: insertResult.error,
          });
        } catch (err) {
          await this.persistSnapshotToDlq(tenantId, snapshot, 'flush_error');
          this.emit({
            type: 'flush_fail',
            tenantId,
            count: snapshot.length,
            error: err instanceof Error ? err.message : String(err),
          });
        }
        this.rearmAfterChunk(key);
        this.drainRecoveryBacklog();
      } finally {
        this.releaseFlushSlot();
      }
    } finally {
      this.inflightFlushes--;
      this.scheduleRecoveryPump();
    }
  }

  /** Effective bulkInsert chunk size (at least 1). */
  private flushChunkSize(): number {
    return Math.max(1, this.config.maxSize);
  }

  /** Re-arm remainder after a chunk, or drop empty queue keys. */
  private rearmAfterChunk(key: string): void {
    const queue = this.queues.get(key);
    if (queue && queue.length > 0) {
      this.armFlush(key, queue);
    } else {
      this.maybePruneQueue(key);
    }
  }

  /** Acquire a global bulkInsert concurrency slot. */
  private async acquireFlushSlot(): Promise<void> {
    if (this.flushSlots < this.maxFlushConcurrency) {
      this.flushSlots++;
      return;
    }
    await new Promise<void>((resolve) => {
      this.flushWaiters.push(resolve);
    });
    // Slot transferred from a releaser — count unchanged.
  }

  /** Release a global bulkInsert concurrency slot (wake one waiter if any). */
  private releaseFlushSlot(): void {
    const next = this.flushWaiters.shift();
    if (next) {
      next();
    } else {
      this.flushSlots = Math.max(0, this.flushSlots - 1);
    }
  }

  /**
   * Drain in-memory queues. Does **not** wipe the WAL.
   * Successful flushes / DLQ paths acknowledge ids; remaining unacked
   * entries stay on disk for the next process. Compacts the WAL (safe).
   *
   * Bounded by {@link flushTimeoutMs} overall (not per-key race): hung
   * inserts can still block until the deadline, then we return. Callers
   * must not close adapters while inflight flushes remain if they need
   * in-flight safety — shutdown is best-effort after the deadline.
   */
  async flushAll(): Promise<void> {
    this.drainRecoveryBacklog(true);

    const deadline =
      this.flushTimeoutMs > 0
        ? Date.now() + this.flushTimeoutMs
        : Number.POSITIVE_INFINITY;
    const maxSpins = 1_000;

    for (let spin = 0; spin < maxSpins; spin++) {
      if (Date.now() >= deadline) break;

      // Fair-share: flush largest queues first under the concurrency semaphore
      const keys = [...this.queues.keys()]
        .filter((k) => (this.queues.get(k)?.length ?? 0) > 0)
        .sort(
          (a, b) =>
            (this.queues.get(b)?.length ?? 0) -
            (this.queues.get(a)?.length ?? 0)
        );
      if (keys.length > 0) {
        await Promise.allSettled(keys.map((key) => this.flush(key)));
      }

      if (this.inflightFlushes === 0) {
        const remaining = [...this.queues.values()].some((q) => q.length > 0);
        if (!remaining && this.recoveryBacklog.length === 0) {
          if (await this.loadRecoveryWave()) continue;
          break;
        }
        if (this.recoveryBacklog.length > 0) {
          const before = this.recoveryBacklog.length;
          this.drainRecoveryBacklog(true);
          // Caps may leave backlog undrained; avoid spinning when no progress
          if (
            this.recoveryBacklog.length >= before &&
            ![...this.queues.values()].some((q) => q.length > 0)
          ) {
            break;
          }
          continue;
        }
      } else {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    }

    // Wait for in-flight flushes until deadline so shutdown does not close
    // adapters under active bulkInsert when possible.
    while (this.inflightFlushes > 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    if (this.mode === 'durable' && this.reliability.persistent) {
      try {
        await this.reliability.compactJournal();
      } catch {
        // Non-fatal; unacked entries remain for next boot
      }
    }
  }

  /**
   * Inject WAL recovery logs in bounded batches to avoid OOM.
   * Excess is held in {@link recoveryBacklog} and drained after flushes.
   * Respects {@link maxActiveTenants}, per-key maxQueueSize, and
   * {@link maxTotalQueued}: logs that cannot be admitted stay in the
   * recovery backlog.
   */
  injectRecovered(logs: LogbunLog[]): void {
    if (logs.length === 0) return;

    const batchSize = Math.max(1, this.maxRecoveryBatch);
    const first = logs.slice(0, batchSize);
    const rest = logs.slice(batchSize);
    if (rest.length > 0) {
      this.recoveryBacklog.push(...rest);
    }
    this.injectWave(first);
  }

  /**
   * Continue a truncated persistent recovery only after currently recovered
   * records have been settled. This keeps boot memory bounded and prevents a
   * second journal read from replaying records that are still in flight.
   */
  setRecoveryLoader(loader: RecoveryWaveLoader): void {
    this.recoveryLoader = loader;
    this.recoverySourceMayHaveMore = true;
    this.scheduleRecoveryPump();
  }

  scheduleRecoveredFlush(keys?: Iterable<string>): void {
    const keyList = keys ? [...keys] : [...this.queues.keys()];
    for (const key of keyList) {
      const queue = this.queues.get(key);
      if (!queue || queue.length === 0) continue;
      this.armFlush(key, queue);
    }
  }

  // ─── Enqueue helpers ────────────────────────────────────────────────

  /**
   * Admit a recovery wave into queues, respecting active-tenant, per-queue,
   * and global queue caps. Logs that cannot be admitted are prepended to
   * {@link recoveryBacklog} (priority for the next drain).
   *
   * Policy:
   * - Existing queue keys always eligible (subject to maxQueueSize / maxTotalQueued).
   * - New keys only until `queues.size` reaches maxActiveTenants.
   * - Never push past per-key maxQueueSize or maxTotalQueued; remainder stays
   *   in recoveryBacklog.
   */
  private injectWave(logs: LogbunLog[]): void {
    if (logs.length === 0) return;

    const byKey = new Map<string, LogbunLog[]>();
    const keyOrder: string[] = [];
    for (const log of logs) {
      const key = log.tenantId ?? '__global__';
      let group = byKey.get(key);
      if (!group) {
        group = [];
        byKey.set(key, group);
        keyOrder.push(key);
      }
      group.push(log);
    }

    const deferred: LogbunLog[] = [];
    const affected = new Set<string>();
    let totalQueued = this.totalOccupancy();
    const maxQueueSize = this.config.maxQueueSize;

    for (const key of keyOrder) {
      const recovered = byKey.get(key)!;
      const hasQueue = this.queues.has(key);

      if (!hasQueue) {
        // getOrCreateQueue mutates `queues` immediately, so its current size
        // is the authoritative count. Do not add a second per-wave count.
        if (this.queues.size >= this.maxActiveTenants) {
          deferred.push(...recovered);
          continue;
        }
      }

      const inQueue = this.queues.get(key)?.length ?? 0;
      const accepted: LogbunLog[] = [];
      for (const log of recovered) {
        if (totalQueued >= this.maxTotalQueued) {
          deferred.push(log);
          continue;
        }
        // Per-tenant cap: do not push past maxQueueSize for this key
        if (inQueue + accepted.length >= maxQueueSize) {
          deferred.push(log);
          continue;
        }
        accepted.push(log);
        totalQueued++;
      }

      if (accepted.length === 0) continue;

      const queue = this.getOrCreateQueue(key);
      queue.unshift(...accepted);
      affected.add(key);
    }

    if (deferred.length > 0) {
      this.recoveryBacklog.unshift(...deferred);
    }
    if (affected.size > 0) {
      this.scheduleRecoveredFlush(affected);
    }
  }

  /**
   * Move more recovery backlog into queues when capacity allows.
   * @param force ignore soft occupancy and inject up to maxRecoveryBatch
   *   (still hard-respects maxActiveTenants / maxQueueSize / maxTotalQueued
   *   via injectWave)
   */
  private drainRecoveryBacklog(force = false): void {
    if (this.recoveryBacklog.length === 0) return;

    const batchSize = Math.max(1, this.maxRecoveryBatch);
    if (!force) {
      // Soft caps: only inject when under per-queue soft ceiling and global cap
      let total = 0;
      for (const q of this.queues.values()) total += q.length;
      if (total >= this.config.maxQueueSize) return;
      if (this.totalOccupancy() >= this.maxTotalQueued) return;
    }

    const wave = this.recoveryBacklog.splice(0, batchSize);
    this.injectWave(wave);
  }

  private hasQueuedRecoveryWork(): boolean {
    return [...this.queues.values()].some((queue) => queue.length > 0);
  }

  private scheduleRecoveryPump(): void {
    if (
      !this.recoveryLoader ||
      !this.recoverySourceMayHaveMore ||
      this.recoveryPumpQueued
    ) {
      return;
    }
    this.recoveryPumpQueued = true;
    queueMicrotask(() => {
      this.recoveryPumpQueued = false;
      void this.loadRecoveryWave();
    });
  }

  /** Load at most one persisted wave, only when no prior wave is in flight. */
  private async loadRecoveryWave(): Promise<boolean> {
    if (
      !this.recoveryLoader ||
      !this.recoverySourceMayHaveMore ||
      this.recoveryBacklog.length > 0 ||
      this.hasQueuedRecoveryWork() ||
      this.inflightFlushes > 0
    ) {
      return false;
    }
    if (this.recoveryLoad) return this.recoveryLoad;

    const run = (async (): Promise<boolean> => {
      try {
        const wave = await this.recoveryLoader!();
        // An empty truncated response cannot make progress; stop rather than
        // spinning maintenance forever on a broken adapter.
        this.recoverySourceMayHaveMore =
          wave.truncated && wave.logs.length > 0;
        if (wave.logs.length === 0) return false;
        this.injectRecovered(wave.logs);
        return true;
      } catch (error) {
        this.recoverySourceMayHaveMore = false;
        this.emit({
          type: 'flush_fail',
          count: 0,
          detail: 'recovery',
          error: error instanceof Error ? error.message : String(error),
        });
        return false;
      }
    })();
    this.recoveryLoad = run;
    try {
      return await run;
    } finally {
      if (this.recoveryLoad === run) this.recoveryLoad = null;
    }
  }

  private async enqueueDuringShutdown(
    log: LogbunLog,
    tenantId: string | null
  ): Promise<boolean> {
    try {
      if (this.mode !== 'durable') {
        this.emit({ type: 'drop', tenantId, count: 1, detail: 'shutdown' });
        return false;
      }
      if (this.reliability.persistent) {
        try {
          await this.reliability.appendJournal(log);
        } catch {
          this.emit({ type: 'wal_fail', tenantId, detail: 'shutdown_enqueue' });
        }
      }
      try {
        await this.reliability.writeDlq(tenantId, [log]);
        // Ack journal only after DLQ success so shutdown path cannot lose the only copy
        await this.acknowledgeJournal([log]);
        this.emit({ type: 'dlq', tenantId, count: 1, detail: 'shutdown' });
        return true;
      } catch (err) {
        // WAL still holds the log if append succeeded
        this.emitDlqWriteFail(tenantId, 1, err, 'shutdown_dlq_fail');
        return false;
      }
    } catch {
      // fire() never-throws contract
      return false;
    }
  }

  private async admitUnderBackpressure(
    key: string,
    queue: LogbunLog[],
    tenantId: string | null
  ): Promise<boolean> {
    if (this.occupancy(key, queue) < this.config.maxQueueSize) return true;

    if (this.config.onQueueFull === 'drop') {
      this.emit({ type: 'drop', tenantId, count: 1, detail: 'queue_full' });
      return false;
    }

    if (!(await this.dumpQueueToDlq(key, queue, tenantId))) return false;

    if (this.occupancy(key, queue) >= this.config.maxQueueSize) {
      return this.dumpQueueToDlq(key, queue, tenantId);
    }
    return true;
  }

  /**
   * Global occupancy cap across all tenants.
   * When full: dump largest non-empty queues first (fair-share) or drop per onQueueFull.
   */
  private async admitGlobalCap(
    key: string,
    tenantId: string | null
  ): Promise<boolean> {
    if (this.totalOccupancy() < this.maxTotalQueued) return true;

    if (this.config.onQueueFull === 'drop') {
      this.emit({
        type: 'drop',
        tenantId,
        count: 1,
        detail: 'max_total_queued',
      });
      return false;
    }

    // Fair-share: dump largest non-empty queues first (noisy-neighbor relief)
    let spins = 0;
    while (this.totalOccupancy() >= this.maxTotalQueued && spins < 64) {
      spins++;
      const victimKey = this.findLargestNonEmptyKey(key);
      if (!victimKey) {
        this.emit({
          type: 'drop',
          tenantId,
          count: 1,
          detail: 'max_total_queued',
        });
        return false;
      }
      const victimQueue = this.queues.get(victimKey);
      if (!victimQueue || victimQueue.length === 0) {
        this.emit({
          type: 'drop',
          tenantId,
          count: 1,
          detail: 'max_total_queued',
        });
        return false;
      }
      const victimTenant = victimKey === '__global__' ? null : victimKey;
      if (!(await this.dumpQueueToDlq(victimKey, victimQueue, victimTenant))) {
        this.emit({
          type: 'drop',
          tenantId,
          count: 1,
          detail: 'max_total_queued',
        });
        return false;
      }
    }

    // Prefer not to block the admitting key forever if still full after dumps
    if (this.totalOccupancy() >= this.maxTotalQueued) {
      // Last resort: dump current key if it already has data
      const queue = this.queues.get(key);
      if (queue && queue.length > 0) {
        if (!(await this.dumpQueueToDlq(key, queue, tenantId))) {
          this.emit({
            type: 'drop',
            tenantId,
            count: 1,
            detail: 'max_total_queued',
          });
          return false;
        }
      }
    }

    if (this.totalOccupancy() >= this.maxTotalQueued) {
      this.emit({
        type: 'drop',
        tenantId,
        count: 1,
        detail: 'max_total_queued',
      });
      return false;
    }
    return true;
  }

  /**
   * Fair-share victim: largest queue (noisy neighbor first).
   * Prefer not to dump the admitting key while others have work.
   */
  private findLargestNonEmptyKey(preferExclude?: string): string | null {
    let best: string | null = null;
    let bestLen = -1;
    for (const [k, q] of this.queues) {
      if (preferExclude && k === preferExclude) continue;
      if (q.length > bestLen) {
        bestLen = q.length;
        best = k;
      }
    }
    if (best && bestLen > 0) return best;
    // Fallback: include admitter's own key if it is the only non-empty
    if (preferExclude) {
      const self = this.queues.get(preferExclude);
      if (self && self.length > 0) return preferExclude;
    }
    return null;
  }

  private async ensureQueueRoom(
    key: string,
    queue: LogbunLog[],
    tenantId: string | null
  ): Promise<boolean> {
    while (queue.length >= this.config.maxQueueSize) {
      if (this.config.onQueueFull === 'drop') {
        this.emit({ type: 'drop', tenantId, count: 1, detail: 'queue_full' });
        return false;
      }
      if (!(await this.dumpQueueToDlq(key, queue, tenantId))) {
        // dumpQueueToDlq already emits dlq_full when applicable
        this.emit({ type: 'drop', tenantId, count: 1, detail: 'dlq_fail' });
        return false;
      }
    }
    return true;
  }

  /**
   * After a successful durable WAL append, if the RAM queue cannot accept the
   * log, write a single-log DLQ batch (and ack WAL) so retry continues live.
   * Volatile: the log is dropped (nothing on disk).
   */
  private async persistSingleAfterRoomFail(
    log: LogbunLog,
    tenantId: string | null
  ): Promise<boolean> {
    if (this.mode === 'durable' && this.reliability.persistent) {
      try {
        await this.reliability.writeDlq(tenantId, [log]);
        await this.acknowledgeJournal([log]);
        this.emit({
          type: 'dlq',
          tenantId,
          count: 1,
          detail: 'queue_room_fail',
        });
        return true;
      } catch (err) {
        // Unacked WAL still holds the log for crash recovery
        if (this.isDlqFullError(err)) {
          this.emitDlqWriteFail(tenantId, 1, err, 'queue_room_fail');
        } else {
          this.emit({
            type: 'flush_fail',
            tenantId,
            count: 1,
            detail: 'queue_room_fail',
          });
        }
        return false;
      }
    }
    this.emit({ type: 'drop', tenantId, count: 1, detail: 'queue_full' });
    return false;
  }

  /**
   * Durable WAL append with DLQ fallback on failure.
   * - ok: WAL written (or volatile / no WAL)
   * - dlq: WAL failed, single-log DLQ succeeded (do not queue)
   * - fail: both failed (caller returns false from enqueue)
   */
  private async appendDurable(
    log: LogbunLog,
    tenantId: string | null
  ): Promise<'ok' | 'dlq' | 'fail'> {
    if (this.mode !== 'durable' || !this.reliability.persistent) return 'ok';
    try {
      await this.reliability.appendJournal(log);
      return 'ok';
    } catch (err) {
      const walMsg = err instanceof Error ? err.message : String(err);
      const walFull = walMsg.includes('wal_full');
      try {
        await this.reliability.writeDlq(tenantId, [log]);
        this.emit({
          type: 'wal_fail',
          tenantId,
          detail: walFull ? 'wal_full' : 'append',
          error: walMsg,
        });
        this.emit({
          type: 'dlq',
          tenantId,
          count: 1,
          detail: 'wal_fail',
        });
        return 'dlq';
      } catch (err) {
        this.emit({ type: 'wal_fail', tenantId, detail: 'append' });
        this.emitDlqWriteFail(tenantId, 1, err, 'wal_and_dlq_fail');
        return 'fail';
      }
    }
  }

  private armFlush(key: string, queue: LogbunLog[]): void {
    if (queue.length >= this.flushChunkSize()) {
      void this.flush(key);
      return;
    }
    if (this.timers.has(key)) return;
    const timer = setTimeout(() => {
      this.timers.delete(key);
      void this.flush(key);
    }, this.config.flushInterval);
    this.timers.set(key, timer);
  }

  // ─── Flush helpers ──────────────────────────────────────────────────

  /**
   * Insert snapshot via pool.withAdapter (pins tenant adapters).
   * null tenantId uses the base adapter (no pool).
   * Returns 'adapter_fail' when tenant resolve failed and snapshot was DLQ'd.
   */
  private async insertSnapshot(
    tenantId: string | null,
    snapshot: LogbunLog[]
  ): Promise<{ ok: boolean; error?: string } | 'adapter_fail'> {
    if (!tenantId) {
      return this.bulkInsertWithBackoff(this.adapter, null, snapshot);
    }

    try {
      return await this.pool.withAdapter(tenantId, (adapter) =>
        this.bulkInsertWithBackoff(adapter, tenantId, snapshot)
      );
    } catch (err) {
      try {
        await this.reliability.writeDlq(tenantId, snapshot);
        await this.acknowledgeJournal(snapshot);
        this.emit({
          type: 'dlq',
          tenantId,
          count: snapshot.length,
          detail: 'tenant_adapter',
        });
      } catch (dlqErr) {
        // both failed — WAL still holds durable copy
        this.emitDlqWriteFail(
          tenantId,
          snapshot.length,
          dlqErr,
          'tenant_adapter_dlq_fail'
        );
      }
      this.emit({
        type: 'flush_fail',
        tenantId,
        count: snapshot.length,
        error: err instanceof Error ? err.message : String(err),
        detail: 'tenant_adapter',
      });
      return 'adapter_fail';
    }
  }

  /**
   * bulkInsert with exponential backoff.
   * insertMaxRetries is the **total** number of attempts (default 3).
   * Delays apply between retries only.
   */
  private async bulkInsertWithBackoff(
    adapter: IAdapter,
    tenantId: string | null,
    snapshot: LogbunLog[]
  ): Promise<{ ok: boolean; error?: string }> {
    // Floor at 1 so insertMaxRetries: 0 still does a single attempt
    const maxAttempts = Math.max(1, this.insertMaxRetries);
    let lastError: string | undefined;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      if (attempt > 0) {
        const delay = this.insertBaseDelayMs * Math.pow(2, attempt - 1);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
      try {
        if (await adapter.bulkInsert(tenantId, snapshot)) {
          return { ok: true };
        }
        lastError = lastError ?? 'bulkInsert returned false';
      } catch (err) {
        lastError = err instanceof Error ? err.message : String(err);
      }
    }
    return { ok: false, error: lastError };
  }

  private async persistSnapshotToDlq(
    tenantId: string | null,
    snapshot: LogbunLog[],
    detail: string
  ): Promise<void> {
    try {
      await this.reliability.writeDlq(tenantId, snapshot);
      await this.acknowledgeJournal(snapshot);
      this.emit({
        type: 'dlq',
        tenantId,
        count: snapshot.length,
        detail,
      });
    } catch (err) {
      // Durable data may still be in WAL — do NOT ack
      this.emitDlqWriteFail(tenantId, snapshot.length, err, detail);
    }
  }

  private async acknowledgeJournal(logs: LogbunLog[]): Promise<void> {
    if (this.mode !== 'durable' || !this.reliability.persistent || logs.length === 0) return;
    try {
      await this.reliability.acknowledgeJournal(logs.map((l) => l.id));
    } catch {
      // Non-fatal — adapters are idempotent on replay
    }
  }

  /**
   * Snapshot queue, clear **synchronously** (before any await), then DLQ write.
   * On write failure: restore snapshot to front of queue and return false
   * without acknowledging WAL.
   */
  private async dumpQueueToDlq(
    key: string,
    queue: LogbunLog[],
    tenantId: string | null
  ): Promise<boolean> {
    if (queue.length === 0) return true;
    const snapshot = [...queue];
    // Sync clear before any await — concurrent flush cannot double-take
    queue.length = 0;
    this.clearTimer(key);
    try {
      await this.reliability.writeDlq(tenantId, snapshot);
    } catch (err) {
      // Restore without acking journal — never leave the only copy only in-flight
      queue.unshift(...snapshot);
      // Always emit: dlq_full is the common ops signal; other write errors surface too
      this.emitDlqWriteFail(tenantId, snapshot.length, err, 'backpressure_dlq_fail');
      return false;
    }
    await this.acknowledgeJournal(snapshot);
    this.maybePruneQueue(key);
    this.emit({
      type: 'dlq',
      tenantId,
      count: snapshot.length,
      detail: 'backpressure',
    });
    return true;
  }

  // ─── Queue bookkeeping ──────────────────────────────────────────────

  private getOrCreateQueue(key: string): LogbunLog[] {
    let queue = this.queues.get(key);
    if (!queue) {
      queue = [];
      this.queues.set(key, queue);
    }
    return queue;
  }

  /** Drop empty queue entries to avoid unbounded Map growth. */
  private maybePruneQueue(key: string): void {
    const queue = this.queues.get(key);
    if (!queue) return;
    if (
      queue.length === 0 &&
      (this.inflightReservations.get(key) ?? 0) === 0 &&
      !this.timers.has(key)
    ) {
      this.queues.delete(key);
    }
  }

  private reserve(key: string): void {
    this.inflightReservations.set(
      key,
      (this.inflightReservations.get(key) ?? 0) + 1
    );
  }

  private releaseReserve(key: string): void {
    const cur = this.inflightReservations.get(key) ?? 0;
    if (cur <= 1) this.inflightReservations.delete(key);
    else this.inflightReservations.set(key, cur - 1);
  }

  private occupancy(key: string, queue: LogbunLog[]): number {
    return queue.length + (this.inflightReservations.get(key) ?? 0);
  }

  /** Sum of all queue lengths + all reservations (global occupancy). */
  private totalOccupancy(): number {
    let total = 0;
    for (const q of this.queues.values()) total += q.length;
    for (const r of this.inflightReservations.values()) total += r;
    return total;
  }

  private clearTimer(key: string): void {
    const timer = this.timers.get(key);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(key);
    }
  }
}
