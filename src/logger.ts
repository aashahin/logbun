import type {
  DLQEntry,
  LogbunConfig,
  LogbunEvent,
  LogbunLogInput,
  LogbunLog,
  LogbunQueryFilters,
  LogbunQueryResult,
  LogbunRequestContext,
} from './types';
import { bootstrap, runRetentionPrune, type BootstrapResult } from './bootstrap';
import { safeEmit } from './events';
import { capString } from './utils/json';
import { isTenantIdPresent } from './utils/tenant';
import {
  INTEGRITY_GENESIS,
  sealIntegrity,
  verifyIntegrityChain,
} from './utils/crypto';
import { randomUUIDv7 } from './utils/uuidv7';

const DEFAULT_MAX_QUERY_LIMIT = 500;
const DEFAULT_MAX_PAYLOAD_BYTES = 64_000;
const DEFAULT_MAX_STRING_FIELD_BYTES = 2_048;
/** Pre-ready buffer is volatile (even in durable mode) until {@link AuditLogger.ready}. */
const DEFAULT_MAX_PRE_READY_BUFFER = 10_000;

/**
 * Snapshot returned by {@link AuditLogger.getStats} / {@link AuditLogger.getStatsDetailed}.
 *
 * Sync {@link AuditLogger.getStats} always fills the required fields (including
 * `inflightFlushes`). Optional `walApproxBytes` / `dlq*` counts are filled only by
 * the async {@link AuditLogger.getStatsDetailed} path.
 */
export interface AuditLoggerStats {
  queued: number;
  tenants: number;
  degraded: boolean;
  recoveryBacklog: number;
  /** Concurrent bulkInsert flush paths currently in flight. */
  inflightFlushes: number;
  /** Best-effort journal size in bytes; 0 if volatile / not ready. Filled by getStatsDetailed. */
  walApproxBytes?: number;
  /** Pending DLQ entry count. Filled by getStatsDetailed. */
  dlqPending?: number;
  /** In-flight (processing) DLQ entry count. Filled by getStatsDetailed. */
  dlqProcessing?: number;
  /** Poisoned (dead) DLQ entry count. Filled by getStatsDetailed. */
  dlqDead?: number;
}

/**
 * AuditLogger — main public class for the Logbun library.
 *
 * Usage:
 * ```typescript
 * const audit = new AuditLogger<MyActions>({
 *   namespace: 'my-app',
 *   mode: 'durable',
 *   reliability: new FileReliabilityAdapter({ namespace: 'my-app', dataDir }),
 *   adapter: new BunSQLiteAdapter(),
 *   requireTenantId: true, // recommended for multi-tenant SaaS
 * });
 *
 * await audit.ready;
 * audit.fire('user.created', { actorId: 'u1', tenantId: 't1', entityId: 'u2' });
 * await audit.fireAsync('user.updated', { actorId: 'u1', tenantId: 't1' });
 * await audit.runMaintenance(); // host-scheduled flush + DLQ retry + retention
 * await audit.shutdown();
 * ```
 *
 * Notes:
 * - {@link fire} is never-throws fire-and-forget; pass `context.waitUntil` on Workers.
 * - {@link fireAsync} awaits full enqueue (including journal in durable mode) and may reject.
 * - Logs accepted before {@link ready} sit in a **volatile** pre-ready buffer; durable
 *   journal/DLQ paths apply only after bootstrap when the buffer is flushed.
 * - Volatile request runtimes: use `fireAsync()` + `flush()` for delivery guarantees.
 *   Detached `fire()` alone is not durable across isolate exit.
 */
export class AuditLogger<TActions extends string = string> {
  private engine: BootstrapResult | null = null;
  private readonly config: LogbunConfig<TActions>;
  private _shutdownCalled = false;
  private _degraded = false;
  /** Bounded buffer for fire() before bootstrap completes (volatile). */
  private preReadyBuffer: LogbunLog<TActions>[] = [];
  private preReadyDropped = 0;
  /** Tip of the integrity hash chain (last contentHash). */
  private integrityTip = INTEGRITY_GENESIS;
  /** Serializes integrity sealing so concurrent fire() keeps a total order. */
  private integrityChain: Promise<void> = Promise.resolve();
  /** Single-flight {@link runMaintenance}. */
  private maintenancePromise: Promise<void> | null = null;

  /**
   * Promise that resolves when bootstrap is complete.
   * Never rejects — bootstrap failures set {@link degraded} instead.
   */
  public readonly ready: Promise<void>;

  constructor(config: LogbunConfig<TActions>) {
    this.config = config;

    const mode = config.mode ?? 'volatile';
    // Durable requires a persistent reliability adapter (synchronous reject).
    if (mode === 'durable') {
      if (!config.reliability) {
        throw new Error(
          'durable mode requires LogbunConfig.reliability with a persistent adapter ' +
            '(e.g. FileReliabilityAdapter from "logbun/durability/filesystem" or ' +
            'CloudflareReliabilityAdapter from "logbun/durability/cloudflare")'
        );
      }
      if (!config.reliability.persistent) {
        throw new Error(
          'durable mode requires a persistent ReliabilityAdapter ' +
            '(MemoryReliabilityAdapter is not durable)'
        );
      }
    }

    // Back-compat defaults are unsafe for multi-tenant SaaS — emit once for ops.
    // Explicit mode:'volatile' still warns (name keeps "default" for alert continuity).
    if (mode === 'volatile') {
      this.emit({ type: 'limit', detail: 'unsafe_default_volatile' });
    }
    if (
      config.requireTenantId !== true &&
      config.tenancy?.mode !== 'database_per_tenant'
    ) {
      this.emit({ type: 'limit', detail: 'unsafe_default_require_tenant' });
    }

    this.ready = this.initialize().catch((err: unknown) => {
      this._degraded = true;
      this.preReadyBuffer = [];
      const error = err instanceof Error ? err.message : String(err);
      this.emit({ type: 'bootstrap_fail', error });
      this.emit({ type: 'degraded', error, detail: 'bootstrap_fail' });
    });
  }

  get degraded(): boolean {
    return this._degraded;
  }

  /**
   * Effective requireTenantId: explicit config, or forced for database_per_tenant.
   * Default remains false for back-compat unless tenancy forces it.
   */
  private get requireTenant(): boolean {
    return (
      this.config.requireTenantId === true ||
      this.config.tenancy?.mode === 'database_per_tenant'
    );
  }

  /**
   * Fire-and-forget enqueue. **Never throws.**
   * Failures are swallowed; observability via `onEvent` (`drop`, `wal_fail`, …).
   * For awaited durable enqueue, use {@link fireAsync}.
   */
  fire(
    action: TActions,
    input: Omit<LogbunLogInput<TActions>, 'action'>,
    context?: LogbunRequestContext
  ): void {
    if (this._degraded) {
      this.emit({
        type: 'drop',
        tenantId: input.tenantId ?? null,
        detail: 'degraded',
      });
      return;
    }

    const tenantMissing = !isTenantIdPresent(input.tenantId);
    if (tenantMissing && this.requireTenant) {
      this.emit({
        type: 'drop',
        tenantId: input.tenantId ?? null,
        detail: 'require_tenant_id',
      });
      return;
    }

    const log = this.buildLog(action, input, context);

    // Engine still up (including mid-shutdown): batcher owns durable shutdown path.
    if (this.engine) {
      const task = this.sealAndEnqueue(log).catch(() => {
        /* fire() never throws */
      });
      this.registerWaitUntil(context, task);
      return;
    }

    // Fully shut down — do not accumulate a pre-ready buffer again
    if (this._shutdownCalled) {
      this.emit({
        type: 'drop',
        tenantId: log.tenantId ?? null,
        detail: 'shutdown',
      });
      return;
    }

    // Pre-ready bounded buffer (bootstrap not finished) — volatile until ready.
    const maxPre =
      this.config.maxPreReadyBuffer ?? DEFAULT_MAX_PRE_READY_BUFFER;
    if (this.preReadyBuffer.length >= maxPre) {
      this.preReadyDropped++;
      this.emit({
        type: 'drop',
        tenantId: log.tenantId ?? null,
        detail: 'pre_ready_buffer_full',
        count: this.preReadyDropped,
      });
      return;
    }
    this.preReadyBuffer.push(log);
    // waitUntil covers ready + pre-ready drain (buffer flushed before ready resolves)
    this.registerWaitUntil(
      context,
      this.ready.then(
        () => undefined,
        () => undefined
      )
    );
  }

  /**
   * Await full enqueue including WAL append in durable mode.
   * May reject on WAL/DLQ hard failures or when the logger is degraded/unavailable.
   */
  async fireAsync(
    action: TActions,
    input: Omit<LogbunLogInput<TActions>, 'action'>,
    context?: LogbunRequestContext
  ): Promise<void> {
    if (this._degraded) {
      this.emit({
        type: 'drop',
        tenantId: input.tenantId ?? null,
        detail: 'degraded',
      });
      throw new Error('AuditLogger is degraded — fireAsync unavailable');
    }

    const tenantMissing = !isTenantIdPresent(input.tenantId);
    if (tenantMissing && this.requireTenant) {
      this.emit({
        type: 'drop',
        tenantId: input.tenantId ?? null,
        detail: 'require_tenant_id',
      });
      throw new Error(
        this.config.tenancy?.mode === 'database_per_tenant'
          ? 'tenantId is required when tenancy.mode is "database_per_tenant"'
          : 'tenantId is required when requireTenantId is true'
      );
    }

    const log = this.buildLog(action, input, context);

    await this.ready;

    if (this._degraded || !this.engine) {
      throw new Error('AuditLogger is not initialized — fireAsync unavailable');
    }

    try {
      const admitted = await this.sealAndEnqueue(log);
      if (admitted === false) {
        throw new Error(
          'Failed to enqueue audit log (durable hard fail / backpressure)'
        );
      }
    } catch (err) {
      if (err instanceof Error) throw err;
      throw new Error(String(err));
    }
  }

  /**
   * Verify a list of logs as an integrity chain (oldest first).
   * Requires logs that were sealed with `integrityChain: true`.
   */
  async verifyIntegrity(
    logs: LogbunLog<TActions>[],
    opts?: { genesis?: string }
  ): Promise<{ ok: boolean; failedAt: number; error?: string }> {
    return verifyIntegrityChain(logs, opts?.genesis ?? INTEGRITY_GENESIS);
  }

  /**
   * Sync queue / recovery snapshot for ops dashboards.
   * Always includes `inflightFlushes`. Does **not** hit disk for WAL/DLQ sizes —
   * use {@link getStatsDetailed} for those.
   * When degraded, returns zeros with `degraded: true`.
   * Before ready (engine not yet set), `queued` reflects {@link preReadyBuffer}
   * length (and `tenants` is `1` when the buffer is non-empty) so dashboards do
   * not look idle while fire() is buffering; other fields stay zero until ready.
   * After shutdown with no engine, returns zeros with `degraded: false`.
   */
  getStats(): AuditLoggerStats {
    if (this._degraded) {
      return {
        queued: 0,
        tenants: 0,
        degraded: true,
        recoveryBacklog: 0,
        inflightFlushes: 0,
      };
    }
    if (!this.engine) {
      // Pre-ready: surface buffer pressure so ops does not see false "healthy zeros".
      if (!this._shutdownCalled) {
        const queued = this.preReadyBuffer.length;
        return {
          queued,
          tenants: queued > 0 ? 1 : 0,
          degraded: false,
          recoveryBacklog: 0,
          inflightFlushes: 0,
        };
      }
      return {
        queued: 0,
        tenants: 0,
        degraded: false,
        recoveryBacklog: 0,
        inflightFlushes: 0,
      };
    }

    const s = this.engine.batcher.getStats();
    return {
      queued: s.queued,
      tenants: s.tenants,
      degraded: false,
      recoveryBacklog: s.recoveryBacklog,
      inflightFlushes: s.inflightFlushes,
    };
  }

  /**
   * Async stats including best-effort WAL size and DLQ file counts.
   * Falls back to zeros for disk metrics when WAL/DLQ are unavailable.
   */
  async getStatsDetailed(): Promise<AuditLoggerStats> {
    const base = this.getStats();
    if (this._degraded || !this.engine) {
      return {
        ...base,
        walApproxBytes: 0,
        dlqPending: 0,
        dlqProcessing: 0,
        dlqDead: 0,
      };
    }

    try {
      const rs = await this.engine.reliability.getStats();
      return {
        ...base,
        walApproxBytes: rs.journalApproxBytes,
        dlqPending: rs.dlqPending,
        dlqProcessing: rs.dlqProcessing,
        dlqDead: rs.dlqDead,
      };
    } catch {
      return {
        ...base,
        walApproxBytes: 0,
        dlqPending: 0,
        dlqProcessing: 0,
        dlqDead: 0,
      };
    }
  }

  async query(opts: {
    tenantId?: string;
    filters?: LogbunQueryFilters<TActions>;
    pagination?: { cursor?: string; limit?: number };
  }): Promise<LogbunQueryResult<TActions>> {
    await this.ready;

    if (!this.engine || this._degraded) {
      throw new Error('AuditLogger is not initialized — query unavailable');
    }

    const tenancyMode = this.config.tenancy?.mode;
    const tenantId = opts.tenantId;
    const tenantMissing = !isTenantIdPresent(tenantId);

    if (this.requireTenant && tenantMissing) {
      throw new Error(
        tenancyMode === 'database_per_tenant'
          ? 'tenantId is required when tenancy.mode is "database_per_tenant"'
          : 'tenantId is required when requireTenantId is true'
      );
    }

    const maxLimit = this.config.maxQueryLimit ?? DEFAULT_MAX_QUERY_LIMIT;
    const requested = opts.pagination?.limit ?? 50;
    const limit = Math.min(
      Math.max(1, Number.isFinite(requested) ? requested : 50),
      maxLimit
    );

    const filters = opts.filters ?? {};
    const pagination = {
      cursor: opts.pagination?.cursor,
      limit,
    };

    if (isTenantIdPresent(tenantId)) {
      try {
        // Pin tenant adapter for the query duration (LRU-safe)
        return await this.engine.pool.withAdapter(tenantId, (adapter) =>
          adapter.query(tenantId, filters, pagination) as Promise<
            LogbunQueryResult<TActions>
          >
        );
      } catch (err) {
        if (tenancyMode === 'database_per_tenant') {
          const message = err instanceof Error ? err.message : String(err);
          throw new Error(
            `Failed to resolve tenant adapter for tenantId "${tenantId}": ${message}`
          );
        }
        // single_database: fall through to base adapter
      }
    }

    return this.config.adapter.query(
      tenantId ?? null,
      filters,
      pagination
    ) as Promise<LogbunQueryResult<TActions>>;
  }

  /**
   * List DLQ entries (ops / admin). Includes pending by default; pass flags for dead.
   * Entries use opaque stable ids — pass those to requeueDead / deleteDead.
   */
  async listDlq(opts?: {
    includePending?: boolean;
    includeProcessing?: boolean;
    includeDead?: boolean;
  }): Promise<DLQEntry[]> {
    await this.ready;
    if (!this.engine || this._degraded) {
      throw new Error('AuditLogger is not initialized');
    }
    return this.engine.reliability.listDlq(opts);
  }

  /**
   * Re-queue a poisoned dead batch. Accepts opaque id; preserves id, resets attempts.
   */
  async requeueDead(id: string): Promise<string> {
    await this.ready;
    if (!this.engine || this._degraded) {
      throw new Error('AuditLogger is not initialized');
    }
    return this.engine.reliability.requeueDead(id);
  }

  /** Permanently delete a poisoned dead batch by opaque id. */
  async deleteDead(id: string): Promise<void> {
    await this.ready;
    if (!this.engine || this._degraded) {
      throw new Error('AuditLogger is not initialized');
    }
    await this.engine.reliability.deleteDead(id);
  }

  /**
   * Force an immediate DLQ retry scan (ops convenience).
   * Same scan logic as {@link runMaintenance} without flush/retention.
   */
  async retryDlqNow(): Promise<void> {
    await this.ready;
    if (!this.engine || this._degraded) {
      throw new Error('AuditLogger is not initialized');
    }
    await this.engine.retryEngine.scan();
  }

  /**
   * Flush all in-memory queues (and compact journal when durable).
   * Prefer for request-end volatile delivery: `await fireAsync(...); await flush()`.
   */
  async flush(): Promise<void> {
    await this.ready;
    if (!this.engine || this._degraded) {
      throw new Error('AuditLogger is not initialized');
    }
    await this.engine.batcher.flushAll();
  }

  /**
   * Single-flight maintenance: flush queued work, one DLQ retry scan,
   * and retention prune when configured. Hosts schedule this (cron, DO alarm).
   * Concurrent calls share the same in-flight promise.
   */
  async runMaintenance(): Promise<void> {
    if (this.maintenancePromise) return this.maintenancePromise;
    this.maintenancePromise = this.doMaintenance().finally(() => {
      this.maintenancePromise = null;
    });
    return this.maintenancePromise;
  }

  private async doMaintenance(): Promise<void> {
    await this.ready;
    if (!this.engine || this._degraded) {
      throw new Error('AuditLogger is not initialized');
    }
    const failures: unknown[] = [];
    let retentionFailure: unknown;
    try {
      await this.engine.batcher.flushAll();
    } catch (err) {
      failures.push(err);
      this.emit({
        type: 'flush_fail',
        error: err instanceof Error ? err.message : String(err),
        detail: 'maintenance_flush',
      });
    }
    try {
      await this.engine.retryEngine.scan();
    } catch (err) {
      failures.push(err);
      this.emit({
        type: 'flush_fail',
        error: err instanceof Error ? err.message : String(err),
        detail: 'maintenance_dlq_scan',
      });
    }
    if (this.config.retention) {
      try {
        await runRetentionPrune({
          tenancyMode: this.config.tenancy?.mode,
          knownTenantIds: this.config.tenancy?.knownTenantIds,
          pool: this.engine.pool,
          baseAdapter: this.config.adapter,
          retentionDays: this.config.retention.days,
          onEvent: this.config.onEvent,
        });
      } catch (err) {
        failures.push(err);
        retentionFailure = err;
      }
    }

    const requestMaintenance =
      this.engine.reliability.requestMaintenance ??
      this.engine.reliability.rearmMaintenance;
    if (requestMaintenance) {
      try {
        await requestMaintenance.call(this.engine.reliability);
      } catch (rearmError) {
        if (failures.length === 0) throw rearmError;
        throw new AggregateError(
          [...failures, rearmError],
          'maintenance failed and its host wake-up could not be restored',
        );
      }
      if (failures.length === 1) throw failures[0];
      if (failures.length > 1) {
        throw new AggregateError(failures, 'multiple maintenance phases failed');
      }
    }
    // Retention failures historically propagated on adapters without a host
    // rearm seam; retain that contract. Flush/scan remain observable events.
    if (retentionFailure !== undefined) throw retentionFailure;
  }

  async shutdown(): Promise<void> {
    if (this._shutdownCalled) return;
    this._shutdownCalled = true;

    try {
      await this.ready;
    } catch {
      /* init failed */
    }

    if (this.engine) {
      this.engine.batcher.beginShutdown();

      // Flush any leftover pre-ready items that arrived during init race
      await this.flushPreReadyBuffer();

      // Batcher.flushAll already honors flushTimeoutMs as an overall deadline
      // and waits for inflightFlushes until that deadline.
      // Do not race-abandon flushAll here — that closed adapters under bulkInsert.
      try {
        await this.engine.batcher.flushAll();
      } catch (err) {
        this.emit({
          type: 'flush_fail',
          error: err instanceof Error ? err.message : String(err),
          detail: 'shutdown_flush',
        });
      }

      // Optionally surface deadline miss: still-inflight flushes after flushAll.
      const afterFlush = this.engine.batcher.getStats();
      if (afterFlush.inflightFlushes > 0) {
        this.emit({
          type: 'limit',
          detail: 'shutdown_deadline',
          count: afterFlush.inflightFlushes,
        });
      }

      this.engine.retryEngine.stop();
      await this.engine.pool.closeAll();
      await this.engine.reliability.close();
      this.engine = null;
    }
  }

  private async initialize(): Promise<void> {
    this.engine = await bootstrap(this.config);
    await this.flushPreReadyBuffer();
  }

  /**
   * Drain pre-ready buffer through the real enqueue path (WAL when durable).
   * Awaits each enqueue so durable mode is respected after ready.
   */
  private async flushPreReadyBuffer(): Promise<void> {
    if (!this.engine || this.preReadyBuffer.length === 0) return;
    const pending = this.preReadyBuffer;
    this.preReadyBuffer = [];
    for (const log of pending) {
      try {
        await this.sealAndEnqueue(log);
      } catch {
        this.emit({
          type: 'drop',
          tenantId: log.tenantId ?? null,
          detail: 'pre_ready_enqueue_fail',
        });
      }
    }
  }

  /**
   * Seal integrity chain (if enabled) then enqueue.
   * Integrity seals are serialized for a total order.
   */
  private async sealAndEnqueue(log: LogbunLog<TActions>): Promise<boolean> {
    if (this.config.integrityChain) {
      await this.applyIntegrity(log);
    }
    if (!this.engine) return false;
    return this.engine.batcher.enqueue(log);
  }

  private async applyIntegrity(log: LogbunLog<TActions>): Promise<void> {
    const run = this.integrityChain.then(async () => {
      const sealed = await sealIntegrity(log, this.integrityTip);
      log.prevHash = sealed.prevHash;
      log.contentHash = sealed.contentHash;
      this.integrityTip = sealed.contentHash;
    });
    this.integrityChain = run.then(
      () => undefined,
      () => undefined
    );
    await run;
  }

  private buildLog(
    action: TActions,
    input: Omit<LogbunLogInput<TActions>, 'action'>,
    context?: LogbunRequestContext
  ): LogbunLog<TActions> {
    const log: LogbunLog<TActions> = {
      ...input,
      action,
      id: randomUUIDv7(),
      createdAt: new Date().toISOString(),
      ipAddress: context?.ipAddress,
      userAgent: context?.userAgent,
    };

    this.applyStringFieldCaps(log);
    this.applyRedactPaths(log);
    this.applyMaxPayload(log);
    return log;
  }

  private emit(event: LogbunEvent): void {
    safeEmit(this.config.onEvent, event);
  }

  /**
   * Register admission task with host waitUntil if provided.
   * Host errors never break the audit pipeline; fire() still never throws.
   */
  private registerWaitUntil(
    context: LogbunRequestContext | undefined,
    task: Promise<unknown>
  ): void {
    const waitUntil = context?.waitUntil;
    if (typeof waitUntil !== 'function') return;
    try {
      waitUntil(
        Promise.resolve(task).catch(() => {
          /* never reject into host */
        })
      );
    } catch {
      /* waitUntil must not break fire() */
    }
  }

  private applyStringFieldCaps(log: LogbunLog<TActions>): void {
    const max =
      this.config.maxStringFieldBytes ?? DEFAULT_MAX_STRING_FIELD_BYTES;
    if (max <= 0) return;

    let any = false;
    // Never cap tenantId — it is a routing/isolation identity; truncating
    // would merge distinct tenants that share a long common prefix.
    const capField = (
      current: string | undefined,
      set: (next: string) => void
    ) => {
      if (typeof current !== 'string') return;
      const { value, truncated } = capString(current, max);
      if (truncated && value !== undefined) {
        set(value);
        any = true;
      }
    };
    capField(log.actorId, (v) => {
      log.actorId = v;
    });
    capField(log.action, (v) => {
      log.action = v as TActions;
    });
    capField(log.entityId, (v) => {
      log.entityId = v;
    });
    capField(log.userAgent, (v) => {
      log.userAgent = v;
    });
    capField(log.ipAddress, (v) => {
      log.ipAddress = v;
    });
    if (any) {
      this.emit({
        type: 'truncated',
        tenantId: log.tenantId ?? null,
        detail: 'max_string_field_bytes',
      });
    }
  }

  /**
   * Deep redaction of configured paths.
   * - Bare key: deleted on log root and recursively inside bags (objects + arrays)
   * - Dotted path: walk from log root (`metadata.user.ssn`, `metadata.items.0.ssn`)
   */
  private applyRedactPaths(log: LogbunLog<TActions>): void {
    const paths = this.config.redactPaths;
    if (!paths || paths.length === 0) return;

    const record = log as unknown as Record<string, unknown>;

    for (const path of paths) {
      if (!path) continue;

      if (!path.includes('.')) {
        if (path in record) delete record[path];
        for (const bag of ['oldValues', 'newValues', 'metadata'] as const) {
          const obj = log[bag];
          if (obj && typeof obj === 'object') {
            deepDeleteKey(obj, path);
          }
        }
        continue;
      }

      deleteAtPath(record, path.split('.'));
    }
  }

  /**
   * Progressive payload shrink to fit maxPayloadBytes:
   * 1) drop metadata, 2) drop oldValues, 3) drop newValues;
   * only then mark any leftover bags as `{_truncated:true}`.
   */
  private applyMaxPayload(log: LogbunLog<TActions>): void {
    const maxBytes =
      this.config.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;
    if (maxBytes <= 0) return;

    const measure = (): number => {
      const payload = {
        oldValues: log.oldValues,
        newValues: log.newValues,
        metadata: log.metadata,
      };
      try {
        return new TextEncoder().encode(JSON.stringify(payload)).length;
      } catch {
        return maxBytes + 1;
      }
    };

    let byteLength = measure();
    if (byteLength <= maxBytes) return;

    // 1) Drop metadata entirely if present
    if (log.metadata !== undefined) {
      delete log.metadata;
      byteLength = measure();
    }

    // 2) Drop oldValues if still over
    if (byteLength > maxBytes && log.oldValues !== undefined) {
      delete log.oldValues;
      byteLength = measure();
    }

    // 3) Drop newValues if still over
    if (byteLength > maxBytes && log.newValues !== undefined) {
      delete log.newValues;
      byteLength = measure();
    }

    // 4) Leftovers still over — mark remaining bags as truncated placeholders
    if (byteLength > maxBytes) {
      if (log.oldValues !== undefined) {
        log.oldValues = { _truncated: true };
      }
      if (log.newValues !== undefined) {
        log.newValues = { _truncated: true };
      }
      if (log.metadata !== undefined) {
        log.metadata = { _truncated: true };
      }
      byteLength = measure();
    }

    this.emit({
      type: 'truncated',
      tenantId: log.tenantId ?? null,
      detail: 'max_payload_bytes',
      count: byteLength,
    });
  }
}

/**
 * Walk a dotted path and delete the leaf.
 * Supports numeric path segments for arrays (`items.0.ssn`).
 */
function deleteAtPath(obj: unknown, parts: string[]): void {
  if (parts.length === 0 || obj == null || typeof obj !== 'object') return;
  const [head, ...rest] = parts;
  if (!head) return;

  if (Array.isArray(obj)) {
    const idx = Number(head);
    if (!Number.isInteger(idx) || idx < 0 || idx >= obj.length) return;
    if (rest.length === 0) {
      // Leave a hole rather than re-index (path-based redaction semantics)
      delete obj[idx];
      return;
    }
    deleteAtPath(obj[idx], rest);
    return;
  }

  const record = obj as Record<string, unknown>;
  if (rest.length === 0) {
    delete record[head];
    return;
  }
  deleteAtPath(record[head], rest);
}

/**
 * Recursively delete a bare key from objects and arrays-of-objects.
 */
function deepDeleteKey(value: unknown, key: string): void {
  if (value == null || typeof value !== 'object') return;

  if (Array.isArray(value)) {
    for (const item of value) {
      deepDeleteKey(item, key);
    }
    return;
  }

  const obj = value as Record<string, unknown>;
  if (key in obj) delete obj[key];
  for (const child of Object.values(obj)) {
    deepDeleteKey(child, key);
  }
}
