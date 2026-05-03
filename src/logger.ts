import type {
  LogbunConfig,
  LogbunLogInput,
  LogbunLog,
  LogbunQueryFilters,
  LogbunQueryResult,
  LogbunRequestContext,
} from './types';
import { bootstrap, type BootstrapResult } from './bootstrap';

/**
 * AuditLogger — main public class for the Logbun library.
 *
 * Usage:
 * ```typescript
 * const audit = new AuditLogger<MyActions>({
 *   namespace: 'my-app',
 *   mode: 'durable',
 *   adapter: new BunSQLiteAdapter(),
 * });
 *
 * await audit.ready;  // Wait for bootstrap to complete
 *
 * // Fire & forget — never blocks, never throws
 * audit.fire('user.created', { actorId: 'u1', entityId: 'u2' });
 *
 * // Query
 * const result = await audit.query({ tenantId: 'tenant_1', filters: { action: 'user.created' } });
 *
 * // Graceful shutdown
 * await audit.shutdown();
 * ```
 */
export class AuditLogger<TActions extends string = string> {
  private engine: BootstrapResult | null = null;
  private readonly config: LogbunConfig<TActions>;
  private _shutdownCalled = false;

  /**
   * Promise that resolves when bootstrap is complete.
   * You can await this to ensure the logger is ready before
   * firing logs, though fire() will buffer logs even before ready.
   */
  public readonly ready: Promise<void>;

  constructor(config: LogbunConfig<TActions>) {
    this.config = config;
    // Catch init failure so `ready` never rejects — prevents
    // unhandledRejection crashes when fire() chains off ready.
    this.ready = this.initialize().catch(() => {
      // Bootstrap failed — logger operates in degraded mode.
      // fire() calls will be silently dropped, query() will throw a clear error.
    });
  }

  /**
   * Fire & Forget — enqueue an audit log entry.
   *
   * This method never blocks the event loop and never throws.
   * If the logger is not yet initialized, logs are buffered.
   *
   * @param action - The action identifier (type-safe via TActions generic)
   * @param input - Log data provided by the developer
   * @param context - Optional request context (injected by framework plugins)
   */
  fire(
    action: TActions,
    input: Omit<LogbunLogInput<TActions>, 'action'>,
    context?: LogbunRequestContext
  ): void {
    // Build the full log entry with library-generated fields
    const log: LogbunLog<TActions> = {
      ...input,
      action,
      id: Bun.randomUUIDv7(),
      createdAt: new Date().toISOString(),
      ipAddress: context?.ipAddress,
      userAgent: context?.userAgent,
    };

    // Fire & forget — never throw, never block
    if (this.engine) {
      void this.engine.batcher.enqueue(log).catch(() => {
        // Swallow — fire() contract: never throws
      });
    } else {
      // Logger not ready yet — wait for init then enqueue
      void this.ready.then(() => {
        if (this.engine) {
          void this.engine.batcher.enqueue(log).catch(() => {
            // Swallow — fire() contract: never throws
          });
        }
      }).catch(() => {
        // Init failed — log is lost. fire() contract: never throws.
      });
    }
  }

  /**
   * Query audit logs with filters and cursor-based pagination.
   *
   * @param opts.tenantId - Tenant to query (null for global/single-tenant)
   * @param opts.filters - Filter criteria (action, actorId, entityId, date range)
   * @param opts.pagination - Pagination options (cursor, limit)
   */
  async query(opts: {
    tenantId?: string;
    filters?: LogbunQueryFilters<TActions>;
    pagination?: { cursor?: string; limit?: number };
  }): Promise<LogbunQueryResult<TActions>> {
    await this.ready;

    if (!this.engine) {
      throw new Error('AuditLogger is not initialized — query unavailable');
    }

    // Use the pool to get the correct adapter for database_per_tenant mode
    let targetAdapter = this.config.adapter;
    if (opts.tenantId && this.engine) {
      try {
        targetAdapter = await this.engine.pool.get(opts.tenantId);
      } catch {
        // Fall back to base adapter
      }
    }

    return targetAdapter.query(
      opts.tenantId ?? null,
      opts.filters ?? {},
      {
        cursor: opts.pagination?.cursor,
        limit: opts.pagination?.limit ?? 50,
      }
    ) as Promise<LogbunQueryResult<TActions>>;
  }

  /**
   * Graceful shutdown — flushes all pending queues, stops the retry
   * engine and retention cron, then closes all connections.
   *
   * Call this on SIGTERM/SIGINT.
   */
  async shutdown(): Promise<void> {
    if (this._shutdownCalled) return;
    this._shutdownCalled = true;

    // Don't throw if initialization failed — shutdown must always succeed
    try { await this.ready; } catch { /* init failed, nothing to clean up */ }

    if (this.engine) {
      // 1. Flush all pending queues
      await this.engine.batcher.flushAll();

      // 2. Stop the retry engine
      this.engine.retryEngine.stop();

      // 3. Stop the retention cron
      if (this.engine.retentionCron) {
        this.engine.retentionCron.stop();
      }

      // 4. Close the WAL
      if (this.engine.wal) {
        await this.engine.wal.close();
      }

      // 5. Close all connections
      await this.engine.pool.closeAll();

      this.engine = null;
    }
  }

  /** Run the bootstrap sequence */
  private async initialize(): Promise<void> {
    this.engine = await bootstrap(this.config);
  }
}
