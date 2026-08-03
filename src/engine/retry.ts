import type {
  IAdapter,
  LogbunEvent,
  LogbunLog,
  ReliabilityAdapter,
  RetryConfig,
} from '../types';
import type { ConnectionPool } from './pool';
import { safeEmit, type LogbunEventHandler } from '../events';

const DEFAULT_MAX_SCAN_ATTEMPTS = 10;
/** insertMaxRetries = total bulkInsert attempts (default 3). */
const DEFAULT_INSERT_MAX_RETRIES = 3;
const DEFAULT_INSERT_BASE_DELAY_MS = 1_000;
/** Max pending DLQ entries processed concurrently per scan. */
const PROCESS_CONCURRENCY = 4;

/** Constructor deps for {@link RetryEngine}. */
export interface RetryEngineDeps {
  reliability: ReliabilityAdapter;
  adapter: IAdapter;
  pool: ConnectionPool;
  retry?: RetryConfig;
  onEvent?: LogbunEventHandler;
}

/**
 * DLQ Retry Engine — processes failed batches with exponential backoff.
 *
 * Poison pill uses the durable envelope `attempts` field only (survives restarts).
 * Tenant adapters never fall back to the base adapter.
 *
 * Hosts schedule scans via {@link RetryEngine.scan} (from AuditLogger.runMaintenance
 * or retryDlqNow). No recurring timers.
 */
export class RetryEngine {
  private readonly reliability: ReliabilityAdapter;
  private readonly pool: ConnectionPool;
  private readonly adapter: IAdapter;
  private readonly onEvent?: LogbunEventHandler;
  private readonly maxScanAttempts: number;
  private readonly insertMaxRetries: number;
  private readonly insertBaseDelayMs: number;

  private running = false;

  constructor(deps: RetryEngineDeps) {
    this.reliability = deps.reliability;
    this.adapter = deps.adapter;
    this.pool = deps.pool;
    this.maxScanAttempts =
      deps.retry?.maxScanAttempts ?? DEFAULT_MAX_SCAN_ATTEMPTS;
    this.insertMaxRetries =
      deps.retry?.insertMaxRetries ?? DEFAULT_INSERT_MAX_RETRIES;
    this.insertBaseDelayMs =
      deps.retry?.insertBaseDelayMs ?? DEFAULT_INSERT_BASE_DELAY_MS;
    this.onEvent = deps.onEvent;
  }

  private emit(e: LogbunEvent): void {
    safeEmit(this.onEvent, e);
  }

  /**
   * @deprecated Recurring timers removed in 1.0 — use host-scheduled
   * {@link scan} via AuditLogger.runMaintenance(). No-op for compatibility.
   */
  start(_initialDelayMs?: number): void {
    /* no-op: hosts schedule maintenance */
  }

  /**
   * @deprecated No recurring timers in 1.0. No-op for compatibility.
   */
  stop(): void {
    /* no-op */
  }

  async scan(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      // A storage failure after claim can leave an entry in processing. Every
      // host-driven pass is a recovery boundary, not just process bootstrap.
      await this.reliability.recoverOrphans();
      const pending = await this.reliability.listDlq({
        includePending: true,
        includeProcessing: false,
        includeDead: false,
      });
      const failures: unknown[] = [];
      for (let i = 0; i < pending.length; i += PROCESS_CONCURRENCY) {
        const chunk = pending.slice(i, i + PROCESS_CONCURRENCY);
        const results = await Promise.allSettled(
          chunk.map((entry) => this.processBatch(entry.id))
        );
        for (const result of results) {
          if (result.status === 'rejected') failures.push(result.reason);
        }
      }
      if (failures.length === 1) throw failures[0];
      if (failures.length > 1) {
        throw new AggregateError(failures, 'multiple DLQ retry batches failed');
      }
    } finally {
      this.running = false;
    }
  }

  private async processBatch(id: string): Promise<void> {
    const claimed = await this.reliability.claimDlq(id);
    if (!claimed) return;

    const { tenantId, logs } = claimed;
    const attempts = claimed.attempts;

    if (attempts >= this.maxScanAttempts) {
      await this.poison(claimed.id, tenantId, attempts, id);
      return;
    }

    if (!Array.isArray(logs) || logs.length === 0) {
      await this.poison(claimed.id, tenantId, attempts, 'empty_or_invalid');
      return;
    }

    const insertResult = await this.insertWithAdapter(
      tenantId,
      claimed.id,
      attempts,
      logs
    );
    if (!insertResult) return;

    const { ok, error } = insertResult;
    if (ok) {
      await this.reliability.settleDlqSuccess(claimed.id);
      this.emit({
        type: 'flush_ok',
        tenantId,
        count: logs.length,
        detail: 'dlq_retry',
      });
      return;
    }

    await this.failWithAttempts(claimed.id, attempts);
    if (error) {
      this.emit({
        type: 'flush_fail',
        tenantId,
        count: logs.length,
        error,
        detail: 'dlq_retry',
      });
    }
  }

  private async insertWithAdapter(
    tenantId: string | null,
    claimId: string,
    attempts: number,
    logs: LogbunLog[]
  ): Promise<{ ok: boolean; error?: string } | null> {
    if (!tenantId) {
      return this.bulkInsertWithBackoff(this.adapter, null, logs);
    }

    try {
      return await this.pool.withAdapter(tenantId, (adapter) =>
        this.bulkInsertWithBackoff(adapter, tenantId, logs)
      );
    } catch (err) {
      await this.failWithAttempts(claimId, attempts);
      this.emit({
        type: 'flush_fail',
        tenantId,
        count: logs.length,
        error: err instanceof Error ? err.message : String(err),
        detail: 'tenant_adapter',
      });
      return null;
    }
  }

  private async bulkInsertWithBackoff(
    adapter: IAdapter,
    tenantId: string | null,
    logs: LogbunLog[]
  ): Promise<{ ok: boolean; error?: string }> {
    const maxAttempts = Math.max(1, this.insertMaxRetries);
    let lastError: string | undefined;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      if (attempt > 0) {
        const delay = this.insertBaseDelayMs * Math.pow(2, attempt - 1);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
      try {
        if (await adapter.bulkInsert(tenantId, logs)) {
          return { ok: true };
        }
        lastError = lastError ?? 'bulkInsert returned false';
      } catch (err) {
        lastError = err instanceof Error ? err.message : String(err);
      }
    }
    return { ok: false, error: lastError };
  }

  private async poison(
    id: string,
    tenantId: string | null,
    attempts: number,
    detail: string
  ): Promise<void> {
    await this.reliability.poisonDlq(id);
    this.emit({
      type: 'poison',
      tenantId,
      count: attempts,
      detail,
    });
  }

  private async failWithAttempts(
    id: string,
    currentAttempts: number
  ): Promise<void> {
    await this.reliability.settleDlqFailure(id, currentAttempts + 1);
  }
}
