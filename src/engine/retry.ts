import {
  tenantIdFromFilename,
  type DLQStorage,
} from '../storage/dlq';
import type { IAdapter, LogbunEvent, LogbunLog, RetryConfig } from '../types';
import type { ConnectionPool } from './pool';
import { safeEmit, type LogbunEventHandler } from '../events';

const DEFAULT_MAX_SCAN_ATTEMPTS = 10;
/** insertMaxRetries = total bulkInsert attempts (default 3). */
const DEFAULT_INSERT_MAX_RETRIES = 3;
const DEFAULT_INSERT_BASE_DELAY_MS = 1_000;
const DEFAULT_SCAN_INTERVAL_MS = 60_000;
/** Max pending DLQ files processed concurrently per scan. */
const PROCESS_CONCURRENCY = 4;

/** Constructor deps for {@link RetryEngine}. */
export interface RetryEngineDeps {
  dlq: DLQStorage;
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
 */
export class RetryEngine {
  private readonly dlq: DLQStorage;
  private readonly pool: ConnectionPool;
  private readonly adapter: IAdapter;
  private readonly onEvent?: LogbunEventHandler;
  private readonly scanInterval: number;
  private readonly maxScanAttempts: number;
  private readonly insertMaxRetries: number;
  private readonly insertBaseDelayMs: number;

  private intervalTimer: ReturnType<typeof setInterval> | null = null;
  private initialTimer: ReturnType<typeof setTimeout> | null = null;
  private running = false;

  constructor(deps: RetryEngineDeps) {
    this.dlq = deps.dlq;
    this.adapter = deps.adapter;
    this.pool = deps.pool;
    this.scanInterval = deps.retry?.scanIntervalMs ?? DEFAULT_SCAN_INTERVAL_MS;
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

  start(initialDelayMs?: number): void {
    const delay = initialDelayMs ?? 10_000;
    this.initialTimer = setTimeout(() => {
      this.initialTimer = null;
      void this.scan();
      this.intervalTimer = setInterval(() => {
        void this.scan();
      }, this.scanInterval);
    }, delay);
  }

  stop(): void {
    if (this.initialTimer) {
      clearTimeout(this.initialTimer);
      this.initialTimer = null;
    }
    if (this.intervalTimer) {
      clearInterval(this.intervalTimer);
      this.intervalTimer = null;
    }
  }

  async scan(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const pending = await this.dlq.listPending();
      // Bounded parallel: process up to PROCESS_CONCURRENCY files at a time
      for (let i = 0; i < pending.length; i += PROCESS_CONCURRENCY) {
        const chunk = pending.slice(i, i + PROCESS_CONCURRENCY);
        await Promise.allSettled(
          chunk.map((filePath) => this.processBatch(filePath))
        );
      }
    } finally {
      this.running = false;
    }
  }

  private async processBatch(filePath: string): Promise<void> {
    let processingPath: string;
    try {
      processingPath = await this.dlq.markProcessing(filePath);
    } catch {
      return;
    }

    let logs: LogbunLog[] = [];
    let tenantId: string | null = null;
    let attempts = 0;

    try {
      const batch = await this.dlq.readBatchFile(processingPath);
      logs = batch.logs;
      // Prefer envelope tenantId (raw) over sanitized filename key
      tenantId = batch.tenantId ?? tenantIdFromFilename(processingPath);
      attempts =
        typeof batch.attempts === 'number' && Number.isFinite(batch.attempts)
          ? batch.attempts
          : 0;

      if (attempts >= this.maxScanAttempts) {
        await this.poison(processingPath, tenantId, attempts, filePath);
        return;
      }

      if (!Array.isArray(logs) || logs.length === 0) {
        await this.poison(
          processingPath,
          tenantId,
          attempts,
          'empty_or_invalid'
        );
        return;
      }

      const insertResult = await this.insertWithAdapter(
        tenantId,
        processingPath,
        attempts,
        logs
      );
      if (!insertResult) return;

      const { ok, error } = insertResult;
      if (ok) {
        await this.dlq.markDone(processingPath);
        this.emit({
          type: 'flush_ok',
          tenantId,
          count: logs.length,
          detail: 'dlq_retry',
        });
        return;
      }

      await this.failWithAttempts(processingPath, attempts);
      if (error) {
        this.emit({
          type: 'flush_fail',
          tenantId,
          count: logs.length,
          error,
          detail: 'dlq_retry',
        });
      }
    } catch {
      try {
        await this.failWithAttempts(processingPath, attempts);
      } catch {
        // nothing we can do
      }
    }
  }

  /**
   * Resolve adapter and bulk-insert. Never falls back to base for a tenant.
   * @returns null when tenant adapter resolution failed (already re-queued)
   */
  private async insertWithAdapter(
    tenantId: string | null,
    processingPath: string,
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
      // Never fall back to base adapter for a real tenant
      await this.failWithAttempts(processingPath, attempts);
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

  /**
   * bulkInsert with exponential backoff.
   * insertMaxRetries is the **total** number of attempts (default 3).
   * Delays apply between retries only.
   */
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
    processingPath: string,
    tenantId: string | null,
    attempts: number,
    detail: string
  ): Promise<void> {
    try {
      await this.dlq.markPoisoned(processingPath);
      this.emit({
        type: 'poison',
        tenantId,
        count: attempts,
        detail,
      });
    } catch {
      // ignore
    }
  }

  private async failWithAttempts(
    processingPath: string,
    currentAttempts: number
  ): Promise<void> {
    try {
      await this.dlq.incrementAttempts(processingPath, currentAttempts);
    } catch {
      // still try to re-queue
    }
    try {
      await this.dlq.markFailed(processingPath);
    } catch {
      // filesystem error
    }
  }
}
