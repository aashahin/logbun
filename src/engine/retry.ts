import type { DLQStorage } from '../storage/dlq';
import type { IAdapter, LogbunLog } from '../types';
import type { ConnectionPool } from './pool';

/**
 * DLQ Retry Engine — processes failed batches with exponential backoff.
 *
 * Scans the DLQ directory for pending .batch files and attempts to
 * re-insert them via the adapter. Uses atomic file rename to prevent
 * race conditions.
 *
 * Backoff schedule per batch (within a single scan):
 *   Attempt 1: immediate
 *   Attempt 2: wait 1s → retry
 *   Attempt 3: wait 2s → retry
 *   Attempt 4: wait 4s → retry → GIVE UP for this scan
 *
 * Poison pill: after MAX_SCAN_ATTEMPTS total scan-level failures,
 * the batch is moved to .dead to prevent infinite retry loops.
 */
export class RetryEngine {
  private readonly dlq: DLQStorage;
  private readonly pool: ConnectionPool;
  private readonly adapter: IAdapter;
  private intervalTimer: ReturnType<typeof setInterval> | null = null;
  private initialTimer: ReturnType<typeof setTimeout> | null = null;
  private running = false;

  /** Retry scan interval in milliseconds. Default: 60s */
  private readonly scanInterval: number;

  /**
   * Tracks how many scan-level failures each batch file has accumulated.
   * Key: base filename (without directory path), Value: failure count.
   * Reset on successful processing or process restart.
   */
  private readonly failureCounts: Map<string, number> = new Map();

  /** Maximum scan-level failures before a batch is poisoned */
  private static readonly MAX_SCAN_ATTEMPTS = 10;

  constructor(dlq: DLQStorage, adapter: IAdapter, pool: ConnectionPool, scanInterval: number = 60_000) {
    this.dlq = dlq;
    this.adapter = adapter;
    this.pool = pool;
    this.scanInterval = scanInterval;
  }

  /**
   * Start the retry engine with an initial delay.
   * Gives network connections time to stabilize before retrying.
   */
  start(initialDelayMs: number = 10_000): void {
    // Schedule first scan after initial delay
    this.initialTimer = setTimeout(() => {
      this.initialTimer = null;
      void this.scan();
      // Then schedule periodic scans
      this.intervalTimer = setInterval(() => {
        void this.scan();
      }, this.scanInterval);
    }, initialDelayMs);
  }

  /** Stop the retry engine — clears both initial timeout and periodic interval */
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

  /** Run a single scan of the DLQ and attempt to process pending batches */
  async scan(): Promise<void> {
    // Prevent overlapping scans
    if (this.running) return;
    this.running = true;

    try {
      const pending = await this.dlq.listPending();

      for (const filePath of pending) {
        await this.processBatch(filePath);
      }
    } finally {
      this.running = false;
    }
  }

  /**
   * Process a single batch file with exponential backoff.
   * Max 3 retries (1s, 2s, 4s) before giving up for this scan cycle.
   * After MAX_SCAN_ATTEMPTS total failures, the batch is poisoned.
   */
  private async processBatch(filePath: string): Promise<void> {
    const baseFilename = filePath.split('/').pop() ?? '';

    // Check if this batch has exceeded max scan-level attempts
    const priorFailures = this.failureCounts.get(baseFilename) ?? 0;
    if (priorFailures >= RetryEngine.MAX_SCAN_ATTEMPTS) {
      // Poison pill — move to .dead to stop retrying
      try {
        const processingPath = await this.dlq.markProcessing(filePath);
        await this.dlq.markPoisoned(processingPath);
        this.failureCounts.delete(baseFilename);
      } catch {
        // File may have been picked up by another scan — skip
      }
      return;
    }

    let processingPath: string;
    try {
      processingPath = await this.dlq.markProcessing(filePath);
    } catch {
      // File may have been picked up by another scan — skip
      return;
    }

    try {
      // Parse the batch file
      const file = Bun.file(processingPath);
      const content = await file.text();
      const logs: LogbunLog[] = JSON.parse(content) as LogbunLog[];

      // Extract tenantId from the filename: {tenantId}_{timestamp}_{rand}.batch.processing
      // TenantId may contain underscores (e.g. "tenant_123"), so we split and
      // pop the last two segments (rand.batch.processing + timestamp), rejoin the rest.
      const filename = processingPath.split('/').pop() ?? '';
      const segments = filename.split('_');
      // Remove the "{rand}.batch.processing" segment
      segments.pop();
      // Remove the "{timestamp}" segment
      segments.pop();
      const tenantIdRaw = segments.join('_');
      const tenantId = tenantIdRaw === '__global__' ? null : (tenantIdRaw || null);

      // Get the correct adapter via pool (for database_per_tenant support)
      let targetAdapter: IAdapter;
      if (tenantId) {
        try {
          targetAdapter = await this.pool.get(tenantId);
        } catch {
          targetAdapter = this.adapter;
        }
      } else {
        targetAdapter = this.adapter;
      }

      // Exponential backoff retry
      const maxRetries = 3;
      const baseDelay = 1000;

      for (let attempt = 0; attempt <= maxRetries; attempt++) {
        if (attempt > 0) {
          // Wait with exponential backoff: 1s, 2s, 4s
          const delay = baseDelay * Math.pow(2, attempt - 1);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }

        const success = await targetAdapter.bulkInsert(tenantId, logs);
        if (success) {
          await this.dlq.markDone(processingPath);
          // Clear failure count on success
          this.failureCounts.delete(baseFilename);
          return;
        }
      }

      // All retries exhausted — put back in DLQ for next scan cycle
      await this.dlq.markFailed(processingPath);
      // Increment failure count
      this.failureCounts.set(baseFilename, priorFailures + 1);
    } catch {
      // Parse error or unexpected failure — put back in DLQ
      try {
        await this.dlq.markFailed(processingPath);
        this.failureCounts.set(baseFilename, priorFailures + 1);
      } catch {
        // File system error — nothing we can do
      }
    }
  }
}
