/**
 * CloudflareReliabilityAdapter — Durable Object SQLite storage.
 *
 * Isolated from the root package: only structural types (no cloudflare: imports
 * in the published root graph). Standard Workers call a DO binding; the owning
 * DO alarm handler should invoke `audit.runMaintenance()`.
 *
 * Scope: Durable Object SQLite only — not D1 or generic SQLite.
 */
import type { LogbunLog } from '../../types';
import type {
  ClaimedDlqBatch,
  DLQEntry,
  JournalRecoveryResult,
  ReliabilityAdapter,
  ReliabilityStats,
} from '../../reliability/types';
import { randomUUIDv7 } from '../../utils/uuidv7';
import { DurableAdmissionSchedulingError } from '../../reliability/scheduling-error';

/**
 * Structural subset of Cloudflare Durable Object SQLite storage.
 * Avoids importing `cloudflare:` types into the package graph.
 */
export interface DurableObjectSqlStorage {
  exec(query: string, ...bindings: unknown[]): {
    toArray?: () => Record<string, unknown>[];
    one?: () => Record<string, unknown> | null;
    raw?: () => unknown[][];
    [Symbol.iterator]?: () => Iterator<Record<string, unknown>>;
  };
}

export interface DurableObjectStateLike {
  storage: {
    sql: DurableObjectSqlStorage;
    setAlarm?: (scheduledTime: number | Date) => Promise<void>;
    getAlarm?: () => Promise<number | null>;
    transactionSync?: <T>(closure: () => T) => T;
  };
}

export interface CloudflareReliabilityAdapterOptions {
  /**
   * Durable Object state (from the DO constructor: `ctx` / `state`).
   * Must expose `storage.sql` (SQLite-backed DO).
   */
  state: DurableObjectStateLike;
  /**
   * Optional table name prefix (default `logbun`).
   */
  tablePrefix?: string;
  /** Soft cap on journal rows. @default 100_000 */
  maxJournalEntries?: number;
  /** Cap pending+processing DLQ rows. @default 10_000 */
  maxDlqEntries?: number;
  /**
   * When true (default), schedule a DO alarm when pending work exists so the
   * host can call runMaintenance from alarm().
   */
  scheduleAlarms?: boolean;
  /** Alarm delay when work is pending (ms). @default 1_000 */
  alarmDelayMs?: number;
}

function rowsOf(
  result: ReturnType<DurableObjectSqlStorage['exec']>
): Record<string, unknown>[] {
  if (result && typeof result.toArray === 'function') {
    return result.toArray();
  }
  if (result && typeof result[Symbol.iterator] === 'function') {
    return Array.from(result as Iterable<Record<string, unknown>>);
  }
  return [];
}

function oneOf(
  result: ReturnType<DurableObjectSqlStorage['exec']>
): Record<string, unknown> | null {
  if (result && typeof result.one === 'function') {
    try {
      return result.one() ?? null;
    } catch (err) {
      // Durable Object SqlStorage#one() throws when SELECT matched no row.
      if (
        err instanceof Error &&
        /no results/i.test(err.message)
      ) {
        return null;
      }
      throw err;
    }
  }
  const rows = rowsOf(result);
  return rows[0] ?? null;
}

/** SQL identifiers cannot be bound; keep the configurable prefix structural. */
function sanitizeTablePrefix(prefix: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(prefix)) {
    throw new Error(
      'CloudflareReliabilityAdapter tablePrefix must be an SQL identifier (letters, digits, underscore)'
    );
  }
  return prefix;
}

/**
 * Persistent reliability backed by Cloudflare Durable Object SQLite.
 */
export class CloudflareReliabilityAdapter implements ReliabilityAdapter {
  readonly persistent = true;

  private readonly sql: DurableObjectSqlStorage;
  private readonly storage: DurableObjectStateLike['storage'];
  private readonly prefix: string;
  private readonly maxJournal: number;
  private readonly maxDlq: number;
  private readonly scheduleAlarms: boolean;
  private readonly alarmDelayMs: number;
  private ready = false;

  constructor(options: CloudflareReliabilityAdapterOptions) {
    if (!options?.state?.storage?.sql) {
      throw new Error(
        'CloudflareReliabilityAdapter requires Durable Object state with storage.sql'
      );
    }
    this.storage = options.state.storage;
    this.sql = options.state.storage.sql;
    this.prefix = sanitizeTablePrefix(options.tablePrefix ?? 'logbun');
    this.maxJournal = options.maxJournalEntries ?? 100_000;
    this.maxDlq = options.maxDlqEntries ?? 10_000;
    this.scheduleAlarms = options.scheduleAlarms !== false;
    this.alarmDelayMs = options.alarmDelayMs ?? 1_000;
  }

  private jTable(): string {
    return `${this.prefix}_journal`;
  }
  private dTable(): string {
    return `${this.prefix}_dlq`;
  }

  async init(): Promise<void> {
    if (this.ready) return;
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS ${this.jTable()} (
        id TEXT PRIMARY KEY,
        payload TEXT NOT NULL,
        acked INTEGER NOT NULL DEFAULT 0,
        created_ms INTEGER NOT NULL
      )`
    );
    this.sql.exec(
      `CREATE TABLE IF NOT EXISTS ${this.dTable()} (
        id TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        tenant_id TEXT,
        attempts INTEGER NOT NULL DEFAULT 0,
        logs TEXT NOT NULL,
        created_ms INTEGER NOT NULL
      )`
    );
    this.sql.exec(
      `CREATE INDEX IF NOT EXISTS idx_${this.prefix}_journal_acked ON ${this.jTable()} (acked, created_ms)`
    );
    this.sql.exec(
      `CREATE INDEX IF NOT EXISTS idx_${this.prefix}_dlq_state ON ${this.dTable()} (state, created_ms)`
    );
    this.ready = true;
    // A DO can be reconstructed with rows but no scheduled alarm (for
    // example, after an isolate restart). Recover an interrupted claim before
    // looking at pending work, then restore the host maintenance wake-up.
    await this.recoverOrphans();
    await this.requestMaintenance();
  }

  async close(): Promise<void> {
    this.ready = false;
  }

  private ensureReady(): void {
    if (!this.ready) {
      throw new Error('CloudflareReliabilityAdapter not initialized');
    }
  }

  private runTx<T>(fn: () => T): T {
    if (typeof this.storage.transactionSync === 'function') {
      return this.storage.transactionSync(fn);
    }
    return fn();
  }

  async requestMaintenance(): Promise<void> {
    this.ensureReady();
    if (!this.scheduleAlarms || typeof this.storage.setAlarm !== 'function') {
      return;
    }
    const delay = await this.pendingMaintenanceDelayMs();
    if (delay == null) return;
    const existing =
      typeof this.storage.getAlarm === 'function'
        ? await this.storage.getAlarm()
        : null;
    if (existing != null) return;
    await this.storage.setAlarm(Date.now() + Math.max(0, delay));
  }

  private async scheduleAfterDurableAdmission(): Promise<void> {
    try {
      await this.requestMaintenance();
    } catch (error) {
      throw new DurableAdmissionSchedulingError(error);
    }
  }

  async appendJournal(log: LogbunLog): Promise<void> {
    this.ensureReady();
    this.runTx(() => {
      const countRow = oneOf(
        this.sql.exec(
          `SELECT COUNT(*) AS c FROM ${this.jTable()} WHERE acked = 0`
        )
      );
      const c = Number(countRow?.c ?? 0);
      if (c >= this.maxJournal) {
        throw new Error(
          `wal_full: journal entries ${c} >= maxJournalEntries ${this.maxJournal}`
        );
      }
      this.sql.exec(
        `INSERT INTO ${this.jTable()} (id, payload, acked, created_ms) VALUES (?, ?, 0, ?)`,
        log.id,
        JSON.stringify(log),
        Date.now()
      );
    });
    await this.scheduleAfterDurableAdmission();
  }

  async acknowledgeJournal(ids: string[]): Promise<void> {
    this.ensureReady();
    if (ids.length === 0) return;
    this.runTx(() => {
      for (const id of ids) {
        this.sql.exec(
          `UPDATE ${this.jTable()} SET acked = 1 WHERE id = ?`,
          id
        );
      }
      // Prune acked rows periodically
      this.sql.exec(`DELETE FROM ${this.jTable()} WHERE acked = 1`);
    });
  }

  async recoverJournal(opts?: {
    maxLogs?: number;
    maxBytes?: number;
  }): Promise<JournalRecoveryResult> {
    this.ensureReady();
    const maxLogs =
      typeof opts?.maxLogs === 'number' && Number.isFinite(opts.maxLogs)
        ? Math.max(0, opts.maxLogs)
        : 10_000;
    const maxBytes =
      typeof opts?.maxBytes === 'number' && Number.isFinite(opts.maxBytes)
        ? Math.max(0, opts.maxBytes)
        : Number.POSITIVE_INFINITY;
    const result = this.sql.exec(
      `SELECT id, payload FROM ${this.jTable()} WHERE acked = 0 ORDER BY created_ms ASC, id ASC LIMIT ?`,
      maxLogs + 1
    );
    const rows = rowsOf(result);
    let truncated = false;
    let slice = rows;
    if (rows.length > maxLogs) {
      truncated = true;
      slice = rows.slice(0, maxLogs);
    }
    const logs: LogbunLog[] = [];
    let bytes = 0;
    for (const row of slice) {
      try {
        const payload = String(row.payload);
        const payloadBytes = new TextEncoder().encode(payload).byteLength;
        if (bytes + payloadBytes > maxBytes) {
          truncated = true;
          break;
        }
        logs.push(JSON.parse(payload) as LogbunLog);
        bytes += payloadBytes;
      } catch {
        /* skip corrupt */
      }
    }
    return {
      logs,
      truncated,
      approxBytes: bytes,
    };
  }

  async compactJournal(): Promise<void> {
    this.ensureReady();
    this.sql.exec(`DELETE FROM ${this.jTable()} WHERE acked = 1`);
  }

  async writeDlq(tenantId: string | null, logs: LogbunLog[]): Promise<string> {
    this.ensureReady();
    const id = this.runTx(() => {
      const countRow = oneOf(
        this.sql.exec(
          `SELECT COUNT(*) AS c FROM ${this.dTable()} WHERE state IN ('pending','processing')`
        )
      );
      const c = Number(countRow?.c ?? 0);
      if (c >= this.maxDlq) {
        throw new Error(
          `dlq_full: pending+processing (${c}) >= maxFiles (${this.maxDlq})`
        );
      }
      const newId = randomUUIDv7();
      this.sql.exec(
        `INSERT INTO ${this.dTable()} (id, state, tenant_id, attempts, logs, created_ms) VALUES (?, 'pending', ?, 0, ?, ?)`,
        newId,
        tenantId,
        JSON.stringify(logs),
        Date.now()
      );
      return newId;
    });
    await this.scheduleAfterDurableAdmission();
    return id;
  }

  async listDlq(opts?: {
    includePending?: boolean;
    includeProcessing?: boolean;
    includeDead?: boolean;
  }): Promise<DLQEntry[]> {
    this.ensureReady();
    const includePending = opts?.includePending !== false;
    const includeProcessing = opts?.includeProcessing === true;
    const includeDead = opts?.includeDead === true;
    const states: string[] = [];
    if (includePending) states.push('pending');
    if (includeProcessing) states.push('processing');
    if (includeDead) states.push('dead');
    if (states.length === 0) return [];

    const placeholders = states.map(() => '?').join(',');
    const rows = rowsOf(
      this.sql.exec(
        `SELECT id, state, tenant_id, attempts, logs FROM ${this.dTable()} WHERE state IN (${placeholders}) ORDER BY id`,
        ...states
      )
    );
    return rows.map((row) => {
      let logCount = 0;
      try {
        const logs = JSON.parse(String(row.logs)) as unknown[];
        logCount = Array.isArray(logs) ? logs.length : 0;
      } catch {
        logCount = 0;
      }
      const state = String(row.state) as DLQEntry['state'];
      return {
        id: String(row.id),
        state,
        kind: state,
        tenantId:
          row.tenant_id == null || row.tenant_id === ''
            ? null
            : String(row.tenant_id),
        attempts: Number(row.attempts) || 0,
        logCount,
      };
    });
  }

  async claimDlq(id?: string): Promise<ClaimedDlqBatch | null> {
    this.ensureReady();
    return this.runTx(() => {
      let row: Record<string, unknown> | null = null;
      if (id) {
        row = oneOf(
          this.sql.exec(
            `SELECT id, tenant_id, attempts, logs, state FROM ${this.dTable()} WHERE id = ?`,
            id
          )
        );
        if (!row || String(row.state) !== 'pending') return null;
      } else {
        row = oneOf(
          this.sql.exec(
            `SELECT id, tenant_id, attempts, logs, state FROM ${this.dTable()} WHERE state = 'pending' ORDER BY created_ms ASC, id ASC LIMIT 1`
          )
        );
        if (!row) return null;
      }
      const claimId = String(row.id);
      this.sql.exec(
        `UPDATE ${this.dTable()} SET state = 'processing' WHERE id = ? AND state = 'pending'`,
        claimId
      );
      // Verify claim won
      const check = oneOf(
        this.sql.exec(
          `SELECT state FROM ${this.dTable()} WHERE id = ?`,
          claimId
        )
      );
      if (!check || String(check.state) !== 'processing') return null;

      let logs: LogbunLog[] = [];
      try {
        logs = JSON.parse(String(row.logs)) as LogbunLog[];
      } catch {
        logs = [];
      }
      return {
        id: claimId,
        tenantId:
          row.tenant_id == null || row.tenant_id === ''
            ? null
            : String(row.tenant_id),
        attempts: Number(row.attempts) || 0,
        logs,
      };
    });
  }

  async settleDlqSuccess(id: string): Promise<void> {
    this.ensureReady();
    this.sql.exec(
      `DELETE FROM ${this.dTable()} WHERE id = ? AND state = 'processing'`,
      id
    );
  }

  async settleDlqFailure(id: string, nextAttempts: number): Promise<void> {
    this.ensureReady();
    this.sql.exec(
      `UPDATE ${this.dTable()} SET state = 'pending', attempts = ? WHERE id = ? AND state = 'processing'`,
      nextAttempts,
      id
    );
    await this.scheduleAfterDurableAdmission();
  }

  async poisonDlq(id: string): Promise<void> {
    this.ensureReady();
    this.sql.exec(
      `UPDATE ${this.dTable()} SET state = 'dead' WHERE id = ? AND state = 'processing'`,
      id
    );
  }

  async requeueDead(id: string): Promise<string> {
    this.ensureReady();
    this.runTx(() => {
      const row = oneOf(
        this.sql.exec(
          `SELECT state FROM ${this.dTable()} WHERE id = ?`,
          id
        )
      );
      if (!row || String(row.state) !== 'dead') {
        throw new Error(`requeueDead: id ${id} is not a dead DLQ entry`);
      }
      const countRow = oneOf(
        this.sql.exec(
          `SELECT COUNT(*) AS c FROM ${this.dTable()} WHERE state IN ('pending','processing')`
        )
      );
      const c = Number(countRow?.c ?? 0);
      if (c >= this.maxDlq) {
        throw new Error(
          `dlq_full: pending+processing (${c}) >= maxFiles (${this.maxDlq})`
        );
      }
      this.sql.exec(
        `UPDATE ${this.dTable()} SET state = 'pending', attempts = 0 WHERE id = ?`,
        id
      );
    });
    await this.scheduleAfterDurableAdmission();
    return id;
  }

  async deleteDead(id: string): Promise<void> {
    this.ensureReady();
    this.sql.exec(
      `DELETE FROM ${this.dTable()} WHERE id = ? AND state = 'dead'`,
      id
    );
  }

  async readDlq(id: string): Promise<ClaimedDlqBatch | null> {
    this.ensureReady();
    const row = oneOf(
      this.sql.exec(
        `SELECT id, tenant_id, attempts, logs FROM ${this.dTable()} WHERE id = ?`,
        id
      )
    );
    if (!row) return null;
    let logs: LogbunLog[] = [];
    try {
      logs = JSON.parse(String(row.logs)) as LogbunLog[];
    } catch {
      logs = [];
    }
    return {
      id: String(row.id),
      tenantId:
        row.tenant_id == null || row.tenant_id === ''
          ? null
          : String(row.tenant_id),
      attempts: Number(row.attempts) || 0,
      logs,
    };
  }

  async recoverOrphans(): Promise<void> {
    this.ensureReady();
    this.sql.exec(
      `UPDATE ${this.dTable()} SET state = 'pending' WHERE state = 'processing'`
    );
  }

  async getStats(): Promise<ReliabilityStats> {
    this.ensureReady();
    const j = oneOf(
      this.sql.exec(
        `SELECT COUNT(*) AS c FROM ${this.jTable()} WHERE acked = 0`
      )
    );
    const p = oneOf(
      this.sql.exec(
        `SELECT COUNT(*) AS c FROM ${this.dTable()} WHERE state = 'pending'`
      )
    );
    const pr = oneOf(
      this.sql.exec(
        `SELECT COUNT(*) AS c FROM ${this.dTable()} WHERE state = 'processing'`
      )
    );
    const d = oneOf(
      this.sql.exec(
        `SELECT COUNT(*) AS c FROM ${this.dTable()} WHERE state = 'dead'`
      )
    );
    const journalCount = Number(j?.c ?? 0);
    const dlqPending = Number(p?.c ?? 0);
    const dlqProcessing = Number(pr?.c ?? 0);
    const dlqDead = Number(d?.c ?? 0);
    return {
      journalApproxBytes: journalCount * 256,
      dlqPending,
      dlqProcessing,
      dlqDead,
      hasPendingWork: journalCount + dlqPending + dlqProcessing > 0,
    };
  }

  async pendingMaintenanceDelayMs(): Promise<number | null> {
    const s = await this.getStats();
    if (!s.hasPendingWork) return null;
    return this.alarmDelayMs;
  }

  /** Restore a consumed alarm after maintenance failed. */
  async rearmMaintenance(): Promise<void> {
    await this.requestMaintenance();
  }
}
