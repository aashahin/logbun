/**
 * In-memory ReliabilityAdapter — no filesystem I/O.
 * Suitable for volatile mode and tests. `persistent` is always false.
 */
import type { LogbunLog } from '../types';
import { randomUUIDv7 } from '../utils/uuidv7';
import type {
  ClaimedDlqBatch,
  DLQEntry,
  JournalRecoveryResult,
  ReliabilityAdapter,
  ReliabilityStats,
  DlqState,
} from './types';

interface MemDlqRecord {
  id: string;
  state: DlqState;
  tenantId: string | null;
  attempts: number;
  logs: LogbunLog[];
}

/** Default max pending+processing DLQ entries before write refuses. */
const DEFAULT_MAX_DLQ = 10_000;

export interface MemoryReliabilityOptions {
  /** Cap pending+processing DLQ entries. @default 10_000 */
  maxDlqEntries?: number;
  /**
   * When true, keep an in-process journal for crash-within-process recovery
   * tests. Still `persistent: false` (lost on process exit).
   * @default false
   */
  enableJournal?: boolean;
  maxJournalEntries?: number;
}

/**
 * Volatile reliability: memory DLQ + optional in-process journal.
 * Never performs filesystem I/O.
 */
export class MemoryReliabilityAdapter implements ReliabilityAdapter {
  readonly persistent = false;

  private readonly maxDlq: number;
  private readonly enableJournal: boolean;
  private readonly maxJournal: number;
  private readonly journal: LogbunLog[] = [];
  private readonly acked = new Set<string>();
  private readonly dlq = new Map<string, MemDlqRecord>();
  private opChain: Promise<void> = Promise.resolve();
  private ready = false;

  constructor(options?: MemoryReliabilityOptions) {
    this.maxDlq = options?.maxDlqEntries ?? DEFAULT_MAX_DLQ;
    this.enableJournal = options?.enableJournal === true;
    this.maxJournal = options?.maxJournalEntries ?? 100_000;
  }

  private runExclusive<T>(fn: () => Promise<T> | T): Promise<T> {
    const run = this.opChain.then(() => fn(), () => fn());
    this.opChain = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  async init(): Promise<void> {
    this.ready = true;
  }

  async close(): Promise<void> {
    return this.runExclusive(async () => {
      this.ready = false;
      this.journal.length = 0;
      this.acked.clear();
      this.dlq.clear();
    });
  }

  async appendJournal(log: LogbunLog): Promise<void> {
    if (!this.enableJournal) return;
    return this.runExclusive(() => {
      if (!this.ready) throw new Error('ReliabilityAdapter not initialized');
      const unacked = this.journal.filter((l) => !this.acked.has(l.id)).length;
      if (unacked >= this.maxJournal) {
        throw new Error(
          `wal_full: journal entries ${unacked} >= maxJournalEntries ${this.maxJournal}`
        );
      }
      this.journal.push(log);
    });
  }

  async acknowledgeJournal(ids: string[]): Promise<void> {
    if (!this.enableJournal || ids.length === 0) return;
    return this.runExclusive(() => {
      for (const id of ids) this.acked.add(id);
      // Compact: drop acked from front
      while (this.journal.length > 0 && this.acked.has(this.journal[0]!.id)) {
        const gone = this.journal.shift()!;
        this.acked.delete(gone.id);
      }
    });
  }

  async recoverJournal(opts?: {
    maxLogs?: number;
    maxBytes?: number;
  }): Promise<JournalRecoveryResult> {
    return this.runExclusive(() => {
      if (!this.enableJournal) {
        return { logs: [], truncated: false, approxBytes: 0 };
      }
      const maxLogs = opts?.maxLogs;
      const maxBytes =
        typeof opts?.maxBytes === 'number' && Number.isFinite(opts.maxBytes)
          ? Math.max(0, opts.maxBytes)
          : Number.POSITIVE_INFINITY;
      const logs: LogbunLog[] = [];
      let bytes = 0;
      let truncated = false;
      for (const log of this.journal) {
        if (this.acked.has(log.id)) continue;
        if (
          typeof maxLogs === 'number' &&
          Number.isFinite(maxLogs) &&
          logs.length >= maxLogs
        ) {
          truncated = true;
          break;
        }
        const serializedBytes = new TextEncoder().encode(
          JSON.stringify(log)
        ).byteLength;
        if (bytes + serializedBytes > maxBytes) {
          truncated = true;
          break;
        }
        logs.push(log);
        bytes += serializedBytes;
      }
      return {
        logs,
        truncated,
        approxBytes: bytes,
      };
    });
  }

  async compactJournal(): Promise<void> {
    return this.runExclusive(() => {
      if (!this.enableJournal) return;
      const kept = this.journal.filter((l) => !this.acked.has(l.id));
      this.journal.length = 0;
      this.journal.push(...kept);
      this.acked.clear();
    });
  }

  private countLive(): number {
    let n = 0;
    for (const r of this.dlq.values()) {
      if (r.state === 'pending' || r.state === 'processing') n++;
    }
    return n;
  }

  async writeDlq(tenantId: string | null, logs: LogbunLog[]): Promise<string> {
    return this.runExclusive(() => {
      if (!this.ready) throw new Error('ReliabilityAdapter not initialized');
      if (this.countLive() >= this.maxDlq) {
        throw new Error(
          `dlq_full: pending+processing (${this.countLive()}) >= maxFiles (${this.maxDlq})`
        );
      }
      const id = randomUUIDv7();
      this.dlq.set(id, {
        id,
        state: 'pending',
        tenantId,
        attempts: 0,
        logs: [...logs],
      });
      return id;
    });
  }

  async listDlq(opts?: {
    includePending?: boolean;
    includeProcessing?: boolean;
    includeDead?: boolean;
  }): Promise<DLQEntry[]> {
    const includePending = opts?.includePending !== false;
    const includeProcessing = opts?.includeProcessing === true;
    const includeDead = opts?.includeDead === true;
    const out: DLQEntry[] = [];
    for (const r of this.dlq.values()) {
      if (r.state === 'pending' && !includePending) continue;
      if (r.state === 'processing' && !includeProcessing) continue;
      if (r.state === 'dead' && !includeDead) continue;
      out.push({
        id: r.id,
        state: r.state,
        kind: r.state,
        tenantId: r.tenantId,
        attempts: r.attempts,
        logCount: r.logs.length,
      });
    }
    return out.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  }

  async claimDlq(id?: string): Promise<ClaimedDlqBatch | null> {
    return this.runExclusive(() => {
      let rec: MemDlqRecord | undefined;
      if (id) {
        rec = this.dlq.get(id);
        if (!rec || rec.state !== 'pending') return null;
      } else {
        for (const r of this.dlq.values()) {
          if (r.state === 'pending') {
            rec = r;
            break;
          }
        }
        if (!rec) return null;
      }
      rec.state = 'processing';
      return {
        id: rec.id,
        tenantId: rec.tenantId,
        attempts: rec.attempts,
        logs: [...rec.logs],
      };
    });
  }

  async settleDlqSuccess(id: string): Promise<void> {
    return this.runExclusive(() => {
      const rec = this.dlq.get(id);
      if (!rec || rec.state !== 'processing') return;
      this.dlq.delete(id);
    });
  }

  async settleDlqFailure(id: string, nextAttempts: number): Promise<void> {
    return this.runExclusive(() => {
      const rec = this.dlq.get(id);
      if (!rec || rec.state !== 'processing') return;
      rec.attempts = nextAttempts;
      rec.state = 'pending';
    });
  }

  async poisonDlq(id: string): Promise<void> {
    return this.runExclusive(() => {
      const rec = this.dlq.get(id);
      if (!rec || rec.state !== 'processing') return;
      rec.state = 'dead';
    });
  }

  async requeueDead(id: string): Promise<string> {
    return this.runExclusive(() => {
      const rec = this.dlq.get(id);
      if (!rec || rec.state !== 'dead') {
        throw new Error(`requeueDead: id ${id} is not a dead DLQ entry`);
      }
      if (this.countLive() >= this.maxDlq) {
        throw new Error(
          `dlq_full: pending+processing (${this.countLive()}) >= maxFiles (${this.maxDlq})`
        );
      }
      rec.state = 'pending';
      rec.attempts = 0;
      return id;
    });
  }

  async deleteDead(id: string): Promise<void> {
    return this.runExclusive(() => {
      const rec = this.dlq.get(id);
      if (!rec || rec.state !== 'dead') {
        throw new Error(`deleteDead: id ${id} is not a dead DLQ entry`);
      }
      this.dlq.delete(id);
    });
  }

  async readDlq(id: string): Promise<ClaimedDlqBatch | null> {
    const rec = this.dlq.get(id);
    if (!rec) return null;
    return {
      id: rec.id,
      tenantId: rec.tenantId,
      attempts: rec.attempts,
      logs: [...rec.logs],
    };
  }

  async recoverOrphans(): Promise<void> {
    return this.runExclusive(() => {
      for (const rec of this.dlq.values()) {
        if (rec.state === 'processing') rec.state = 'pending';
      }
    });
  }

  async getStats(): Promise<ReliabilityStats> {
    let pending = 0;
    let processing = 0;
    let dead = 0;
    for (const r of this.dlq.values()) {
      if (r.state === 'pending') pending++;
      else if (r.state === 'processing') processing++;
      else dead++;
    }
    let journalApproxBytes = 0;
    if (this.enableJournal) {
      journalApproxBytes = this.journal.filter((l) => !this.acked.has(l.id))
        .length * 256;
    }
    return {
      journalApproxBytes,
      dlqPending: pending,
      dlqProcessing: processing,
      dlqDead: dead,
      hasPendingWork: pending + processing > 0 || journalApproxBytes > 0,
    };
  }

  async pendingMaintenanceDelayMs(): Promise<number | null> {
    const s = await this.getStats();
    return s.hasPendingWork ? 0 : null;
  }
}
