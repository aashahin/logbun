/**
 * Runtime-neutral reliability seam: journal (WAL), DLQ lifecycle, recovery.
 * Implementations: MemoryReliabilityAdapter (core), FileReliabilityAdapter,
 * CloudflareReliabilityAdapter (subpaths).
 */
/** Minimal log shape for reliability storage (avoids circular import with types.ts). */
export interface ReliabilityLog {
  id: string;
  tenantId?: string;
  actorId: string;
  action: string;
  entityId?: string;
  oldValues?: Record<string, unknown>;
  newValues?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  createdAt: string;
  ipAddress?: string;
  userAgent?: string;
  prevHash?: string;
  contentHash?: string;
}

/** DLQ entry lifecycle state. */
export type DlqState = 'pending' | 'processing' | 'dead';

/**
 * Portable DLQ entry for ops APIs (listDlq / requeueDead / deleteDead).
 * `id` is opaque and stable; filesystem paths are diagnostic metadata only.
 */
export interface DLQEntry {
  /** Opaque stable id — authority for requeue/delete/claim. */
  id: string;
  /** Current lifecycle state. */
  state: DlqState;
  /**
   * Alias of {@link state} for readability (`kind: 'pending' | …`).
   * Always equal to `state`.
   */
  kind: DlqState;
  tenantId: string | null;
  attempts: number;
  logCount: number;
  /**
   * Optional adapter-specific diagnostics (e.g. filesystem path).
   * Never accepted as authority for mutations without confined ID lookup.
   */
  metadata?: Record<string, unknown>;
}

/** Batch returned after an atomic DLQ claim. */
export interface ClaimedDlqBatch {
  id: string;
  tenantId: string | null;
  attempts: number;
  logs: ReliabilityLog[];
}

/** Journal recovery wave. */
export interface JournalRecoveryResult {
  logs: ReliabilityLog[];
  truncated: boolean;
  approxBytes: number;
}

/** Ops snapshot for reliability storage. */
export interface ReliabilityStats {
  /** Approximate journal size in bytes (0 when volatile / empty). */
  journalApproxBytes: number;
  dlqPending: number;
  dlqProcessing: number;
  dlqDead: number;
  /**
   * Hint for host schedulers (e.g. Cloudflare DO alarm): whether work remains
   * that maintenance should drain.
   */
  hasPendingWork?: boolean;
}

/**
 * ReliabilityAdapter — journal + DLQ + ownership lifecycle.
 *
 * Ordering invariants (durable / persistent implementations):
 * 1. Durable append commits before admission / fireAsync resolves
 * 2. Destination success → journal ACK
 * 3. Destination failure → DLQ durable commit, then journal ACK
 * 4. If both destination and DLQ fail → journal stays unacked
 *
 * DLQ claim/settle is atomic: claim moves pending→processing; success
 * deletes; failure rewrites attempts and returns to pending; poison→dead.
 */
export interface ReliabilityAdapter {
  /**
   * When true, journal survives process restarts and durable mode is valid.
   * Memory implementation is `false`.
   */
  readonly persistent: boolean;

  init(): Promise<void>;
  close(): Promise<void>;

  /**
   * Append one log to the durable journal (WAL).
   * Volatile/memory may no-op. May throw with message containing `wal_full`.
   */
  appendJournal(log: ReliabilityLog): Promise<void>;

  /** Mark journal entries as successfully delivered or safely DLQ'd. */
  acknowledgeJournal(ids: string[]): Promise<void>;

  /**
   * Read unacked journal entries (bounded). A finite `maxBytes` is strict:
   * returned encoded records never exceed it, and an oversized first record
   * yields zero logs with `truncated: true`. Finite negative values normalize
   * to zero; non-finite values are treated as unbounded. Unread entries remain
   * available for a later call with a larger or absent bound.
   */
  recoverJournal(opts?: {
    maxLogs?: number;
    maxBytes?: number;
  }): Promise<JournalRecoveryResult>;

  /** Compact/ack-prune journal. Best-effort; never drops unacked entries. */
  compactJournal(): Promise<void>;

  /**
   * Write a failed batch to the DLQ. Returns the opaque entry id.
   * Throws with message containing `dlq_full` when at capacity.
   */
  writeDlq(tenantId: string | null, logs: ReliabilityLog[]): Promise<string>;

  listDlq(opts?: {
    includePending?: boolean;
    includeProcessing?: boolean;
    includeDead?: boolean;
  }): Promise<DLQEntry[]>;

  /**
   * Atomic claim: pending → processing.
   * - With `id`: claim that entry (or null if race/missing)
   * - Without `id`: claim one pending entry (or null if none)
   */
  claimDlq(id?: string): Promise<ClaimedDlqBatch | null>;

  /** Successful delivery — remove processing entry. */
  settleDlqSuccess(id: string): Promise<void>;

  /**
   * Delivery failed — set attempts and return entry to pending.
   * `nextAttempts` is the new absolute attempts value.
   */
  settleDlqFailure(id: string, nextAttempts: number): Promise<void>;

  /** Poison — processing → dead. */
  poisonDlq(id: string): Promise<void>;

  /**
   * Re-queue a dead entry as pending. **Preserves id**, resets attempts to 0.
   * @returns the same id
   */
  requeueDead(id: string): Promise<string>;

  /** Permanently delete a dead entry. */
  deleteDead(id: string): Promise<void>;

  /** Read batch by id without claiming (ops / tests). */
  readDlq(id: string): Promise<ClaimedDlqBatch | null>;

  /** Move orphaned processing entries back to pending (crash recovery). */
  recoverOrphans(): Promise<void>;

  getStats(): Promise<ReliabilityStats>;

  /**
   * Optional: recommended delay until next host-driven maintenance (ms),
   * or null when idle. Used by Cloudflare DO alarm scheduling.
   */
  pendingMaintenanceDelayMs?(): Promise<number | null>;

  /**
   * Optional: restore a host wake-up after a maintenance attempt failed.
   * Implementations must not swallow scheduling failures; the caller can then
   * propagate so platforms such as Durable Objects retry the alarm handler.
   */
  rearmMaintenance?(): Promise<void>;
}
