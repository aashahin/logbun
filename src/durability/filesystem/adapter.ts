/**
 * FileReliabilityAdapter — durable journal + DLQ on local filesystem.
 *
 * Works on Node.js, Bun, and Deno (via node:fs compatibility).
 * Deno permissions: `--allow-read` and `--allow-write` for `dataDir`.
 */
import type { LogbunLog } from '../../types';
import type {
  ClaimedDlqBatch,
  DLQEntry,
  JournalRecoveryResult,
  ReliabilityAdapter,
  ReliabilityStats,
} from '../../reliability/types';
import { normalizeEncryptionKey } from '../../utils/crypto';
import {
  WALStorage,
  WAL_SEGMENT_BYTES_DEFAULT,
  WAL_SIZE_SOFT_LIMIT_BYTES,
  type WALStorageOptions,
} from './wal';
import { DLQStorage, type DLQStorageOptions } from './dlq';
import { InstanceLock } from './instance-lock';
import { assertNoSymlinkPath, resolveLogbunDir } from './path';
import { join } from 'node:path';

export interface FileReliabilityWalOptions {
  /** fsync after each WAL append and compact. @default true */
  fsync?: boolean;
  compactAckThreshold?: number;
  segmentBytes?: number;
  hardMaxBytes?: boolean;
}

export interface FileReliabilityDlqOptions {
  /** fsync after DLQ writes. @default true */
  fsync?: boolean;
  /** Max pending+processing entries. @default 10_000 */
  maxEntries?: number;
}

/** @internal Constructor seams kept out of the supported adapter options. */
interface FileReliabilityAdapterInternals {
  walDirectorySync?: WALStorageOptions['directorySync'];
  dlqDirectorySync?: DLQStorageOptions['directorySync'];
}

export interface FileReliabilityAdapterOptions {
  /** Isolates data under dataDir/namespace. */
  namespace: string;
  /**
   * Root directory for WAL/DLQ/lock. Namespace is joined under this root.
   * @default '.logbun'
   */
  dataDir?: string;
  /** WAL options (segment size, fsync, hard max). */
  wal?: FileReliabilityWalOptions;
  /** DLQ options. */
  dlq?: FileReliabilityDlqOptions;
  /**
   * WAL size limit in bytes.
   * @default 64 MiB
   */
  maxWalBytes?: number;
  /**
   * AES-256-GCM at-rest encryption for WAL lines and DLQ files.
   */
  encryptionKey?: string | Uint8Array;
  /**
   * Exclusive multi-process lock on the namespace data dir.
   * @default true
   */
  instanceLock?: boolean;
}

/**
 * Persistent filesystem reliability for durable mode on Node/Bun/Deno.
 */
export class FileReliabilityAdapter implements ReliabilityAdapter {
  readonly persistent = true;

  private readonly namespace: string;
  private readonly dataDir?: string;
  private readonly wantLock: boolean;
  private readonly maxWalBytes: number;
  private readonly encryptionKeyMaterial?: string | Uint8Array;
  private readonly walOpts: FileReliabilityWalOptions;
  private readonly dlqOpts: FileReliabilityDlqOptions;
  private readonly internals: FileReliabilityAdapterInternals;

  private wal: WALStorage | null = null;
  private dlq: DLQStorage | null = null;
  private lock: InstanceLock | null = null;
  private ready = false;

  constructor(
    options: FileReliabilityAdapterOptions,
    internals: FileReliabilityAdapterInternals = {},
  ) {
    if (!options?.namespace) {
      throw new Error('FileReliabilityAdapter requires namespace');
    }
    this.namespace = options.namespace;
    this.dataDir = options.dataDir;
    this.wantLock = options.instanceLock !== false;
    this.maxWalBytes = options.maxWalBytes ?? WAL_SIZE_SOFT_LIMIT_BYTES;
    this.encryptionKeyMaterial = options.encryptionKey;
    this.walOpts = options.wal ?? {};
    this.dlqOpts = options.dlq ?? {};
    this.internals = internals;
  }

  /** Underlying WAL (tests / advanced). */
  get walStorage(): WALStorage | null {
    return this.wal;
  }

  /** Underlying DLQ (tests / advanced). */
  get dlqStorage(): DLQStorage | null {
    return this.dlq;
  }

  async init(): Promise<void> {
    if (this.ready) return;

    // Validate before the instance lock or WAL can create anything through a
    // redirected ancestor. Storage implementations validate again after mkdir.
    const root = resolveLogbunDir(this.namespace, this.dataDir);
    await assertNoSymlinkPath(root, 'FileReliabilityAdapter data root');
    await assertNoSymlinkPath(join(root, 'wal'), 'FileReliabilityAdapter WAL path');
    await assertNoSymlinkPath(join(root, 'dlq'), 'FileReliabilityAdapter DLQ path');
    if (this.wantLock) {
      await assertNoSymlinkPath(
        join(root, '.instance.lock'),
        'FileReliabilityAdapter lock path',
      );
    }

    if (this.wantLock) {
      this.lock = new InstanceLock(this.namespace, this.dataDir);
      await this.lock.acquire();
    }

    let encryptionKeyBytes: Uint8Array | undefined;
    if (this.encryptionKeyMaterial != null) {
      encryptionKeyBytes = await normalizeEncryptionKey(
        this.encryptionKeyMaterial
      );
    }

    const walOptions: WALStorageOptions = {
      fsync: this.walOpts.fsync ?? true,
      compactAckThreshold: this.walOpts.compactAckThreshold ?? 256,
      maxBytes: this.maxWalBytes,
      maxWalBytes: this.maxWalBytes,
      hardMaxBytes: this.walOpts.hardMaxBytes !== false,
      segmentBytes: this.walOpts.segmentBytes ?? WAL_SEGMENT_BYTES_DEFAULT,
      encryptionKey: encryptionKeyBytes,
      createdHierarchyStart: this.lock?.createdHierarchyStart,
      directorySync: this.internals.walDirectorySync,
    };
    this.wal = new WALStorage(this.namespace, this.dataDir, walOptions);
    await this.wal.init();

    const dlqOptions: DLQStorageOptions = {
      fsync: this.dlqOpts.fsync ?? true,
      maxFiles: this.dlqOpts.maxEntries ?? 10_000,
      encryptionKey: encryptionKeyBytes,
      directorySync: this.internals.dlqDirectorySync,
      createdHierarchyStart:
        this.lock?.createdHierarchyStart ?? this.wal.createdHierarchyStart,
    };
    this.dlq = new DLQStorage(this.namespace, this.dataDir, dlqOptions);
    await this.dlq.init();

    this.ready = true;
  }

  async close(): Promise<void> {
    if (this.wal) {
      await this.wal.close();
      this.wal = null;
    }
    this.dlq = null;
    if (this.lock) {
      await this.lock.release();
      this.lock = null;
    }
    this.ready = false;
  }

  private ensure(): { wal: WALStorage; dlq: DLQStorage } {
    if (!this.ready || !this.wal || !this.dlq) {
      throw new Error('FileReliabilityAdapter not initialized — call init()');
    }
    return { wal: this.wal, dlq: this.dlq };
  }

  async appendJournal(log: LogbunLog): Promise<void> {
    const { wal } = this.ensure();
    await wal.append(log);
  }

  async acknowledgeJournal(ids: string[]): Promise<void> {
    const { wal } = this.ensure();
    await wal.acknowledge(ids);
  }

  async recoverJournal(opts?: {
    maxLogs?: number;
    maxBytes?: number;
  }): Promise<JournalRecoveryResult> {
    const { wal } = this.ensure();
    const result = await wal.readAllBounded({
      maxLogs: opts?.maxLogs,
      maxBytes: opts?.maxBytes,
    });
    return {
      logs: result.logs,
      truncated: result.truncated,
      approxBytes: result.approxBytes,
    };
  }

  async compactJournal(): Promise<void> {
    const { wal } = this.ensure();
    await wal.compact();
  }

  async writeDlq(tenantId: string | null, logs: LogbunLog[]): Promise<string> {
    const { dlq } = this.ensure();
    return dlq.write(tenantId, logs);
  }

  async listDlq(opts?: {
    includePending?: boolean;
    includeProcessing?: boolean;
    includeDead?: boolean;
  }): Promise<DLQEntry[]> {
    const { dlq } = this.ensure();
    return dlq.listAll(opts);
  }

  async claimDlq(id?: string): Promise<ClaimedDlqBatch | null> {
    const { dlq } = this.ensure();
    const claimed = await dlq.claim(id);
    if (!claimed) return null;
    return {
      id: claimed.id,
      tenantId: claimed.batch.tenantId,
      attempts: claimed.batch.attempts,
      logs: claimed.batch.logs,
    };
  }

  async settleDlqSuccess(id: string): Promise<void> {
    const { dlq } = this.ensure();
    await dlq.markDone(id);
  }

  async settleDlqFailure(id: string, nextAttempts: number): Promise<void> {
    const { dlq } = this.ensure();
    await dlq.setAttempts(id, nextAttempts);
    await dlq.markFailed(id);
  }

  async poisonDlq(id: string): Promise<void> {
    const { dlq } = this.ensure();
    await dlq.markPoisoned(id);
  }

  async requeueDead(id: string): Promise<string> {
    const { dlq } = this.ensure();
    return dlq.requeueDead(id);
  }

  async deleteDead(id: string): Promise<void> {
    const { dlq } = this.ensure();
    await dlq.deleteDead(id);
  }

  async readDlq(id: string): Promise<ClaimedDlqBatch | null> {
    const { dlq } = this.ensure();
    const batch = await dlq.readById(id);
    if (!batch) return null;
    return {
      id,
      tenantId: batch.tenantId,
      attempts: batch.attempts,
      logs: batch.logs,
    };
  }

  async recoverOrphans(): Promise<void> {
    const { dlq } = this.ensure();
    await dlq.recoverOrphans();
  }

  async getStats(): Promise<ReliabilityStats> {
    const { wal, dlq } = this.ensure();
    let journalApproxBytes = 0;
    try {
      journalApproxBytes = await wal.approximateSize();
    } catch {
      journalApproxBytes = 0;
    }
    const counts = await dlq.countByKind();
    return {
      journalApproxBytes,
      dlqPending: counts.pending,
      dlqProcessing: counts.processing,
      dlqDead: counts.dead,
      hasPendingWork:
        counts.pending + counts.processing > 0 || journalApproxBytes > 0,
    };
  }

  async pendingMaintenanceDelayMs(): Promise<number | null> {
    const s = await this.getStats();
    return s.hasPendingWork ? 0 : null;
  }
}
