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
import {
  fileReliabilityAdapterTestHooksFor,
  type FileReliabilityAdapterTestHooks,
} from './adapter-test-hooks';
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
  private readonly testHooks?: FileReliabilityAdapterTestHooks;

  private wal: WALStorage | null = null;
  private dlq: DLQStorage | null = null;
  private lock: InstanceLock | null = null;
  private ready = false;
  private initPromise: Promise<void> | null = null;
  private queuedInitPromise: Promise<void> | null = null;
  private closePromise: Promise<void> | null = null;
  private acceptingOperations = false;
  private activeOperations = 0;
  private operationDrainWaiters: Array<() => void> = [];

  constructor(options: FileReliabilityAdapterOptions) {
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
    this.testHooks = fileReliabilityAdapterTestHooksFor(options);
  }

  /** Underlying WAL (tests / advanced). */
  get walStorage(): WALStorage | null {
    return this.wal;
  }

  /** Underlying DLQ (tests / advanced). */
  get dlqStorage(): DLQStorage | null {
    return this.dlq;
  }

  init(): Promise<void> {
    if (this.closePromise) {
      if (this.queuedInitPromise) return this.queuedInitPromise;
      const queued = this.closePromise.then(() => this.init());
      const tracked = queued.finally(() => {
        if (this.queuedInitPromise === tracked) this.queuedInitPromise = null;
      });
      this.queuedInitPromise = tracked;
      return tracked;
    }
    if (this.ready) return Promise.resolve();
    if (this.initPromise) return this.initPromise;

    const initializing = this.initialize();
    const tracked = initializing.finally(() => {
      if (this.initPromise === tracked) this.initPromise = null;
    });
    this.initPromise = tracked;
    return tracked;
  }

  private async initialize(): Promise<void> {
    let localLock: InstanceLock | null = null;
    let localWal: WALStorage | null = null;
    let localDlq: DLQStorage | null = null;

    try {
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
        localLock = new InstanceLock(
          this.namespace,
          this.dataDir,
          this.testHooks?.instanceLockOptions,
        );
        await localLock.acquire();
        await this.testHooks?.afterLockAcquire?.();
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
        createdHierarchyStart: localLock?.createdHierarchyStart,
        directorySync: this.testHooks?.walDirectorySync,
      };
      localWal = new WALStorage(this.namespace, this.dataDir, walOptions);
      await localWal.init();

      const dlqOptions: DLQStorageOptions = {
        fsync: this.dlqOpts.fsync ?? true,
        maxFiles: this.dlqOpts.maxEntries ?? 10_000,
        encryptionKey: encryptionKeyBytes,
        directorySync: this.testHooks?.dlqDirectorySync,
        createdHierarchyStart:
          localLock?.createdHierarchyStart ?? localWal.createdHierarchyStart,
      };
      localDlq = new DLQStorage(this.namespace, this.dataDir, dlqOptions);
      await localDlq.init();

      // Publish the initialized resource set only after every component succeeds.
      this.lock = localLock;
      this.wal = localWal;
      this.dlq = localDlq;
      this.ready = true;
      this.acceptingOperations = this.closePromise === null;
    } catch (error) {
      const cleanupErrors: unknown[] = [];
      try {
        await localWal?.close();
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
      try {
        await localLock?.release();
      } catch (cleanupError) {
        cleanupErrors.push(cleanupError);
      }
      if (cleanupErrors.length > 0) {
        throw new AggregateError(
          [error, ...cleanupErrors],
          'FileReliabilityAdapter initialization failed and cleanup was incomplete',
        );
      }
      throw error;
    }
  }

  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    // Admission closes synchronously, before close performs its first await.
    this.acceptingOperations = false;
    const closing = (async () => {
      const initializing = this.initPromise;
      if (initializing) await initializing.catch(() => undefined);
      await this.waitForOperationsToDrain();
      await this.testHooks?.beforeStorageClose?.();

      const wal = this.wal;
      const lock = this.lock;
      this.ready = false;
      this.wal = null;
      this.dlq = null;

      const closeErrors: unknown[] = [];
      try {
        await wal?.close();
      } catch (error) {
        closeErrors.push(error);
      }
      try {
        await lock?.release();
        if (this.lock === lock) this.lock = null;
      } catch (error) {
        closeErrors.push(error);
      }
      if (closeErrors.length === 1) throw closeErrors[0];
      if (closeErrors.length > 1) {
        throw new AggregateError(
          closeErrors,
          'FileReliabilityAdapter storage close and lock release both failed',
        );
      }
    })();
    const tracked = closing.finally(() => {
      if (this.closePromise === tracked) this.closePromise = null;
    });
    this.closePromise = tracked;
    return tracked;
  }

  private waitForOperationsToDrain(): Promise<void> {
    if (this.activeOperations === 0) return Promise.resolve();
    return new Promise((resolve) => { this.operationDrainWaiters.push(resolve); });
  }

  private runOperation<T>(
    operation: (storage: { wal: WALStorage; dlq: DLQStorage }) => Promise<T>,
  ): Promise<T> {
    if (!this.ready || !this.wal || !this.dlq) {
      return Promise.reject(
        new Error('FileReliabilityAdapter not initialized — call init()'),
      );
    }
    if (!this.acceptingOperations) {
      return Promise.reject(
        new Error('FileReliabilityAdapter is closing — no new operations are accepted'),
      );
    }
    const storage = { wal: this.wal, dlq: this.dlq };
    this.activeOperations++;
    return (async () => {
      try {
        return await operation(storage);
      } finally {
        this.activeOperations--;
        if (this.activeOperations === 0) {
          const waiters = this.operationDrainWaiters;
          this.operationDrainWaiters = [];
          for (const resolve of waiters) resolve();
        }
      }
    })();
  }

  appendJournal(log: LogbunLog): Promise<void> {
    return this.runOperation(({ wal }) => wal.append(log));
  }

  acknowledgeJournal(ids: string[]): Promise<void> {
    return this.runOperation(({ wal }) => wal.acknowledge(ids));
  }

  recoverJournal(opts?: {
    maxLogs?: number;
    maxBytes?: number;
  }): Promise<JournalRecoveryResult> {
    return this.runOperation(async ({ wal }) => {
      const result = await wal.readAllBounded({
        maxLogs: opts?.maxLogs,
        maxBytes: opts?.maxBytes,
      });
      return {
        logs: result.logs,
        truncated: result.truncated,
        approxBytes: result.approxBytes,
      };
    });
  }

  compactJournal(): Promise<void> {
    return this.runOperation(({ wal }) => wal.compact());
  }

  writeDlq(tenantId: string | null, logs: LogbunLog[]): Promise<string> {
    return this.runOperation(({ dlq }) => dlq.write(tenantId, logs));
  }

  listDlq(opts?: {
    includePending?: boolean;
    includeProcessing?: boolean;
    includeDead?: boolean;
  }): Promise<DLQEntry[]> {
    return this.runOperation(({ dlq }) => dlq.listAll(opts));
  }

  claimDlq(id?: string): Promise<ClaimedDlqBatch | null> {
    return this.runOperation(async ({ dlq }) => {
      const claimed = await dlq.claim(id);
      if (!claimed) return null;
      return {
        id: claimed.id,
        tenantId: claimed.batch.tenantId,
        attempts: claimed.batch.attempts,
        logs: claimed.batch.logs,
      };
    });
  }

  settleDlqSuccess(id: string): Promise<void> {
    return this.runOperation(({ dlq }) => dlq.markDone(id));
  }

  settleDlqFailure(id: string, nextAttempts: number): Promise<void> {
    return this.runOperation(async ({ dlq }) => {
      await dlq.setAttempts(id, nextAttempts);
      await dlq.markFailed(id);
    });
  }

  poisonDlq(id: string): Promise<void> {
    return this.runOperation(({ dlq }) => dlq.markPoisoned(id));
  }

  requeueDead(id: string): Promise<string> {
    return this.runOperation(({ dlq }) => dlq.requeueDead(id));
  }

  deleteDead(id: string): Promise<void> {
    return this.runOperation(({ dlq }) => dlq.deleteDead(id));
  }

  readDlq(id: string): Promise<ClaimedDlqBatch | null> {
    return this.runOperation(async ({ dlq }) => {
      const batch = await dlq.readById(id);
      if (!batch) return null;
      return {
        id,
        tenantId: batch.tenantId,
        attempts: batch.attempts,
        logs: batch.logs,
      };
    });
  }

  recoverOrphans(): Promise<void> {
    return this.runOperation(({ dlq }) => dlq.recoverOrphans());
  }

  private async getStatsFrom(
    { wal, dlq }: { wal: WALStorage; dlq: DLQStorage },
  ): Promise<ReliabilityStats> {
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

  getStats(): Promise<ReliabilityStats> {
    return this.runOperation((storage) => this.getStatsFrom(storage));
  }

  pendingMaintenanceDelayMs(): Promise<number | null> {
    return this.runOperation(async (storage) => {
      const stats = await this.getStatsFrom(storage);
      return stats.hasPendingWork ? 0 : null;
    });
  }
}
