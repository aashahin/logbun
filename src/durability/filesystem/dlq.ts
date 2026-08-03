/**
 * Filesystem Dead Letter Queue — opaque IDs, atomic claim/settle.
 *
 * Filename: `{opaqueId}.batch` | `.batch.processing` | `.batch.dead`
 * Envelope holds id, tenantId, attempts, logs. Paths are diagnostics only.
 *
 * Deno: `--allow-read` / `--allow-write` on the data directory.
 */
import type { LogbunLog } from '../../types';
import type { DLQEntry, DlqState } from '../../reliability/types';
import { resolveLogbunDir } from './path';
import {
  decryptUtf8,
  encryptUtf8,
  type EncryptionKeyBytes,
} from '../../utils/crypto';
import { randomUUIDv7 } from '../../utils/uuidv7';
import { constants as FsConstants } from 'node:fs';
import {
  mkdir,
  link,
  open,
  lstat,
  readdir,
  realpath,
  rename,
  unlink,
  writeFile,
  readFile,
} from 'node:fs/promises';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';

/** On-disk DLQ batch envelope (v2 — opaque id). Legacy v1 / bare arrays supported on read. */
export interface DLQBatchEnvelope {
  v: 1 | 2;
  id?: string;
  tenantId: string | null;
  attempts: number;
  logs: LogbunLog[];
}

export interface ParsedDLQBatch {
  id: string;
  tenantId: string | null;
  attempts: number;
  logs: LogbunLog[];
  v?: number;
}

export interface DLQStorageOptions {
  fsync?: boolean;
  maxFiles?: number;
  maxDlqFiles?: number;
  encryptionKey?: EncryptionKeyBytes;
  /** @internal Test crash-window hook, invoked after temp fsync and before rename. */
  beforeAtomicRename?: (targetPath: string) => void | Promise<void>;
  /** @internal Invoked immediately before the no-overwrite dead→pending link. */
  beforeRequeueLink?: (
    deadPath: string,
    pendingPath: string,
  ) => void | Promise<void>;
  /** @internal Invoked after linked state is durable but before dead unlink. */
  afterRequeueLink?: (
    deadPath: string,
    pendingPath: string,
  ) => void | Promise<void>;
  /** @internal Deterministic hard-link seam for unsupported-platform tests. */
  requeueLink?: (deadPath: string, pendingPath: string) => Promise<void>;
  /** @internal Deterministic directory-fsync seam for fault/order tests. */
  directorySync?: (
    directory: string,
    reason: DLQDirectorySyncReason,
  ) => void | Promise<void>;
  /** @internal Observes whether an operation was queued behind prior work. */
  operationQueued?: (queuedBehindPriorOperation: boolean) => void;
  /** @internal Earliest directory created by adapter setup before DLQ init. */
  createdHierarchyStart?: string;
}

export type DLQDirectorySyncReason = 'initialize-hierarchy' | 'mutation';

interface PendingDirectorySync {
  directory: string;
  reason: DLQDirectorySyncReason;
  allowPermissionBoundary: boolean;
}

export const DLQ_MAX_FILES_DEFAULT = 10_000;

const ID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
/** Current UUIDv7 IDs plus confined legacy opaque IDs, never filesystem paths. */
const OPAQUE_ID_RE = /^[A-Za-z0-9_-]{1,128}$/;

/** Distinguishes a failed durability barrier after rename already mutated state. */
class DLQPostRenameSyncError extends Error {
  readonly originalError: unknown;

  constructor(error: unknown) {
    super(error instanceof Error ? error.message : String(error), { cause: error });
    this.name = 'DLQPostRenameSyncError';
    this.originalError = error;
  }
}

/**
 * Parse DLQ file content.
 * v2: `{ v:2, id, tenantId, attempts, logs }`
 * v1: `{ v:1, tenantId, attempts, logs }` — id from filename
 * Legacy: bare JSON array
 */
export function parseBatch(
  content: string,
  filenameId?: string | null
): ParsedDLQBatch {
  const parsed: unknown = JSON.parse(content);
  const fallbackId = filenameId && ID_RE.test(filenameId) ? filenameId : filenameId ?? randomUUIDv7();

  if (Array.isArray(parsed)) {
    return {
      id: typeof fallbackId === 'string' ? fallbackId : randomUUIDv7(),
      tenantId: null,
      attempts: 0,
      logs: parsed as LogbunLog[],
    };
  }

  if (parsed && typeof parsed === 'object') {
    const obj = parsed as Record<string, unknown>;
    const logs = Array.isArray(obj.logs) ? (obj.logs as LogbunLog[]) : [];
    const attempts =
      typeof obj.attempts === 'number' && Number.isFinite(obj.attempts)
        ? obj.attempts
        : 0;
    const tenantId =
      obj.tenantId === undefined ? null : (obj.tenantId as string | null);
    const id =
      typeof obj.id === 'string' && obj.id
        ? obj.id
        : typeof fallbackId === 'string'
          ? fallbackId
          : randomUUIDv7();
    return {
      v: typeof obj.v === 'number' ? obj.v : undefined,
      id,
      tenantId,
      attempts,
      logs,
    };
  }

  return {
    id: typeof fallbackId === 'string' ? fallbackId : randomUUIDv7(),
    tenantId: null,
    attempts: 0,
    logs: [],
  };
}

/** Extract opaque id from filename `{id}.batch[.processing|.dead]`. */
export function idFromFilename(filePath: string): string | null {
  const filename = filePath.split(/[/\\]/).pop() ?? '';
  const base = filename
    .replace(/\.batch\.processing$/, '')
    .replace(/\.batch\.dead$/, '')
    .replace(/\.batch$/, '');
  return base || null;
}

/**
 * @deprecated Prefer envelope.tenantId / idFromFilename. Kept for migration tests.
 * Legacy filenames: `{safeTenantKey}_{ts}_{rand}.batch`
 */
export function tenantIdFromFilename(filePath: string): string | null {
  const filename = filePath.split(/[/\\]/).pop() ?? '';
  const base = filename
    .replace(/\.batch\.processing$/, '')
    .replace(/\.batch\.dead$/, '')
    .replace(/\.batch$/, '');
  // Opaque UUID filenames have no tenant key
  if (ID_RE.test(base)) return null;
  const segments = base.split('_');
  if (segments.length < 3) {
    const key = segments[0] ?? '';
    return key === '__global__' || !key ? null : key;
  }
  segments.pop();
  segments.pop();
  const key = segments.join('_');
  return key === '__global__' || !key ? null : key;
}

export async function readBatch(
  path: string,
  encryptionKey?: EncryptionKeyBytes
): Promise<ParsedDLQBatch> {
  let content = await readFile(path, 'utf8');
  if (encryptionKey) {
    content = await decryptUtf8(content.trim(), encryptionKey);
  }
  return parseBatch(content, idFromFilename(path));
}

export class DLQStorage {
  private readonly dataDir: string;
  private readonly namespaceDir: string;
  private readonly dir: string;
  private readonly fsync: boolean;
  private readonly maxFiles: number;
  private readonly encryptionKey?: EncryptionKeyBytes;
  private readonly beforeAtomicRename?: (targetPath: string) => void | Promise<void>;
  private readonly beforeRequeueLink?: DLQStorageOptions['beforeRequeueLink'];
  private readonly afterRequeueLink?: DLQStorageOptions['afterRequeueLink'];
  private readonly requeueLink: NonNullable<DLQStorageOptions['requeueLink']>;
  private readonly directorySync?: DLQStorageOptions['directorySync'];
  private readonly operationQueued?: DLQStorageOptions['operationQueued'];
  private readonly createdHierarchyStart?: string;
  private hierarchyStartPending?: string;
  private readonly settledHierarchySyncs = new Set<string>();
  private pendingDirectorySyncs: PendingDirectorySync[] = [];
  private ready = false;
  private requeueRepairPending = false;
  private requeueTransitionInFlight = false;
  private pendingOperations = 0;
  private operationChain: Promise<void> = Promise.resolve();

  constructor(
    namespace: string,
    dataDir?: string,
    options?: DLQStorageOptions
  ) {
    const root = resolveLogbunDir(namespace, dataDir);
    this.dataDir = resolve(dirname(root));
    this.namespaceDir = resolve(root);
    this.dir = join(root, 'dlq');
    this.fsync = options?.fsync ?? false;
    this.maxFiles =
      options?.maxFiles ?? options?.maxDlqFiles ?? DLQ_MAX_FILES_DEFAULT;
    this.encryptionKey = options?.encryptionKey;
    this.beforeAtomicRename = options?.beforeAtomicRename;
    this.beforeRequeueLink = options?.beforeRequeueLink;
    this.afterRequeueLink = options?.afterRequeueLink;
    this.requeueLink = options?.requeueLink ?? ((deadPath, pendingPath) => link(deadPath, pendingPath));
    this.directorySync = options?.directorySync;
    this.operationQueued = options?.operationQueued;
    this.createdHierarchyStart = options?.createdHierarchyStart
      ? resolve(options.createdHierarchyStart)
      : undefined;
  }

  private runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const queuedBehindPriorOperation = this.pendingOperations > 0;
    this.pendingOperations++;
    try {
      this.operationQueued?.(queuedBehindPriorOperation);
    } catch (error) {
      this.pendingOperations--;
      return Promise.reject(error);
    }
    const execute = async (): Promise<T> => {
      try {
        return await fn();
      } finally {
        this.pendingOperations--;
      }
    };
    const run = this.operationChain.then(execute, execute);
    this.operationChain = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  private runReadyExclusive<T>(fn: () => Promise<T>): Promise<T> {
    return this.runExclusive(async () => {
      if (!this.ready) {
        throw new Error('DLQ not initialized — call init() first');
      }
      await this.retryPendingDirectorySync();
      return fn();
    });
  }

  get directory(): string {
    return this.dir;
  }

  get maxFilesCap(): number {
    return this.maxFiles;
  }

  async init(): Promise<void> {
    return this.runExclusive(async () => {
      if (this.ready) return;
      await this.assertExistingDataDirAncestor();
      await this.assertStorageSegments(true);
      const createdHierarchyStart = await mkdir(this.dir, { recursive: true });
      if (createdHierarchyStart && !this.hierarchyStartPending) {
        this.hierarchyStartPending = resolve(createdHierarchyStart);
      }
      await this.assertSecureDirectory();
      const retried = await this.retryPendingDirectorySync();
      await this.publishCreatedHierarchy(retried);
      await this.settleRequeueState(true);
      this.hierarchyStartPending = undefined;
      this.ready = true;
    });
  }

  async countByKind(): Promise<{
    pending: number;
    processing: number;
    dead: number;
    total: number;
  }> {
    return this.runReadyExclusive(() => this.countByKindUnlocked());
  }

  private async countByKindUnlocked(): Promise<{
    pending: number;
    processing: number;
    dead: number;
    total: number;
  }> {
    await this.settleRequeueState();
    await this.assertSecureDirectory();
    let entries: string[];
    try {
      entries = await readdir(this.dir);
    } catch {
      return { pending: 0, processing: 0, dead: 0, total: 0 };
    }

    let pending = 0;
    let processing = 0;
    let dead = 0;
    for (const f of entries) {
      const path = join(this.dir, f);
      if (!(await this.isSafeRegularFile(path))) continue;
      if (f.endsWith('.batch.processing')) processing++;
      else if (f.endsWith('.batch.dead')) dead++;
      else if (f.endsWith('.batch')) pending++;
    }
    return {
      pending,
      processing,
      dead,
      total: pending + processing + dead,
    };
  }

  async countFiles(): Promise<{
    pending: number;
    processing: number;
    dead: number;
    total: number;
  }> {
    return this.countByKind();
  }

  async canWrite(): Promise<boolean> {
    const { pending, processing } = await this.countByKind();
    return pending + processing < this.maxFiles;
  }

  private assertUnderDir(filePath: string): void {
    if (typeof filePath !== 'string' || filePath.length === 0) {
      throw new Error('DLQ path must be a non-empty string');
    }
    if (filePath.includes('\0')) {
      throw new Error('DLQ path contains null byte');
    }
    const resolved = resolve(filePath);
    const root = resolve(this.dir);
    const rel = relative(root, resolved);
    if (
      rel.startsWith('..') ||
      rel.split(sep).includes('..') ||
      isAbsolute(rel)
    ) {
      throw new Error('DLQ path escaped storage directory');
    }
  }

  private async fsyncFile(filePath: string): Promise<void> {
    const fh = await open(filePath, 'r+');
    try {
      await fh.sync();
    } finally {
      await fh.close();
    }
  }

  /** Reject any configured storage segment that has become a symbolic link. */
  private async assertExistingDataDirAncestor(): Promise<void> {
    let candidate = this.dataDir;
    let crossedMissingSegment = false;
    for (;;) {
      try {
        const info = await lstat(candidate);
        if (info.isSymbolicLink()) {
          throw new Error('DLQ data directory contains a symbolic link segment');
        }
        const physical = await realpath(candidate);
        if (physical !== candidate) {
          throw new Error('DLQ data directory contains a symbolic link segment');
        }
        return;
      } catch (error) {
        const code = (error as NodeJS.ErrnoException).code;
        if (code === 'ENOENT') {
          crossedMissingSegment = true;
          const parent = dirname(candidate);
          if (parent === candidate) return;
          candidate = parent;
          continue;
        }
        // Accept a Deno permission boundary only after observing a missing
        // configured segment. Direct capability failures remain fatal.
        if (
          crossedMissingSegment &&
          (code === 'ERR_DENO_NOT_CAPABLE' || (error as Error).name === 'NotCapable')
        ) {
          return;
        }
        throw error;
      }
    }
  }

  private async assertStorageSegments(allowMissing = false): Promise<void> {
    for (const [path, label] of [
      [this.dataDir, 'data directory'],
      [this.namespaceDir, 'namespace directory'],
      [resolve(this.dir), 'DLQ storage directory'],
    ] as const) {
      try {
        const info = await lstat(path);
        if (info.isSymbolicLink()) {
          throw new Error(`DLQ ${label} must not be a symbolic link`);
        }
        const physical = await realpath(path);
        if (physical !== path) {
          throw new Error(`DLQ ${label} contains a symbolic link segment`);
        }
      } catch (error) {
        if (allowMissing && (error as NodeJS.ErrnoException).code === 'ENOENT') {
          continue;
        }
        throw error;
      }
    }
  }

  /** Reject a symlinked DLQ directory before any file operation. */
  private async assertSecureDirectory(): Promise<string> {
    const resolvedDir = resolve(this.dir);
    await this.assertStorageSegments();
    const info = await lstat(resolvedDir);
    if (info.isSymbolicLink()) {
      throw new Error('DLQ storage directory must not be a symbolic link');
    }
    if (!info.isDirectory()) {
      throw new Error('DLQ storage path is not a directory');
    }
    return realpath(resolvedDir);
  }

  /** Physical containment check for an existing DLQ batch. */
  private async assertSecureExistingFile(path: string): Promise<void> {
    this.assertUnderDir(path);
    const root = await this.assertSecureDirectory();
    const info = await lstat(path);
    if (info.isSymbolicLink()) {
      throw new Error('DLQ batch entry must not be a symbolic link');
    }
    if (!info.isFile()) {
      throw new Error('DLQ batch entry is not a regular file');
    }
    const physical = await realpath(path);
    const rel = relative(root, physical);
    if (rel.startsWith('..') || isAbsolute(rel)) {
      throw new Error('DLQ batch escaped storage directory');
    }
  }

  /** Physical containment check for a file that may be atomically replaced. */
  private async assertSecureWriteTarget(path: string): Promise<void> {
    this.assertUnderDir(path);
    await this.assertSecureDirectory();
    try {
      await this.assertSecureExistingFile(path);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
  }

  private async isSafeRegularFile(path: string): Promise<boolean> {
    try {
      await this.assertSecureExistingFile(path);
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false;
      // Listings must never follow an attacker-controlled link. Explicit ID
      // operations call assertSecureExistingFile and report the rejection.
      if (String(error).includes('symbolic link')) return false;
      throw error;
    }
  }

  private clearDirectorySyncDebt(
    directory: string,
    reason: DLQDirectorySyncReason,
  ): void {
    const index = this.pendingDirectorySyncs.findIndex(
      (pending) => pending.directory === directory && pending.reason === reason,
    );
    if (index >= 0) this.pendingDirectorySyncs.splice(index, 1);
  }

  private markDirectoryMutation(
    directory: string,
    reason: DLQDirectorySyncReason,
    allowPermissionBoundary = false,
  ): void {
    if (!this.fsync) return;
    if (
      this.pendingDirectorySyncs.some(
        (pending) => pending.directory === directory && pending.reason === reason,
      )
    ) {
      return;
    }
    this.pendingDirectorySyncs.push({ directory, reason, allowPermissionBoundary });
  }

  private async fsyncDirectory(
    directory = this.dir,
    reason: DLQDirectorySyncReason = 'mutation',
    allowPermissionBoundary = false,
  ): Promise<void> {
    if (!this.fsync) return;
    this.markDirectoryMutation(directory, reason, allowPermissionBoundary);
    try {
      if (this.directorySync) {
        await this.directorySync(directory, reason);
      } else {
        const fh = await open(directory, 'r');
        try {
          await fh.sync();
        } finally {
          await fh.close();
        }
      }
    } catch (error) {
      // Directory fsync is unavailable on a few supported filesystem/runtime
      // combinations. Other errors mean the requested durability was not met.
      const code = (error as NodeJS.ErrnoException).code;
      const permissionBoundary =
        allowPermissionBoundary &&
        (code === 'EACCES' ||
          code === 'ERR_DENO_NOT_CAPABLE' ||
          (error as Error).name === 'NotCapable');
      if (
        !['EINVAL', 'ENOTSUP', 'EOPNOTSUPP', 'EPERM'].includes(code ?? '') &&
        !permissionBoundary
      ) {
        throw error;
      }
    }
    this.clearDirectorySyncDebt(directory, reason);
    if (reason === 'initialize-hierarchy') {
      this.settledHierarchySyncs.add(directory);
    }
  }

  private async retryPendingDirectorySync(): Promise<PendingDirectorySync[]> {
    const retried: PendingDirectorySync[] = [];
    while (this.pendingDirectorySyncs.length > 0) {
      const pending = this.pendingDirectorySyncs[0]!;
      await this.fsyncDirectory(
        pending.directory,
        pending.reason,
        pending.allowPermissionBoundary,
      );
      retried.push(pending);
    }
    return retried;
  }

  private async publishCreatedHierarchy(
    retried: PendingDirectorySync[],
  ): Promise<void> {
    if (!this.fsync) return;
    const createdStart = this.createdHierarchyStart ?? this.hierarchyStartPending;
    // mkdir only tells the process that created the hierarchy which segment was
    // first missing. A replacement process cannot reconstruct where an earlier
    // directory-fsync failed, so it conservatively republishes to the filesystem
    // root when it has no creation marker.
    const outermostTarget = createdStart
      ? dirname(resolve(createdStart))
      : undefined;
    let directory = this.namespaceDir;
    for (;;) {
      const allowPermissionBoundary =
        directory !== this.namespaceDir && directory !== this.dataDir;
      const wasRetried = retried.some(
        (pending) =>
          pending.directory === directory &&
          pending.reason === 'initialize-hierarchy',
      );
      if (!wasRetried && !this.settledHierarchySyncs.has(directory)) {
        await this.fsyncDirectory(
          directory,
          'initialize-hierarchy',
          allowPermissionBoundary,
        );
      }
      if (directory === outermostTarget) return;
      const parent = dirname(directory);
      if (parent === directory) {
        if (!outermostTarget) return;
        throw new Error('DLQ created hierarchy escaped its configured data directory');
      }
      directory = parent;
    }
  }

  private async renameSecurely(from: string, to: string): Promise<void> {
    await this.assertSecureExistingFile(from);
    await this.assertSecureWriteTarget(to);
    await rename(from, to);
    if (this.fsync) {
      try {
        await this.fsyncDirectory();
      } catch (error) {
        throw new DLQPostRenameSyncError(error);
      }
    }
  }

  private async assertDestinationAbsent(path: string): Promise<void> {
    this.assertUnderDir(path);
    await this.assertSecureDirectory();
    try {
      await lstat(path);
      throw new Error('dlq_state_collision: destination state already exists');
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return;
      throw error;
    }
  }

  private async unlinkSecurely(path: string): Promise<void> {
    await this.assertSecureExistingFile(path);
    await unlink(path);
    if (this.fsync) await this.fsyncDirectory();
  }

  private async linkRequeueState(deadPath: string, pendingPath: string): Promise<void> {
    await this.assertSecureExistingFile(deadPath);
    this.assertUnderDir(pendingPath);
    await this.assertSecureDirectory();
    try {
      await this.requeueLink(deadPath, pendingPath);
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code === 'EEXIST') {
        throw new Error('dlq_state_collision: destination state already exists');
      }
      if (['EXDEV', 'ENOSYS', 'ENOTSUP', 'EOPNOTSUPP', 'EPERM'].includes(code ?? '')) {
        throw new Error(
          `dlq_requeue_link_unsupported: atomic requeue requires same-filesystem hard links (${code ?? 'unknown'})`,
        );
      }
      throw error;
    }
    this.requeueRepairPending = true;
    if (this.fsync) await this.fsyncDirectory();
  }

  private async healRequeueLinkDuplicates(): Promise<void> {
    await this.assertSecureDirectory();
    const entries = await readdir(this.dir);
    for (const deadName of entries.filter((name) => name.endsWith('.batch.dead'))) {
      const id = idFromFilename(deadName);
      if (!id) continue;
      const deadPath = this.pathFor(id, 'dead');
      const pendingPath = this.pathFor(id, 'pending');
      let pendingInfo;
      try {
        pendingInfo = await lstat(pendingPath);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') continue;
        throw error;
      }
      await this.assertSecureExistingFile(deadPath);
      await this.assertSecureExistingFile(pendingPath);
      const deadInfo = await lstat(deadPath);
      if (deadInfo.dev !== pendingInfo.dev || deadInfo.ino !== pendingInfo.ino) {
        throw new Error(
          `dlq_state_collision: pending and dead states for ${id} are different files`,
        );
      }
      this.requeueRepairPending = true;
      await unlink(deadPath);
      if (this.fsync) await this.fsyncDirectory();
    }
  }

  private async settleRequeueState(forceScan = false): Promise<void> {
    if (this.requeueTransitionInFlight) {
      throw new Error('dlq_requeue_in_progress: requeue transition is in progress');
    }
    if (!forceScan && !this.requeueRepairPending) return;
    await this.healRequeueLinkDuplicates();
    if (this.requeueRepairPending && this.fsync) await this.fsyncDirectory();
    this.requeueRepairPending = false;
  }

  private async writeFileBody(path: string, body: string): Promise<void> {
    const payload = this.encryptionKey
      ? await encryptUtf8(body, this.encryptionKey)
      : body;
    await this.assertSecureWriteTarget(path);
    let tempPath: string | null = null;
    try {
      // `wx` gives the temporary file exclusive creation semantics. It cannot
      // follow a pre-created symlink and a collision simply gets another UUID.
      for (let attempt = 0; attempt < 3; attempt++) {
        const candidate = join(
          this.dir,
          `.${idFromFilename(path) ?? randomUUIDv7()}.${randomUUIDv7()}.tmp`
        );
        try {
          await writeFile(candidate, payload, { encoding: 'utf8', flag: 'wx' });
          tempPath = candidate;
          break;
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
        }
      }
      if (!tempPath) {
        throw new Error('DLQ could not create a unique temporary batch file');
      }
      if (this.fsync) await this.fsyncFile(tempPath);
      await this.beforeAtomicRename?.(path);
      await rename(tempPath, path);
      if (this.fsync) await this.fsyncDirectory();
    } catch (error) {
      if (tempPath) await unlink(tempPath).catch(() => undefined);
      throw error;
    }
  }

  async readBatchFile(path: string): Promise<ParsedDLQBatch> {
    return this.runReadyExclusive(async () => {
      await this.settleRequeueState();
      return this.readBatchFileUnlocked(path);
    });
  }

  private async readBatchFileUnlocked(path: string): Promise<ParsedDLQBatch> {
    await this.assertSecureExistingFile(path);
    // O_NOFOLLOW closes the check-to-use window for an entry that is swapped
    // to a symlink after physical validation but before its body is read.
    const fh = await open(path, FsConstants.O_RDONLY | FsConstants.O_NOFOLLOW);
    try {
      const info = await fh.stat();
      if (!info.isFile()) {
        throw new Error('DLQ batch entry is not a regular file');
      }
      let content = await fh.readFile({ encoding: 'utf8' });
      if (this.encryptionKey) {
        content = await decryptUtf8(content.trim(), this.encryptionKey);
      }
      return parseBatch(content, idFromFilename(path));
    } finally {
      await fh.close();
    }
  }

  /** Atomically resolve and read an opaque id under the DLQ operation chain. */
  async readById(id: string): Promise<ParsedDLQBatch | null> {
    return this.runReadyExclusive(async () => {
      const resolved = await this.resolvePathUnlocked(id);
      if (!resolved) return null;
      return this.readBatchFileUnlocked(resolved.path);
    });
  }

  /** Resolve opaque id → absolute path for any state, or null. */
  async resolvePath(id: string): Promise<{ path: string; state: DlqState } | null> {
    return this.runReadyExclusive(() => this.resolvePathUnlocked(id));
  }

  private async resolvePathUnlocked(
    id: string,
  ): Promise<{ path: string; state: DlqState } | null> {
    await this.settleRequeueState();
    if (typeof id !== 'string' || !OPAQUE_ID_RE.test(id)) {
      return null;
    }
    const candidates: { suffix: string; state: DlqState }[] = [
      { suffix: '.batch', state: 'pending' },
      { suffix: '.batch.processing', state: 'processing' },
      { suffix: '.batch.dead', state: 'dead' },
    ];
    for (const c of candidates) {
      const path = join(this.dir, `${id}${c.suffix}`);
      try {
        // lstat deliberately precedes every ID-resolved operation: `access`
        // and `stat` would follow an attacker-controlled symbolic link.
        await lstat(path);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') continue;
        throw error;
      }
      await this.assertSecureExistingFile(path);
      return { path, state: c.state };
    }
    return null;
  }

  private pathFor(id: string, state: DlqState): string {
    if (state === 'pending') return join(this.dir, `${id}.batch`);
    if (state === 'processing') return join(this.dir, `${id}.batch.processing`);
    return join(this.dir, `${id}.batch.dead`);
  }

  /**
   * Write a failed batch. Returns opaque id.
   * Capacity check + create are serialized (TOCTOU-safe).
   */
  async write(tenantId: string | null, logs: LogbunLog[]): Promise<string> {
    return this.runReadyExclusive(async () => {
      await this.assertCanWriteUnlocked();
      const id = randomUUIDv7();
      const path = this.pathFor(id, 'pending');
      this.assertUnderDir(path);
      const envelope: DLQBatchEnvelope = {
        v: 2,
        id,
        tenantId,
        attempts: 0,
        logs,
      };
      await this.writeFileBody(path, JSON.stringify(envelope));
      return id;
    });
  }

  async listPending(): Promise<string[]> {
    return this.runReadyExclusive(() => this.listIdsByStateUnlocked('pending'));
  }

  async listDead(): Promise<string[]> {
    return this.runReadyExclusive(() => this.listIdsByStateUnlocked('dead'));
  }

  async listProcessing(): Promise<string[]> {
    return this.runReadyExclusive(() => this.listIdsByStateUnlocked('processing'));
  }

  /** @deprecated Prefer list ids; returns absolute paths for internal tests. */
  async listPendingPaths(): Promise<string[]> {
    return this.runReadyExclusive(() => this.listPathsBySuffixUnlocked('.batch'));
  }

  private async listIdsByStateUnlocked(state: DlqState): Promise<string[]> {
    const suffix =
      state === 'pending'
        ? '.batch'
        : state === 'processing'
          ? '.batch.processing'
          : '.batch.dead';
    const paths = await this.listPathsBySuffixUnlocked(suffix);
    return paths
      .map((p) => idFromFilename(p))
      .filter((id): id is string => !!id)
      .sort();
  }

  async listAll(opts?: {
    includePending?: boolean;
    includeProcessing?: boolean;
    includeDead?: boolean;
  }): Promise<DLQEntry[]> {
    return this.runReadyExclusive(() => this.listAllUnlocked(opts));
  }

  private async listAllUnlocked(opts?: {
    includePending?: boolean;
    includeProcessing?: boolean;
    includeDead?: boolean;
  }): Promise<DLQEntry[]> {
    const includePending = opts?.includePending !== false;
    const includeProcessing = opts?.includeProcessing === true;
    const includeDead = opts?.includeDead === true;

    const items: { id: string; state: DlqState; path: string }[] = [];
    if (includePending) {
      for (const p of await this.listPathsBySuffixUnlocked('.batch')) {
        const id = idFromFilename(p);
        if (id) items.push({ id, state: 'pending', path: p });
      }
    }
    if (includeProcessing) {
      for (const p of await this.listPathsBySuffixUnlocked('.batch.processing')) {
        const id = idFromFilename(p);
        if (id) items.push({ id, state: 'processing', path: p });
      }
    }
    if (includeDead) {
      for (const p of await this.listPathsBySuffixUnlocked('.batch.dead')) {
        const id = idFromFilename(p);
        if (id) items.push({ id, state: 'dead', path: p });
      }
    }

    const out: DLQEntry[] = [];
    for (const { id, state, path } of items) {
      try {
        const batch = await this.readBatchFileUnlocked(path);
        out.push({
          // The confined filename is authority; an envelope is data only.
          id,
          state,
          kind: state,
          tenantId: batch.tenantId,
          attempts: batch.attempts,
          logCount: batch.logs.length,
          metadata: { path },
        });
      } catch {
        out.push({
          id,
          state,
          kind: state,
          tenantId: tenantIdFromFilename(path),
          attempts: 0,
          logCount: 0,
          metadata: { path },
        });
      }
    }
    return out;
  }

  /**
   * Re-queue dead entry. **Preserves id**, resets attempts to 0.
   * Accepts an opaque stable id only; metadata paths are never authority.
   */
  async requeueDead(id: string): Promise<string> {
    return this.runReadyExclusive(async () => {
      await this.settleRequeueState();
      if (typeof id !== 'string' || !OPAQUE_ID_RE.test(id)) {
        throw new Error('requeueDead expects a dead DLQ entry id');
      }
      const deadPath = this.pathFor(id, 'dead');
      try {
        await this.assertSecureExistingFile(deadPath);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
          throw new Error('requeueDead expects a dead DLQ entry id');
        }
        throw error;
      }
      const newPath = this.pathFor(id, 'pending');
      await this.assertDestinationAbsent(newPath);
      await this.assertCanWriteUnlocked();

      const batch = await this.readBatchFileUnlocked(deadPath);
      const stableId = idFromFilename(deadPath) || id;
      const envelope: DLQBatchEnvelope = {
        v: 2,
        id: stableId,
        tenantId: batch.tenantId,
        attempts: 0,
        logs: batch.logs,
      };
      // Reset metadata atomically while the entry is still dead. Requeue then
      // links dead→pending without overwrite, syncs that directory entry,
      // unlinks dead, and syncs again. A crash in between leaves a same-inode
      // pair that init/recovery heals without creating duplicate delivery.
      if (batch.v !== 2 || batch.id !== stableId || batch.attempts !== 0) {
        await this.writeFileBody(deadPath, JSON.stringify(envelope));
      }
      this.requeueTransitionInFlight = true;
      try {
        await this.beforeRequeueLink?.(deadPath, newPath);
        await this.linkRequeueState(deadPath, newPath);
        await this.afterRequeueLink?.(deadPath, newPath);
        await this.unlinkSecurely(deadPath);
        this.requeueRepairPending = false;
        return stableId;
      } finally {
        this.requeueTransitionInFlight = false;
      }
    });
  }

  private async assertCanWriteUnlocked(): Promise<void> {
    const counts = await this.countByKindUnlocked();
    if (counts.pending + counts.processing >= this.maxFiles) {
      throw new Error(
        `dlq_full: pending+processing (${counts.pending + counts.processing}) >= maxFiles (${this.maxFiles})`
      );
    }
  }

  async deleteDead(id: string): Promise<void> {
    return this.runReadyExclusive(async () => {
      const resolved = await this.resolvePathUnlocked(id);
      if (!resolved || resolved.state !== 'dead') {
        throw new Error('deleteDead expects a dead DLQ entry id');
      }
      this.assertUnderDir(resolved.path);
      await this.unlinkSecurely(resolved.path);
    });
  }

  private async listPathsBySuffixUnlocked(suffix: string): Promise<string[]> {
    await this.settleRequeueState();
    await this.assertSecureDirectory();
    let entries: string[];
    try {
      entries = await readdir(this.dir);
    } catch {
      return [];
    }
    const out: string[] = [];
    for (const f of entries.sort()) {
      if (!f.endsWith(suffix)) continue;
      const path = join(this.dir, f);
      if (await this.isSafeRegularFile(path)) out.push(path);
    }
    return out;
  }

  /**
   * Atomic claim pending → processing by id, or first pending if id omitted.
   * @returns processing path + batch, or null
   */
  async claim(
    id?: string
  ): Promise<{ id: string; path: string; batch: ParsedDLQBatch } | null> {
    return this.runReadyExclusive(async () => {
      let pendingPath: string | null = null;
      let claimId: string | null = id ?? null;

      if (id) {
        const resolved = await this.resolvePathUnlocked(id);
        if (!resolved || resolved.state !== 'pending') return null;
        pendingPath = resolved.path;
        claimId = id;
      } else {
        const paths = await this.listPathsBySuffixUnlocked('.batch');
        pendingPath = paths[0] ?? null;
        if (!pendingPath) return null;
        claimId = idFromFilename(pendingPath);
      }

      if (!pendingPath || !claimId) return null;
      this.assertUnderDir(pendingPath);
      const processingPath = this.pathFor(claimId, 'processing');
      this.assertUnderDir(processingPath);
      try {
        await this.renameSecurely(pendingPath, processingPath);
      } catch (error) {
        if (error instanceof DLQPostRenameSyncError) {
          try {
            // A claim is not visible until pending -> processing is durable.
            // Restore the deliverable state before reporting that barrier.
            await this.renameSecurely(processingPath, pendingPath);
          } catch (rollbackError) {
            throw new AggregateError(
              [error.originalError, rollbackError],
              'DLQ claim durability failed and rollback could not be made durable',
            );
          }
          throw error.originalError;
        }
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null;
        throw error;
      }
      const batch = await this.readBatchFileUnlocked(processingPath);
      return { id: claimId, path: processingPath, batch };
    });
  }

  /** @deprecated Prefer claim(id). Path-based mark for internal tests. */
  async markProcessing(filePath: string): Promise<string> {
    return this.runReadyExclusive(async () => {
      await this.settleRequeueState();
      this.assertUnderDir(filePath);
      const id = idFromFilename(filePath);
      if (!id) throw new Error('invalid DLQ path');
      const processingPath = this.pathFor(id, 'processing');
      this.assertUnderDir(processingPath);
      await this.renameSecurely(filePath, processingPath);
      return processingPath;
    });
  }

  async markDone(idOrPath: string): Promise<void> {
    return this.runReadyExclusive(async () => {
      const resolved = await this.resolvePathUnlocked(idOrPath);
      if (!resolved) {
        // try as path
        try {
          this.assertUnderDir(idOrPath);
          await this.unlinkSecurely(idOrPath);
        } catch {
          /* ignore */
        }
        return;
      }
      this.assertUnderDir(resolved.path);
      await this.unlinkSecurely(resolved.path);
    });
  }

  async markFailed(idOrPath: string): Promise<void> {
    return this.runReadyExclusive(async () => {
      const resolved = await this.resolvePathUnlocked(idOrPath);
      if (!resolved || resolved.state !== 'processing') {
        if (idOrPath.endsWith('.processing')) {
          this.assertUnderDir(idOrPath);
          const id = idFromFilename(idOrPath);
          if (!id) throw new Error('invalid processing path');
          const original = this.pathFor(id, 'pending');
          await this.renameSecurely(idOrPath, original);
        }
        return;
      }
      const id = idFromFilename(resolved.path) || idOrPath;
      const pending = this.pathFor(id, 'pending');
      this.assertUnderDir(resolved.path);
      this.assertUnderDir(pending);
      await this.renameSecurely(resolved.path, pending);
    });
  }

  async incrementAttempts(
    idOrPath: string,
    currentAttempts: number
  ): Promise<void> {
    return this.runReadyExclusive(async () => {
      const resolved = await this.resolvePathUnlocked(idOrPath);
      const path = resolved?.path ?? idOrPath;
      this.assertUnderDir(path);
      const parsed = await this.readBatchFileUnlocked(path);
      const id = parsed.id || idFromFilename(path) || idOrPath;
      const envelope: DLQBatchEnvelope = {
        v: 2,
        id,
        tenantId: parsed.tenantId,
        attempts: currentAttempts + 1,
        logs: parsed.logs,
      };
      await this.writeFileBody(path, JSON.stringify(envelope));
    });
  }

  async setAttempts(idOrPath: string, attempts: number): Promise<void> {
    return this.runReadyExclusive(async () => {
      const resolved = await this.resolvePathUnlocked(idOrPath);
      if (!resolved) throw new Error('DLQ entry not found');
      this.assertUnderDir(resolved.path);
      const parsed = await this.readBatchFileUnlocked(resolved.path);
      const id = parsed.id || idFromFilename(resolved.path) || idOrPath;
      const envelope: DLQBatchEnvelope = {
        v: 2,
        id,
        tenantId: parsed.tenantId,
        attempts,
        logs: parsed.logs,
      };
      await this.writeFileBody(resolved.path, JSON.stringify(envelope));
    });
  }

  async markPoisoned(idOrPath: string): Promise<void> {
    return this.runReadyExclusive(async () => {
      const resolved = await this.resolvePathUnlocked(idOrPath);
      if (resolved && resolved.state === 'processing') {
        const id = idFromFilename(resolved.path) || idOrPath;
        const deadPath = this.pathFor(id, 'dead');
        this.assertUnderDir(resolved.path);
        this.assertUnderDir(deadPath);
        await this.renameSecurely(resolved.path, deadPath);
        return;
      }
      if (idOrPath.endsWith('.processing')) {
        this.assertUnderDir(idOrPath);
        const id = idFromFilename(idOrPath);
        if (!id) throw new Error('invalid processing path');
        const deadPath = this.pathFor(id, 'dead');
        await this.renameSecurely(idOrPath, deadPath);
      }
    });
  }

  async recoverOrphans(): Promise<void> {
    return this.runReadyExclusive(async () => {
      await this.assertSecureDirectory();
      await this.settleRequeueState(true);
      let entries: string[];
      try {
        entries = await readdir(this.dir);
      } catch {
        return;
      }

      const orphans = entries.filter((f) => f.endsWith('.batch.processing'));
      for (const orphan of orphans) {
        const processingPath = join(this.dir, orphan);
        const id = idFromFilename(processingPath);
        if (!id) continue;
        const batchPath = this.pathFor(id, 'pending');
        await this.renameSecurely(processingPath, batchPath);
      }
    });
  }
}
