import type { DLQFileInfo, LogbunLog } from '../types';
import { resolveLogbunDir } from '../utils/path';
import { sanitizeTenantKey } from '../utils/tenant';
import {
  decryptUtf8,
  encryptUtf8,
  type EncryptionKeyBytes,
} from '../utils/crypto';
import { mkdir, open, readdir, rename, unlink } from 'node:fs/promises';
import { isAbsolute, join, relative, resolve, sep } from 'node:path';

/** On-disk DLQ batch envelope (v1). Legacy files are a bare LogbunLog[]. */
export interface DLQBatchEnvelope {
  v: 1;
  tenantId: string | null;
  attempts: number;
  logs: LogbunLog[];
}

/** Parsed batch shape returned by {@link parseBatch} / {@link readBatch}. */
export interface ParsedDLQBatch {
  tenantId: string | null;
  attempts: number;
  logs: LogbunLog[];
  /** Present when content was already a v1 envelope */
  v?: number;
}

export interface DLQStorageOptions {
  /**
   * fsync after each batch write / attempt rewrite.
   * Durable bootstrap should pass `true`; volatile callers default `false`.
   * @default false
   */
  fsync?: boolean;
  /**
   * Refuse new {@link DLQStorage.write} when pending + processing file count
   * is >= this. Dead files do not count (ops can clean them).
   * @default 10_000
   */
  maxFiles?: number;
  /**
   * Alias for {@link DLQStorageOptions.maxFiles} (LogbunConfig / bootstrap naming).
   * Prefer `maxFiles` or config `maxDlqFiles` at the public API.
   */
  maxDlqFiles?: number;
  /** Optional AES-256-GCM key for batch file bodies (same material as WAL). */
  encryptionKey?: EncryptionKeyBytes;
}

/** Default max pending+processing DLQ files before write refuses. */
export const DLQ_MAX_FILES_DEFAULT = 10_000;

/**
 * Parse DLQ file content.
 * New format: `{ v:1, tenantId, attempts, logs }`.
 * Legacy: bare JSON array → attempts 0; tenantId from optional filename hint.
 */
export function parseBatch(
  content: string,
  filenameTenantId?: string | null
): ParsedDLQBatch {
  const parsed: unknown = JSON.parse(content);

  if (Array.isArray(parsed)) {
    return {
      tenantId: filenameTenantId ?? null,
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
      obj.tenantId === undefined
        ? (filenameTenantId ?? null)
        : (obj.tenantId as string | null);
    return {
      v: typeof obj.v === 'number' ? obj.v : undefined,
      tenantId,
      attempts,
      logs,
    };
  }

  return {
    tenantId: filenameTenantId ?? null,
    attempts: 0,
    logs: [],
  };
}

/**
 * Extract tenant key from DLQ filename:
 * `{safeTenantKey}_{unixTimestampMs}_{rand}.batch[.processing|.dead]`
 *
 * Note: key is the sanitized filename key, not necessarily the raw tenantId.
 * Prefer envelope.tenantId for the real tenant.
 */
export function tenantIdFromFilename(filePath: string): string | null {
  const filename = filePath.split(/[/\\]/).pop() ?? '';
  const base = filename
    .replace(/\.batch\.processing$/, '')
    .replace(/\.batch\.dead$/, '')
    .replace(/\.batch$/, '');
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

/** Read and parse a DLQ batch file from disk (plaintext or e1-encrypted body). */
export async function readBatch(
  path: string,
  encryptionKey?: EncryptionKeyBytes
): Promise<ParsedDLQBatch> {
  let content = await Bun.file(path).text();
  if (encryptionKey) {
    content = await decryptUtf8(content.trim(), encryptionKey);
  }
  return parseBatch(content, tenantIdFromFilename(path));
}

function randomSuffix(): string {
  if (typeof Bun !== 'undefined' && typeof Bun.randomUUIDv7 === 'function') {
    return Bun.randomUUIDv7().replace(/-/g, '').slice(0, 8);
  }
  const bytes = new Uint8Array(4);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}

/**
 * Dead Letter Queue — directory-per-namespace, one file per failed batch.
 *
 * Filename convention: {safeTenantKey}_{unixTimestampMs}_{rand}.batch
 * Tenant id in the filename is always sanitized (path-traversal safe).
 * Real tenantId lives in the envelope.
 *
 * Mutating ops that accept external paths call {@link DLQStorage.assertUnderDir}
 * so callers cannot escape the DLQ directory.
 */
export class DLQStorage {
  private readonly dir: string;
  private readonly fsync: boolean;
  private readonly maxFiles: number;
  private readonly encryptionKey?: EncryptionKeyBytes;
  /**
   * Serializes capacity-checked writes so concurrent writers cannot TOCTOU
   * past the maxFiles cap (count then write without mutual exclusion).
   */
  private writeChain: Promise<void> = Promise.resolve();

  constructor(
    namespace: string,
    dataDir?: string,
    options?: DLQStorageOptions
  ) {
    const root = resolveLogbunDir(namespace, dataDir);
    this.dir = join(root, 'dlq');
    this.fsync = options?.fsync ?? false;
    this.maxFiles =
      options?.maxFiles ??
      options?.maxDlqFiles ??
      DLQ_MAX_FILES_DEFAULT;
    this.encryptionKey = options?.encryptionKey;
  }

  /** Run `fn` exclusively with other capacity-checked writes. */
  private runWriteExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.writeChain.then(fn, fn);
    this.writeChain = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  /** Absolute DLQ directory path. */
  get directory(): string {
    return this.dir;
  }

  /** Configured write cap (pending + processing). */
  get maxFilesCap(): number {
    return this.maxFiles;
  }

  async init(): Promise<void> {
    await mkdir(this.dir, { recursive: true });
  }

  /**
   * Count DLQ files by kind with a single readdir.
   * Pending = `*.batch`, processing = `*.batch.processing`, dead = `*.batch.dead`.
   */
  async countByKind(): Promise<{
    pending: number;
    processing: number;
    dead: number;
    total: number;
  }> {
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
      if (f.endsWith('.batch.processing')) {
        processing++;
      } else if (f.endsWith('.batch.dead')) {
        dead++;
      } else if (f.endsWith('.batch')) {
        pending++;
      }
    }
    return {
      pending,
      processing,
      dead,
      total: pending + processing + dead,
    };
  }

  /**
   * Alias of {@link countByKind} for ops / {@link AuditLogger.getStatsDetailed}.
   * Prefer this name in public stats paths; both share one readdir implementation.
   */
  async countFiles(): Promise<{
    pending: number;
    processing: number;
    dead: number;
    total: number;
  }> {
    return this.countByKind();
  }

  /**
   * True when a new pending batch file may be written under the maxFiles cap.
   * Dead files do not consume the write budget.
   */
  async canWrite(): Promise<boolean> {
    const { pending, processing } = await this.countByKind();
    return pending + processing < this.maxFiles;
  }

  /**
   * Reject paths that escape this DLQ directory or contain null bytes.
   * Uses resolve + relative; rejects `..` segments and cross-root absolutes.
   */
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

  private async writeFile(path: string, body: string): Promise<void> {
    const payload = this.encryptionKey
      ? await encryptUtf8(body, this.encryptionKey)
      : body;
    await Bun.write(path, payload);
    if (this.fsync) {
      await this.fsyncFile(path);
    }
  }

  /** Read batch using this instance's encryption key. */
  async readBatchFile(path: string): Promise<ParsedDLQBatch> {
    return readBatch(path, this.encryptionKey);
  }

  /**
   * Write a failed batch to the DLQ as a v1 envelope.
   * Filename uses a sanitized tenant key — original tenantId is in the envelope.
   *
   * Throws when pending + processing >= maxFiles (message includes `dlq_full`)
   * so the batcher can emit drop/dlq events. Dead files do not count.
   *
   * Capacity check + create are serialized so concurrent writers cannot
   * slightly exceed the file cap (TOCTOU on countByKind → write).
   */
  async write(tenantId: string | null, logs: LogbunLog[]): Promise<void> {
    return this.runWriteExclusive(async () => {
      await this.assertCanWriteUnlocked();

      const key = sanitizeTenantKey(tenantId);
      const ts = Date.now();
      const rand = randomSuffix();
      const filename = `${key}_${ts}_${rand}.batch`;
      const path = join(this.dir, filename);
      this.assertUnderDir(path);
      const envelope: DLQBatchEnvelope = {
        v: 1,
        tenantId,
        attempts: 0,
        logs,
      };
      await this.writeFile(path, JSON.stringify(envelope));
    });
  }

  async listPending(): Promise<string[]> {
    // Only plain `.batch` (not `.batch.processing` / `.batch.dead`)
    return this.listBySuffix('.batch');
  }

  async listDead(): Promise<string[]> {
    return this.listBySuffix('.batch.dead');
  }

  async listProcessing(): Promise<string[]> {
    return this.listBySuffix('.batch.processing');
  }

  /**
   * Ops helper: list pending / processing / dead files with envelope metadata.
   */
  async listAll(opts?: {
    includePending?: boolean;
    includeProcessing?: boolean;
    includeDead?: boolean;
  }): Promise<DLQFileInfo[]> {
    const includePending = opts?.includePending !== false;
    const includeProcessing = opts?.includeProcessing === true;
    const includeDead = opts?.includeDead === true;

    const paths: { path: string; kind: DLQFileInfo['kind'] }[] = [];
    if (includePending) {
      for (const p of await this.listPending()) {
        paths.push({ path: p, kind: 'pending' });
      }
    }
    if (includeProcessing) {
      for (const p of await this.listProcessing()) {
        paths.push({ path: p, kind: 'processing' });
      }
    }
    if (includeDead) {
      for (const p of await this.listDead()) {
        paths.push({ path: p, kind: 'dead' });
      }
    }

    const out: DLQFileInfo[] = [];
    for (const { path, kind } of paths) {
      try {
        const batch = await this.readBatchFile(path);
        out.push({
          path,
          kind,
          tenantId: batch.tenantId,
          attempts: batch.attempts,
          logCount: batch.logs.length,
        });
      } catch {
        out.push({
          path,
          kind,
          tenantId: tenantIdFromFilename(path),
          attempts: 0,
          logCount: 0,
        });
      }
    }
    return out;
  }

  /**
   * Re-queue a poisoned `.dead` file as a fresh `.batch` (attempts reset to 0).
   * Same capacity rules as {@link write}; on `dlq_full` the `.dead` file is kept.
   */
  async requeueDead(deadPath: string): Promise<string> {
    this.assertUnderDir(deadPath);
    if (!deadPath.endsWith('.batch.dead')) {
      throw new Error('requeueDead expects a .batch.dead path');
    }

    return this.runWriteExclusive(async () => {
      this.assertUnderDir(deadPath);
      await this.assertCanWriteUnlocked();

      const batch = await this.readBatchFile(deadPath);
      const envelope: DLQBatchEnvelope = {
        v: 1,
        tenantId: batch.tenantId,
        attempts: 0,
        logs: batch.logs,
      };
      const key = sanitizeTenantKey(batch.tenantId);
      const filename = `${key}_${Date.now()}_${randomSuffix()}.batch`;
      const newPath = join(this.dir, filename);
      this.assertUnderDir(newPath);
      await this.writeFile(newPath, JSON.stringify(envelope));
      await unlink(deadPath);
      return newPath;
    });
  }

  /**
   * Capacity check for exclusive writers. Call only under {@link runWriteExclusive}.
   * @throws Error including `dlq_full` when at/over maxFiles
   */
  private async assertCanWriteUnlocked(): Promise<void> {
    const counts = await this.countByKind();
    if (counts.pending + counts.processing >= this.maxFiles) {
      throw new Error(
        `dlq_full: pending+processing (${counts.pending + counts.processing}) >= maxFiles (${this.maxFiles})`
      );
    }
  }

  /** Delete a dead/poison file permanently. */
  async deleteDead(deadPath: string): Promise<void> {
    this.assertUnderDir(deadPath);
    if (!deadPath.endsWith('.batch.dead')) {
      throw new Error('deleteDead expects a .batch.dead path');
    }
    await unlink(deadPath);
  }

  private async listBySuffix(suffix: string): Promise<string[]> {
    let entries: string[];
    try {
      entries = await readdir(this.dir);
    } catch {
      return [];
    }

    return entries
      .filter((f) => f.endsWith(suffix))
      .sort()
      .map((f) => join(this.dir, f));
  }

  async markProcessing(filePath: string): Promise<string> {
    this.assertUnderDir(filePath);
    const processingPath = `${filePath}.processing`;
    this.assertUnderDir(processingPath);
    await rename(filePath, processingPath);
    return processingPath;
  }

  async markDone(processingPath: string): Promise<void> {
    this.assertUnderDir(processingPath);
    await unlink(processingPath);
  }

  async markFailed(processingPath: string): Promise<void> {
    this.assertUnderDir(processingPath);
    const originalPath = processingPath.replace(/\.processing$/, '');
    this.assertUnderDir(originalPath);
    await rename(processingPath, originalPath);
  }

  async incrementAttempts(
    processingPath: string,
    currentAttempts: number
  ): Promise<void> {
    this.assertUnderDir(processingPath);
    const parsed = await this.readBatchFile(processingPath);
    const envelope: DLQBatchEnvelope = {
      v: 1,
      tenantId: parsed.tenantId,
      attempts: currentAttempts + 1,
      logs: parsed.logs,
    };
    await this.writeFile(processingPath, JSON.stringify(envelope));
  }

  async markPoisoned(processingPath: string): Promise<void> {
    this.assertUnderDir(processingPath);
    const deadPath = processingPath.replace(/\.processing$/, '.dead');
    this.assertUnderDir(deadPath);
    await rename(processingPath, deadPath);
  }

  async recoverOrphans(): Promise<void> {
    let entries: string[];
    try {
      entries = await readdir(this.dir);
    } catch {
      return;
    }

    const orphans = entries.filter((f) => f.endsWith('.batch.processing'));
    for (const orphan of orphans) {
      const processingPath = join(this.dir, orphan);
      const batchPath = processingPath.replace(/\.processing$/, '');
      await rename(processingPath, batchPath);
    }
  }
}
