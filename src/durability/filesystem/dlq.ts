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
import {
  access,
  constants as FsConstants,
  mkdir,
  open,
  readdir,
  rename,
  unlink,
  writeFile,
  readFile,
} from 'node:fs/promises';
import { isAbsolute, join, relative, resolve, sep } from 'node:path';

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
}

export const DLQ_MAX_FILES_DEFAULT = 10_000;

const ID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
/** Current UUIDv7 IDs plus confined legacy opaque IDs, never filesystem paths. */
const OPAQUE_ID_RE = /^[A-Za-z0-9_-]{1,128}$/;

async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path, FsConstants.F_OK);
    return true;
  } catch {
    return false;
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
  private readonly dir: string;
  private readonly fsync: boolean;
  private readonly maxFiles: number;
  private readonly encryptionKey?: EncryptionKeyBytes;
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
      options?.maxFiles ?? options?.maxDlqFiles ?? DLQ_MAX_FILES_DEFAULT;
    this.encryptionKey = options?.encryptionKey;
  }

  private runWriteExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.writeChain.then(fn, fn);
    this.writeChain = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  get directory(): string {
    return this.dir;
  }

  get maxFilesCap(): number {
    return this.maxFiles;
  }

  async init(): Promise<void> {
    await mkdir(this.dir, { recursive: true });
  }

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

  private async writeFileBody(path: string, body: string): Promise<void> {
    const payload = this.encryptionKey
      ? await encryptUtf8(body, this.encryptionKey)
      : body;
    await writeFile(path, payload, 'utf8');
    if (this.fsync) {
      await this.fsyncFile(path);
    }
  }

  async readBatchFile(path: string): Promise<ParsedDLQBatch> {
    return readBatch(path, this.encryptionKey);
  }

  /** Resolve opaque id → absolute path for any state, or null. */
  async resolvePath(id: string): Promise<{ path: string; state: DlqState } | null> {
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
      if (await fileExists(path)) {
        return { path, state: c.state };
      }
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
    return this.runWriteExclusive(async () => {
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
    return this.listIdsByState('pending');
  }

  async listDead(): Promise<string[]> {
    return this.listIdsByState('dead');
  }

  async listProcessing(): Promise<string[]> {
    return this.listIdsByState('processing');
  }

  /** @deprecated Prefer list ids; returns absolute paths for internal tests. */
  async listPendingPaths(): Promise<string[]> {
    return this.listPathsBySuffix('.batch');
  }

  private async listIdsByState(state: DlqState): Promise<string[]> {
    const suffix =
      state === 'pending'
        ? '.batch'
        : state === 'processing'
          ? '.batch.processing'
          : '.batch.dead';
    const paths = await this.listPathsBySuffix(suffix);
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
    const includePending = opts?.includePending !== false;
    const includeProcessing = opts?.includeProcessing === true;
    const includeDead = opts?.includeDead === true;

    const items: { id: string; state: DlqState; path: string }[] = [];
    if (includePending) {
      for (const p of await this.listPathsBySuffix('.batch')) {
        const id = idFromFilename(p);
        if (id) items.push({ id, state: 'pending', path: p });
      }
    }
    if (includeProcessing) {
      for (const p of await this.listPathsBySuffix('.batch.processing')) {
        const id = idFromFilename(p);
        if (id) items.push({ id, state: 'processing', path: p });
      }
    }
    if (includeDead) {
      for (const p of await this.listPathsBySuffix('.batch.dead')) {
        const id = idFromFilename(p);
        if (id) items.push({ id, state: 'dead', path: p });
      }
    }

    const out: DLQEntry[] = [];
    for (const { id, state, path } of items) {
      try {
        const batch = await this.readBatchFile(path);
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
    return this.runWriteExclusive(async () => {
      const resolved = await this.resolvePath(id);
      if (!resolved || resolved.state !== 'dead') {
        throw new Error('requeueDead expects a dead DLQ entry id');
      }
      this.assertUnderDir(resolved.path);
      await this.assertCanWriteUnlocked();

      const batch = await this.readBatchFile(resolved.path);
      const stableId = idFromFilename(resolved.path) || id;
      const envelope: DLQBatchEnvelope = {
        v: 2,
        id: stableId,
        tenantId: batch.tenantId,
        attempts: 0,
        logs: batch.logs,
      };
      const newPath = this.pathFor(stableId, 'pending');
      this.assertUnderDir(newPath);
      await this.writeFileBody(newPath, JSON.stringify(envelope));
      await unlink(resolved.path);
      return stableId;
    });
  }

  private async assertCanWriteUnlocked(): Promise<void> {
    const counts = await this.countByKind();
    if (counts.pending + counts.processing >= this.maxFiles) {
      throw new Error(
        `dlq_full: pending+processing (${counts.pending + counts.processing}) >= maxFiles (${this.maxFiles})`
      );
    }
  }

  async deleteDead(id: string): Promise<void> {
    const resolved = await this.resolvePath(id);
    if (!resolved || resolved.state !== 'dead') {
      throw new Error('deleteDead expects a dead DLQ entry id');
    }
    this.assertUnderDir(resolved.path);
    await unlink(resolved.path);
  }

  private async listPathsBySuffix(suffix: string): Promise<string[]> {
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

  /**
   * Atomic claim pending → processing by id, or first pending if id omitted.
   * @returns processing path + batch, or null
   */
  async claim(
    id?: string
  ): Promise<{ id: string; path: string; batch: ParsedDLQBatch } | null> {
    return this.runWriteExclusive(async () => {
      let pendingPath: string | null = null;
      let claimId: string | null = id ?? null;

      if (id) {
        const resolved = await this.resolvePath(id);
        if (!resolved || resolved.state !== 'pending') return null;
        pendingPath = resolved.path;
        claimId = id;
      } else {
        const paths = await this.listPathsBySuffix('.batch');
        pendingPath = paths[0] ?? null;
        if (!pendingPath) return null;
        claimId = idFromFilename(pendingPath);
      }

      if (!pendingPath || !claimId) return null;
      this.assertUnderDir(pendingPath);
      const processingPath = this.pathFor(claimId, 'processing');
      this.assertUnderDir(processingPath);
      try {
        await rename(pendingPath, processingPath);
      } catch {
        return null;
      }
      const batch = await this.readBatchFile(processingPath);
      return { id: claimId, path: processingPath, batch };
    });
  }

  /** @deprecated Prefer claim(id). Path-based mark for internal tests. */
  async markProcessing(filePath: string): Promise<string> {
    this.assertUnderDir(filePath);
    const id = idFromFilename(filePath);
    if (!id) throw new Error('invalid DLQ path');
    const processingPath = this.pathFor(id, 'processing');
    this.assertUnderDir(processingPath);
    await rename(filePath, processingPath);
    return processingPath;
  }

  async markDone(idOrPath: string): Promise<void> {
    const resolved = await this.resolvePath(idOrPath);
    if (!resolved) {
      // try as path
      try {
        this.assertUnderDir(idOrPath);
        await unlink(idOrPath);
      } catch {
        /* ignore */
      }
      return;
    }
    this.assertUnderDir(resolved.path);
    await unlink(resolved.path);
  }

  async markFailed(idOrPath: string): Promise<void> {
    const resolved = await this.resolvePath(idOrPath);
    if (!resolved || resolved.state !== 'processing') {
      if (idOrPath.endsWith('.processing')) {
        this.assertUnderDir(idOrPath);
        const id = idFromFilename(idOrPath);
        if (!id) throw new Error('invalid processing path');
        const original = this.pathFor(id, 'pending');
        await rename(idOrPath, original);
      }
      return;
    }
    const id = idFromFilename(resolved.path) || idOrPath;
    const pending = this.pathFor(id, 'pending');
    this.assertUnderDir(resolved.path);
    this.assertUnderDir(pending);
    await rename(resolved.path, pending);
  }

  async incrementAttempts(
    idOrPath: string,
    currentAttempts: number
  ): Promise<void> {
    const resolved = await this.resolvePath(idOrPath);
    const path = resolved?.path ?? idOrPath;
    this.assertUnderDir(path);
    const parsed = await this.readBatchFile(path);
    const id = parsed.id || idFromFilename(path) || idOrPath;
    const envelope: DLQBatchEnvelope = {
      v: 2,
      id,
      tenantId: parsed.tenantId,
      attempts: currentAttempts + 1,
      logs: parsed.logs,
    };
    await this.writeFileBody(path, JSON.stringify(envelope));
  }

  async setAttempts(idOrPath: string, attempts: number): Promise<void> {
    const resolved = await this.resolvePath(idOrPath);
    if (!resolved) throw new Error('DLQ entry not found');
    this.assertUnderDir(resolved.path);
    const parsed = await this.readBatchFile(resolved.path);
    const id = parsed.id || idFromFilename(resolved.path) || idOrPath;
    const envelope: DLQBatchEnvelope = {
      v: 2,
      id,
      tenantId: parsed.tenantId,
      attempts,
      logs: parsed.logs,
    };
    await this.writeFileBody(resolved.path, JSON.stringify(envelope));
  }

  async markPoisoned(idOrPath: string): Promise<void> {
    const resolved = await this.resolvePath(idOrPath);
    if (resolved && resolved.state === 'processing') {
      const id = idFromFilename(resolved.path) || idOrPath;
      const deadPath = this.pathFor(id, 'dead');
      this.assertUnderDir(resolved.path);
      this.assertUnderDir(deadPath);
      await rename(resolved.path, deadPath);
      return;
    }
    if (idOrPath.endsWith('.processing')) {
      this.assertUnderDir(idOrPath);
      const id = idFromFilename(idOrPath);
      if (!id) throw new Error('invalid processing path');
      const deadPath = this.pathFor(id, 'dead');
      await rename(idOrPath, deadPath);
    }
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
      const id = idFromFilename(processingPath);
      if (!id) continue;
      const batchPath = this.pathFor(id, 'pending');
      await rename(processingPath, batchPath);
    }
  }
}
