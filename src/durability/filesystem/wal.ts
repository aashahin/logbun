import type { LogbunLog } from '../../types';
import { assertNoSymlinkPath, resolveLogbunDir } from './path';
import {
  decryptUtf8,
  encryptUtf8,
  type EncryptionKeyBytes,
} from '../../utils/crypto';
import {
  lstat,
  mkdir,
  open,
  readdir,
  realpath,
  rename,
  unlink,
} from 'node:fs/promises';
import { constants as FsConstants } from 'node:fs';
import { Buffer } from 'node:buffer';
import { basename, isAbsolute, join, relative, resolve } from 'node:path';
import { randomUUIDv7 } from '../../utils/uuidv7';


/** Soft-size hint (64 MiB). Default maxWalBytes / hard refuse threshold. */
export const WAL_SIZE_SOFT_LIMIT_BYTES = 64 * 1024 * 1024;

/** Default sealed segment size before rotating `current.aof`. */
export const WAL_SEGMENT_BYTES_DEFAULT = 16 * 1024 * 1024;

export interface WALStorageOptions {
  /**
   * fsync after append and after compact rewrite.
   * @default true
   */
  fsync?: boolean;
  /**
   * Compact when this many ack ids sit in the sidecar.
   * @default 256
   */
  compactAckThreshold?: number;
  /**
   * Hard size limit in bytes across all segments + current.
   * When `hardMaxBytes` is true (default), append throws `wal_full` if the
   * next fully encoded line would exceed this limit.
   * @default {@link WAL_SIZE_SOFT_LIMIT_BYTES} (64 MiB)
   */
  maxBytes?: number;
  /** Alias for {@link WALStorageOptions.maxBytes}. */
  maxWalBytes?: number;
  /**
   * When true (default), refuse an append whose exact encoded bytes would make
   * the WAL larger than `maxBytes` (`wal_full`). Equality is permitted.
   * Batcher treats this like any WAL append failure (DLQ fallback).
   * @default true
   */
  hardMaxBytes?: boolean;
  /**
   * Rotate `current.aof` into a sealed segment when it reaches this size.
   * Keeps compact/read peak memory closer to segment size, not total WAL size.
   * @default 16 MiB
   */
  segmentBytes?: number;
  /** Optional AES-256-GCM key (32 raw bytes after normalize). */
  encryptionKey?: EncryptionKeyBytes;
  /** @internal Deterministic seam for directory-fsync fault/order tests. */
  directorySync?: (
    directory: string,
    reason: WALDirectorySyncReason,
  ) => void | Promise<void>;
}

export type WALDirectorySyncReason =
  | 'initialize'
  | 'rotate'
  | 'rewrite'
  | 'delete-segment';

export const WAL_DIRECTORY_FSYNC_UNSUPPORTED_CODES = [
  'EINVAL',
  'ENOTSUP',
  'EOPNOTSUPP',
  'EPERM',
] as const;

/** Options for {@link WALStorage.readAllBounded}. */
export interface WALReadBoundedOptions {
  maxLogs?: number;
  maxBytes?: number;
}

export interface WALReadBoundedResult {
  logs: LogbunLog[];
  truncated: boolean;
  approxBytes: number;
}

/**
 * Write-Ahead Log — segmented append-only NDJSON + ack sidecar.
 *
 * Layout under `{dataDir}/{namespace}/wal/`:
 * - `current.aof` — active append target
 * - `seg-NNNNNN.aof` — sealed segments (rotation)
 * - `acked.ids` — append-only ack sidecar
 *
 * Compact rewrites segment-by-segment (bounded peak memory ≈ segment size).
 * All mutations serialized via `runExclusive`.
 */
export class WALStorage {
  private readonly namespaceDir: string;
  private readonly dir: string;
  private readonly path: string;
  private readonly ackPath: string;
  private readonly fsync: boolean;
  private readonly compactAckThreshold: number;
  private readonly maxBytes: number;
  private readonly hardMaxBytes: boolean;
  private readonly segmentBytes: number;
  private readonly encryptionKey?: EncryptionKeyBytes;
  private readonly directorySync?: WALStorageOptions['directorySync'];
  private pendingDirectorySync: WALDirectorySyncReason | null = null;
  private ready = false;
  private pendingAckCount = 0;
  private opChain: Promise<void> = Promise.resolve();
  /** Next segment sequence number (monotonic). */
  private nextSeg = 1;

  constructor(
    namespace: string,
    dataDir?: string,
    options?: WALStorageOptions
  ) {
    this.namespaceDir = resolve(resolveLogbunDir(namespace, dataDir));
    this.dir = join(this.namespaceDir, 'wal');
    this.path = join(this.dir, 'current.aof');
    this.ackPath = join(this.dir, 'acked.ids');
    this.fsync = options?.fsync ?? true;
    this.compactAckThreshold = options?.compactAckThreshold ?? 256;
    this.maxBytes =
      options?.maxBytes ??
      options?.maxWalBytes ??
      WAL_SIZE_SOFT_LIMIT_BYTES;
    this.hardMaxBytes = options?.hardMaxBytes !== false;
    // Allow small segmentBytes in tests; production default is 16 MiB.
    this.segmentBytes = Math.max(
      256,
      options?.segmentBytes ?? WAL_SEGMENT_BYTES_DEFAULT
    );
    this.encryptionKey = options?.encryptionKey;
    this.directorySync = options?.directorySync;
  }

  get softMaxBytes(): number {
    return this.maxBytes;
  }

  get walDir(): string {
    return this.dir;
  }

  private runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.opChain.then(fn, fn);
    this.opChain = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  private noFollowFlag(): number {
    if (typeof FsConstants.O_NOFOLLOW !== 'number') {
      throw new Error(
        'WAL secure file operations require O_NOFOLLOW support in this runtime',
      );
    }
    return FsConstants.O_NOFOLLOW;
  }

  private async syncDirectory(reason: WALDirectorySyncReason): Promise<void> {
    if (!this.fsync) return;
    try {
      if (this.directorySync) {
        await this.directorySync(this.dir, reason);
      } else {
        const fh = await open(this.dir, FsConstants.O_RDONLY);
        try {
          await fh.sync();
        } finally {
          await fh.close();
        }
      }
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (
        WAL_DIRECTORY_FSYNC_UNSUPPORTED_CODES.includes(
          code as (typeof WAL_DIRECTORY_FSYNC_UNSUPPORTED_CODES)[number],
        )
      ) {
        this.pendingDirectorySync = null;
        return;
      }
      throw error;
    }
    this.pendingDirectorySync = null;
  }

  private markDirectoryMutation(reason: WALDirectorySyncReason): void {
    if (this.fsync) this.pendingDirectorySync = reason;
  }

  private async retryPendingDirectorySync(): Promise<void> {
    if (this.pendingDirectorySync) {
      await this.syncDirectory(this.pendingDirectorySync);
    }
  }

  private async assertSecureDirectory(): Promise<void> {
    await assertNoSymlinkPath(this.namespaceDir, 'WAL namespace path');
    await assertNoSymlinkPath(this.dir, 'WAL directory path');
    const info = await lstat(this.dir);
    if (info.isSymbolicLink()) {
      throw new Error('WAL directory must not be a symbolic link');
    }
    if (!info.isDirectory()) {
      throw new Error('WAL storage path is not a directory');
    }
    const physical = await realpath(this.dir);
    if (physical !== resolve(this.dir)) {
      throw new Error('WAL directory contains a symbolic link segment');
    }
  }

  private async assertSecureFile(filePath: string): Promise<void> {
    await this.assertSecureDirectory();
    const info = await lstat(filePath);
    if (info.isSymbolicLink()) {
      throw new Error('WAL file must not be a symbolic link');
    }
    if (!info.isFile()) {
      throw new Error('WAL entry is not a regular file');
    }
    const physical = await realpath(filePath);
    const rel = relative(await realpath(this.dir), physical);
    if (rel.startsWith('..') || isAbsolute(rel)) {
      throw new Error('WAL file escaped its storage directory');
    }
  }

  private async secureFileExists(filePath: string): Promise<boolean> {
    try {
      await lstat(filePath);
      await this.assertSecureFile(filePath);
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false;
      throw error;
    }
  }

  private async createSecureFile(filePath: string): Promise<void> {
    await this.assertSecureDirectory();
    const fh = await open(
      filePath,
      FsConstants.O_WRONLY |
        FsConstants.O_CREAT |
        FsConstants.O_EXCL |
        this.noFollowFlag(),
      0o600,
    );
    await fh.close();
  }

  private async ensureSecureFile(filePath: string): Promise<boolean> {
    if (await this.secureFileExists(filePath)) return false;
    let created = false;
    try {
      await this.createSecureFile(filePath);
      created = true;
    } catch (error) {
      // `instanceLock: false` deliberately permits concurrent initializers.
      // An exclusive-create loser may accept the winner only after validating
      // that it is a confined regular file rather than an attacker symlink.
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
    }
    await this.assertSecureFile(filePath);
    return created;
  }

  private async openSecureFile(filePath: string, flags: number) {
    await this.assertSecureFile(filePath);
    return open(filePath, flags | this.noFollowFlag());
  }

  private async readTextFileSecure(filePath: string): Promise<string> {
    const fh = await this.openSecureFile(filePath, FsConstants.O_RDONLY);
    try {
      return await fh.readFile({ encoding: 'utf8' });
    } finally {
      await fh.close();
    }
  }

  private async appendTextSecure(filePath: string, body: string): Promise<void> {
    const fh = await this.openSecureFile(
      filePath,
      FsConstants.O_WRONLY | FsConstants.O_APPEND,
    );
    try {
      await fh.writeFile(body, 'utf8');
      if (this.fsync) await fh.sync();
    } finally {
      await fh.close();
    }
  }

  async init(): Promise<void> {
    await assertNoSymlinkPath(this.namespaceDir, 'WAL namespace path');
    await assertNoSymlinkPath(this.dir, 'WAL directory path');
    await mkdir(this.dir, { recursive: true });
    await this.assertSecureDirectory();
    this.markDirectoryMutation('initialize');
    await this.ensureSecureFile(this.path);
    const ackExisted = await this.secureFileExists(this.ackPath);
    await this.ensureSecureFile(this.ackPath);
    // Sync on every init so a retry or fresh instance settles directory-entry
    // mutations left behind by an earlier unexpected directory-sync failure.
    await this.syncDirectory('initialize');
    if (ackExisted) {
      const ackText = await this.readTextFileSecure(this.ackPath);
      this.pendingAckCount = ackText
        .split('\n')
        .filter((l) => l.trim().length > 0).length;
    }
    // Discover highest sealed segment index. Security errors are fatal.
    const entries = await readdir(this.dir);
    let max = 0;
    for (const e of entries) {
      const m = /^seg-(\d+)\.aof$/.exec(e);
      if (m) {
        const n = parseInt(m[1]!, 10);
        const segmentPath = join(this.dir, e);
        await this.assertSecureFile(segmentPath);
        if (n > max) max = n;
      }
    }
    this.nextSeg = max + 1;
    this.ready = true;
  }

  /**
   * Append a log line.
   * @throws Error with message containing `wal_full` when hard max is enabled
   *         and total on-disk size is already >= maxBytes.
   */
  async append(log: LogbunLog): Promise<void> {
    return this.runExclusive(async () => {
      if (!this.ready) {
        throw new Error('WAL not initialized — call init() first');
      }
      await this.retryPendingDirectorySync();

      const encodedLine = await this.encodeLogLine(log);
      // Include the separator written by appendFile.  Check after serializing
      // (and encrypting) so hard limits apply to real bytes on disk.
      const appendBytes = new TextEncoder().encode(`${encodedLine}\n`).byteLength;
      const total = await this.sizeOfAllUnlocked();
      if (this.hardMaxBytes && total + appendBytes > this.maxBytes) {
        throw new Error(
          `wal_full: WAL size ${total} + append ${appendBytes} > maxBytes ${this.maxBytes}`
        );
      }

      // Rotate current into a sealed segment when large enough
      const currentSize = await this.sizeOfFileUnlocked(this.path);
      if (currentSize >= this.segmentBytes) {
        await this.rotateCurrentUnlocked();
      }

      await this.appendTextSecure(this.path, encodedLine + '\n');
    });
  }

  async readAll(): Promise<LogbunLog[]> {
    const result = await this.readAllBounded();
    return result.logs;
  }

  async readAllBounded(
    maxLogsOrOpts?: number | WALReadBoundedOptions
  ): Promise<WALReadBoundedResult> {
    let maxLogs: number | undefined;
    let maxBytes: number | undefined;
    if (typeof maxLogsOrOpts === 'number') {
      maxLogs = maxLogsOrOpts;
    } else if (maxLogsOrOpts && typeof maxLogsOrOpts === 'object') {
      maxLogs = maxLogsOrOpts.maxLogs;
      maxBytes =
        typeof maxLogsOrOpts.maxBytes === 'number' &&
        Number.isFinite(maxLogsOrOpts.maxBytes)
          ? Math.max(0, maxLogsOrOpts.maxBytes)
          : undefined;
    }
    return this.runExclusive(() => this.readPendingBoundedUnlocked(maxLogs, maxBytes));
  }

  async approximateSize(): Promise<number> {
    return this.sizeOfAllUnlocked();
  }

  async acknowledge(ids: string[]): Promise<void> {
    if (ids.length === 0) return;

    return this.runExclusive(async () => {
      await this.retryPendingDirectorySync();
      const unique = [...new Set(ids)];
      const body = unique.map((id) => id + '\n').join('');
      await this.appendTextSecure(this.ackPath, body);
      this.pendingAckCount += unique.length;

      if (this.pendingAckCount >= this.compactAckThreshold) {
        await this.compactUnlocked();
      }
    });
  }

  /**
   * Compact segment-by-segment (peak memory ~ one segment of unacked logs).
   * Never deletes unacked entries.
   */
  async compact(): Promise<void> {
    return this.runExclusive(async () => {
      await this.retryPendingDirectorySync();
      await this.compactUnlocked();
    });
  }

  /** @deprecated Prefer {@link compact}. */
  async truncate(): Promise<void> {
    return this.runExclusive(async () => {
      await this.retryPendingDirectorySync();
      const pending = await this.readPendingUnlocked();
      if (pending.length === 0) {
        await this.clearAllSegmentsUnlocked();
        await this.rewriteText(this.ackPath, '');
        this.pendingAckCount = 0;
      } else {
        await this.compactUnlocked();
      }
    });
  }

  async close(): Promise<void> {
    return this.runExclusive(async () => {
      try {
        await this.compactUnlocked();
      } catch {
        /* best-effort */
      }
      this.ready = false;
    });
  }

  private async rotateCurrentUnlocked(): Promise<void> {
    const size = await this.sizeOfFileUnlocked(this.path);
    if (size === 0) return;
    const segName = `seg-${String(this.nextSeg).padStart(6, '0')}.aof`;
    const segPath = join(this.dir, segName);
    this.nextSeg++;
    await this.assertSecureFile(this.path);
    if (await this.secureFileExists(segPath)) {
      throw new Error(`WAL segment collision: ${segName}`);
    }
    this.markDirectoryMutation('rotate');
    await rename(this.path, segPath);
    await this.createSecureFile(this.path);
    await this.syncDirectory('rotate');
  }

  private async listSegmentPathsUnlocked(): Promise<string[]> {
    await this.assertSecureDirectory();
    let entries: string[];
    try {
      entries = await readdir(this.dir);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return [];
      throw error;
    }
    const paths = entries
      .filter((e) => /^seg-\d+\.aof$/.test(e))
      .sort()
      .map((e) => join(this.dir, e));
    for (const path of paths) await this.assertSecureFile(path);
    return paths;
  }

  private async aofPathsInOrderUnlocked(): Promise<string[]> {
    const segs = await this.listSegmentPathsUnlocked();
    return [...segs, this.path];
  }

  private async compactUnlocked(): Promise<void> {
    const acked = await this.readAckedIdsUnlocked();
    const paths = await this.aofPathsInOrderUnlocked();
    const survivors: LogbunLog[] = [];

    // Stream each file; keep only unacked. Peak = survivors array (still O(unacked)).
    // Prefer rewriting per-segment when survivor count is large: rewrite each path alone.
    for (const p of paths) {
      const kept: LogbunLog[] = [];
      await this.forEachLogLineUnlocked(p, (log) => {
        if (!acked.has(log.id)) kept.push(log);
      });
      if (p === this.path) {
        survivors.push(...kept);
      } else if (kept.length === 0) {
        try {
          await this.assertSecureFile(p);
          this.markDirectoryMutation('delete-segment');
          await unlink(p);
          await this.syncDirectory('delete-segment');
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
        }
      } else {
        // Rewrite sealed segment in place (bounded to that segment's unacked set)
        await this.rewrite(p, kept);
      }
    }

    // Collapse remaining current + optionally merge small segs: rewrite current only
    await this.rewrite(this.path, survivors);
    // After compact, sealed segs that still exist are already filtered; current holds
    // only what was in current. Optionally absorb tiny sealed segs into current later.

    await this.rewriteText(this.ackPath, '');
    this.pendingAckCount = 0;
  }

  private async clearAllSegmentsUnlocked(): Promise<void> {
    const segs = await this.listSegmentPathsUnlocked();
    for (const p of segs) {
      try {
        await this.assertSecureFile(p);
        this.markDirectoryMutation('delete-segment');
        await unlink(p);
        await this.syncDirectory('delete-segment');
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      }
    }
    await this.rewriteText(this.path, '');
  }

  private async readPendingUnlocked(): Promise<LogbunLog[]> {
    const { logs } = await this.readPendingBoundedUnlocked();
    return logs;
  }

  private async readPendingBoundedUnlocked(
    maxLogs?: number,
    maxBytes?: number
  ): Promise<WALReadBoundedResult> {
    const approxBytes = await this.sizeOfAllUnlocked();
    const acked = await this.readAckedIdsUnlocked();
    const logs: LogbunLog[] = [];
    let truncated = false;
    const hasCap =
      typeof maxLogs === 'number' && Number.isFinite(maxLogs) && maxLogs >= 0;
    const hasByteCap =
      typeof maxBytes === 'number' && Number.isFinite(maxBytes) && maxBytes >= 0;
    let readBytes = 0;

    const paths = await this.aofPathsInOrderUnlocked();
    outer: for (const p of paths) {
      const stop = await this.forEachLogLineUnlocked(p, (log, encodedBytes) => {
        if (acked.has(log.id)) return 'continue';
        if (hasCap && logs.length >= (maxLogs as number)) {
          truncated = true;
          return 'stop';
        }
        // Strict bound: leave an oversized record unread/unacked so callers
        // can retry with a larger (or absent) byte limit.
        if (hasByteCap && readBytes + encodedBytes > (maxBytes as number)) {
          truncated = true;
          return 'stop';
        }
        logs.push(log);
        readBytes += encodedBytes;
        return 'continue';
      });
      if (stop || truncated) break outer;
    }

    return { logs, truncated, approxBytes };
  }

  /**
   * Stream parse lines from an AOF path.
   * Callback returns `'stop'` to halt early. Returns true if stopped early.
   */
  private async forEachLogLineUnlocked(
    filePath: string,
    onLog: (log: LogbunLog, encodedBytes: number) => 'continue' | 'stop' | void
  ): Promise<boolean> {
    if (!(await this.secureFileExists(filePath))) return false;
    const fh = await this.openSecureFile(filePath, FsConstants.O_RDONLY);
    let carry = Buffer.alloc(0);
    const processLine = async (rawBytes: Buffer, encodedBytes: number) => {
      // Tolerate CRLF migration input while preserving its physical byte count.
      const lineBytes =
        rawBytes.length > 0 && rawBytes[rawBytes.length - 1] === 0x0d
          ? rawBytes.subarray(0, rawBytes.length - 1)
          : rawBytes;
      const rawLine = lineBytes.toString('utf8');
      const trimmed = rawLine.trim();
      if (!trimmed) return false;
      let plain = trimmed;
      try {
        if (this.encryptionKey) {
          plain = await decryptUtf8(trimmed, this.encryptionKey);
        }
        const log = JSON.parse(plain) as LogbunLog;
        if (onLog(log, encodedBytes) === 'stop') return true;
      } catch {
        // Discard malformed / decrypt failures for partial crash lines
      }
      return false;
    };
    try {
      const readBuffer = Buffer.allocUnsafe(64 * 1024);
      for (;;) {
        const { bytesRead } = await fh.read(
          readBuffer,
          0,
          readBuffer.length,
          null,
        );
        if (bytesRead === 0) break;
        const incoming = Buffer.from(readBuffer.subarray(0, bytesRead));
        const data = carry.length > 0 ? Buffer.concat([carry, incoming]) : incoming;
        let start = 0;
        for (;;) {
          const newline = data.indexOf(0x0a, start);
          if (newline < 0) break;
          if (await processLine(data.subarray(start, newline), newline - start + 1)) {
            return true;
          }
          start = newline + 1;
        }
        carry = data.subarray(start);
      }
      // A crash can leave a complete final JSON record without its newline.
      // Count only the bytes actually present, not an imagined separator.
      if (carry.length > 0 && (await processLine(carry, carry.length))) {
        return true;
      }
    } finally {
      await fh.close();
    }
    return false;
  }

  private async sizeOfAllUnlocked(): Promise<number> {
    const paths = await this.aofPathsInOrderUnlocked();
    let total = 0;
    for (const p of paths) {
      total += await this.sizeOfFileUnlocked(p);
    }
    return total;
  }

  private async sizeOfFileUnlocked(filePath: string): Promise<number> {
    try {
      const fh = await this.openSecureFile(filePath, FsConstants.O_RDONLY);
      try {
        const st = await fh.stat();
        return typeof st.size === 'number' && Number.isFinite(st.size) ? st.size : 0;
      } finally {
        await fh.close();
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      return 0;
    }
  }

  private async readAckedIdsUnlocked(): Promise<Set<string>> {
    if (!(await this.secureFileExists(this.ackPath))) return new Set();
    const content = await this.readTextFileSecure(this.ackPath);
    if (!content.trim()) return new Set();
    const set = new Set<string>();
    for (const line of content.split('\n')) {
      const id = line.trim();
      if (id) set.add(id);
    }
    return set;
  }

  private async rewrite(path: string, logs: LogbunLog[]): Promise<void> {
    const lines: string[] = [];
    for (const l of logs) {
      lines.push(await this.encodeLogLine(l));
    }
    const body = lines.length === 0 ? '' : lines.join('\n') + '\n';
    await this.rewriteText(path, body);
  }

  private async rewriteText(path: string, body: string): Promise<void> {
    await this.assertSecureFile(path);
    let tempPath: string | null = null;
    try {
      for (let attempt = 0; attempt < 3; attempt++) {
        const candidate = join(
          this.dir,
          `.${basename(path)}.${randomUUIDv7()}.tmp`,
        );
        try {
          const fh = await open(
            candidate,
            FsConstants.O_WRONLY |
              FsConstants.O_CREAT |
              FsConstants.O_EXCL |
              this.noFollowFlag(),
            0o600,
          );
          try {
            await fh.writeFile(body, 'utf8');
            if (this.fsync) await fh.sync();
          } finally {
            await fh.close();
          }
          tempPath = candidate;
          break;
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
        }
      }
      if (!tempPath) throw new Error('WAL could not create a unique temporary file');
      await this.assertSecureFile(path);
      this.markDirectoryMutation('rewrite');
      await rename(tempPath, path);
      await this.syncDirectory('rewrite');
      tempPath = null;
    } finally {
      if (tempPath) await unlink(tempPath).catch(() => undefined);
    }
  }

  private async encodeLogLine(log: LogbunLog): Promise<string> {
    const plain = JSON.stringify(log);
    return this.encryptionKey
      ? encryptUtf8(plain, this.encryptionKey)
      : plain;
  }

}
