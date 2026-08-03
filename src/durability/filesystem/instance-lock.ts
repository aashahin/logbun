/**
 * Exclusive multi-process instance lock for a Logbun namespace data dir.
 *
 * Prevents two processes from sharing the same local WAL/DLQ (undefined behavior).
 * Uses exclusive file create (`wx`) + PID; steals lock only if the recorded PID is dead.
 *
 * Notes / limitations:
 * - **PID-based**: relies on `process.kill(pid, 0)` to detect live holders. PID reuse
 *   is rare in the short steal window but possible; unique namespaces per replica
 *   remain required.
 * - **Not safe on NFS** (or other network filesystems with weak exclusive-create /
 *   cache coherence): use a single local disk, or disable and coordinate externally.
 * - **Not a same-user security boundary**: this coordinates cooperative owners;
 *   a malicious process with directory write access can replace/remove the lock.
 * - Holds the lock file handle open for the process lifetime after acquire.
 *
 * Deno: path-scoped `--allow-read` / `--allow-write` is enough for normal live
 * exclusivity. Automatic stale-owner recovery additionally needs permission to
 * probe the recorded process (currently `--allow-run`); unknown probes fail closed.
 */

import { open as fsOpen, link, unlink, mkdir, lstat, readdir } from 'node:fs/promises';
import type { FileHandle } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { resolveLogbunDir } from './path';
import { randomUUIDv7 } from '../../utils/uuidv7';

export class InstanceLockError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InstanceLockError';
  }
}

export interface InstanceLockOptions {
  /** @internal Deterministic process-liveness seam for tests. */
  killProcess?: (pid: number, signal: 0) => unknown;
  /** @internal Deterministic metadata write/fsync seam for tests. */
  writeMetadata?: (handle: FileHandle, metadata: string) => void | Promise<void>;
  /** @internal Deterministic recovery-claim metadata write/fsync seam. */
  writeRecoveryMetadata?: (
    handle: FileHandle,
    metadata: string,
  ) => void | Promise<void>;
  /** @internal Deterministic main-lock hard-link seam. */
  mainLink?: (stagedPath: string, canonicalPath: string) => Promise<void>;
  /** @internal Barrier after staged main metadata is synced, before publication. */
  beforeMainPublish?: (
    stagedPath: string,
    canonicalPath: string,
  ) => void | Promise<void>;
  /** @internal Barrier after canonical publication, before staging cleanup. */
  afterMainPublish?: (
    stagedPath: string,
    canonicalPath: string,
  ) => void | Promise<void>;
  /** Safety age for malformed legacy claims and partial main staging remnants. */
  recoveryClaimStaleMs?: number;
  /** @internal Deterministic clock seam for legacy-claim aging tests. */
  now?: () => number;
  /** @internal Process-start identity seam; null/throw means unverifiable. */
  readProcessStartTimeMs?: (
    pid: number,
  ) => number | null | Promise<number | null>;
  /** @internal Barrier after ESRCH and before atomic stale-recovery claim. */
  afterStaleProbe?: () => void | Promise<void>;
  /** @internal Adversarial replacement seam after identity check, before unlink. */
  beforeOwnedUnlink?: (path: string) => void | Promise<void>;
  /** @internal Barrier after the final identity check, immediately before unlink. */
  afterOwnedUnlinkCheck?: (path: string) => void | Promise<void>;
  /** @internal Barrier after a stale main lock has been removed under a claim. */
  afterStaleMainRemoved?: () => void | Promise<void>;
}

interface LockIdentity {
  dev: bigint;
  ino: bigint;
}

interface RecoveryClaim {
  handle: FileHandle;
  identity: LockIdentity;
}

interface LockOwnerMetadata {
  pid: number;
  processStartTimeMs: number | null;
}

interface RecoveryClaimMetadata {
  v: 1;
  pid: number;
  processStartTimeMs: number;
}

const MAIN_STAGE_NAME_RE = /^\.instance\.lock\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.tmp$/i;

function probePidLiveness(
  pid: number,
  killProcess: NonNullable<InstanceLockOptions['killProcess']>,
): 'alive' | 'dead' | 'unknown' {
  if (!Number.isFinite(pid) || pid <= 0) return 'dead';
  try {
    killProcess(pid, 0);
    return 'alive';
  } catch (err) {
    const code =
      err && typeof err === 'object' && 'code' in err
        ? (err as { code?: string }).code
        : undefined;
    return code === 'ESRCH' ? 'dead' : 'unknown';
  }
}

async function readLinuxProcessStartTimeMs(pid: number): Promise<number | null> {
  if (process.platform !== 'linux') return null;
  try {
    // Linux procfs timestamps the per-process directory at process creation.
    // Runtimes without permission to inspect /proc fall through to fail-closed.
    const info = await lstat(`/proc/${pid}`);
    return Number.isFinite(info.ctimeMs) ? info.ctimeMs : null;
  } catch {
    return null;
  }
}

function parseLockPid(raw: string): number | null {
  const line = raw.trim().split(/\r?\n/)[0] ?? '';
  const pid = parseInt(line, 10);
  return Number.isFinite(pid) && pid > 0 ? pid : null;
}

function parseLockOwner(raw: string): LockOwnerMetadata | null {
  const lines = raw.trim().split(/\r?\n/);
  const pid = parseLockPid(raw);
  if (pid === null) return null;
  const start = Number(lines[1]);
  return {
    pid,
    processStartTimeMs: Number.isFinite(start) && start > 0 ? start : null,
  };
}

function parseRecoveryClaim(raw: string): RecoveryClaimMetadata | null {
  try {
    const parsed = JSON.parse(raw) as Partial<RecoveryClaimMetadata>;
    if (
      parsed.v !== 1 ||
      !Number.isFinite(parsed.pid) ||
      parsed.pid! <= 0 ||
      !Number.isFinite(parsed.processStartTimeMs) ||
      parsed.processStartTimeMs! < 0
    ) {
      return null;
    }
    return {
      v: 1,
      pid: parsed.pid!,
      processStartTimeMs: parsed.processStartTimeMs!,
    };
  } catch {
    return null;
  }
}

function processStartTimeMs(): number {
  return Math.floor(Date.now() - process.uptime() * 1000);
}

const CURRENT_PROCESS_START_TIME_MS = processStartTimeMs();

export class InstanceLock {
  private readonly path: string;
  private readonly recoveryPath: string;
  private readonly killProcess: NonNullable<InstanceLockOptions['killProcess']>;
  private readonly writeMetadata: NonNullable<InstanceLockOptions['writeMetadata']>;
  private readonly writeRecoveryMetadata: NonNullable<
    InstanceLockOptions['writeRecoveryMetadata']
  >;
  private readonly mainLink: NonNullable<InstanceLockOptions['mainLink']>;
  private readonly beforeMainPublish?: InstanceLockOptions['beforeMainPublish'];
  private readonly afterMainPublish?: InstanceLockOptions['afterMainPublish'];
  private readonly recoveryClaimStaleMs: number;
  private readonly now: NonNullable<InstanceLockOptions['now']>;
  private readonly readProcessStartTimeMs: NonNullable<
    InstanceLockOptions['readProcessStartTimeMs']
  >;
  private readonly afterStaleProbe?: InstanceLockOptions['afterStaleProbe'];
  private readonly beforeOwnedUnlink?: InstanceLockOptions['beforeOwnedUnlink'];
  private readonly afterOwnedUnlinkCheck?: InstanceLockOptions['afterOwnedUnlinkCheck'];
  private readonly afterStaleMainRemoved?: InstanceLockOptions['afterStaleMainRemoved'];
  private handle: FileHandle | null = null;
  private ownedIdentity: LockIdentity | null = null;
  private createdDirectoryStart: string | undefined;

  constructor(
    namespace: string,
    dataDir?: string,
    options?: InstanceLockOptions,
  ) {
    const root = resolveLogbunDir(namespace, dataDir);
    this.path = join(root, '.instance.lock');
    this.recoveryPath = join(root, '.instance.lock.recovery');
    this.killProcess = options?.killProcess ?? ((pid, signal) => process.kill(pid, signal));
    this.writeMetadata = options?.writeMetadata ?? (async (handle, metadata) => {
      await handle.writeFile(metadata, 'utf8');
      await handle.sync();
    });
    this.writeRecoveryMetadata = options?.writeRecoveryMetadata ?? (async (handle, metadata) => {
      await handle.writeFile(metadata, 'utf8');
      await handle.sync();
    });
    this.mainLink = options?.mainLink ?? link;
    this.beforeMainPublish = options?.beforeMainPublish;
    this.afterMainPublish = options?.afterMainPublish;
    this.recoveryClaimStaleMs = Math.max(0, options?.recoveryClaimStaleMs ?? 60_000);
    this.now = options?.now ?? Date.now;
    this.readProcessStartTimeMs =
      options?.readProcessStartTimeMs ?? readLinuxProcessStartTimeMs;
    this.afterStaleProbe = options?.afterStaleProbe;
    this.beforeOwnedUnlink = options?.beforeOwnedUnlink;
    this.afterOwnedUnlinkCheck = options?.afterOwnedUnlinkCheck;
    this.afterStaleMainRemoved = options?.afterStaleMainRemoved;
  }

  get lockPath(): string {
    return this.path;
  }

  /** @internal Earliest directory created while preparing the lock path. */
  get createdHierarchyStart(): string | undefined {
    return this.createdDirectoryStart;
  }

  private async removePathIfSame(path: string, identity: LockIdentity): Promise<boolean> {
    try {
      const current = await lstat(path, { bigint: true });
      if (current.dev !== identity.dev || current.ino !== identity.ino) return false;
      if (path === this.path) await this.beforeOwnedUnlink?.(path);
      // Revalidate at the last portable seam before unlink. Stale recovery is
      // serialized by recoveryPath, so cooperative contenders cannot replace
      // the entry after this check. A hostile same-user writer can still race
      // the final path-based syscall; portable Node-compatible APIs expose no
      // compare-and-unlink primitive, as documented in the threat model.
      const confirmed = await lstat(path, { bigint: true });
      if (confirmed.dev !== identity.dev || confirmed.ino !== identity.ino) return false;
      if (path === this.path) await this.afterOwnedUnlinkCheck?.(path);
      await unlink(path);
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false;
      throw error;
    }
  }

  private async cleanupCreatedPath(path: string, identity: LockIdentity): Promise<void> {
    try {
      await this.removePathIfSame(path, identity);
    } catch {
      // Cleanup must never mask the original acquisition error.
    }
  }

  /**
   * Staging paths never confer namespace ownership. Retain recent/possibly-live
   * writers, but ownership-clean complete dead stages and aged partial remnants.
   */
  private async cleanupAbandonedMainStages(): Promise<void> {
    let names: string[];
    try {
      names = await readdir(dirname(this.path));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return;
      throw error;
    }

    for (const name of names) {
      if (!MAIN_STAGE_NAME_RE.test(name)) continue;
      const stagedPath = join(dirname(this.path), name);
      let handle: FileHandle | null = null;
      let info: { dev: bigint; ino: bigint; mtimeMs: bigint };
      let raw: string;
      try {
        handle = await fsOpen(stagedPath, 'r');
        info = await handle.stat({ bigint: true });
        raw = await handle.readFile({ encoding: 'utf8' });
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
        continue;
      } finally {
        await handle?.close().catch(() => undefined);
      }
      const parsed = parseLockOwner(raw);
      // Main staging always writes both lines. A PID-only value can be a
      // concurrently written prefix and is therefore an age-gated partial.
      const complete = parsed?.processStartTimeMs != null ? parsed : null;
      const mayRemove = complete
        ? !await this.isOwnerAlive(complete)
        : this.now() - Number(info.mtimeMs) >= this.recoveryClaimStaleMs;
      if (mayRemove) {
        await this.removePathIfSame(stagedPath, {
          dev: info.dev,
          ino: info.ino,
        });
      }
    }
  }

  private async recoveryClaimExists(): Promise<boolean> {
    try {
      await lstat(this.recoveryPath);
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false;
      throw error;
    }
  }

  private async isOwnerAlive(owner: LockOwnerMetadata): Promise<boolean> {
    if (
      owner.pid === process.pid &&
      owner.processStartTimeMs === CURRENT_PROCESS_START_TIME_MS
    ) {
      return true;
    }
    const liveness = probePidLiveness(owner.pid, this.killProcess);
    if (liveness === 'dead') return false;
    if (liveness === 'unknown' || owner.processStartTimeMs === null) return true;
    try {
      const observedStart = await this.readProcessStartTimeMs(owner.pid);
      if (observedStart === null || !Number.isFinite(observedStart)) return true;
      // Wall-clock process starts derived by different runtimes/filesystems can
      // differ slightly. A material mismatch identifies PID reuse.
      return Math.abs(observedStart - owner.processStartTimeMs) <= 5_000;
    } catch {
      return true;
    }
  }

  private async assertRecoveryClaimOwned(claim: RecoveryClaim): Promise<void> {
    try {
      const current = await lstat(this.recoveryPath, { bigint: true });
      if (current.dev !== claim.identity.dev || current.ino !== claim.identity.ino) {
        throw new InstanceLockError(
          `instance_lock_held: recovery ownership changed for ${this.path}`,
        );
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        throw new InstanceLockError(
          `instance_lock_held: recovery ownership was lost for ${this.path}`,
        );
      }
      throw error;
    }
  }

  private async publishRecoveryClaim(): Promise<RecoveryClaim> {
    const tempPath = `${this.recoveryPath}.${randomUUIDv7()}.tmp`;
    let handle: FileHandle | null = null;
    let identity: LockIdentity | null = null;
    let published = false;
    try {
      handle = await fsOpen(tempPath, 'wx');
      const info = await handle.stat({ bigint: true });
      identity = { dev: info.dev, ino: info.ino };
      const metadata: RecoveryClaimMetadata = {
        v: 1,
        pid: process.pid,
        processStartTimeMs: CURRENT_PROCESS_START_TIME_MS,
      };
      await this.writeRecoveryMetadata(handle, `${JSON.stringify(metadata)}\n`);
      const tempInfo = await lstat(tempPath, { bigint: true });
      if (tempInfo.dev !== identity.dev || tempInfo.ino !== identity.ino) {
        throw new InstanceLockError(
          `instance_lock_held: recovery claim staging changed for ${this.path}`,
        );
      }
      // A hard-link publication has no empty/partial destination window: the
      // recovery path either does not exist or names the fully synced inode.
      await link(tempPath, this.recoveryPath);
      published = true;
      const claim: RecoveryClaim = { handle, identity };
      await this.assertRecoveryClaimOwned(claim);
      await this.removePathIfSame(tempPath, identity);
      return claim;
    } catch (error) {
      if (handle) await handle.close().catch(() => undefined);
      if (identity) {
        await this.cleanupCreatedPath(tempPath, identity);
        if (published) await this.cleanupCreatedPath(this.recoveryPath, identity);
      }
      throw error;
    }
  }

  private async readRecoveryOwner(): Promise<{
    metadata: RecoveryClaimMetadata | null;
    identity: LockIdentity;
    mtimeMs: number;
  }> {
    const handle = await fsOpen(this.recoveryPath, 'r');
    try {
      const info = await handle.stat({ bigint: true });
      const raw = await handle.readFile({ encoding: 'utf8' });
      return {
        metadata: parseRecoveryClaim(raw),
        identity: { dev: info.dev, ino: info.ino },
        mtimeMs: Number(info.mtimeMs),
      };
    } finally {
      await handle.close();
    }
  }

  private async acquireRecoveryClaim(): Promise<RecoveryClaim> {
    for (let attempt = 0; attempt < 8; attempt++) {
      try {
        return await this.publishRecoveryClaim();
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
      }

      let owner: Awaited<ReturnType<InstanceLock['readRecoveryOwner']>>;
      try {
        owner = await this.readRecoveryOwner();
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') continue;
        throw new InstanceLockError(
          `instance_lock_held: cannot verify recovery owner for ${this.path}; refusing to steal it`,
        );
      }

      if (owner.metadata) {
        if (await this.isOwnerAlive(owner.metadata)) {
          throw new InstanceLockError(
            `instance_lock_held: stale recovery is already in progress for ${this.path}`,
          );
        }
      } else {
        const claimAgeMs = this.now() - owner.mtimeMs;
        if (!Number.isFinite(claimAgeMs) || claimAgeMs < this.recoveryClaimStaleMs) {
          throw new InstanceLockError(
            `instance_lock_held: recovery metadata is incomplete and may still be active for ${this.path}`,
          );
        }
      }

      if (!await this.removePathIfSame(this.recoveryPath, owner.identity)) {
        continue;
      }
    }

    throw new InstanceLockError(
      `instance_lock_held: could not acquire recovery ownership for ${this.path}`,
    );
  }

  private async releaseRecoveryClaim(claim: RecoveryClaim): Promise<void> {
    try {
      await claim.handle.close();
    } catch {
      /* continue with ownership-checked cleanup */
    }
    await this.cleanupCreatedPath(this.recoveryPath, claim.identity);
  }

  private async readOwner(): Promise<{
    metadata: LockOwnerMetadata | null;
    identity: LockIdentity;
  }> {
    const handle = await fsOpen(this.path, 'r');
    try {
      const info = await handle.stat({ bigint: true });
      const raw = await handle.readFile({ encoding: 'utf8' });
      return {
        metadata: parseLockOwner(raw),
        identity: { dev: info.dev, ino: info.ino },
      };
    } finally {
      await handle.close();
    }
  }

  private async publishMainLock(
    recoveryClaim: RecoveryClaim | null,
  ): Promise<void> {
    const tempPath = `${this.path}.${randomUUIDv7()}.tmp`;
    let createdHandle: FileHandle | null = null;
    let identity: LockIdentity | null = null;
    let published = false;
    try {
      createdHandle = await fsOpen(tempPath, 'wx');
      const createdInfo = await createdHandle.stat({ bigint: true });
      identity = { dev: createdInfo.dev, ino: createdInfo.ino };
      await this.writeMetadata(
        createdHandle,
        `${process.pid}\n${CURRENT_PROCESS_START_TIME_MS}\n`,
      );
      const stagedInfo = await lstat(tempPath, { bigint: true });
      if (stagedInfo.dev !== identity.dev || stagedInfo.ino !== identity.ino) {
        throw new InstanceLockError(
          `instance_lock_held: main lock staging changed for ${this.path}`,
        );
      }
      if (recoveryClaim) {
        await this.assertRecoveryClaimOwned(recoveryClaim);
      } else if (await this.recoveryClaimExists()) {
        throw new InstanceLockError(
          `instance_lock_held: stale recovery is in progress for ${this.path}`,
        );
      }
      await this.beforeMainPublish?.(tempPath, this.path);
      if (recoveryClaim) await this.assertRecoveryClaimOwned(recoveryClaim);
      try {
        await this.mainLink(tempPath, this.path);
      } catch (error) {
        const code = (error as NodeJS.ErrnoException).code;
        if (['EXDEV', 'ENOSYS', 'ENOTSUP', 'EOPNOTSUPP', 'EPERM'].includes(code ?? '')) {
          throw new InstanceLockError(
            `instance_lock_atomic_publication_unsupported: main lock requires same-filesystem hard links (${code ?? 'unknown'})`,
          );
        }
        throw error;
      }
      published = true;
      const canonicalInfo = await lstat(this.path, { bigint: true });
      if (canonicalInfo.dev !== identity.dev || canonicalInfo.ino !== identity.ino) {
        throw new InstanceLockError(
          `instance_lock_held: main lock publication changed for ${this.path}`,
        );
      }
      await this.afterMainPublish?.(tempPath, this.path);
      if (recoveryClaim) {
        await this.assertRecoveryClaimOwned(recoveryClaim);
      } else if (await this.recoveryClaimExists()) {
        throw new InstanceLockError(
          `instance_lock_held: stale recovery started while acquiring ${this.path}`,
        );
      }
      await this.removePathIfSame(tempPath, identity);
      this.handle = createdHandle;
      this.ownedIdentity = identity;
    } catch (error) {
      if (createdHandle) await createdHandle.close().catch(() => undefined);
      if (identity) {
        await this.cleanupCreatedPath(tempPath, identity);
        if (published) await this.cleanupCreatedPath(this.path, identity);
      }
      throw error;
    }
  }

  private async acquireMainUnderClaim(
    claim: RecoveryClaim,
    expectedOwner?: { metadata: LockOwnerMetadata; identity: LockIdentity },
  ): Promise<void> {
    await this.assertRecoveryClaimOwned(claim);
    let owner: Awaited<ReturnType<InstanceLock['readOwner']>> | null = null;
    try {
      owner = await this.readOwner();
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
        throw new InstanceLockError(
          `instance_lock_held: cannot verify the owner of ${this.path}; refusing to steal it`,
        );
      }
    }

    if (owner) {
      if (
        expectedOwner &&
        (owner.identity.dev !== expectedOwner.identity.dev ||
          owner.identity.ino !== expectedOwner.identity.ino)
      ) {
        throw new InstanceLockError(
          `instance_lock_held: owner changed during stale recovery for ${this.path}`,
        );
      }
      if (!owner.metadata) {
        throw new InstanceLockError(
          `instance_lock_held: invalid owner metadata in ${this.path}; refusing to steal it`,
        );
      }
      if (await this.isOwnerAlive(owner.metadata)) {
        throw new InstanceLockError(
          `instance_lock_held: another process (pid ${owner.metadata.pid}) holds ${this.path}. ` +
            `Use a unique namespace per replica, or ensure the previous process shut down cleanly.`,
        );
      }
      if (!expectedOwner) await this.afterStaleProbe?.();
      await this.assertRecoveryClaimOwned(claim);
      const removed = await this.removePathIfSame(this.path, owner.identity);
      if (!removed) {
        throw new InstanceLockError(
          `instance_lock_held: owner changed during stale recovery for ${this.path}`,
        );
      }
      await this.assertRecoveryClaimOwned(claim);
      await this.afterStaleMainRemoved?.();
      await this.assertRecoveryClaimOwned(claim);
    }

    await this.assertRecoveryClaimOwned(claim);
    try {
      await this.publishMainLock(claim);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'EEXIST') {
        throw new InstanceLockError(
          `instance_lock_held: owner changed during stale recovery for ${this.path}`,
        );
      }
      throw error;
    }
  }

  async acquire(): Promise<void> {
    if (this.handle) return;

    const createdDirectoryStart = await mkdir(dirname(this.path), { recursive: true });
    this.createdDirectoryStart ??= createdDirectoryStart;
    await this.cleanupAbandonedMainStages();
    for (let attempt = 0; attempt < 8; attempt++) {
      if (await this.recoveryClaimExists()) {
        const claim = await this.acquireRecoveryClaim();
        try {
          await this.acquireMainUnderClaim(claim);
          return;
        } finally {
          await this.releaseRecoveryClaim(claim);
        }
      }

      try {
        await this.publishMainLock(null);
        return;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'EEXIST') {
          throw error instanceof Error
            ? error
            : new Error(`instance lock acquire failed: ${String(error)}`);
        }

        let owner: Awaited<ReturnType<InstanceLock['readOwner']>>;
        try {
          owner = await this.readOwner();
        } catch (readError) {
          if ((readError as NodeJS.ErrnoException).code === 'ENOENT') continue;
          throw new InstanceLockError(
            `instance_lock_held: cannot verify the owner of ${this.path}; refusing to steal it`,
          );
        }
        if (!owner.metadata) {
          throw new InstanceLockError(
            `instance_lock_held: invalid owner metadata in ${this.path}; refusing to steal it`,
          );
        }
        if (await this.isOwnerAlive(owner.metadata)) {
          throw new InstanceLockError(
            `instance_lock_held: another process (pid ${owner.metadata.pid}) holds ${this.path}. ` +
              `Use a unique namespace per replica, or ensure the previous process shut down cleanly.`,
          );
        }

        await this.afterStaleProbe?.();
        const claim = await this.acquireRecoveryClaim();
        try {
          await this.acquireMainUnderClaim(claim, {
            metadata: owner.metadata,
            identity: owner.identity,
          });
          return;
        } finally {
          await this.releaseRecoveryClaim(claim);
        }
      }
    }

    throw new InstanceLockError(
      `instance_lock_held: could not acquire ${this.path} after retries`,
    );
  }

  async release(): Promise<void> {
    if (!this.handle) return;
    const ownedHandle = this.handle;
    const ownedIdentity = this.ownedIdentity;
    this.handle = null;
    this.ownedIdentity = null;
    try {
      await ownedHandle.close();
    } catch {
      /* continue with ownership-checked cleanup */
    }
    if (!ownedIdentity) return;
    await this.cleanupCreatedPath(this.path, ownedIdentity);
  }
}
