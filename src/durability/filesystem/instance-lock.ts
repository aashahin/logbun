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
 * Deno: requires path-scoped `--allow-read` / `--allow-write` on the data
 * directory; `--allow-run` is not required because unknown PID-probe failures
 * are treated as potentially alive.
 */

import { open as fsOpen, unlink, mkdir, lstat } from 'node:fs/promises';
import type { FileHandle } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { resolveLogbunDir } from './path';

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
  /** @internal Barrier after ESRCH and before atomic stale-recovery claim. */
  afterStaleProbe?: () => void | Promise<void>;
  /** @internal Adversarial replacement seam after identity check, before unlink. */
  beforeOwnedUnlink?: (path: string) => void | Promise<void>;
}

interface LockIdentity {
  dev: bigint;
  ino: bigint;
}

interface RecoveryClaim {
  handle: FileHandle;
  identity: LockIdentity;
}

function pidAlive(
  pid: number,
  killProcess: NonNullable<InstanceLockOptions['killProcess']>,
): boolean {
  if (!Number.isFinite(pid) || pid <= 0) return false;
  try {
    killProcess(pid, 0);
    return true;
  } catch (err) {
    const code =
      err && typeof err === 'object' && 'code' in err
        ? (err as { code?: string }).code
        : undefined;
    return code !== 'ESRCH';
  }
}

function parseLockPid(raw: string): number | null {
  const line = raw.trim().split(/\r?\n/)[0] ?? '';
  const pid = parseInt(line, 10);
  return Number.isFinite(pid) && pid > 0 ? pid : null;
}

function processStartTimeMs(): number {
  return Math.floor(Date.now() - process.uptime() * 1000);
}

export class InstanceLock {
  private readonly path: string;
  private readonly recoveryPath: string;
  private readonly killProcess: NonNullable<InstanceLockOptions['killProcess']>;
  private readonly writeMetadata: NonNullable<InstanceLockOptions['writeMetadata']>;
  private readonly afterStaleProbe?: InstanceLockOptions['afterStaleProbe'];
  private readonly beforeOwnedUnlink?: InstanceLockOptions['beforeOwnedUnlink'];
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
    this.afterStaleProbe = options?.afterStaleProbe;
    this.beforeOwnedUnlink = options?.beforeOwnedUnlink;
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

  private async recoveryClaimExists(): Promise<boolean> {
    try {
      await lstat(this.recoveryPath);
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false;
      throw error;
    }
  }

  private async acquireRecoveryClaim(): Promise<RecoveryClaim> {
    let handle: FileHandle;
    try {
      handle = await fsOpen(this.recoveryPath, 'wx');
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'EEXIST') {
        throw new InstanceLockError(
          `instance_lock_held: stale recovery is already in progress for ${this.path}`,
        );
      }
      throw error;
    }
    try {
      const info = await handle.stat({ bigint: true });
      return { handle, identity: { dev: info.dev, ino: info.ino } };
    } catch (error) {
      try {
        await handle.close();
      } catch {
        /* ignore */
      }
      throw error;
    }
  }

  private async releaseRecoveryClaim(claim: RecoveryClaim): Promise<void> {
    try {
      await claim.handle.close();
    } catch {
      /* continue with ownership-checked cleanup */
    }
    await this.cleanupCreatedPath(this.recoveryPath, claim.identity);
  }

  private async readOwner(): Promise<{ pid: number | null; identity: LockIdentity }> {
    const handle = await fsOpen(this.path, 'r');
    try {
      const info = await handle.stat({ bigint: true });
      const raw = await handle.readFile({ encoding: 'utf8' });
      return {
        pid: parseLockPid(raw),
        identity: { dev: info.dev, ino: info.ino },
      };
    } finally {
      await handle.close();
    }
  }

  async acquire(): Promise<void> {
    if (this.handle) return;

    const createdDirectoryStart = await mkdir(dirname(this.path), { recursive: true });
    this.createdDirectoryStart ??= createdDirectoryStart;
    let recoveryClaim: RecoveryClaim | null = null;

    try {
      for (let attempt = 0; attempt < 3; attempt++) {
        let createdHandle: FileHandle;
        try {
          createdHandle = await fsOpen(this.path, 'wx');
        } catch (err) {
          const code =
            err && typeof err === 'object' && 'code' in err
              ? (err as { code?: string }).code
              : undefined;
          if (code !== 'EEXIST') {
            throw err instanceof Error
              ? err
              : new Error(`instance lock acquire failed: ${String(err)}`);
          }
          if (recoveryClaim) {
            await Promise.resolve();
            continue;
          }

          let owner: { pid: number | null; identity: LockIdentity };
          try {
            owner = await this.readOwner();
          } catch (error) {
            if ((error as NodeJS.ErrnoException).code === 'ENOENT') continue;
            throw new InstanceLockError(
              `instance_lock_held: cannot verify the owner of ${this.path}; refusing to steal it`,
            );
          }
          if (owner.pid === null) {
            throw new InstanceLockError(
              `instance_lock_held: invalid owner metadata in ${this.path}; refusing to steal it`,
            );
          }
          if (pidAlive(owner.pid, this.killProcess)) {
            throw new InstanceLockError(
              `instance_lock_held: another process (pid ${owner.pid}) holds ${this.path}. ` +
                `Use a unique namespace per replica, or ensure the previous process shut down cleanly.`,
            );
          }

          await this.afterStaleProbe?.();
          recoveryClaim = await this.acquireRecoveryClaim();
          const removed = await this.removePathIfSame(this.path, owner.identity);
          if (!removed) {
            throw new InstanceLockError(
              `instance_lock_held: owner changed during stale recovery for ${this.path}`,
            );
          }
          continue;
        }

        let identity: LockIdentity | null = null;
        try {
          const createdInfo = await createdHandle.stat({ bigint: true });
          identity = { dev: createdInfo.dev, ino: createdInfo.ino };
          if (!recoveryClaim && await this.recoveryClaimExists()) {
            throw new InstanceLockError(
              `instance_lock_held: stale recovery is in progress for ${this.path}`,
            );
          }
          await this.writeMetadata(
            createdHandle,
            `${process.pid}\n${processStartTimeMs()}\n`,
          );
          this.handle = createdHandle;
          this.ownedIdentity = identity;
          return;
        } catch (error) {
          try {
            await createdHandle.close();
          } catch {
            /* continue with ownership-checked cleanup */
          }
          if (identity) await this.cleanupCreatedPath(this.path, identity);
          throw error;
        }
      }
    } finally {
      if (recoveryClaim) await this.releaseRecoveryClaim(recoveryClaim);
    }

    throw new InstanceLockError(
      `instance_lock_held: could not acquire ${this.path} after retries`
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
