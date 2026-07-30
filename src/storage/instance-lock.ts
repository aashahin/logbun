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
 * - Holds the lock file handle open for the process lifetime after acquire.
 */

import { open as fsOpen, unlink, readFile, mkdir } from 'node:fs/promises';
import type { FileHandle } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { resolveLogbunDir } from '../utils/path';

export class InstanceLockError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InstanceLockError';
  }
}

function pidAlive(pid: number): boolean {
  if (!Number.isFinite(pid) || pid <= 0) return false;
  try {
    // signal 0 — existence check; throws ESRCH if gone
    process.kill(pid, 0);
    return true;
  } catch (err) {
    const code =
      err && typeof err === 'object' && 'code' in err
        ? (err as { code?: string }).code
        : undefined;
    // EPERM: process exists but we cannot signal it — treat as alive
    if (code === 'EPERM') return true;
    return false;
  }
}

/** Parse holder PID from lock file contents (`pid` or `pid\nstartTime`). */
function parseLockPid(raw: string): number {
  const line = raw.trim().split(/\r?\n/)[0] ?? '';
  const pid = parseInt(line, 10);
  return Number.isFinite(pid) ? pid : 0;
}

/**
 * Approximate process start time (ms since epoch) for lock payload.
 * Helps operators distinguish holders; not used for steal decisions.
 */
function processStartTimeMs(): number {
  return Math.floor(Date.now() - process.uptime() * 1000);
}

export class InstanceLock {
  private readonly path: string;
  private handle: FileHandle | null = null;

  constructor(namespace: string, dataDir?: string) {
    const root = resolveLogbunDir(namespace, dataDir);
    this.path = join(root, '.instance.lock');
  }

  get lockPath(): string {
    return this.path;
  }

  /**
   * Acquire exclusive lock. Throws {@link InstanceLockError} if another live
   * process holds it.
   */
  async acquire(): Promise<void> {
    if (this.handle) return;

    await mkdir(dirname(this.path), { recursive: true });

    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        // Exclusive create — fails if file exists; keep handle open for process lifetime
        this.handle = await fsOpen(this.path, 'wx');
        // Format: pid\nstartTime — parseInt of first line remains backward-compatible
        await this.handle.writeFile(
          `${process.pid}\n${processStartTimeMs()}\n`,
          'utf8'
        );
        return;
      } catch (err) {
        this.handle = null;
        const code =
          err && typeof err === 'object' && 'code' in err
            ? (err as { code?: string }).code
            : undefined;
        if (code !== 'EEXIST') {
          throw err instanceof Error
            ? err
            : new Error(`instance lock acquire failed: ${String(err)}`);
        }

        // Stale lock? steal if PID is dead
        let holderPid = 0;
        try {
          const raw = await readFile(this.path, 'utf8');
          holderPid = parseLockPid(raw);
        } catch {
          holderPid = 0;
        }

        if (holderPid > 0 && pidAlive(holderPid)) {
          throw new InstanceLockError(
            `instance_lock_held: another process (pid ${holderPid}) holds ${this.path}. ` +
              `Use a unique namespace per replica, or ensure the previous process shut down cleanly.`
          );
        }

        // Dead or unreadable — remove and retry (only one wx wins after unlink)
        try {
          await unlink(this.path);
        } catch {
          /* race: another process recreated — loop */
        }
      }
    }

    throw new InstanceLockError(
      `instance_lock_held: could not acquire ${this.path} after retries`
    );
  }

  /**
   * Release lock and remove the lock file if we still own it.
   * Idempotent. Only unlinks when the file's recorded PID matches this process
   * (avoids deleting another process's lock after a steal race).
   */
  async release(): Promise<void> {
    if (this.handle) {
      try {
        await this.handle.close();
      } catch {
        /* ignore */
      }
      this.handle = null;
    }
    try {
      const raw = await readFile(this.path, 'utf8');
      const holderPid = parseLockPid(raw);
      if (holderPid !== process.pid) {
        return;
      }
      await unlink(this.path);
    } catch {
      /* already gone or unreadable */
    }
  }
}
