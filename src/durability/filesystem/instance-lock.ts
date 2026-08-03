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

import { open as fsOpen, unlink, readFile, mkdir } from 'node:fs/promises';
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
  private readonly killProcess: NonNullable<InstanceLockOptions['killProcess']>;
  private handle: FileHandle | null = null;

  constructor(
    namespace: string,
    dataDir?: string,
    options?: InstanceLockOptions,
  ) {
    const root = resolveLogbunDir(namespace, dataDir);
    this.path = join(root, '.instance.lock');
    this.killProcess = options?.killProcess ?? ((pid, signal) => process.kill(pid, signal));
  }

  get lockPath(): string {
    return this.path;
  }

  async acquire(): Promise<void> {
    if (this.handle) return;

    await mkdir(dirname(this.path), { recursive: true });

    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        this.handle = await fsOpen(this.path, 'wx');
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

        let holderPid: number | null;
        try {
          const raw = await readFile(this.path, 'utf8');
          holderPid = parseLockPid(raw);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === 'ENOENT') continue;
          throw new InstanceLockError(
            `instance_lock_held: cannot verify the owner of ${this.path}; refusing to steal it`,
          );
        }

        if (holderPid === null) {
          throw new InstanceLockError(
            `instance_lock_held: invalid owner metadata in ${this.path}; refusing to steal it`,
          );
        }

        if (pidAlive(holderPid, this.killProcess)) {
          throw new InstanceLockError(
            `instance_lock_held: another process (pid ${holderPid}) holds ${this.path}. ` +
              `Use a unique namespace per replica, or ensure the previous process shut down cleanly.`
          );
        }

        try {
          await unlink(this.path);
        } catch {
          /* race */
        }
      }
    }

    throw new InstanceLockError(
      `instance_lock_held: could not acquire ${this.path} after retries`
    );
  }

  async release(): Promise<void> {
    if (!this.handle) return;
    const ownedHandle = this.handle;
    this.handle = null;
    try {
      await ownedHandle.close();
    } catch {
      /* continue with ownership-checked cleanup */
    }
    try {
      const raw = await readFile(this.path, 'utf8');
      const holderPid = parseLockPid(raw);
      if (holderPid !== process.pid) {
        return;
      }
      await unlink(this.path);
    } catch {
      /* already gone */
    }
  }
}
