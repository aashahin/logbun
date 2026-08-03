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
 *
 * Deno: requires `--allow-read` / `--allow-write` on the data directory.
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

function pidAlive(pid: number): boolean {
  if (!Number.isFinite(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    const code =
      err && typeof err === 'object' && 'code' in err
        ? (err as { code?: string }).code
        : undefined;
    if (code === 'EPERM') return true;
    return false;
  }
}

function parseLockPid(raw: string): number {
  const line = raw.trim().split(/\r?\n/)[0] ?? '';
  const pid = parseInt(line, 10);
  return Number.isFinite(pid) ? pid : 0;
}

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
      /* already gone */
    }
  }
}
