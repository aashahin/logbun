import { makeFileReliability } from './helpers';
import { afterEach, expect, test } from 'bun:test';
import { spawn } from 'node:child_process';
import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  unlink,
  utimes,
  writeFile,
} from 'node:fs/promises';
import type { FileHandle } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';

import { InstanceLock, InstanceLockError } from '../src/durability/filesystem';
import { AuditLogger } from '../src/logger';
import { BunSQLiteAdapter } from '../src/adapters/bun-sqlite';

const cleanupPaths: string[] = [];
const crashFixture = fileURLToPath(
  new URL('./fixtures/instance-lock-crash.ts', import.meta.url),
);

function crashDuringMainPublication(
  phase: 'before' | 'after',
  namespace: string,
  dataDir: string,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [crashFixture, phase, namespace, dataDir], {
      stdio: ['ignore', 'ignore', 'pipe'],
    });
    let stderr = '';
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', (chunk) => { stderr += chunk; });
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (code === 0) {
        reject(new Error(`crash fixture exited successfully:\n${stderr}`));
        return;
      }
      if (signal !== 'SIGKILL' && code !== 137) {
        reject(new Error(`crash fixture exited with ${code ?? signal}:\n${stderr}`));
        return;
      }
      resolve();
    });
  });
}

function recoveryMetadata(pid = 2147483646, processStartTimeMs = 0): string {
  return `${JSON.stringify({ v: 1, pid, processStartTimeMs })}\n`;
}

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

test('InstanceLock exclusive: second acquire fails while first holds', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-'));
  cleanupPaths.push(dataDir);

  const a = new InstanceLock('ns-a', dataDir);
  const b = new InstanceLock('ns-a', dataDir);
  await a.acquire();
  await expect(b.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  await a.release();
  await b.acquire();
  await b.release();
});

test('main lock metadata is complete before canonical publication and exactly one contender wins', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-main-publish-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'main-publish');
  const lockPath = join(namespaceDir, '.instance.lock');
  let signalMetadataStaged!: () => void;
  const metadataStaged = new Promise<void>((resolve) => {
    signalMetadataStaged = resolve;
  });
  let finishMetadata!: () => void;
  const metadataMayFinish = new Promise<void>((resolve) => {
    finishMetadata = resolve;
  });
  const first = new InstanceLock('main-publish', dataDir, {
    writeMetadata: async (handle, metadata) => {
      signalMetadataStaged();
      await metadataMayFinish;
      await handle.writeFile(metadata, 'utf8');
      await handle.sync();
    },
  });
  const second = new InstanceLock('main-publish', dataDir);

  const firstAcquire = first.acquire();
  await metadataStaged;
  const canonicalWasAbsent = await readFile(lockPath, 'utf8').then(
    () => false,
    (error: NodeJS.ErrnoException) => error.code === 'ENOENT',
  );
  const secondResult = await second.acquire().then(
    () => 'acquired' as const,
    () => 'rejected' as const,
  );
  finishMetadata();
  const firstResult = await firstAcquire.then(
    () => 'acquired' as const,
    () => 'rejected' as const,
  );

  expect(canonicalWasAbsent).toBe(true);
  expect([firstResult, secondResult].sort()).toEqual(['acquired', 'rejected']);
  expect(await readFile(lockPath, 'utf8')).toStartWith(`${process.pid}\n`);
  await first.release();
  await second.release();
  expect(await readdir(namespaceDir)).toEqual([]);
});

test('a complete live staging inode is not ownership and is not removed from its publisher', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-live-stage-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'live-stage');
  let signalStaged!: () => void;
  const staged = new Promise<void>((resolve) => { signalStaged = resolve; });
  let resumePublisher!: () => void;
  const publisherMayResume = new Promise<void>((resolve) => { resumePublisher = resolve; });
  const publisher = new InstanceLock('live-stage', dataDir, {
    beforeMainPublish: async () => {
      signalStaged();
      await publisherMayResume;
    },
  });
  const contender = new InstanceLock('live-stage', dataDir);

  const publishing = publisher.acquire();
  await staged;
  expect((await readdir(namespaceDir)).filter((name) => name.endsWith('.tmp'))).toHaveLength(1);
  await expect(contender.acquire()).resolves.toBeUndefined();
  resumePublisher();
  await expect(publishing).rejects.toBeInstanceOf(InstanceLockError);
  expect((await readdir(namespaceDir)).filter((name) => name.endsWith('.tmp'))).toEqual([]);
  await contender.release();
});

test.each(['before', 'after'] as const)(
  'a SIGKILL %s canonical publication is recovered without accumulating staging files',
  async (phase) => {
    const dataDir = await mkdtemp(join(tmpdir(), `logbun-ilock-crash-${phase}-`));
    cleanupPaths.push(dataDir);
    const namespace = `crash-${phase}`;
    const namespaceDir = join(dataDir, namespace);
    const lockPath = join(namespaceDir, '.instance.lock');

    await crashDuringMainPublication(phase, namespace, dataDir);
    const crashedEntries = await readdir(namespaceDir);
    expect(crashedEntries.filter((name) => name.endsWith('.tmp'))).toHaveLength(1);
    if (phase === 'before') {
      expect(crashedEntries).not.toContain('.instance.lock');
    } else {
      expect(await readFile(lockPath, 'utf8')).toMatch(/^\d+\n\d+\n$/);
    }

    const replacement = new InstanceLock(namespace, dataDir);
    await expect(replacement.acquire()).resolves.toBeUndefined();
    expect((await readdir(namespaceDir)).filter((name) => name.endsWith('.tmp'))).toEqual([]);
    expect(await readFile(lockPath, 'utf8')).toStartWith(`${process.pid}\n`);
    await replacement.release();
  },
);

test('an aged partial main staging remnant is ownership-cleaned before acquisition', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-partial-stage-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'partial-stage');
  const stagedPath = join(
    namespaceDir,
    '.instance.lock.018f0000-0000-7000-8000-000000000099.tmp',
  );
  await mkdir(namespaceDir);
  await writeFile(stagedPath, `${process.pid}\n`);
  const old = new Date(Date.now() - 120_000);
  await utimes(stagedPath, old, old);

  const lock = new InstanceLock('partial-stage', dataDir, {
    recoveryClaimStaleMs: 1_000,
  });
  await expect(lock.acquire()).resolves.toBeUndefined();
  expect(await readdir(namespaceDir)).not.toContain(
    '.instance.lock.018f0000-0000-7000-8000-000000000099.tmp',
  );
  await lock.release();
});

test('metadata write failure closes and removes only the lock file it created', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-write-failure-'));
  cleanupPaths.push(dataDir);
  let createdHandle: FileHandle | undefined;
  const failed = new InstanceLock('write-failure', dataDir, {
    writeMetadata: async (handle) => {
      createdHandle = handle;
      const error = new Error('simulated metadata write failure') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });

  await expect(failed.acquire()).rejects.toThrow(/metadata write failure/);
  await expect(createdHandle!.stat()).rejects.toThrow();

  const retry = new InstanceLock('write-failure', dataDir);
  await expect(retry.acquire()).resolves.toBeUndefined();
  await retry.release();
});

test('metadata sync failure after writing still closes, cleans up, and permits retry', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-sync-failure-'));
  cleanupPaths.push(dataDir);
  let createdHandle: FileHandle | undefined;
  const failed = new InstanceLock('sync-failure', dataDir, {
    writeMetadata: async (handle, metadata) => {
      createdHandle = handle;
      await handle.writeFile(metadata, 'utf8');
      const error = new Error('simulated metadata sync failure') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });

  await expect(failed.acquire()).rejects.toThrow(/metadata sync failure/);
  await expect(createdHandle!.stat()).rejects.toThrow();

  const retry = new InstanceLock('sync-failure', dataDir);
  await expect(retry.acquire()).resolves.toBeUndefined();
  await retry.release();
});

test('failure after staged main sync but before publication leaves no malformed canonical lock', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-before-publish-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'before-publish');
  const failed = new InstanceLock('before-publish', dataDir, {
    beforeMainPublish: () => {
      const error = new Error('simulated failure before main publication') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });

  await expect(failed.acquire()).rejects.toThrow(/before main publication/);
  expect(await readdir(namespaceDir)).toEqual([]);
  const retry = new InstanceLock('before-publish', dataDir);
  await expect(retry.acquire()).resolves.toBeUndefined();
  await retry.release();
});

test('main lock fails explicitly when atomic hard-link publication is unsupported', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-link-unsupported-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'link-unsupported');
  const lock = new InstanceLock('link-unsupported', dataDir, {
    mainLink: async () => {
      const error = new Error('hard links unavailable') as NodeJS.ErrnoException;
      error.code = 'ENOTSUP';
      throw error;
    },
  });

  await expect(lock.acquire()).rejects.toThrow(
    /instance_lock_atomic_publication_unsupported.*ENOTSUP/,
  );
  expect(await readdir(namespaceDir)).toEqual([]);
});

test('a canonical main lock observed immediately after publication always has valid owner metadata', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-after-publish-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'after-publish');
  const lockPath = join(namespaceDir, '.instance.lock');
  let signalPublished!: () => void;
  const published = new Promise<void>((resolve) => {
    signalPublished = resolve;
  });
  let finishPublication!: () => void;
  const publicationMayFinish = new Promise<void>((resolve) => {
    finishPublication = resolve;
  });
  const owner = new InstanceLock('after-publish', dataDir, {
    afterMainPublish: async () => {
      signalPublished();
      await publicationMayFinish;
    },
  });
  const contender = new InstanceLock('after-publish', dataDir);

  const acquiring = owner.acquire();
  await published;
  expect(await readFile(lockPath, 'utf8')).toStartWith(`${process.pid}\n`);
  await expect(contender.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  finishPublication();
  await expect(acquiring).resolves.toBeUndefined();
  await owner.release();
  expect(await readdir(namespaceDir)).toEqual([]);
});

test('failure after canonical publication ownership-cleans the complete lock and permits retry', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-after-publish-failure-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'after-publish-failure');
  const failed = new InstanceLock('after-publish-failure', dataDir, {
    afterMainPublish: () => {
      const error = new Error('simulated failure after canonical publication') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });

  await expect(failed.acquire()).rejects.toThrow(/after canonical publication/);
  expect(await readdir(namespaceDir)).toEqual([]);
  const retry = new InstanceLock('after-publish-failure', dataDir);
  await expect(retry.acquire()).resolves.toBeUndefined();
  await retry.release();
});

test('metadata write failure never unlinks a replacement lock', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-write-replaced-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'write-replaced');
  const lockPath = join(namespaceDir, '.instance.lock');
  let createdHandle: FileHandle | undefined;
  const failed = new InstanceLock('write-replaced', dataDir, {
    writeMetadata: async (handle, metadata) => {
      createdHandle = handle;
      await handle.writeFile(metadata, 'utf8');
      await handle.sync();
    },
    afterMainPublish: async () => {
      await unlink(lockPath);
      await writeFile(lockPath, '2147483646\n0\n');
      const error = new Error('simulated failure after replacement') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });

  await expect(failed.acquire()).rejects.toThrow(/failure after replacement/);
  await expect(createdHandle!.stat()).rejects.toThrow();
  expect(await readFile(lockPath, 'utf8')).toBe('2147483646\n0\n');

  const contender = new InstanceLock('write-replaced', dataDir, {
    killProcess: () => undefined,
  });
  await expect(contender.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(await readFile(lockPath, 'utf8')).toBe('2147483646\n0\n');
});

test('releasing a failed same-process contender cannot remove the owner lock', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-release-owner-'));
  cleanupPaths.push(dataDir);
  const owner = new InstanceLock('same-process', dataDir);
  const failed = new InstanceLock('same-process', dataDir);
  const third = new InstanceLock('same-process', dataDir);
  await owner.acquire();
  await expect(failed.acquire()).rejects.toBeInstanceOf(InstanceLockError);

  await failed.release();
  await expect(third.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  await owner.release();
  await third.acquire();
  await third.release();
});

test.each([
  { name: 'EPERM', error: Object.assign(new Error('not permitted'), { code: 'EPERM' }) },
  { name: 'EACCES', error: Object.assign(new Error('access denied'), { code: 'EACCES' }) },
  { name: 'Deno NotCapable', error: Object.assign(new Error('not capable'), { name: 'NotCapable' }) },
  { name: 'unknown', error: new Error('unknown liveness failure') },
])('InstanceLock treats $name liveness failures as potentially alive', async ({ error }) => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-fail-closed-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'fail-closed');
  const lockPath = join(namespaceDir, '.instance.lock');
  await mkdir(namespaceDir);
  await writeFile(lockPath, '2147483646\n0\n');

  const lock = new InstanceLock('fail-closed', dataDir, {
    killProcess: () => {
      throw error;
    },
  });
  await expect(lock.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(await readFile(lockPath, 'utf8')).toBe('2147483646\n0\n');
});

test('InstanceLock fails closed when an existing lock owner cannot be parsed', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-invalid-owner-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'invalid-owner');
  const lockPath = join(namespaceDir, '.instance.lock');
  await mkdir(namespaceDir);
  await writeFile(lockPath, 'not-a-pid\n');

  const lock = new InstanceLock('invalid-owner', dataDir, {
    killProcess: () => {
      throw new Error('probe must not run for malformed owner metadata');
    },
  });
  await expect(lock.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(await readFile(lockPath, 'utf8')).toBe('not-a-pid\n');
});

test('InstanceLock recovers a stale lock only when the owner probe returns ESRCH', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-esrch-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'known-stale');
  const lockPath = join(namespaceDir, '.instance.lock');
  await mkdir(namespaceDir);
  await writeFile(lockPath, '2147483646\n0\n');

  const lock = new InstanceLock('known-stale', dataDir, {
    killProcess: () => {
      const error = new Error('process does not exist') as NodeJS.ErrnoException;
      error.code = 'ESRCH';
      throw error;
    },
  });
  await lock.acquire();
  expect(await readFile(lockPath, 'utf8')).toStartWith(`${process.pid}\n`);
  await lock.release();
});

test.each([
  { name: 'empty claim and old stale main', claim: '', main: '2147483645\n0\n', ageClaim: true },
  { name: 'partial claim and old stale main', claim: '{"v":1', main: '2147483645\n0\n', ageClaim: true },
  { name: 'dead claim and old stale main', claim: recoveryMetadata(), main: '2147483645\n0\n', ageClaim: false },
  { name: 'dead claim with main absent', claim: recoveryMetadata(), main: null, ageClaim: false },
  { name: 'dead claim with a newer dead main', claim: recoveryMetadata(), main: '2147483644\n0\n', ageClaim: false },
] as const)(
  'InstanceLock recovers a crash remnant with $name',
  async ({ claim, main, ageClaim }) => {
    const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-claim-remnant-'));
    cleanupPaths.push(dataDir);
    const namespaceDir = join(dataDir, 'claim-remnant');
    const lockPath = join(namespaceDir, '.instance.lock');
    const recoveryPath = join(namespaceDir, '.instance.lock.recovery');
    await mkdir(namespaceDir);
    if (main !== null) await writeFile(lockPath, main);
    await writeFile(recoveryPath, claim);
    if (ageClaim) {
      const old = new Date(Date.now() - 120_000);
      await utimes(recoveryPath, old, old);
    }

    const lock = new InstanceLock('claim-remnant', dataDir, {
      recoveryClaimStaleMs: 1_000,
      killProcess: () => {
        const error = new Error('process does not exist') as NodeJS.ErrnoException;
        error.code = 'ESRCH';
        throw error;
      },
    });
    await expect(lock.acquire()).resolves.toBeUndefined();
    expect(await readFile(lockPath, 'utf8')).toStartWith(`${process.pid}\n`);
    expect(await readdir(namespaceDir)).not.toContain('.instance.lock.recovery');
    await lock.release();
  },
);

test('a recent malformed recovery claim fails closed as potentially active', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-recent-claim-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'recent-claim');
  const recoveryPath = join(namespaceDir, '.instance.lock.recovery');
  await mkdir(namespaceDir);
  await writeFile(recoveryPath, '');
  let probes = 0;
  const lock = new InstanceLock('recent-claim', dataDir, {
    recoveryClaimStaleMs: 60_000,
    killProcess: () => {
      probes++;
      throw new Error('malformed claims have no safe PID to probe');
    },
  });

  await expect(lock.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(probes).toBe(0);
  expect(await readFile(recoveryPath, 'utf8')).toBe('');
});

test('Deno liveness denial fails closed for a potentially live recovery claimant', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-claim-deno-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'claim-deno');
  const recoveryPath = join(namespaceDir, '.instance.lock.recovery');
  await mkdir(namespaceDir);
  await writeFile(recoveryPath, recoveryMetadata());
  const lock = new InstanceLock('claim-deno', dataDir, {
    killProcess: () => {
      const error = new Error('Deno --allow-run not granted') as NodeJS.ErrnoException;
      error.code = 'ERR_DENO_NOT_CAPABLE';
      error.name = 'NotCapable';
      throw error;
    },
  });

  await expect(lock.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(await readFile(recoveryPath, 'utf8')).toBe(recoveryMetadata());
});

test('a live reused PID does not keep a dead recovery claim stranded', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-claim-pid-reuse-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'claim-pid-reuse');
  const lockPath = join(namespaceDir, '.instance.lock');
  const recoveryPath = join(namespaceDir, '.instance.lock.recovery');
  await mkdir(namespaceDir);
  await writeFile(recoveryPath, recoveryMetadata(4242, 1_000));
  const lock = new InstanceLock('claim-pid-reuse', dataDir, {
    killProcess: () => undefined,
    readProcessStartTimeMs: () => 20_000,
  });

  await expect(lock.acquire()).resolves.toBeUndefined();
  expect(await readFile(lockPath, 'utf8')).toStartWith(`${process.pid}\n`);
  expect(await readdir(namespaceDir)).not.toContain('.instance.lock.recovery');
  await lock.release();
});

test('two contenders recovering one dead claim install exactly one owner', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-claim-race-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'claim-race');
  const lockPath = join(namespaceDir, '.instance.lock');
  const recoveryPath = join(namespaceDir, '.instance.lock.recovery');
  await mkdir(namespaceDir);
  await writeFile(recoveryPath, recoveryMetadata());
  const staleProbe = () => {
    const error = new Error('process does not exist') as NodeJS.ErrnoException;
    error.code = 'ESRCH';
    throw error;
  };
  const first = new InstanceLock('claim-race', dataDir, { killProcess: staleProbe });
  const second = new InstanceLock('claim-race', dataDir, { killProcess: staleProbe });

  const results = await Promise.allSettled([first.acquire(), second.acquire()]);
  expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1);
  expect(results.filter((result) => result.status === 'rejected')).toHaveLength(1);
  expect(await readFile(lockPath, 'utf8')).toStartWith(`${process.pid}\n`);
  expect(await readdir(namespaceDir)).not.toContain('.instance.lock.recovery');

  const winner = results[0]!.status === 'fulfilled' ? first : second;
  const loser = winner === first ? second : first;
  await loser.release();
  const third = new InstanceLock('claim-race', dataDir);
  await expect(third.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(await readFile(lockPath, 'utf8')).toStartWith(`${process.pid}\n`);
  await winner.release();
  await third.acquire();
  await third.release();
});

test('recovery claim metadata failure closes and ownership-cleans its staged inode', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-claim-write-failure-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'claim-write-failure');
  const lockPath = join(namespaceDir, '.instance.lock');
  await mkdir(namespaceDir);
  await writeFile(lockPath, '2147483646\n0\n');
  let claimHandle: FileHandle | undefined;
  const staleProbe = () => {
    const error = new Error('process does not exist') as NodeJS.ErrnoException;
    error.code = 'ESRCH';
    throw error;
  };
  const failed = new InstanceLock('claim-write-failure', dataDir, {
    killProcess: staleProbe,
    writeRecoveryMetadata: async (handle) => {
      claimHandle = handle;
      await handle.writeFile('{"v":1', 'utf8');
      const error = new Error('simulated recovery claim sync failure') as NodeJS.ErrnoException;
      error.code = 'EIO';
      throw error;
    },
  });

  await expect(failed.acquire()).rejects.toThrow(/recovery claim sync failure/);
  await expect(claimHandle!.stat()).rejects.toThrow();
  expect(await readdir(namespaceDir)).toEqual(['.instance.lock']);
  expect(await readFile(lockPath, 'utf8')).toBe('2147483646\n0\n');

  const retry = new InstanceLock('claim-write-failure', dataDir, {
    killProcess: staleProbe,
  });
  await expect(retry.acquire()).resolves.toBeUndefined();
  await retry.release();
});

test('a live claimant cannot be preempted after its final main identity check', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-claim-replaced-before-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'claim-replaced-before');
  const lockPath = join(namespaceDir, '.instance.lock');
  await mkdir(namespaceDir);
  await writeFile(lockPath, '2147483646\n0\n');
  let signalFinalCheck!: () => void;
  const finalCheckReached = new Promise<void>((resolve) => {
    signalFinalCheck = resolve;
  });
  let resumeFirst!: () => void;
  const firstMayResume = new Promise<void>((resolve) => {
    resumeFirst = resolve;
  });
  const staleProbe = () => {
    const error = new Error('process does not exist') as NodeJS.ErrnoException;
    error.code = 'ESRCH';
    throw error;
  };
  let paused = false;
  const first = new InstanceLock('claim-replaced-before', dataDir, {
    killProcess: staleProbe,
    afterOwnedUnlinkCheck: async () => {
      if (paused) return;
      paused = true;
      signalFinalCheck();
      await firstMayResume;
    },
  });
  const firstAcquire = first.acquire();
  await finalCheckReached;

  const second = new InstanceLock('claim-replaced-before', dataDir);
  const third = new InstanceLock('claim-replaced-before', dataDir);
  await expect(second.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  await expect(third.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(await readFile(lockPath, 'utf8')).toBe('2147483646\n0\n');
  resumeFirst();
  await expect(firstAcquire).resolves.toBeUndefined();
  expect(await readFile(lockPath, 'utf8')).toStartWith(`${process.pid}\n`);
  await expect(second.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  await first.release();
});

test('a live claimant remains fenced after removing stale main and before publication', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-claim-replaced-after-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'claim-replaced-after');
  const lockPath = join(namespaceDir, '.instance.lock');
  await mkdir(namespaceDir);
  await writeFile(lockPath, '2147483646\n0\n');
  let signalMainRemoved!: () => void;
  const mainRemoved = new Promise<void>((resolve) => {
    signalMainRemoved = resolve;
  });
  let resumeFirst!: () => void;
  const firstMayResume = new Promise<void>((resolve) => {
    resumeFirst = resolve;
  });
  const staleProbe = () => {
    const error = new Error('process does not exist') as NodeJS.ErrnoException;
    error.code = 'ESRCH';
    throw error;
  };
  const first = new InstanceLock('claim-replaced-after', dataDir, {
    killProcess: staleProbe,
    afterStaleMainRemoved: async () => {
      signalMainRemoved();
      await firstMayResume;
    },
  });
  const firstAcquire = first.acquire();
  await mainRemoved;

  const second = new InstanceLock('claim-replaced-after', dataDir);
  const third = new InstanceLock('claim-replaced-after', dataDir);
  await expect(second.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  await expect(third.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  resumeFirst();
  await expect(firstAcquire).resolves.toBeUndefined();
  expect(await readFile(lockPath, 'utf8')).toStartWith(`${process.pid}\n`);
  await expect(second.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  await first.release();
});

test('concurrent stale recovery never unlinks the owner installed after a shared probe', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-stale-race-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'stale-race');
  const lockPath = join(namespaceDir, '.instance.lock');
  await mkdir(namespaceDir);
  await writeFile(lockPath, '2147483646\n0\n');

  let observedCount = 0;
  let resolveBothObserved!: () => void;
  const bothObserved = new Promise<void>((resolve) => {
    resolveBothObserved = resolve;
  });
  let releaseFirst!: () => void;
  const firstMayRecover = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  let releaseSecond!: () => void;
  const secondMayRecover = new Promise<void>((resolve) => {
    releaseSecond = resolve;
  });
  const staleProbe = () => {
    const error = new Error('process does not exist') as NodeJS.ErrnoException;
    error.code = 'ESRCH';
    throw error;
  };
  const afterProbe = (release: Promise<void>) => async () => {
    observedCount++;
    if (observedCount === 2) resolveBothObserved();
    await release;
  };
  const first = new InstanceLock('stale-race', dataDir, {
    killProcess: staleProbe,
    afterStaleProbe: afterProbe(firstMayRecover),
  });
  const second = new InstanceLock('stale-race', dataDir, {
    killProcess: staleProbe,
    afterStaleProbe: afterProbe(secondMayRecover),
  });

  const firstAcquire = first.acquire();
  const secondAcquire = second.acquire();
  await bothObserved;

  releaseFirst();
  await expect(firstAcquire).resolves.toBeUndefined();
  releaseSecond();
  await expect(secondAcquire).rejects.toBeInstanceOf(InstanceLockError);

  await second.release();
  const third = new InstanceLock('stale-race', dataDir);
  await expect(third.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(await readFile(lockPath, 'utf8')).toStartWith(`${process.pid}\n`);
  await first.release();
  await third.acquire();
  await third.release();
});

test('stale recovery fails closed when the owner is replaced after identity check', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-stale-post-check-'));
  cleanupPaths.push(dataDir);
  const namespaceDir = join(dataDir, 'stale-post-check');
  const lockPath = join(namespaceDir, '.instance.lock');
  await mkdir(namespaceDir);
  await writeFile(lockPath, '2147483646\n0\n');
  let replaced = false;
  const lock = new InstanceLock('stale-post-check', dataDir, {
    killProcess: () => {
      const error = new Error('process does not exist') as NodeJS.ErrnoException;
      error.code = 'ESRCH';
      throw error;
    },
    beforeOwnedUnlink: async () => {
      if (replaced) return;
      replaced = true;
      await unlink(lockPath);
      await writeFile(lockPath, `${process.pid}\n0\n`);
    },
  });

  await expect(lock.acquire()).rejects.toThrow(/owner changed during stale recovery/);
  expect(await readFile(lockPath, 'utf8')).toBe(`${process.pid}\n0\n`);
  await lock.release();

  const contender = new InstanceLock('stale-post-check', dataDir);
  await expect(contender.acquire()).rejects.toBeInstanceOf(InstanceLockError);
  expect(await readFile(lockPath, 'utf8')).toBe(`${process.pid}\n0\n`);
});

test('durable mode acquires instance lock by default; second logger degrades', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-aud-'));
  cleanupPaths.push(dataDir);

  const audit1 = new AuditLogger({
    namespace: 'same-ns',
    reliability: makeFileReliability('same-ns', dataDir, {
      instanceLock: true,
    }),
    mode: 'durable',
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a1.db') }),
  });
  await audit1.ready;
  expect(audit1.degraded).toBe(false);

  const audit2 = new AuditLogger({
    namespace: 'same-ns',
    reliability: makeFileReliability('same-ns', dataDir, {
      instanceLock: true,
    }),
    mode: 'durable',
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a2.db') }),
  });
  await audit2.ready;
  expect(audit2.degraded).toBe(true);

  await audit1.shutdown();
  await audit2.shutdown();
});

test('instanceLock: false allows two durable loggers on same namespace', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-ilock-off-'));
  cleanupPaths.push(dataDir);

  const audit1 = new AuditLogger({
    namespace: 'shared',
    reliability: makeFileReliability('shared', dataDir),
    mode: 'durable',
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'b1.db') }),
  });
  const audit2 = new AuditLogger({
    namespace: 'shared',
    reliability: makeFileReliability('shared', dataDir),
    mode: 'durable',
    adapter: new BunSQLiteAdapter({ path: join(dataDir, 'b2.db') }),
  });
  await Promise.all([audit1.ready, audit2.ready]);
  expect(audit1.degraded).toBe(false);
  expect(audit2.degraded).toBe(false);
  await audit1.shutdown();
  await audit2.shutdown();
});
