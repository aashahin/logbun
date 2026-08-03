import { afterEach, expect, test } from 'bun:test';
import { mkdir, mkdtemp, readFile, readdir, rm, symlink, unlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { FileReliabilityAdapter } from '../src/durability/filesystem';
import type { LogbunLog } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

test('filesystem reliability rejects a symlinked ancestor before lock or WAL creation', async () => {
  const parent = await mkdtemp(join(tmpdir(), 'logbun-file-root-link-'));
  const outside = await mkdtemp(join(tmpdir(), 'logbun-file-root-outside-'));
  cleanupPaths.push(parent, outside);
  const safe = join(parent, 'safe');
  const linked = join(safe, 'link');
  await mkdir(safe);
  await symlink(outside, linked);

  const reliability = new FileReliabilityAdapter({
    namespace: 'root-link',
    dataDir: join(linked, 'missing'),
  });

  await expect(reliability.init()).rejects.toThrow(/symbolic link/);
  expect(await readdir(outside)).toEqual([]);
});

test('filesystem reliability rejects a symlinked namespace root before lock creation', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-file-namespace-link-'));
  const outside = await mkdtemp(join(tmpdir(), 'logbun-file-namespace-outside-'));
  cleanupPaths.push(dataDir, outside);
  await symlink(outside, join(dataDir, 'namespace-link'));

  const reliability = new FileReliabilityAdapter({
    namespace: 'namespace-link',
    dataDir,
  });
  await expect(reliability.init()).rejects.toThrow(/symbolic link/);
  expect(await readdir(outside)).toEqual([]);
});

test('filesystem reliability rejects a symlinked WAL directory before lock creation', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-file-wal-link-'));
  const outside = await mkdtemp(join(tmpdir(), 'logbun-file-wal-outside-'));
  cleanupPaths.push(dataDir, outside);
  const namespaceDir = join(dataDir, 'wal-dir-link');
  await mkdir(namespaceDir);
  await symlink(outside, join(namespaceDir, 'wal'));

  const reliability = new FileReliabilityAdapter({
    namespace: 'wal-dir-link',
    dataDir,
  });
  await expect(reliability.init()).rejects.toThrow(/symbolic link/);
  expect(await readdir(namespaceDir)).toEqual(['wal']);
  expect(await readdir(outside)).toEqual([]);
});

function log(id: string): LogbunLog {
  return {
    id,
    actorId: 'actor',
    action: 'filesystem.symlink',
    createdAt: new Date().toISOString(),
  };
}

test('WAL append rejects a current.aof symlink swapped in after initialization', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-swap-'));
  const outside = await mkdtemp(join(tmpdir(), 'logbun-wal-swap-outside-'));
  cleanupPaths.push(dataDir, outside);
  const reliability = new FileReliabilityAdapter({
    namespace: 'wal-swap',
    dataDir,
    instanceLock: false,
    wal: { fsync: false },
  });
  await reliability.init();

  const current = join(dataDir, 'wal-swap', 'wal', 'current.aof');
  const external = join(outside, 'external.aof');
  const original = `${JSON.stringify(log('outside'))}\n`;
  await writeFile(external, original);
  await unlink(current);
  await symlink(external, current);

  await expect(reliability.appendJournal(log('must-not-escape'))).rejects.toThrow(/symbolic link/);
  expect(await readFile(external, 'utf8')).toBe(original);
  await reliability.close().catch(() => undefined);
});

async function setupCurrentSwap(name: string) {
  const dataDir = await mkdtemp(join(tmpdir(), `logbun-wal-${name}-`));
  const outside = await mkdtemp(join(tmpdir(), `logbun-wal-${name}-outside-`));
  cleanupPaths.push(dataDir, outside);
  const reliability = new FileReliabilityAdapter({
    namespace: name,
    dataDir,
    instanceLock: false,
    wal: { fsync: false, compactAckThreshold: 1 },
  });
  await reliability.init();
  const current = join(dataDir, name, 'wal', 'current.aof');
  const external = join(outside, 'external.aof');
  const original = `${JSON.stringify(log('outside'))}\n`;
  await writeFile(external, original);
  await unlink(current);
  await symlink(external, current);
  return { reliability, external, original };
}

test('WAL recovery rejects a current.aof symlink swapped in after initialization', async () => {
  const { reliability, external, original } = await setupCurrentSwap('wal-read-swap');
  await expect(reliability.recoverJournal()).rejects.toThrow(/symbolic link/);
  expect(await readFile(external, 'utf8')).toBe(original);
  await reliability.close().catch(() => undefined);
});

test('WAL compaction rejects a current.aof symlink swapped in after initialization', async () => {
  const { reliability, external, original } = await setupCurrentSwap('wal-compact-swap');
  await expect(reliability.compactJournal()).rejects.toThrow(/symbolic link/);
  expect(await readFile(external, 'utf8')).toBe(original);
  await reliability.close().catch(() => undefined);
});

test('WAL acknowledge cannot compact through a swapped current.aof symlink', async () => {
  const { reliability, external, original } = await setupCurrentSwap('wal-ack-swap');
  await expect(reliability.acknowledgeJournal(['outside'])).rejects.toThrow(/symbolic link/);
  expect(await readFile(external, 'utf8')).toBe(original);
  await reliability.close().catch(() => undefined);
});

test('WAL acknowledge rejects an acked.ids symlink without altering its target', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-ack-file-swap-'));
  const outside = await mkdtemp(join(tmpdir(), 'logbun-wal-ack-file-outside-'));
  cleanupPaths.push(dataDir, outside);
  const reliability = new FileReliabilityAdapter({
    namespace: 'wal-ack-file-swap',
    dataDir,
    instanceLock: false,
    wal: { fsync: false },
  });
  await reliability.init();
  const ackPath = join(dataDir, 'wal-ack-file-swap', 'wal', 'acked.ids');
  const external = join(outside, 'external.ids');
  const original = 'outside-must-remain\n';
  await writeFile(external, original);
  await unlink(ackPath);
  await symlink(external, ackPath);

  await expect(reliability.acknowledgeJournal(['must-not-escape'])).rejects.toThrow(
    /symbolic link/,
  );
  expect(await readFile(external, 'utf8')).toBe(original);
  await reliability.close().catch(() => undefined);
});

test('WAL recovery rejects a sealed-segment symlink swapped in after initialization', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-segment-swap-'));
  const outside = await mkdtemp(join(tmpdir(), 'logbun-wal-segment-outside-'));
  cleanupPaths.push(dataDir, outside);
  const reliability = new FileReliabilityAdapter({
    namespace: 'wal-segment-swap',
    dataDir,
    instanceLock: false,
    wal: { fsync: false, segmentBytes: 1 },
  });
  await reliability.init();
  await reliability.appendJournal({
    ...log('sealed-record'),
    metadata: { padding: 'x'.repeat(512) },
  });
  await reliability.appendJournal(log('current-record'));

  const walDir = join(dataDir, 'wal-segment-swap', 'wal');
  const segment = (await readdir(walDir)).find((name) => /^seg-\d+\.aof$/.test(name));
  expect(segment).toBeDefined();
  const segmentPath = join(walDir, segment!);
  const external = join(outside, 'external.aof');
  const original = `${JSON.stringify(log('outside-segment'))}\n`;
  await writeFile(external, original);
  await unlink(segmentPath);
  await symlink(external, segmentPath);

  await expect(reliability.recoverJournal()).rejects.toThrow(/symbolic link/);
  expect(await readFile(external, 'utf8')).toBe(original);
  await reliability.close().catch(() => undefined);
});
