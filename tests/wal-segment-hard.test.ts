import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { WALStorage } from '../src/durability/filesystem';
import type { LogbunLog } from '../src/types';

const cleanupPaths: string[] = [];

afterEach(async () => {
  await Promise.all(
    cleanupPaths.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

function log(id: string, pad = 0): LogbunLog {
  return {
    id,
    actorId: 'a',
    action: 't',
    createdAt: new Date().toISOString(),
    metadata: pad > 0 ? { blob: 'x'.repeat(pad) } : undefined,
  };
}

test('WAL rotates into sealed segments when segmentBytes exceeded', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-seg-'));
  cleanupPaths.push(dataDir);

  const wal = new WALStorage('seg-ns', dataDir, {
    fsync: false,
    segmentBytes: 400,
    hardMaxBytes: false,
    maxBytes: 10 * 1024 * 1024,
    compactAckThreshold: 10_000,
  });
  await wal.init();

  for (let i = 0; i < 20; i++) {
    await wal.append(log(`id-${i}`, 80));
  }

  const entries = await readdir(join(dataDir, 'seg-ns', 'wal'));
  const segs = entries.filter((e) => e.startsWith('seg-') && e.endsWith('.aof'));
  expect(segs.length).toBeGreaterThan(0);

  const all = await wal.readAll();
  expect(all.map((l) => l.id)).toEqual(
    Array.from({ length: 20 }, (_, i) => `id-${i}`),
  );

  await wal.close();
});

test('hard maxBytes refuses append with wal_full', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-hard-'));
  cleanupPaths.push(dataDir);

  const wal = new WALStorage('hard-ns', dataDir, {
    fsync: false,
    maxBytes: 300,
    hardMaxBytes: true,
    segmentBytes: 10_000,
    compactAckThreshold: 10_000,
  });
  await wal.init();

  await wal.append(log('a', 40));
  await wal.append(log('b', 40));

  let threw = false;
  try {
    // Fill until hard refuse
    for (let i = 0; i < 50; i++) {
      await wal.append(log(`fill-${i}`, 80));
    }
  } catch (err) {
    threw = true;
    expect(String(err)).toContain('wal_full');
  }
  expect(threw).toBe(true);

  // Unacked still readable
  const pending = await wal.readAll();
  expect(pending.length).toBeGreaterThan(0);
  await wal.close();
});

test('hard maxBytes accounts for the encoded line before appending', async () => {
  const probeDir = await mkdtemp(join(tmpdir(), 'logbun-hard-probe-'));
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-hard-exact-'));
  cleanupPaths.push(probeDir, dataDir);

  const entry = log('exact-fit', 80);
  const probe = new WALStorage('hard-exact', probeDir, {
    fsync: false,
    maxBytes: 10_000,
  });
  await probe.init();
  await probe.append(entry);
  const encodedBytes = await probe.approximateSize();
  await probe.close();

  const wal = new WALStorage('hard-exact', dataDir, {
    fsync: false,
    maxBytes: encodedBytes,
    hardMaxBytes: true,
  });
  await wal.init();

  // Equality is permitted; only the next encoded line must be refused.
  await wal.append(entry);
  expect(await wal.approximateSize()).toBe(encodedBytes);
  await expect(wal.append(log('must-not-partially-append', 1))).rejects.toThrow(
    /wal_full/,
  );
  expect(await wal.approximateSize()).toBe(encodedBytes);
  expect((await wal.readAll()).map((item) => item.id)).toEqual(['exact-fit']);
  await wal.close();

  const nearLimit = new WALStorage('hard-near-limit', dataDir, {
    fsync: false,
    maxBytes: encodedBytes + 1,
    hardMaxBytes: true,
  });
  await nearLimit.init();
  await nearLimit.append(entry);
  // The previous implementation checked only the pre-append size and allowed
  // this line to push the journal far past the configured ceiling.
  await expect(nearLimit.append(log('would-overflow', 1))).rejects.toThrow(/wal_full/);
  expect((await nearLimit.readAll()).map((item) => item.id)).toEqual(['exact-fit']);
  await nearLimit.close();
});

test('hard maxBytes applies to encrypted bytes without writing a partial line', async () => {
  const probeDir = await mkdtemp(join(tmpdir(), 'logbun-hard-enc-probe-'));
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-hard-enc-'));
  cleanupPaths.push(probeDir, dataDir);
  const encryptionKey = new Uint8Array(32).fill(7);
  const entry = log('encrypted-exact-fit', 40);

  const probe = new WALStorage('encrypted', probeDir, {
    fsync: false,
    encryptionKey,
    maxBytes: 10_000,
  });
  await probe.init();
  await probe.append(entry);
  const encodedBytes = await probe.approximateSize();
  await probe.close();

  const wal = new WALStorage('encrypted', dataDir, {
    fsync: false,
    encryptionKey,
    maxBytes: encodedBytes + 1,
  });
  await wal.init();
  await wal.append(entry);
  await expect(wal.append(log('encrypted-overflow', 1))).rejects.toThrow(/wal_full/);
  expect(await wal.approximateSize()).toBe(encodedBytes);
  expect((await wal.readAll()).map((item) => item.id)).toEqual(['encrypted-exact-fit']);
  await wal.close();
});

test('bounded recovery counts encoded bytes and returns one oversized first record', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-bounded-bytes-'));
  cleanupPaths.push(dataDir);

  for (const encryptionKey of [undefined, new Uint8Array(32).fill(9)] as const) {
    const namespace = encryptionKey ? 'bounded-encrypted' : 'bounded-plain';
    const wal = new WALStorage(namespace, dataDir, {
      fsync: false,
      encryptionKey,
      hardMaxBytes: false,
    });
    await wal.init();
    await wal.append(log(`${namespace}-large`, 120));
    await wal.append(log(`${namespace}-second`, 120));

    const firstOnly = await wal.readAllBounded({ maxBytes: 1 });
    expect(firstOnly.logs.map((item) => item.id)).toEqual([`${namespace}-large`]);
    expect(firstOnly.truncated).toBe(true);

    const total = await wal.approximateSize();
    const upToFirst = await wal.readAllBounded({ maxBytes: total - 1 });
    expect(upToFirst.logs).toHaveLength(1);
    expect(upToFirst.truncated).toBe(true);
    await wal.close();
  }
});

test('bounded recovery counts a complete crash-final record without inventing a newline', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-bounded-final-line-'));
  cleanupPaths.push(dataDir);

  for (const encryptionKey of [undefined, new Uint8Array(32).fill(5)] as const) {
    const namespace = encryptionKey ? 'final-encrypted' : 'final-plain';
    const wal = new WALStorage(namespace, dataDir, {
      fsync: false,
      encryptionKey,
      hardMaxBytes: false,
    });
    await wal.init();
    await wal.append(log(`${namespace}-one`, 20));
    await wal.append(log(`${namespace}-two`, 20));

    const current = join(dataDir, namespace, 'wal', 'current.aof');
    const raw = await readFile(current);
    expect(raw[raw.length - 1]).toBe(0x0a);
    await writeFile(current, raw.subarray(0, raw.length - 1));
    const exact = await wal.approximateSize();
    const recovered = await wal.readAllBounded({ maxBytes: exact });
    expect(recovered.logs.map((item) => item.id)).toEqual([
      `${namespace}-one`,
      `${namespace}-two`,
    ]);
    expect(recovered.truncated).toBe(false);
    await wal.close();
  }
});
