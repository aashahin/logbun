import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { WALStorage } from '../src/storage/wal';
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
    maxBytes: 200,
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
