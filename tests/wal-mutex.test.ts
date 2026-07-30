import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
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

function makeLog(id: string): LogbunLog {
  return {
    id,
    actorId: 'a',
    action: 'mutex.test',
    createdAt: new Date().toISOString(),
  };
}

test('concurrent append and acknowledge does not lose unacked entries', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-wal-mutex-'));
  cleanupPaths.push(dataDir);

  const wal = new WALStorage('mutex-ns', dataDir);
  await wal.init();

  // Seed a batch that will be acknowledged
  const seeded = Array.from({ length: 20 }, (_, i) => makeLog(`seed-${i}`));
  for (const log of seeded) {
    await wal.append(log);
  }

  const ackIds = seeded.slice(0, 10).map((l) => l.id);
  const keepIds = seeded.slice(10).map((l) => l.id);

  // Race: acknowledge first half while appending new entries
  const newLogs = Array.from({ length: 15 }, (_, i) => makeLog(`new-${i}`));
  await Promise.all([
    wal.acknowledge(ackIds),
    ...newLogs.map((log) => wal.append(log)),
  ]);

  const remaining = await wal.readAll();
  const remainingIds = new Set(remaining.map((l) => l.id));

  // Acknowledged seeds must be gone
  for (const id of ackIds) {
    expect(remainingIds.has(id)).toBe(false);
  }
  // Unacked seeds must remain
  for (const id of keepIds) {
    expect(remainingIds.has(id)).toBe(true);
  }
  // Concurrent appends must all land
  for (const log of newLogs) {
    expect(remainingIds.has(log.id)).toBe(true);
  }

  await wal.close();
});
