import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
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

function makeLog(id: string, action = 'test.action'): LogbunLog {
  return {
    id,
    actorId: 'actor-1',
    action,
    createdAt: new Date().toISOString(),
  };
}

async function openWal(prefix: string): Promise<WALStorage> {
  const dataDir = await mkdtemp(join(tmpdir(), prefix));
  cleanupPaths.push(dataDir);
  const wal = new WALStorage('ack-ns', dataDir);
  await wal.init();
  return wal;
}

test('acknowledge removes subset; truncate only empties when no unacked remain', async () => {
  const wal = await openWal('logbun-wal-ack-');

  await wal.append(makeLog('id-a'));
  await wal.append(makeLog('id-b'));
  await wal.append(makeLog('id-c'));

  expect((await wal.readAll()).map((l) => l.id)).toEqual(['id-a', 'id-b', 'id-c']);

  await wal.acknowledge(['id-a', 'id-c']);
  const remaining = await wal.readAll();
  expect(remaining.map((l) => l.id)).toEqual(['id-b']);
  expect(remaining[0]?.action).toBe('test.action');

  await wal.acknowledge(['id-b']);
  expect(await wal.readAll()).toEqual([]);

  await wal.append(makeLog('id-e'));
  expect((await wal.readAll()).map((l) => l.id)).toEqual(['id-e']);

  // Safe truncate: refuses full wipe while unacked data remains
  await wal.truncate();
  expect((await wal.readAll()).map((l) => l.id)).toEqual(['id-e']);

  await wal.acknowledge(['id-e']);
  await wal.compact();
  expect(await wal.readAll()).toEqual([]);
  await wal.truncate();
  expect(await wal.readAll()).toEqual([]);

  await wal.append(makeLog('id-f'));
  expect((await wal.readAll()).map((l) => l.id)).toEqual(['id-f']);

  await wal.close();
});

test.each([
  {
    name: 'unknown ids leave existing entries intact',
    seed: ['keep-me'],
    ack: ['does-not-exist'],
    expectIds: ['keep-me'],
  },
  {
    name: 'empty ack list is a no-op',
    seed: ['only'],
    ack: [] as string[],
    expectIds: ['only'],
  },
  {
    name: 'acknowledging all ids empties WAL',
    seed: ['x1', 'x2'],
    ack: ['x1', 'x2'],
    expectIds: [] as string[],
  },
])('acknowledge: $name', async ({ seed, ack, expectIds }) => {
  const wal = await openWal('logbun-wal-ack-case-');
  for (const id of seed) {
    await wal.append(makeLog(id));
  }
  await wal.acknowledge(ack);
  expect((await wal.readAll()).map((l) => l.id)).toEqual(expectIds);
  await wal.close();
});
