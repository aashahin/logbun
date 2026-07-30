import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { WALStorage } from '../src/storage/wal';
import { DLQStorage } from '../src/storage/dlq';
import { normalizeEncryptionKey, ENC_PREFIX } from '../src/utils/crypto';
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
    actorId: 'actor',
    action: 'secret.event',
    tenantId: 't1',
    newValues: { token: 'super-secret-value' },
    createdAt: new Date().toISOString(),
  };
}

test('WAL encrypts lines at rest; decrypts on read', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-enc-wal-'));
  cleanupPaths.push(dataDir);

  const key = await normalizeEncryptionKey('test-passphrase-for-logbun-enc');
  const wal = new WALStorage('enc', dataDir, {
    fsync: false,
    encryptionKey: key,
    hardMaxBytes: false,
    compactAckThreshold: 10_000,
  });
  await wal.init();
  await wal.append(makeLog('e1'));

  const raw = await readFile(join(dataDir, 'enc', 'wal', 'current.aof'), 'utf8');
  expect(raw).toContain(ENC_PREFIX);
  expect(raw).not.toContain('super-secret-value');

  const logs = await wal.readAll();
  expect(logs).toHaveLength(1);
  expect(logs[0]!.id).toBe('e1');
  expect(logs[0]!.newValues).toEqual({ token: 'super-secret-value' });
  await wal.close();
});

test('DLQ encrypts batch body at rest', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-enc-dlq-'));
  cleanupPaths.push(dataDir);

  const key = await normalizeEncryptionKey(
    '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
  );
  const dlq = new DLQStorage('encd', dataDir, {
    fsync: false,
    maxFiles: 100,
    encryptionKey: key,
  });
  await dlq.init();
  await dlq.write('t1', [makeLog('d1')]);

  const pending = await dlq.listPending();
  expect(pending).toHaveLength(1);
  const raw = await readFile(pending[0]!, 'utf8');
  expect(raw.startsWith(ENC_PREFIX) || raw.includes(ENC_PREFIX)).toBe(true);
  expect(raw).not.toContain('super-secret-value');

  const batch = await dlq.readBatchFile(pending[0]!);
  expect(batch.logs[0]!.newValues).toEqual({ token: 'super-secret-value' });
});
