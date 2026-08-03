import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { MemoryReliabilityAdapter } from '../src/reliability/memory';
import type { ReliabilityAdapter } from '../src/types';
import type { LogbunLog } from '../src/types';
import { makeFileReliability } from './helpers';

const cleanup: string[] = [];
afterEach(async () => {
  await Promise.all(
    cleanup.splice(0).map((p) => rm(p, { recursive: true, force: true })),
  );
});

function log(id: string, tenantId = 't1'): LogbunLog {
  return {
    id,
    tenantId,
    actorId: 'a',
    action: 'contract.test',
    createdAt: new Date().toISOString(),
  };
}

function contractSuite(
  name: string,
  create: () => Promise<ReliabilityAdapter>,
  opts?: { journal?: boolean },
) {
  describe(`ReliabilityAdapter contract: ${name}`, () => {
    if (opts?.journal !== false) {
      test('append/recover/ack order (persistent only)', async () => {
        const r = await create();
        await r.init();
        if (!r.persistent) {
          await r.close();
          return;
        }
        await r.appendJournal(log('j1'));
        await r.appendJournal(log('j2'));
        const bounded = await r.recoverJournal({ maxLogs: 1 });
        expect(bounded.logs.map((l) => l.id)).toEqual(['j1']);
        expect(bounded.truncated).toBe(true);
        const rec = await r.recoverJournal();
        expect(rec.logs.map((l) => l.id)).toEqual(['j1', 'j2']);
        await r.acknowledgeJournal(['j1']);
        const rec2 = await r.recoverJournal();
        expect(rec2.logs.map((l) => l.id)).toEqual(['j2']);
        await r.close();
      });
    }

    test('opaque DLQ ids, atomic claim/settle, requeue preserves id', async () => {
      const r = await create();
      await r.init();
      const id = await r.writeDlq('t1', [log('d1')]);
      expect(typeof id).toBe('string');

      const listed = await r.listDlq({ includePending: true });
      expect(listed.some((e) => e.id === id)).toBe(true);

      const claimed = await r.claimDlq(id);
      expect(claimed?.id).toBe(id);

      await r.settleDlqFailure(id, 1);
      const claimed2 = await r.claimDlq(id);
      expect(claimed2?.attempts).toBe(1);
      await r.poisonDlq(id);

      const requeued = await r.requeueDead(id);
      expect(requeued).toBe(id);
      const after = await r.readDlq(id);
      expect(after?.attempts).toBe(0);

      const c3 = await r.claimDlq(id);
      expect(c3).not.toBeNull();
      await r.poisonDlq(id);
      await r.deleteDead(id);
      expect(await r.readDlq(id)).toBeNull();
      await r.close();
    });

    test('orphan recovery moves processing → pending', async () => {
      const r = await create();
      await r.init();
      const id = await r.writeDlq(null, [log('o1')]);
      await r.claimDlq(id);
      await r.recoverOrphans();
      const pending = await r.listDlq({ includePending: true });
      expect(pending.some((e) => e.id === id)).toBe(true);
      await r.close();
    });
  });
}

contractSuite('memory', async () => new MemoryReliabilityAdapter({ maxDlqEntries: 100 }));

contractSuite('filesystem', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-contract-'));
  cleanup.push(dataDir);
  return makeFileReliability('contract', dataDir, { maxDlqEntries: 100 });
});

test('memory: dlq_full at max entries', async () => {
  const r = new MemoryReliabilityAdapter({ maxDlqEntries: 1 });
  await r.init();
  await r.writeDlq(null, [log('a')]);
  await expect(r.writeDlq(null, [log('b')])).rejects.toThrow(/dlq_full/);
  await r.close();
});

test('filesystem: dlq_full at max entries', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'logbun-contract-cap-'));
  cleanup.push(dataDir);
  const r = makeFileReliability('cap', dataDir, { maxDlqEntries: 1 });
  await r.init();
  await r.writeDlq(null, [log('a')]);
  await expect(r.writeDlq(null, [log('b')])).rejects.toThrow(/dlq_full/);
  await r.close();
});

test('memory journal (enableJournal) supports recover', async () => {
  const r = new MemoryReliabilityAdapter({ enableJournal: true });
  await r.init();
  await r.appendJournal(log('m1'));
  const rec = await r.recoverJournal();
  expect(rec.logs.map((l) => l.id)).toEqual(['m1']);
  await r.acknowledgeJournal(['m1']);
  expect((await r.recoverJournal()).logs).toHaveLength(0);
  await r.close();
});

test('memory journal honors recovery byte bounds', async () => {
  const r = new MemoryReliabilityAdapter({ enableJournal: true });
  await r.init();
  await r.appendJournal(log('m-byte'));
  const none = await r.recoverJournal({ maxBytes: 0 });
  expect(none.logs).toHaveLength(0);
  expect(none.truncated).toBe(true);
  await r.close();
});
