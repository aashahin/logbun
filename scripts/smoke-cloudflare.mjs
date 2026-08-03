import { build } from 'esbuild';
import { Miniflare } from 'miniflare';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const root = new URL('..', import.meta.url).pathname;
const temp = await mkdtemp(join(tmpdir(), 'logbun-workers-'));
const worker = join(temp, 'worker.mjs');
const persistence = join(temp, 'do-persist');
let mf;

async function request(path) {
  const response = await mf.dispatchFetch(`http://worker${path}`);
  if (!response.ok) throw new Error(`${path}: ${response.status} ${await response.text()}`);
  return response.json();
}

try {
  await build({
    absWorkingDir: root,
    entryPoints: ['scripts/cloudflare-worker.ts'],
    outfile: worker,
    bundle: true,
    platform: 'browser',
    format: 'esm',
    target: 'es2022',
    logLevel: 'silent',
  });

  const createMiniflare = async () => {
    mf = new Miniflare({
      script: await readFile(worker, 'utf8'),
      modules: true,
      compatibilityDate: '2026-08-03',
      durableObjects: { AUDIT: { className: 'AuditDO', useSQLite: true } },
      durableObjectsPersist: persistence,
    });
    await mf.ready;
  };

  await createMiniflare();

  // Admit, intentionally remove the alarm, then tear down the entire workerd
  // isolate. The next instance must discover persisted work and restore it.
  await request('/admit?action=journal');
  await request('/clear-alarm');
  await mf.dispose();
  mf = undefined;

  await createMiniflare();
  const recoveredAfterRestart = await request('/state');
  if (recoveredAfterRestart.journalRows !== 1 || recoveredAfterRestart.alarm == null) {
    throw new Error('recreated DO did not restore an alarm for its persisted journal');
  }

  // Actual alarm execution flushes the persisted journal and acknowledges it.
  await new Promise((resolve) => setTimeout(resolve, 450));
  const afterAlarm = await request('/state');
  if (afterAlarm.delivered < 1 || afterAlarm.journalRows !== 0) {
    throw new Error('recreated DO alarm did not run journal maintenance');
  }

  // Persist a failed delivery in the DLQ, clear its alarm, and restart a
  // second time. The new isolate has a fresh destination instance.
  await request('/fail?value=true');
  await request('/admit?action=dlq');
  await request('/maintenance');
  const pending = await request('/dlq');
  if (pending.length !== 1 || pending[0].state !== 'pending') throw new Error('failed delivery was not atomically DLQd');
  const orphan = await request('/orphan');
  await request('/clear-alarm');
  await mf.dispose();
  mf = undefined;

  await createMiniflare();
  const dlqAfterRestart = await request('/state');
  if (dlqAfterRestart.alarm == null) {
    throw new Error('recreated DO did not restore an alarm for its persisted DLQ');
  }
  const recoveredOrphans = await request('/dlq');
  if (!recoveredOrphans.some((entry) => entry.id === orphan.id && entry.state === 'pending')) {
    throw new Error('recreated DO did not recover persisted processing work');
  }
  await new Promise((resolve) => setTimeout(resolve, 450));
  const settled = await request('/dlq');
  const afterDlqAlarm = await request('/state');
  if (settled.length !== 0 || afterDlqAlarm.delivered < 2) {
    throw new Error('recreated DO alarm did not settle persisted DLQ work');
  }

  // Atomic claim and orphan recovery execute against real workerd SQLite.
  const atomic = await request('/atomic');
  if (
    atomic.claims !== 1 ||
    !atomic.orphanPending.includes(atomic.id) ||
    atomic.requeued !== atomic.id ||
    atomic.attemptsAfterRequeue !== 0 ||
    atomic.deleted !== null
  ) throw new Error('atomic Cloudflare DLQ lifecycle failed');

  console.log('Cloudflare smoke OK (recreated workerd DO SQLite persistence, recovery, DLQ, alarm)');
} finally {
  await mf?.dispose();
  await rm(temp, { recursive: true, force: true });
}
