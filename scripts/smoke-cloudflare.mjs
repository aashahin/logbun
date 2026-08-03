import { build } from 'esbuild';
import { Miniflare } from 'miniflare';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const root = new URL('..', import.meta.url).pathname;
const temp = await mkdtemp(join(tmpdir(), 'logbun-workers-'));
const worker = join(temp, 'worker.mjs');
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

  mf = new Miniflare({
    script: await readFile(worker, 'utf8'),
    modules: true,
    compatibilityDate: '2026-08-03',
    durableObjects: { AUDIT: { className: 'AuditDO', useSQLite: true } },
  });
  await mf.ready;

  // Standard Worker -> owning DO binding; root and Cloudflare subpath are bundled.
  await request('/admit?action=journal');
  const byteBounded = await request('/recovery?maxBytes=1');
  if (byteBounded.ids.length !== 0 || byteBounded.truncated !== true) throw new Error('DO bounded recovery exceeded byte cap');
  const recovered = await request('/recovery');
  if (recovered.ids.length !== 1) throw new Error(`DO journal recovery did not observe durable admission: ${JSON.stringify(recovered)}`);
  const before = await request('/state');
  if (before.journalRows !== 1 || before.alarm == null) throw new Error('DO SQLite journal/alarm missing');

  // Actual alarm execution flushes the journal and acknowledges it.
  await new Promise((resolve) => setTimeout(resolve, 450));
  const afterAlarm = await request('/state');
  if (afterAlarm.delivered < 1 || afterAlarm.journalRows !== 0) throw new Error('DO alarm did not run maintenance');

  // Destination failure -> durable DLQ -> host maintenance delivery / settlement.
  await request('/fail?value=true');
  await request('/admit?action=dlq');
  await request('/maintenance');
  const pending = await request('/dlq');
  if (pending.length !== 1 || pending[0].state !== 'pending') throw new Error('failed delivery was not atomically DLQd');
  await request('/fail?value=false');
  await request('/maintenance');
  const settled = await request('/dlq');
  if (settled.length !== 0) throw new Error('DLQ retry did not settle after destination success');

  // Atomic claim and orphan recovery execute against real workerd SQLite.
  const atomic = await request('/atomic');
  if (
    atomic.claims !== 1 ||
    !atomic.orphanPending.includes(atomic.id) ||
    atomic.requeued !== atomic.id ||
    atomic.attemptsAfterRequeue !== 0 ||
    atomic.deleted !== null
  ) throw new Error('atomic Cloudflare DLQ lifecycle failed');

  console.log('Cloudflare smoke OK (workerd/Miniflare DO SQLite, recovery, DLQ, alarm)');
} finally {
  await mf?.dispose();
  await rm(temp, { recursive: true, force: true });
}
