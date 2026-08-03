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

async function waitForCondition(description, probe, timeoutMs = 5_000, intervalMs = 25) {
  const deadline = Date.now() + timeoutMs;
  let observation;
  for (;;) {
    observation = await probe();
    if (observation.done) return observation.value;
    if (Date.now() >= deadline) {
      throw new Error(
        `${description} timed out after ${timeoutMs}ms; last observation: ${JSON.stringify(observation.value)}`,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
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

  // Seed bounded-recovery state without scheduling an alarm, then tear down
  // the entire workerd isolate. The next instance must discover persisted work
  // and restore a real alarm through its normal adapter initialization.
  await request('/admit-unscheduled?action=journal');
  await waitForCondition(
    'unscheduled bounded-recovery fixture was not stable',
    async () => {
      const state = await request('/state');
      return {
        done: state.journalRows === 1 && state.alarm == null,
        value: state,
      };
    },
  );
  const strictBound = await request('/recovery?maxBytes=1');
  if (strictBound.ids.length !== 0 || strictBound.truncated !== true) {
    throw new Error('DO recovery returned a record larger than maxBytes');
  }
  for (const value of ['0', '-1']) {
    const bounded = await request(`/recovery?maxBytes=${value}`);
    if (bounded.ids.length !== 0 || bounded.truncated !== true) {
      throw new Error(`DO recovery did not normalize maxBytes=${value} to a strict zero bound`);
    }
  }
  for (const value of ['NaN', 'Infinity']) {
    const unbounded = await request(`/recovery?maxBytes=${value}`);
    if (unbounded.ids.length !== 1 || unbounded.truncated !== false) {
      throw new Error(`DO recovery did not treat maxBytes=${value} as unbounded`);
    }
  }
  await request('/clear-alarm');
  await mf.dispose();
  mf = undefined;

  await createMiniflare();
  const recoveredAfterRestart = await request('/state');
  if (recoveredAfterRestart.journalRows !== 1 || recoveredAfterRestart.alarm == null) {
    throw new Error('recreated DO did not restore an alarm for its persisted journal');
  }

  // Actual alarm execution flushes the persisted journal and acknowledges it.
  const afterAlarm = await waitForCondition(
    'recreated DO alarm did not run journal maintenance',
    async () => {
      const state = await request('/state');
      return {
        done: state.delivered >= 1 && state.journalRows === 0,
        value: state,
      };
    },
  );

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
  await waitForCondition(
    'recreated DO alarm did not settle persisted DLQ work',
    async () => {
      const settled = await request('/dlq');
      const state = await request('/state');
      return {
        done: settled.length === 0 && state.delivered >= 2,
        value: { settled, state },
      };
    },
  );

  // Atomic claim and orphan recovery execute against real workerd SQLite.
  const atomic = await request('/atomic');
  if (
    atomic.claims !== 1 ||
    !atomic.orphanPending.includes(atomic.id) ||
    atomic.requeued !== atomic.id ||
    atomic.attemptsAfterRequeue !== 0 ||
    atomic.deleted !== null
  ) throw new Error('atomic Cloudflare DLQ lifecycle failed');

  // A consumed alarm must be restored when maintenance fails, and the handler
  // must reject so the platform can also apply its retry policy.
  const deliveredBeforeFailure = (await request('/state')).delivered;
  await request('/fail?value=true');
  await request('/admit?action=alarm-rearm');
  await request('/clear-alarm');
  await request('/fail-maintenance-once');
  let maintenanceFailed = false;
  try {
    const response = await mf.dispatchFetch('http://worker/maintenance');
    maintenanceFailed = !response.ok;
  } catch {
    maintenanceFailed = true;
  }
  if (!maintenanceFailed) {
    throw new Error('failed DO maintenance did not propagate to the alarm host');
  }
  const rearmed = await request('/state');
  if (rearmed.alarm == null || rearmed.dlqPending < 1) {
    throw new Error('failed DO maintenance consumed its only pending-work alarm');
  }
  await request('/fail?value=false');
  await waitForCondition(
    'rearmed DO alarm did not retry and settle maintenance work',
    async () => {
      const state = await request('/state');
      return {
        done: state.delivered > deliveredBeforeFailure && state.dlqPending === 0,
        value: state,
      };
    },
  );

  console.log('Cloudflare smoke OK (recreated workerd DO persistence, DLQ, alarm rearm/retry)');
} finally {
  await mf?.dispose();
  await rm(temp, { recursive: true, force: true });
}
