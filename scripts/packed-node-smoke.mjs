import { AuditLogger } from 'logbun';
import { FileReliabilityAdapter } from 'logbun/durability/filesystem';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const dataDir = await mkdtemp(join(tmpdir(), 'logbun-packed-node-'));
const delivered = [];
const destination = {
  async init() {},
  async bulkInsert(_tenantId, logs) {
    delivered.push(...logs);
    return true;
  },
  async query() {
    return { logs: [], nextCursor: null };
  },
  async prune() {},
  async close() {},
};

try {
  const audit = new AuditLogger({
    namespace: 'packed-node',
    mode: 'durable',
    reliability: new FileReliabilityAdapter({ namespace: 'packed-node', dataDir, instanceLock: false }),
    adapter: destination,
    batching: { maxSize: 100, flushInterval: 60_000 },
  });
  await audit.ready;
  await audit.fireAsync('node.packed', { actorId: 'node-user', tenantId: 'node-tenant' });
  if ((await audit.getStatsDetailed()).walApproxBytes === 0) throw new Error('journal did not commit before fireAsync resolved');
  await audit.flush();
  if (delivered.length !== 1) throw new Error('packed filesystem delivery failed');
  await audit.shutdown();

  const first = new FileReliabilityAdapter({ namespace: 'packed-restart', dataDir, instanceLock: false });
  await first.init();
  await first.appendJournal({ id: '018f0000-0000-7000-8000-000000000002', actorId: 'node-user', action: 'node.recover', createdAt: new Date().toISOString() });
  await first.close();
  const second = new FileReliabilityAdapter({ namespace: 'packed-restart', dataDir, instanceLock: false });
  await second.init();
  const recovered = await second.recoverJournal({ maxLogs: 10 });
  if (recovered.logs.length !== 1) throw new Error('packed filesystem restart recovery failed');
  await second.close();
  console.log('Node packed smoke OK (ESM durable filesystem/restart)');
} finally {
  await rm(dataDir, { recursive: true, force: true });
}
