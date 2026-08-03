import { AuditLogger, type IAdapter, type LogbunLog } from 'logbun';
import { FileReliabilityAdapter } from 'logbun/durability/filesystem';

const dataDir = Deno.args[0];
if (!dataDir) throw new Error('usage: deno-smoke.ts <data-dir>');

const written: LogbunLog[] = [];
const adapter: IAdapter = {
  async init() {},
  async bulkInsert(_tenantId, logs) {
    written.push(...logs);
    return true;
  },
  async query() {
    return { logs: [], nextCursor: null };
  },
  async prune() {},
  async close() {},
};

const reliability = new FileReliabilityAdapter({
  namespace: 'deno-packed',
  dataDir,
  instanceLock: false,
});
const audit = new AuditLogger({
  namespace: 'deno-packed',
  mode: 'durable',
  reliability,
  adapter,
  batching: { maxSize: 100, flushInterval: 60_000 },
});
await audit.ready;
await audit.fireAsync('deno.packed', { actorId: 'deno-user', tenantId: 'deno-tenant' });
const beforeFlush = await audit.getStatsDetailed();
if ((beforeFlush.walApproxBytes ?? 0) <= 0) throw new Error('durable journal was not committed before admission resolved');
await audit.flush();
if (written.length !== 1 || written[0]?.action !== 'deno.packed') throw new Error('Deno filesystem flush failed');
await audit.shutdown();

// Direct restart recovery verifies the packed filesystem subpath under Deno's
// Node-compatibility layer with explicit filesystem permissions.
const first = new FileReliabilityAdapter({ namespace: 'deno-restart', dataDir, instanceLock: false });
await first.init();
await first.appendJournal({
  id: '018f0000-0000-7000-8000-000000000001',
  actorId: 'deno-user',
  action: 'deno.recover',
  createdAt: new Date().toISOString(),
});
await first.close();
const second = new FileReliabilityAdapter({ namespace: 'deno-restart', dataDir, instanceLock: false });
await second.init();
const recovered = await second.recoverJournal({ maxLogs: 10 });
if (recovered.logs.length !== 1) throw new Error('Deno restart recovery failed');
await second.acknowledgeJournal(recovered.logs.map((log) => log.id));
await second.close();

console.log('Deno smoke OK (packed npm artifact, root + filesystem durability)');
