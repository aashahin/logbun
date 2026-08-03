import { AuditLogger, randomUUIDv7, type IAdapter, type LogbunLog } from '../dist/index.js';
import { CloudflareReliabilityAdapter } from '../dist/durability/cloudflare/index.js';

type StateLike = {
  storage: {
    sql: {
      exec(query: string, ...bindings: unknown[]): {
        one?: () => Record<string, unknown> | null;
      };
    };
    getAlarm?: () => Promise<number | null>;
    deleteAlarm?: () => Promise<void>;
  };
};

class DestinationAdapter implements IAdapter {
  constructor(private readonly owner: AuditDO) {}

  async init(): Promise<void> {}

  async bulkInsert(_tenantId: string | null, logs: LogbunLog[]): Promise<boolean> {
    if (this.owner.failDestination) return false;
    this.owner.delivered += logs.length;
    return true;
  }

  async query() {
    return { logs: [], nextCursor: null };
  }

  async prune(): Promise<void> {}
  async close(): Promise<void> {}
}

export class AuditDO {
  readonly reliability: CloudflareReliabilityAdapter;
  readonly audit: AuditLogger;
  delivered = 0;
  failDestination = false;
  failNextMaintenanceScan = false;

  constructor(readonly state: StateLike) {
    this.reliability = new CloudflareReliabilityAdapter({
      state,
      tablePrefix: 'audit_test',
      alarmDelayMs: 250,
    });
    const listDlq = this.reliability.listDlq.bind(this.reliability);
    this.reliability.listDlq = async (options) => {
      if (this.failNextMaintenanceScan) {
        this.failNextMaintenanceScan = false;
        throw new Error('simulated consumed-alarm maintenance failure');
      }
      return listDlq(options);
    };
    this.audit = new AuditLogger({
      namespace: 'worker-do',
      mode: 'durable',
      reliability: this.reliability,
      adapter: new DestinationAdapter(this),
      batching: { maxSize: 100, flushInterval: 60_000 },
      retry: { insertMaxRetries: 1, insertBaseDelayMs: 1 },
    });
  }

  async fetch(request: Request): Promise<Response> {
    await this.audit.ready;
    const url = new URL(request.url);
    const json = (value: unknown) =>
      new Response(JSON.stringify(value), {
        headers: { 'content-type': 'application/json' },
      });

    if (url.pathname === '/admit-unscheduled') {
      const unscheduled = new CloudflareReliabilityAdapter({
        state: this.state,
        tablePrefix: 'audit_test',
        scheduleAlarms: false,
      });
      await unscheduled.init();
      const id = randomUUIDv7();
      await unscheduled.appendJournal({
        id,
        actorId: 'worker-user',
        action: url.searchParams.get('action') ?? 'audit.unscheduled',
        tenantId: 'tenant-a',
        createdAt: new Date().toISOString(),
      });
      await unscheduled.close();
      return json({ id });
    }
    if (url.pathname === '/admit') {
      const action = url.searchParams.get('action') ?? 'audit.admit';
      await this.audit.fireAsync(action, { actorId: 'worker-user', tenantId: 'tenant-a' });
      return json({ id: randomUUIDv7() });
    }
    if (url.pathname === '/fail') {
      this.failDestination = url.searchParams.get('value') !== 'false';
      return json({ failDestination: this.failDestination });
    }
    if (url.pathname === '/maintenance') {
      await this.audit.runMaintenance();
      return json(await this.audit.getStatsDetailed());
    }
    if (url.pathname === '/fail-maintenance-once') {
      this.failNextMaintenanceScan = true;
      return json({ armed: true });
    }
    if (url.pathname === '/clear-alarm') {
      await this.state.storage.deleteAlarm?.();
      return json({ cleared: true });
    }
    if (url.pathname === '/recovery') {
      const fresh = new CloudflareReliabilityAdapter({
        state: this.state,
        tablePrefix: 'audit_test',
        scheduleAlarms: false,
      });
      await fresh.init();
      const maxBytes = url.searchParams.get('maxBytes');
      const recovered = await fresh.recoverJournal({
        maxLogs: 10,
        maxBytes: maxBytes == null ? undefined : Number(maxBytes),
      });
      await fresh.close();
      return json({ ids: recovered.logs.map((log) => log.id), truncated: recovered.truncated });
    }
    if (url.pathname === '/atomic') {
      const id = await this.reliability.writeDlq('tenant-a', [{
        id: randomUUIDv7(),
        actorId: 'worker-user',
        action: 'atomic',
        createdAt: new Date().toISOString(),
      }]);
      const [first, second] = await Promise.all([
        this.reliability.claimDlq(id),
        this.reliability.claimDlq(id),
      ]);
      await this.reliability.recoverOrphans();
      const pending = await this.reliability.listDlq({ includePending: true });
      const poisonClaim = await this.reliability.claimDlq(id);
      if (!poisonClaim) throw new Error('orphan was not returned to pending');
      await this.reliability.poisonDlq(id);
      const requeued = await this.reliability.requeueDead(id);
      const requeuedEntry = await this.reliability.readDlq(id);
      const deleteClaim = await this.reliability.claimDlq(id);
      if (!deleteClaim) throw new Error('requeued DLQ was not claimable');
      await this.reliability.poisonDlq(id);
      await this.reliability.deleteDead(id);
      return json({
        id,
        claims: Number(Boolean(first)) + Number(Boolean(second)),
        orphanPending: pending.map((e) => e.id),
        requeued,
        attemptsAfterRequeue: requeuedEntry?.attempts,
        deleted: await this.reliability.readDlq(id),
      });
    }
    if (url.pathname === '/orphan') {
      const id = await this.reliability.writeDlq('tenant-a', [{
        id: randomUUIDv7(),
        actorId: 'worker-user',
        action: 'orphan',
        createdAt: new Date().toISOString(),
      }]);
      const claimed = await this.reliability.claimDlq(id);
      if (!claimed) throw new Error('could not create processing orphan');
      return json({ id });
    }
    if (url.pathname === '/state') {
      const stats = await this.audit.getStatsDetailed();
      const alarm = await this.state.storage.getAlarm?.();
      const row = this.state.storage.sql.exec(
        'SELECT COUNT(*) AS c FROM audit_test_journal WHERE acked = 0',
      ).one?.();
      return json({ ...stats, delivered: this.delivered, alarm, journalRows: Number(row?.c ?? 0) });
    }
    if (url.pathname === '/dlq') {
      return json(await this.audit.listDlq({ includePending: true, includeProcessing: true, includeDead: true }));
    }
    return new Response('not found', { status: 404 });
  }

  async alarm(): Promise<void> {
    await this.audit.runMaintenance();
  }
}

export default {
  async fetch(request: Request, env: { AUDIT: DurableObjectNamespace }) {
    return env.AUDIT.get(env.AUDIT.idFromName('singleton')).fetch(request);
  },
};
