import { Database } from 'bun:sqlite';
import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from './base';

const CREATE_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS audit_logs (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT,
    actor_id    TEXT NOT NULL,
    action      TEXT NOT NULL,
    entity_id   TEXT,
    old_values  TEXT,
    new_values  TEXT,
    metadata    TEXT,
    ip_address  TEXT,
    user_agent  TEXT,
    created_at  TEXT NOT NULL
  )
`;

const CREATE_INDEXES_SQL = [
  `CREATE INDEX IF NOT EXISTS idx_audit_tenant_created ON audit_logs (tenant_id, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_logs (action)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_logs (actor_id)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_logs (entity_id)`,
];

const INSERT_SQL = `
  INSERT OR IGNORE INTO audit_logs (id, tenant_id, actor_id, action, entity_id, old_values, new_values, metadata, ip_address, user_agent, created_at)
  VALUES ($id, $tenant_id, $actor_id, $action, $entity_id, $old_values, $new_values, $metadata, $ip_address, $user_agent, $created_at)
`;

export interface BunSQLiteAdapterConfig {
  /** Path to the SQLite database file. Default: '.logbun/audit.db' */
  path?: string;
}

/**
 * BunSQLiteAdapter — zero-dependency adapter using bun:sqlite.
 *
 * Best for: development, single-instance deployments, or when you
 * want zero external dependencies.
 */
export class BunSQLiteAdapter implements IAdapter {
  private db: Database | null = null;
  private readonly dbPath: string;

  constructor(config?: BunSQLiteAdapterConfig) {
    this.dbPath = config?.path ?? '.logbun/audit.db';
  }

  async init(): Promise<void> {
    // Ensure directory exists
    const dir = this.dbPath.substring(0, this.dbPath.lastIndexOf('/'));
    if (dir) {
      const { mkdir } = await import('node:fs/promises');
      await mkdir(dir, { recursive: true });
    }

    this.db = new Database(this.dbPath);
    this.db.run('PRAGMA journal_mode = WAL');
    this.db.run('PRAGMA synchronous = NORMAL');
    this.db.run(CREATE_TABLE_SQL);
    for (const sql of CREATE_INDEXES_SQL) {
      this.db.run(sql);
    }
  }

  async bulkInsert(tenantId: string | null, logs: LogbunLog[]): Promise<boolean> {
    if (!this.db) return false;

    try {
      const insert = this.db.prepare(INSERT_SQL);
      const transaction = this.db.transaction((batch: LogbunLog[]) => {
        for (const log of batch) {
          insert.run({
            $id: log.id,
            $tenant_id: log.tenantId ?? tenantId,
            $actor_id: log.actorId,
            $action: log.action,
            $entity_id: log.entityId ?? null,
            $old_values: log.oldValues ? JSON.stringify(log.oldValues) : null,
            $new_values: log.newValues ? JSON.stringify(log.newValues) : null,
            $metadata: log.metadata ? JSON.stringify(log.metadata) : null,
            $ip_address: log.ipAddress ?? null,
            $user_agent: log.userAgent ?? null,
            $created_at: log.createdAt,
          });
        }
      });
      transaction(logs);
      return true;
    } catch {
      return false;
    }
  }

  async query(
    tenantId: string | null,
    filters: LogbunQueryFilters,
    pagination: { cursor?: string; limit: number }
  ): Promise<LogbunQueryResult> {
    if (!this.db) return { logs: [], nextCursor: null };

    const conditions: string[] = [];
    const params: Record<string, string | number> = {};

    if (tenantId !== null) {
      conditions.push('tenant_id = $tenant_id');
      params['$tenant_id'] = tenantId;
    }
    if (filters.action) {
      conditions.push('action = $action');
      params['$action'] = filters.action;
    }
    if (filters.actorId) {
      conditions.push('actor_id = $actor_id');
      params['$actor_id'] = filters.actorId;
    }
    if (filters.entityId) {
      conditions.push('entity_id = $entity_id');
      params['$entity_id'] = filters.entityId;
    }
    if (filters.startDate) {
      conditions.push('created_at >= $start_date');
      params['$start_date'] = filters.startDate;
    }
    if (filters.endDate) {
      conditions.push('created_at <= $end_date');
      params['$end_date'] = filters.endDate;
    }
    if (pagination.cursor) {
      // UUIDv7 is lexicographically sortable, so descending pages advance with smaller ids.
      conditions.push('id < $cursor');
      params['$cursor'] = pagination.cursor;
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    // Fetch limit + 1 to determine if there are more pages
    const fetchLimit = pagination.limit + 1;
    params['$limit'] = fetchLimit;

    const sql = `SELECT * FROM audit_logs ${where} ORDER BY id DESC LIMIT $limit`;
    const rows = this.db.prepare(sql).all(params) as Record<string, unknown>[];

    const hasMore = rows.length > pagination.limit;
    if (hasMore) rows.pop();

    const logs: LogbunLog[] = rows.map((row) => ({
      id: row['id'] as string,
      tenantId: (row['tenant_id'] as string) ?? undefined,
      actorId: row['actor_id'] as string,
      action: row['action'] as string,
      entityId: (row['entity_id'] as string) ?? undefined,
      oldValues: row['old_values'] ? JSON.parse(row['old_values'] as string) as Record<string, unknown> : undefined,
      newValues: row['new_values'] ? JSON.parse(row['new_values'] as string) as Record<string, unknown> : undefined,
      metadata: row['metadata'] ? JSON.parse(row['metadata'] as string) as Record<string, unknown> : undefined,
      ipAddress: (row['ip_address'] as string) ?? undefined,
      userAgent: (row['user_agent'] as string) ?? undefined,
      createdAt: row['created_at'] as string,
    }));

    const lastLog = logs[logs.length - 1];
    return {
      logs,
      nextCursor: hasMore && lastLog ? lastLog.id : null,
    };
  }

  async prune(days: number): Promise<void> {
    if (!this.db) return;
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    this.db.run('DELETE FROM audit_logs WHERE created_at < ?', [cutoff]);
  }

  async close(): Promise<void> {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }
}
