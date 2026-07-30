import { Database } from 'bun:sqlite';
import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from './base';
import { safeJsonParse } from '../utils/json';

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
    created_at  TEXT NOT NULL,
    prev_hash   TEXT,
    content_hash TEXT
  )
`;

/** Best-effort add columns for DBs created before integrity fields existed. */
const MIGRATE_COLUMNS_SQL = [
  `ALTER TABLE audit_logs ADD COLUMN prev_hash TEXT`,
  `ALTER TABLE audit_logs ADD COLUMN content_hash TEXT`,
];

const CREATE_INDEXES_SQL = [
  `CREATE INDEX IF NOT EXISTS idx_audit_tenant_created ON audit_logs (tenant_id, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_tenant_id ON audit_logs (tenant_id, id DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_logs (action)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_logs (actor_id)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_logs (entity_id)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_tenant_action_created ON audit_logs (tenant_id, action, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_tenant_actor_created ON audit_logs (tenant_id, actor_id, created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_tenant_entity ON audit_logs (tenant_id, entity_id)`,
];

const INSERT_SQL = `
  INSERT OR IGNORE INTO audit_logs (id, tenant_id, actor_id, action, entity_id, old_values, new_values, metadata, ip_address, user_agent, created_at, prev_hash, content_hash)
  VALUES ($id, $tenant_id, $actor_id, $action, $entity_id, $old_values, $new_values, $metadata, $ip_address, $user_agent, $created_at, $prev_hash, $content_hash)
`;

export interface BunSQLiteAdapterConfig {
  /** Path to the SQLite database file. Default: '.logbun/audit.db' */
  path?: string;
  /**
   * SQLite synchronous mode.
   * - `FULL` — safer for audit durability (default)
   * - `NORMAL` — faster, weaker power-loss guarantees
   */
  synchronous?: 'FULL' | 'NORMAL' | 'OFF';
  /** Busy timeout in ms when the DB is locked. @default 5000 */
  busyTimeoutMs?: number;
}

/**
 * BunSQLiteAdapter — zero-dependency adapter using bun:sqlite.
 *
 * Best for: development, single-instance deployments, or when you
 * want zero external dependencies. Not multi-writer HA.
 */
export class BunSQLiteAdapter implements IAdapter {
  private db: Database | null = null;
  private readonly dbPath: string;
  private readonly synchronous: 'FULL' | 'NORMAL' | 'OFF';
  private readonly busyTimeoutMs: number;

  constructor(config?: BunSQLiteAdapterConfig) {
    this.dbPath = config?.path ?? '.logbun/audit.db';
    this.synchronous = config?.synchronous ?? 'FULL';
    this.busyTimeoutMs = config?.busyTimeoutMs ?? 5_000;
  }

  async init(): Promise<void> {
    const { dirname } = await import('node:path');
    const dir = dirname(this.dbPath);
    // dirname('.') === '.' and dirname('file.db') === '.' — skip mkdir for cwd
    if (dir && dir !== '.') {
      const { mkdir } = await import('node:fs/promises');
      await mkdir(dir, { recursive: true });
    }

    this.db = new Database(this.dbPath);
    this.db.run('PRAGMA journal_mode = WAL');
    this.db.run(`PRAGMA synchronous = ${this.synchronous}`);
    this.db.run(`PRAGMA busy_timeout = ${Math.max(0, this.busyTimeoutMs)}`);
    this.db.run(CREATE_TABLE_SQL);
    for (const sql of MIGRATE_COLUMNS_SQL) {
      try {
        this.db.run(sql);
      } catch {
        // column already exists
      }
    }
    for (const sql of CREATE_INDEXES_SQL) {
      this.db.run(sql);
    }
  }

  async bulkInsert(tenantId: string | null, logs: LogbunLog[]): Promise<boolean> {
    if (logs.length === 0) return true;
    if (!this.db) {
      throw new Error('BunSQLiteAdapter not initialized');
    }

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
            $prev_hash: log.prevHash ?? null,
            $content_hash: log.contentHash ?? null,
          });
        }
      });
      transaction(logs);
      return true;
    } catch (err) {
      throw err instanceof Error
        ? err
        : new Error(`BunSQLiteAdapter.bulkInsert failed: ${String(err)}`);
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
      conditions.push('id < $cursor');
      params['$cursor'] = pagination.cursor;
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const fetchLimit = pagination.limit + 1;
    params['$limit'] = fetchLimit;

    const sql = `SELECT * FROM audit_logs ${where} ORDER BY id DESC LIMIT $limit`;
    const rows = this.db.prepare(sql).all(params) as Record<string, unknown>[];

    const hasMore = rows.length > pagination.limit;
    if (hasMore) rows.pop();

    const logs: LogbunLog[] = rows.map((row) => mapSqliteRow(row));

    const lastLog = logs[logs.length - 1];
    return {
      logs,
      nextCursor: hasMore && lastLog ? lastLog.id : null,
    };
  }

  /**
   * Delete logs older than `days` in batches to avoid long write locks.
   * Uses `DELETE ... WHERE id IN (SELECT ... LIMIT n)` (Bun SQLite supports this).
   * Safety cap: 10_000 batches × 1_000 rows.
   */
  async prune(days: number): Promise<void> {
    if (!this.db) return;
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    const BATCH = 1_000;
    const MAX_BATCHES = 10_000;
    // Prefer id-subquery form: plain DELETE LIMIT is not portable across SQLite builds.
    const stmt = this.db.prepare(
      `DELETE FROM audit_logs WHERE created_at < ? AND id IN (
         SELECT id FROM audit_logs WHERE created_at < ? LIMIT ?
       )`
    );
    for (let i = 0; i < MAX_BATCHES; i++) {
      const result = stmt.run(cutoff, cutoff, BATCH);
      if (result.changes === 0) break;
    }
  }

  async close(): Promise<void> {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }
}

function mapSqliteRow(row: Record<string, unknown>): LogbunLog {
  return {
    id: row['id'] as string,
    tenantId: (row['tenant_id'] as string) ?? undefined,
    actorId: row['actor_id'] as string,
    action: row['action'] as string,
    entityId: (row['entity_id'] as string) ?? undefined,
    oldValues: safeJsonParse<Record<string, unknown>>(row['old_values']),
    newValues: safeJsonParse<Record<string, unknown>>(row['new_values']),
    metadata: safeJsonParse<Record<string, unknown>>(row['metadata']),
    ipAddress: (row['ip_address'] as string) ?? undefined,
    userAgent: (row['user_agent'] as string) ?? undefined,
    createdAt: row['created_at'] as string,
    prevHash: (row['prev_hash'] as string) ?? undefined,
    contentHash: (row['content_hash'] as string) ?? undefined,
  };
}
