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

export interface TursoAdapterConfig {
  /** Turso database URL */
  url: string;
  /** Turso auth token */
  authToken: string;
}

/** Minimal interface for @libsql/client — avoids hard dependency on the package types */
interface LibSQLClient {
  execute(stmtOrSql: string | { sql: string; args: unknown[] }): Promise<{
    rows: Record<string, unknown>[];
    rowsAffected?: number;
  }>;
  batch(stmts: { sql: string; args: unknown[] }[], mode?: string): Promise<unknown>;
  close(): void;
}

/**
 * TursoAdapter — uses @libsql/client for Turso/LibSQL databases.
 *
 * Best for: multi-tenant SaaS with database-per-tenant isolation,
 * edge deployments, or when you need embedded replicas.
 *
 * @requires @libsql/client — install as peer dependency
 */
export class TursoAdapter implements IAdapter {
  private client: LibSQLClient | null = null;
  private readonly config: TursoAdapterConfig;

  constructor(config: TursoAdapterConfig) {
    this.config = config;
  }

  async init(): Promise<void> {
    // @ts-expect-error — @libsql/client is an optional peer dependency
    const mod = await import('@libsql/client');
    this.client = (mod as { createClient: (config: TursoAdapterConfig) => LibSQLClient }).createClient({
      url: this.config.url,
      authToken: this.config.authToken,
    });

    await this.client.execute(CREATE_TABLE_SQL);
    for (const sql of MIGRATE_COLUMNS_SQL) {
      try {
        await this.client.execute(sql);
      } catch {
        // column already exists
      }
    }
    for (const sql of CREATE_INDEXES_SQL) {
      await this.client.execute(sql);
    }
  }

  async bulkInsert(tenantId: string | null, logs: LogbunLog[]): Promise<boolean> {
    if (logs.length === 0) return true;
    if (!this.client) {
      throw new Error('TursoAdapter not initialized');
    }

    try {
      const statements = logs.map((log) => ({
        sql: `INSERT OR IGNORE INTO audit_logs (id, tenant_id, actor_id, action, entity_id, old_values, new_values, metadata, ip_address, user_agent, created_at, prev_hash, content_hash)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          log.id,
          log.tenantId ?? tenantId ?? null,
          log.actorId,
          log.action,
          log.entityId ?? null,
          log.oldValues ? JSON.stringify(log.oldValues) : null,
          log.newValues ? JSON.stringify(log.newValues) : null,
          log.metadata ? JSON.stringify(log.metadata) : null,
          log.ipAddress ?? null,
          log.userAgent ?? null,
          log.createdAt,
          log.prevHash ?? null,
          log.contentHash ?? null,
        ] as unknown[],
      }));

      await this.client.batch(statements, 'write');
      return true;
    } catch (err) {
      throw err instanceof Error
        ? err
        : new Error(`TursoAdapter.bulkInsert failed: ${String(err)}`);
    }
  }

  async query(
    tenantId: string | null,
    filters: LogbunQueryFilters,
    pagination: { cursor?: string; limit: number }
  ): Promise<LogbunQueryResult> {
    if (!this.client) return { logs: [], nextCursor: null };

    const conditions: string[] = [];
    const args: (string | number | null)[] = [];

    if (tenantId !== null) {
      conditions.push('tenant_id = ?');
      args.push(tenantId);
    }
    if (filters.action) {
      conditions.push('action = ?');
      args.push(filters.action);
    }
    if (filters.actorId) {
      conditions.push('actor_id = ?');
      args.push(filters.actorId);
    }
    if (filters.entityId) {
      conditions.push('entity_id = ?');
      args.push(filters.entityId);
    }
    if (filters.startDate) {
      conditions.push('created_at >= ?');
      args.push(filters.startDate);
    }
    if (filters.endDate) {
      conditions.push('created_at <= ?');
      args.push(filters.endDate);
    }
    if (pagination.cursor) {
      conditions.push('id < ?');
      args.push(pagination.cursor);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const fetchLimit = pagination.limit + 1;
    args.push(fetchLimit);

    const sql = `SELECT * FROM audit_logs ${where} ORDER BY id DESC LIMIT ?`;
    const result = await this.client.execute({ sql, args });

    const rows = result.rows;
    const hasMore = rows.length > pagination.limit;
    if (hasMore) rows.pop();

    const logs: LogbunLog[] = rows.map((row: Record<string, unknown>) => ({
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
    }));

    const lastLog = logs[logs.length - 1];
    return {
      logs,
      nextCursor: hasMore && lastLog ? lastLog.id : null,
    };
  }

  /**
   * Delete logs older than `days` in batches to avoid long write locks / large txns.
   * LibSQL/SQLite supports `DELETE ... WHERE id IN (SELECT ... LIMIT n)`.
   * If the driver omits `rowsAffected`, falls back to a single unbatched DELETE
   * for any remaining rows (comment: single DELETE is acceptable when batch
   * progress cannot be observed).
   * Safety cap: 10_000 batches × 1_000 rows.
   */
  async prune(days: number): Promise<void> {
    if (!this.client) return;
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    const BATCH = 1_000;
    const MAX_BATCHES = 10_000;
    const sql = `DELETE FROM audit_logs WHERE created_at < ? AND id IN (
      SELECT id FROM audit_logs WHERE created_at < ? LIMIT ?
    )`;

    for (let i = 0; i < MAX_BATCHES; i++) {
      const result = await this.client.execute({
        sql,
        args: [cutoff, cutoff, BATCH],
      });
      const affected = result.rowsAffected;
      if (affected === undefined) {
        // Driver did not report rowsAffected — finish with one-shot DELETE.
        await this.client.execute({
          sql: 'DELETE FROM audit_logs WHERE created_at < ?',
          args: [cutoff],
        });
        return;
      }
      if (affected === 0) break;
    }
  }

  async close(): Promise<void> {
    if (this.client) {
      this.client.close();
      this.client = null;
    }
  }
}
