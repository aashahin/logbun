import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from './base';

export interface ClickHouseAdapterConfig {
  /** ClickHouse server URL (e.g., 'http://localhost:8123') */
  url: string;
  /** ClickHouse database name. Default: 'default' */
  database?: string;
  /** ClickHouse username. Default: 'default' */
  username?: string;
  /** ClickHouse password */
  password?: string;
  /** Retention days for TTL. Default: 90 */
  retentionDays?: number;
}

/** Minimal interface for @clickhouse/client — avoids hard dependency on the package types */
interface ClickHouseClient {
  command(opts: { query: string }): Promise<unknown>;
  insert(opts: { table: string; values: Record<string, unknown>[]; format: string }): Promise<unknown>;
  query(opts: { query: string; query_params?: Record<string, unknown>; format: string }): Promise<{ json<T>(): Promise<T> }>;
  close(): Promise<void>;
}

/**
 * ClickHouseAdapter — optimized for high-volume analytics workloads.
 *
 * Forces single_database mode regardless of config — ClickHouse uses
 * PARTITION BY toYYYYMM(created_at) for physical tenant data locality
 * instead of database-per-tenant (which exhausts ZooKeeper metadata).
 *
 * @requires @clickhouse/client — install as peer dependency
 */
export class ClickHouseAdapter implements IAdapter {
  private client: ClickHouseClient | null = null;
  private readonly config: ClickHouseAdapterConfig;

  constructor(config: ClickHouseAdapterConfig) {
    this.config = config;
  }

  async init(): Promise<void> {
    // @ts-expect-error — @clickhouse/client is an optional peer dependency
    const mod = await import('@clickhouse/client');
    this.client = (mod as { createClient: (config: Record<string, unknown>) => ClickHouseClient }).createClient({
      url: this.config.url,
      database: this.config.database ?? 'default',
      username: this.config.username ?? 'default',
      password: this.config.password,
    });

    const retentionDays = this.config.retentionDays ?? 90;

    await this.client.command({
      query: `
        CREATE TABLE IF NOT EXISTS audit_logs (
          id           String,
          tenant_id    String DEFAULT '',
          actor_id     String,
          action       String,
          entity_id    Nullable(String),
          old_values   Nullable(String),
          new_values   Nullable(String),
          metadata     Nullable(String),
          ip_address   Nullable(String),
          user_agent   Nullable(String),
          created_at   DateTime64(3, 'UTC')
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(created_at)
        ORDER BY (tenant_id, created_at)
        TTL created_at + INTERVAL ${retentionDays} DAY DELETE
      `,
    });
  }

  async bulkInsert(tenantId: string | null, logs: LogbunLog[]): Promise<boolean> {
    if (!this.client) return false;

    try {
      const rows = logs.map((log) => ({
        id: log.id,
        tenant_id: log.tenantId ?? tenantId ?? '',
        actor_id: log.actorId,
        action: log.action,
        entity_id: log.entityId ?? null,
        old_values: log.oldValues ? JSON.stringify(log.oldValues) : null,
        new_values: log.newValues ? JSON.stringify(log.newValues) : null,
        metadata: log.metadata ? JSON.stringify(log.metadata) : null,
        ip_address: log.ipAddress ?? null,
        user_agent: log.userAgent ?? null,
        created_at: log.createdAt,
      }));

      await this.client.insert({
        table: 'audit_logs',
        values: rows,
        format: 'JSONEachRow',
      });

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
    if (!this.client) return { logs: [], nextCursor: null };

    const conditions: string[] = [];
    const params: Record<string, string | number> = {};

    if (tenantId !== null) {
      conditions.push('tenant_id = {tenant_id:String}');
      params['tenant_id'] = tenantId;
    } else {
      // In single_database mode with no tenant, don't filter by tenant_id
    }
    if (filters.action) {
      conditions.push('action = {action:String}');
      params['action'] = filters.action;
    }
    if (filters.actorId) {
      conditions.push('actor_id = {actor_id:String}');
      params['actor_id'] = filters.actorId;
    }
    if (filters.entityId) {
      conditions.push('entity_id = {entity_id:String}');
      params['entity_id'] = filters.entityId;
    }
    if (filters.startDate) {
      conditions.push('created_at >= {start_date:DateTime}');
      params['start_date'] = filters.startDate;
    }
    if (filters.endDate) {
      conditions.push('created_at <= {end_date:DateTime}');
      params['end_date'] = filters.endDate;
    }
    if (pagination.cursor) {
      conditions.push('id < {cursor:String}');
      params['cursor'] = pagination.cursor;
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const fetchLimit = pagination.limit + 1;
    params['fetch_limit'] = fetchLimit;

    const result = await this.client.query({
      query: `SELECT * FROM audit_logs ${where} ORDER BY id DESC LIMIT {fetch_limit:UInt32}`,
      query_params: params,
      format: 'JSONEachRow',
    });

    const rows = await result.json<Record<string, unknown>[]>();
    const hasMore = rows.length > pagination.limit;
    if (hasMore) rows.pop();

    const logs: LogbunLog[] = rows.map((row: Record<string, unknown>) => ({
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

  /**
   * Prune expired data using ALTER TABLE DROP PARTITION for O(1) removal.
   * Falls back to TTL as secondary safety net.
   */
  async prune(days: number): Promise<void> {
    if (!this.client) return;

    // Calculate the oldest year-month partition that should be kept
    const cutoffDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    const cutoffYYYYMM = cutoffDate.getFullYear() * 100 + (cutoffDate.getMonth() + 1);

    // Get all existing partitions
    const result = await this.client.query({
      query: `SELECT DISTINCT partition FROM system.parts WHERE table = 'audit_logs' AND active = 1`,
      format: 'JSONEachRow',
    });

    const partitions = await result.json<{ partition: string }[]>();

    // Drop partitions that are fully expired
    for (const { partition } of partitions) {
      const partitionNum = parseInt(partition, 10);
      // Validate partition format (YYYYMM) to prevent injection
      if (!isNaN(partitionNum) && partitionNum < cutoffYYYYMM && /^\d{6}$/.test(partition)) {
        await this.client.command({
          query: `ALTER TABLE audit_logs DROP PARTITION '${partition}'`,
        });
      }
    }
  }

  async close(): Promise<void> {
    if (this.client) {
      await this.client.close();
      this.client = null;
    }
  }
}
