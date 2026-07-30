import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from './base';
import { safeJsonParse } from '../utils/json';

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
  /**
   * When false, queries skip FINAL (faster, may show pre-merge duplicates).
   * @default true
   */
  queryFinal?: boolean;
}

/** Minimal interface for @clickhouse/client — avoids hard dependency on the package types */
interface ClickHouseClient {
  command(opts: { query: string }): Promise<unknown>;
  insert(opts: { table: string; values: Record<string, unknown>[]; format: string }): Promise<unknown>;
  query(opts: { query: string; query_params?: Record<string, unknown>; format: string }): Promise<{ json<T>(): Promise<T> }>;
  close(): Promise<void>;
}

/**
 * Convert ISO-8601 (or similar) timestamps to ClickHouse DateTime64 text format:
 * 'YYYY-MM-DD HH:mm:ss.SSS' (UTC).
 */
export function toClickHouseDateTime(iso: string): string {
  const d = new Date(iso);
  if (!Number.isNaN(d.getTime())) {
    const pad = (n: number, len = 2) => String(n).padStart(len, '0');
    return (
      `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ` +
      `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}.` +
      `${pad(d.getUTCMilliseconds(), 3)}`
    );
  }
  return iso.replace('T', ' ').replace(/Z$/i, '').replace(/([+-]\d{2}:\d{2})$/, '').slice(0, 23);
}

/**
 * ClickHouseAdapter — optimized for high-volume analytics workloads.
 *
 * Forces single_database mode regardless of config — ClickHouse uses
 * PARTITION BY toYYYYMM(created_at) for physical data locality.
 *
 * Idempotency: ReplacingMergeTree with ORDER BY (tenant_id, id) collapses
 * duplicate rows that share the same sorting key (same tenant + log id).
 *
 * @requires @clickhouse/client — install as peer dependency
 */
export class ClickHouseAdapter implements IAdapter {
  private client: ClickHouseClient | null = null;
  private readonly config: ClickHouseAdapterConfig;
  private readonly database: string;

  constructor(config: ClickHouseAdapterConfig) {
    this.config = config;
    this.database = config.database ?? 'default';
  }

  async init(): Promise<void> {
    // @ts-expect-error — @clickhouse/client is an optional peer dependency
    const mod = await import('@clickhouse/client');
    this.client = (mod as { createClient: (config: Record<string, unknown>) => ClickHouseClient }).createClient({
      url: this.config.url,
      database: this.database,
      username: this.config.username ?? 'default',
      password: this.config.password,
    });

    const retentionDays = this.config.retentionDays ?? 90;

    // CREATE TABLE IF NOT EXISTS is intentionally no-migrate for engine/schema:
    // if audit_logs already exists (any engine), this is a no-op. Operators must
    // migrate manually for engine changes (e.g. old MergeTree → ReplacingMergeTree).
    // We keep ReplacingMergeTree for idempotent inserts (ORDER BY tenant_id, id).
    // Query path uses FINAL by default (queryFinal !== false) for correct
    // post-merge reads; disable only when eventual consistency is acceptable.
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
          created_at   DateTime64(3, 'UTC'),
          prev_hash    Nullable(String),
          content_hash Nullable(String)
        )
        ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(created_at)
        ORDER BY (tenant_id, id)
        TTL created_at + INTERVAL ${retentionDays} DAY DELETE
      `,
    });

    // Best-effort column migrate for tables created before integrity fields.
    // CREATE TABLE IF NOT EXISTS is a no-op on existing schemas, so ADD COLUMN
    // is required when integrityChain is enabled later. Swallow failures
    // (column exists, older CH without IF NOT EXISTS, or insufficient grants).
    for (const sql of [
      'ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS prev_hash Nullable(String)',
      'ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS content_hash Nullable(String)',
    ]) {
      try {
        await this.client.command({ query: sql });
      } catch {
        // column already exists / permission / version differences
      }
    }
  }

  async bulkInsert(tenantId: string | null, logs: LogbunLog[]): Promise<boolean> {
    if (logs.length === 0) return true;
    if (!this.client) {
      throw new Error('ClickHouseAdapter not initialized');
    }

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
        created_at: toClickHouseDateTime(log.createdAt),
        prev_hash: log.prevHash ?? null,
        content_hash: log.contentHash ?? null,
      }));

      await this.client.insert({
        table: 'audit_logs',
        values: rows,
        format: 'JSONEachRow',
      });

      return true;
    } catch (err) {
      throw err instanceof Error
        ? err
        : new Error(`ClickHouseAdapter.bulkInsert failed: ${String(err)}`);
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
      conditions.push('created_at >= {start_date:DateTime64(3)}');
      params['start_date'] = toClickHouseDateTime(filters.startDate);
    }
    if (filters.endDate) {
      conditions.push('created_at <= {end_date:DateTime64(3)}');
      params['end_date'] = toClickHouseDateTime(filters.endDate);
    }
    if (pagination.cursor) {
      conditions.push('id < {cursor:String}');
      params['cursor'] = pagination.cursor;
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const fetchLimit = pagination.limit + 1;
    params['fetch_limit'] = fetchLimit;

    const useFinal = this.config.queryFinal !== false;
    const fromClause = useFinal ? 'audit_logs FINAL' : 'audit_logs';

    const result = await this.client.query({
      query: `SELECT * FROM ${fromClause} ${where} ORDER BY id DESC LIMIT {fetch_limit:UInt32}`,
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
      oldValues: safeJsonParse<Record<string, unknown>>(row['old_values']),
      newValues: safeJsonParse<Record<string, unknown>>(row['new_values']),
      metadata: safeJsonParse<Record<string, unknown>>(row['metadata']),
      ipAddress: (row['ip_address'] as string) ?? undefined,
      userAgent: (row['user_agent'] as string) ?? undefined,
      createdAt: String(row['created_at'] ?? ''),
      prevHash: (row['prev_hash'] as string) || undefined,
      contentHash: (row['content_hash'] as string) || undefined,
    }));

    const lastLog = logs[logs.length - 1];
    return {
      logs,
      nextCursor: hasMore && lastLog ? lastLog.id : null,
    };
  }

  /**
   * Prune expired data using ALTER TABLE DROP PARTITION for O(1) removal.
   *
   * Month granularity: partitions are toYYYYMM, so we only DROP whole months
   * strictly older than the cutoff month (partitionNum < cutoffYYYYMM). The
   * current cutoff month is left intact — partial-month cleanup is handled by
   * the table TTL (created_at + INTERVAL retentionDays DAY DELETE) as a safety
   * net. Partition names are validated as YYYYMM; database is scoped. Cutoff uses UTC.
   */
  async prune(days: number): Promise<void> {
    if (!this.client) return;

    const cutoffDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    // YYYYMM safety: only numeric 6-digit partitions strictly before cutoff month
    const cutoffYYYYMM =
      cutoffDate.getUTCFullYear() * 100 + (cutoffDate.getUTCMonth() + 1);

    const result = await this.client.query({
      query: `
        SELECT DISTINCT partition
        FROM system.parts
        WHERE database = {db:String}
          AND table = 'audit_logs'
          AND active = 1
      `,
      query_params: { db: this.database },
      format: 'JSONEachRow',
    });

    const partitions = await result.json<{ partition: string }[]>();

    for (const { partition } of partitions) {
      // YYYYMM safety: reject non-matching partition names
      if (!/^\d{6}$/.test(partition)) continue;
      const partitionNum = parseInt(partition, 10);
      if (!isNaN(partitionNum) && partitionNum < cutoffYYYYMM) {
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
