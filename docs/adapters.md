# Adapters

Adapters implement `IAdapter` and are **tree-shaken** via package subpath exports. Install only the peer deps you need.

## BunSQLiteAdapter

```typescript
import { BunSQLiteAdapter } from 'logbun/adapters/sqlite';

const adapter = new BunSQLiteAdapter({
  path: '.logbun/audit.db',      // default
  synchronous: 'FULL',           // FULL | NORMAL | OFF — default FULL
  busyTimeoutMs: 5_000,          // default 5000
});
```

| | |
|--|--|
| **Deps** | None (`bun:sqlite`) |
| **Best for** | Dev, single-instance |
| **Idempotency** | `INSERT OR IGNORE` on primary key `id` |
| **Journal** | `PRAGMA journal_mode = WAL` |
| **Prune** | `DELETE WHERE created_at < cutoff` |
| **Indexes** | `(tenant_id, created_at)`, `(tenant_id, id)`, `(tenant_id, action, created_at)`, `(tenant_id, actor_id, created_at)`, `(tenant_id, entity_id)`, plus standalone action/actor/entity |
| **Integrity** | Columns `prev_hash`, `content_hash`; best-effort `ALTER TABLE … ADD COLUMN` for older DBs |

**Not multi-writer HA.** Multiple processes must not share one SQLite file as concurrent writers.

Parent directories for `path` are created with `dirname` (Windows-safe).

---

## TursoAdapter

```typescript
import { TursoAdapter } from 'logbun/adapters/turso';

const adapter = new TursoAdapter({
  url: 'libsql://my-db.turso.io',
  authToken: process.env.TURSO_TOKEN!,
});
```

| | |
|--|--|
| **Peer** | `@libsql/client` ≥ 0.6 |
| **Best for** | Multi-tenant SaaS, edge, `database_per_tenant` |
| **Idempotency** | `INSERT OR IGNORE` |
| **Writes** | `client.batch(..., 'write')` |
| **Prune** | Same DELETE pattern as SQLite |
| **Indexes** | Same composite set as SQLite |
| **Integrity** | Same `prev_hash` / `content_hash` columns + best-effort migrate as SQLite |

### database_per_tenant example

```typescript
const audit = new AuditLogger({
  namespace: process.env.INSTANCE_ID!,
  mode: 'durable',
  adapter: new TursoAdapter({
    url: process.env.TURSO_URL!,
    authToken: process.env.TURSO_TOKEN!,
  }),
  tenancy: {
    mode: 'database_per_tenant',
    resolveConnection: async (tenantId) => ({
      url: `libsql://audit-${tenantId}.turso.io`,
      authToken: process.env.TURSO_TOKEN!,
    }),
  },
  adapterFactory: (config) =>
    new TursoAdapter({
      url: String(config.url),
      authToken: String(config.authToken),
    }),
  pool: { maxActiveConnections: 50 },
  requireTenantId: true,
});
```

---

## ClickHouseAdapter

```typescript
import { ClickHouseAdapter } from 'logbun/adapters/clickhouse';

const adapter = new ClickHouseAdapter({
  url: 'http://localhost:8123',
  database: 'analytics',       // default 'default'
  username: 'default',
  password: process.env.CH_PASSWORD,
  retentionDays: 90,           // TTL on CREATE
  queryFinal: true,            // default true — deduped reads
});
```

| | |
|--|--|
| **Peer** | `@clickhouse/client` ≥ 1.0 |
| **Best for** | High-volume analytics / audit warehouse |
| **Engine** | `ReplacingMergeTree` · `ORDER BY (tenant_id, id)` · `PARTITION BY toYYYYMM(created_at)` |
| **Idempotency** | Sorting-key dedup after merges; query uses `FINAL` by default |
| **Prune** | Drop partitions with YYYYMM &lt; cutoff month (UTC) + TTL safety net |
| **Timestamps** | ISO → `YYYY-MM-DD HH:mm:ss.SSS` UTC via `toClickHouseDateTime` |
| **Integrity** | Nullable `prev_hash` / `content_hash`; best-effort `ADD COLUMN IF NOT EXISTS` for older tables |

### Notes

- `CREATE TABLE IF NOT EXISTS` does **not** migrate an existing MergeTree **engine**. Old installs need a manual migration to ReplacingMergeTree.
- Integrity **columns** may be added best-effort on existing tables; engine changes still require ops.
- Partition drops are **month-granular**; TTL is the fine-grained safety net.
- Prefer **single shared ClickHouse database** (not one DB per tenant) for this adapter’s design.

Exported helper:

```typescript
import { toClickHouseDateTime } from 'logbun/adapters/clickhouse';
```

---

## Custom adapters

```typescript
import type {
  IAdapter,
  LogbunLog,
  LogbunQueryFilters,
  LogbunQueryResult,
} from 'logbun';

class PostgresAuditAdapter implements IAdapter {
  async init(): Promise<void> {
    // migrations, pool connect
  }

  async bulkInsert(
    tenantId: string | null,
    logs: LogbunLog[],
  ): Promise<boolean> {
    // Prefer idempotent insert on log.id
    // return true on success; false or throw on failure
    return true;
  }

  async query(
    tenantId: string | null,
    filters: LogbunQueryFilters,
    pagination: { cursor?: string; limit: number },
  ): Promise<LogbunQueryResult> {
    return { logs: [], nextCursor: null };
  }

  async prune(days: number): Promise<void> {
    // delete older than days
  }

  async close(): Promise<void> {
    // pool end
  }
}
```

### Adapter contract tips

1. **Idempotent on `id`** so WAL replay / retries do not duplicate rows.
2. **Honor `tenantId` filter** on query when non-null.
3. **Cursor**: engine expects newest-first pages and `nextCursor` = last row’s `id` when more pages exist.
4. **Empty `bulkInsert`**: return `true`.
5. **Integrity fields**: when `integrityChain` is enabled, logs carry optional `prevHash` / `contentHash` — persist and round-trip them if you need verify after read.
