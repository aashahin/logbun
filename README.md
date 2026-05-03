# Logbun

> Production-ready audit logging for Bun-based multi-tenant SaaS platforms.

Zero runtime dependencies. Type-safe. Fire-and-forget. Crash-resilient.

---

## Features

- **Zero-Latency Logging** — `fire()` never blocks the event loop, never throws
- **Type-Safe Actions** — Generic constraint ensures compile-time validation of action strings
- **Multi-Tenant** — Per-tenant queues, connection pooling, and `database_per_tenant` isolation
- **Crash Resilience** — Write-Ahead Log (WAL) + Dead Letter Queue (DLQ) with automatic recovery
- **Backpressure** — RAM-bounded queues with configurable overflow (DLQ escalation or drop)
- **Exponential Backoff** — Failed flushes retry 3x (1s → 2s → 4s) before DLQ escalation
- **Poison Pill Detection** — Permanently failing batches moved to `.dead` after 10 scan cycles
- **Retention Pruning** — Native `Bun.cron` scheduling with O(1) ClickHouse partition drops
- **Idempotent Inserts** — `INSERT OR IGNORE` prevents duplicates on WAL replay
- **Tree-Shakable** — Adapters and plugins are separate entry points, never bundled unless imported

## Installation

```bash
bun add logbun
```

### Optional Peer Dependencies

Install only what you need:

```bash
# Database adapters (pick one)
bun add @libsql/client       # For TursoAdapter
bun add @clickhouse/client    # For ClickHouseAdapter
# BunSQLiteAdapter has zero deps (uses bun:sqlite)

# Framework plugins (pick one)
bun add elysia                # For ElysiaJS plugin
bun add hono                  # For Hono middleware
```

## Quick Start

```typescript
import { AuditLogger } from 'logbun';
import { BunSQLiteAdapter } from 'logbun/adapters/sqlite';

// Define your action types
type Actions = 'course.created' | 'course.deleted' | 'lesson.updated';

// Initialize
const audit = new AuditLogger<Actions>({
  namespace: 'my-app',
  mode: 'durable',
  adapter: new BunSQLiteAdapter(),
});

// Wait for bootstrap (WAL recovery, DLQ cleanup, etc.)
await audit.ready;

// Fire & forget — never blocks, never throws
audit.fire('course.created', {
  actorId: user.id,
  entityId: course.id,
  newValues: { title: 'Advanced TypeScript' },
});

// Query with cursor-based pagination
const result = await audit.query({
  filters: { action: 'course.deleted', actorId: 'user_42' },
  pagination: { limit: 50 },
});

// Graceful shutdown (call on SIGTERM/SIGINT)
await audit.shutdown();
```

## Adapters

### BunSQLiteAdapter

Zero-dependency adapter using `bun:sqlite`. Best for development and single-instance deployments.

```typescript
import { BunSQLiteAdapter } from 'logbun/adapters/sqlite';

const adapter = new BunSQLiteAdapter({
  path: '.logbun/audit.db', // Default
});
```

### TursoAdapter

Uses `@libsql/client` for Turso/LibSQL databases. Best for multi-tenant SaaS with database-per-tenant isolation and edge deployments.

```typescript
import { TursoAdapter } from 'logbun/adapters/turso';

const adapter = new TursoAdapter({
  url: 'libsql://my-db.turso.io',
  authToken: process.env.TURSO_TOKEN!,
});
```

### ClickHouseAdapter

Optimized for high-volume analytics workloads. Forces `single_database` mode with `PARTITION BY toYYYYMM(created_at)` for physical data locality and O(1) partition-based pruning.

```typescript
import { ClickHouseAdapter } from 'logbun/adapters/clickhouse';

const adapter = new ClickHouseAdapter({
  url: 'http://localhost:8123',
  database: 'analytics',
  username: 'default',
  password: process.env.CH_PASSWORD,
  retentionDays: 90, // TTL safety net
});
```

## Multi-Tenant Configuration

### Single Database (Default)

All tenants share one database. Filtering is done via `tenant_id` column.

```typescript
const audit = new AuditLogger<Actions>({
  namespace: 'my-app',
  adapter: new BunSQLiteAdapter(),
  // tenancy defaults to { mode: 'single_database' }
});
```

### Database Per Tenant

Each tenant gets an isolated database. The LRU connection pool manages adapter instances.

```typescript
const audit = new AuditLogger<Actions>({
  namespace: 'my-app',
  adapter: new TursoAdapter({ url: '...', authToken: '...' }),
  tenancy: {
    mode: 'database_per_tenant',
    resolveConnection: async (tenantId) => ({
      url: `libsql://audit-${tenantId}.turso.io`,
      authToken: process.env.TURSO_TOKEN!,
    }),
  },
  pool: { maxActiveConnections: 50 },
});
```

## Durability Modes

### Volatile (Default)

Logs are buffered in RAM only. Fastest possible — zero disk I/O on `fire()`. Logs are lost if the process crashes before flush.

```typescript
const audit = new AuditLogger<Actions>({
  namespace: 'my-app',
  mode: 'volatile',
  adapter: new BunSQLiteAdapter(),
});
```

### Durable

Every log is appended to a Write-Ahead Log (NDJSON file) before entering the in-memory queue. On crash recovery, the WAL is replayed automatically. Slight I/O overhead per `fire()` call.

```typescript
const audit = new AuditLogger<Actions>({
  namespace: 'my-app',
  mode: 'durable',
  adapter: new BunSQLiteAdapter(),
  batching: {
    onQueueFull: 'dlq', // Required — 'drop' is invalid with durable mode
  },
});
```

## Batching & Backpressure

```typescript
const audit = new AuditLogger<Actions>({
  namespace: 'my-app',
  adapter: new BunSQLiteAdapter(),
  batching: {
    maxSize: 100,        // Flush when queue reaches 100 logs
    flushInterval: 5000, // Or flush every 5 seconds (whichever comes first)
    maxQueueSize: 1000,  // Backpressure threshold
    onQueueFull: 'dlq',  // 'dlq' (persist to disk) or 'drop' (volatile only)
  },
});
```

**Backpressure behavior:**
- `dlq` — Dumps the current queue to a DLQ file on disk, clears RAM, then enqueues the new log. Zero data loss.
- `drop` — Silently discards the incoming log. Only valid in `volatile` mode.

## Retention Pruning

```typescript
const audit = new AuditLogger<Actions>({
  namespace: 'my-app',
  adapter: new ClickHouseAdapter({ url: '...', retentionDays: 90 }),
  retention: {
    days: 90,
    cronExpression: '0 0 * * *', // Daily at midnight UTC (default)
  },
});
```

- **SQLite/Turso**: `DELETE FROM audit_logs WHERE created_at < ?`
- **ClickHouse**: `ALTER TABLE audit_logs DROP PARTITION` (O(1)) + TTL safety net

## Framework Plugins

### ElysiaJS

```typescript
import { Elysia } from 'elysia';
import { auditPlugin } from 'logbun/plugins/elysia';

const app = new Elysia()
  .use(auditPlugin(audit))
  .post('/courses', ({ auditLog, body }) => {
    // IP and User-Agent are auto-extracted from request headers
    auditLog.fire('course.created', {
      actorId: user.id,
      entityId: course.id,
    });
  });
```

### Hono

```typescript
import { Hono } from 'hono';
import { createAuditMiddleware } from 'logbun/plugins/hono';

const app = new Hono();
app.use('*', createAuditMiddleware(audit));

app.post('/courses', (c) => {
  const auditLog = c.get('auditLog');
  auditLog.fire('course.created', {
    actorId: user.id,
    entityId: course.id,
  });
});
```

Both plugins automatically extract the client IP from `X-Forwarded-For` (first entry, proxy-safe) and the `User-Agent` header.

## Querying

Cursor-based pagination using UUIDv7 (lexicographically sortable):

```typescript
// First page
const page1 = await audit.query({
  tenantId: 'tenant_123',
  filters: {
    action: 'course.deleted',
    actorId: 'user_42',
    startDate: '2026-01-01T00:00:00Z',
    endDate: '2026-12-31T23:59:59Z',
  },
  pagination: { limit: 50 },
});

// Next page
if (page1.nextCursor) {
  const page2 = await audit.query({
    tenantId: 'tenant_123',
    filters: { action: 'course.deleted' },
    pagination: { limit: 50, cursor: page1.nextCursor },
  });
}
```

## Graceful Shutdown

```typescript
process.on('SIGTERM', async () => {
  await audit.shutdown();
  process.exit(0);
});
```

Shutdown sequence:
1. Flush all pending in-memory queues
2. Truncate the WAL (all data is now flushed or in DLQ)
3. Stop the retry engine
4. Stop the retention cron
5. Close all adapter connections

## Architecture

```
fire() → [WAL append?] → [Backpressure?] → In-Memory Queue → Flush → Adapter.bulkInsert()
                                  ↓                                         ↓ (failure)
                              DLQ.write()                         Exponential Backoff (3x)
                                  ↑                                         ↓ (all failed)
                           Retry Engine ← ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  DLQ.write()
                              (60s scan)
                                  ↓ (10 failures)
                            .dead (poison pill)
```

### File Layout

```
.logbun/{namespace}/
├── wal/
│   └── current.aof              # Write-Ahead Log (NDJSON, append-only)
└── dlq/
    ├── {tenant}_{ts}_{rand}.batch              # Pending retry
    ├── {tenant}_{ts}_{rand}.batch.processing   # Currently retrying
    └── {tenant}_{ts}_{rand}.batch.dead         # Poison pill (permanent failure)
```

## Error Resilience

| Failure | Behavior |
|---------|----------|
| WAL write fails | Log still queued in-memory (not crash-safe) |
| Adapter `bulkInsert` fails | 3 retries with backoff → DLQ |
| DLQ write fails during backpressure | Queue stays in memory, retries next cycle |
| Adapter + DLQ both fail | Data lost (extremely rare — disk full) |
| `fire()` before `ready` | Logs buffered, enqueued after bootstrap |
| Process crash mid-retry | `.processing` → `.batch` on next startup |
| `shutdown()` called twice | Idempotent no-op |
| Bootstrap fails | Logger enters degraded mode — `fire()` silently drops, `query()` throws |
| WAL truncation fails | Stale entries replayed, safe via `INSERT OR IGNORE` |
| Batch permanently failing | Poisoned to `.dead` after 10 scan-level failures |
| Retention cron fails | ClickHouse TTL acts as safety net |

## Custom Adapters

Implement the `IAdapter` interface to create your own adapter:

```typescript
import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from 'logbun';

class MyCustomAdapter implements IAdapter {
  async init(): Promise<void> { /* Create tables, connect, etc. */ }

  async bulkInsert(tenantId: string | null, logs: LogbunLog[]): Promise<boolean> {
    // Return true on success, false to route to DLQ
  }

  async query(
    tenantId: string | null,
    filters: LogbunQueryFilters,
    pagination: { cursor?: string; limit: number }
  ): Promise<LogbunQueryResult> {
    // Return { logs, nextCursor }
  }

  async prune(days: number): Promise<void> { /* Delete old records */ }
  async close(): Promise<void> { /* Clean up connections */ }
}
```

## API Reference

### `AuditLogger<TActions>`

| Method | Returns | Description |
|--------|---------|-------------|
| `new AuditLogger(config)` | `AuditLogger` | Creates and bootstraps the logger |
| `.ready` | `Promise<void>` | Resolves when bootstrap completes |
| `.fire(action, input, context?)` | `void` | Fire & forget — never blocks, never throws |
| `.query(opts)` | `Promise<LogbunQueryResult>` | Cursor-based query with filters |
| `.shutdown()` | `Promise<void>` | Graceful shutdown — flushes everything |

### `LogbunConfig<TActions>`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `namespace` | `string` | — | **Required.** Isolates WAL/DLQ files per instance |
| `mode` | `'volatile' \| 'durable'` | `'volatile'` | Durability mode |
| `adapter` | `IAdapter` | — | **Required.** Database adapter |
| `tenancy` | `TenancyConfig` | `{ mode: 'single_database' }` | Multi-tenancy mode |
| `batching` | `Partial<BatchingConfig>` | See below | Batching configuration |
| `retention` | `RetentionConfig` | — | Retention pruning schedule |
| `pool` | `{ maxActiveConnections?: number }` | `{ maxActiveConnections: 50 }` | Connection pool size |

### `BatchingConfig`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxSize` | `number` | `100` | Flush when queue reaches this count |
| `flushInterval` | `number` | `5000` | Flush after this many ms |
| `maxQueueSize` | `number` | `1000` | Backpressure threshold |
| `onQueueFull` | `'dlq' \| 'drop'` | `'dlq'` | Overflow behavior |

## Requirements

- **Bun** ≥ 1.0 (uses `Bun.file().writer()`, `Bun.randomUUIDv7()`, `Bun.cron()`)
- **TypeScript** ≥ 5.0

## License

MIT
