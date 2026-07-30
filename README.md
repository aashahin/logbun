# Logbun

Production-oriented **audit logging** for **Bun** multi-tenant SaaS.

Zero runtime dependencies. Type-safe actions. Fire-and-forget or awaitable durable enqueue. Crash-resilient local WAL + DLQ with remote adapters as the multi-replica source of truth.

**Requires Bun ≥ 1.3.0.**

---

## Documentation map

| Doc | Contents |
|-----|----------|
| **This README** | Install, quick start, concepts, production checklist |
| [docs/architecture.md](./docs/architecture.md) | Pipeline, WAL/DLQ, pooling, recovery |
| [docs/configuration.md](./docs/configuration.md) | Full config reference + defaults |
| [docs/api-reference.md](./docs/api-reference.md) | Public API, events, helpers |
| [docs/adapters.md](./docs/adapters.md) | SQLite, Turso, ClickHouse, custom adapters |
| [docs/plugins.md](./docs/plugins.md) | Elysia & Hono, IP trust |
| [docs/production.md](./docs/production.md) | Multi-replica SaaS, ops, failure matrix |
| [docs/changelog-notes.md](./docs/changelog-notes.md) | Full capability inventory of the current tree |

---

## Features

- **`fire()`** — never blocks, never throws (async durability in durable mode)
- **`fireAsync()`** — awaits full enqueue / WAL (or DLQ escalation); may reject
- **Type-safe actions** — generic `AuditLogger<TActions>`
- **Multi-tenant** — shared DB or `database_per_tenant` with pool + `adapterFactory`
- **Crash resilience** — segmented WAL + ack sidecar + DLQ envelopes + poison / requeue
- **Backpressure** — per-tenant queues + global caps + fair-share (largest queues first)
- **Flush concurrency** — global `maxFlushConcurrency` (default 16) around bulkInsert
- **Disk caps** — hard `maxWalBytes` (64 MiB), `maxDlqFiles` (10k)
- **Instance lock** — exclusive per-namespace lock in durable mode (default on)
- **Encryption at rest** — optional AES-256-GCM for local WAL/DLQ (`encryptionKey`)
- **Integrity chain** — optional `prevHash`/`contentHash` + `verifyIntegrity()`
- **Retries** — `insertMaxRetries` **total** attempts with exponential backoff
- **Retention** — `Bun.cron` prune; ClickHouse partition drop + TTL
- **Safety** — redaction (objects + arrays), payload/string caps, query limit, path sanitization
- **Observability** — `onEvent`, `getStats()` / `getStatsDetailed()`, degraded mode
- **Tree-shakable** — adapters and plugins as separate entry points

---

## Installation

```bash
bun add logbun
```

### Optional peers

```bash
bun add @libsql/client        # TursoAdapter
bun add @clickhouse/client    # ClickHouseAdapter
# BunSQLiteAdapter uses bun:sqlite (no peer)

bun add elysia                # logbun/plugins/elysia
bun add hono                  # logbun/plugins/hono
```

### Package exports

```text
logbun
logbun/adapters/sqlite
logbun/adapters/turso
logbun/adapters/clickhouse
logbun/plugins/elysia
logbun/plugins/hono
```

---

## Quick start

```typescript
import { AuditLogger, ENTERPRISE_DEFAULTS } from 'logbun';
import { BunSQLiteAdapter } from 'logbun/adapters/sqlite';

type Actions = 'course.created' | 'course.deleted' | 'lesson.updated';

const audit = new AuditLogger<Actions>({
  ...ENTERPRISE_DEFAULTS, // mode: 'durable', requireTenantId: true
  namespace: process.env.INSTANCE_ID ?? 'my-app', // unique per replica
  adapter: new BunSQLiteAdapter({ path: '.logbun/audit.db' }),
  wal: { fsync: true },
  dlqFsync: true,
  redactPaths: ['password', 'token', 'secret'],
});

await audit.ready;

// Fire-and-forget — never throws; does not wait for WAL
audit.fire('course.created', {
  tenantId: 'tenant_123',
  actorId: user.id,
  entityId: course.id,
  newValues: { title: 'Advanced TypeScript' },
});

// Critical path — await enqueue/WAL before responding to the client
await audit.fireAsync('course.deleted', {
  tenantId: 'tenant_123',
  actorId: user.id,
  entityId: course.id,
});

const page = await audit.query({
  tenantId: 'tenant_123',
  filters: { action: 'course.deleted' },
  pagination: { limit: 50 },
});

const stats = audit.getStats();
// { queued, tenants, degraded, recoveryBacklog, inflightFlushes }

const detailed = await audit.getStatsDetailed();
// + walApproxBytes?, dlqPending?, dlqProcessing?, dlqDead?

await audit.shutdown();
```

### `fire()` vs `fireAsync()`

| API | Throws? | Durability at return |
|-----|---------|----------------------|
| `fire(action, input, context?)` | **Never** | Enqueue/WAL may still be in flight |
| `fireAsync(action, input, context?)` | May reject | Full enqueue path finished (WAL or DLQ path in durable mode) |

`fireAsync` **awaits `ready` first** (no pre-ready buffer). `fire()` before `ready` uses a **RAM-only** pre-ready buffer even in durable mode.

### `getStats` vs `getStatsDetailed`

| API | Cost | Returns |
|-----|------|---------|
| `getStats()` | In-memory | `queued`, `tenants`, `degraded`, `recoveryBacklog`, `inflightFlushes` |
| `getStatsDetailed()` | Best-effort disk | Above + `walApproxBytes?`, `dlqPending?`, `dlqProcessing?`, `dlqDead?` |

---

## Concepts

### Durability modes

| Mode | Default | Behavior |
|------|---------|----------|
| `volatile` | **yes** (back-compat) | RAM queues only; crash loses unflushed logs |
| `durable` | no | Append to local WAL before queue; recover unacked lines on boot |

**Enterprise must set `mode: 'durable'`.** The default remains `volatile` only for back-compat.

Durable mode details:

- WAL fsync (`wal.fsync`, default `true`)
- DLQ fsync (`dlqFsync`, default `true` in durable)
- Segmented WAL (`current.aof` + `seg-*.aof` at `wal.segmentBytes`, default 16 MiB)
- Hard size cap via `maxWalBytes` + `wal.hardMaxBytes` (default refuse → `wal_full` → DLQ)
- WAL append failure → **DLQ or drop** (not silent RAM-only “success”)
- Successful flush / DLQ → **WAL.acknowledge**
- Compact never deletes unacked entries
- Recovery injects in waves (`maxRecoveryBatch`), respects `maxActiveTenants` / `maxTotalQueued`; **excess backlog stays on disk**
- DLQ write cap via `maxDlqFiles` (default 10k)
- **Instance lock** default on (`.instance.lock`) so two processes cannot share the same local WAL/DLQ
- Optional **`encryptionKey`** (AES-256-GCM for local WAL/DLQ) and **`integrityChain`**

`onQueueFull: 'drop'` is **invalid** with durable mode (constructor throws).

### Multi-tenant

**Shared database** (default):

```typescript
new AuditLogger({
  namespace: 'api',
  adapter: new BunSQLiteAdapter(),
  requireTenantId: true, // set true for SaaS to avoid cross-tenant queries
});
```

**Database per tenant:**

```typescript
new AuditLogger({
  namespace: process.env.INSTANCE_ID!,
  adapter: new TursoAdapter({ url: '...', authToken: '...' }),
  tenancy: {
    mode: 'database_per_tenant',
    resolveConnection: async (tenantId) => ({
      url: `libsql://audit-${tenantId}.turso.io`,
      authToken: process.env.TURSO_TOKEN!,
    }),
    knownTenantIds: () => listTenantIds(), // retention for cold tenants
  },
  adapterFactory: (cfg) =>
    new TursoAdapter({
      url: String(cfg.url),
      authToken: String(cfg.authToken),
    }),
  pool: { maxActiveConnections: 50 },
});
```

Tenant writes/queries **never fall back** to the base adapter. Pool uses **refCount / `withAdapter`** so in-use connections are not closed under LRU pressure.

### Batching & backpressure

```typescript
batching: {
  maxSize: 100,         // flush by count
  flushInterval: 5_000, // or by timer (ms)
  maxQueueSize: 1_000,  // per-tenant threshold
  onQueueFull: 'dlq',   // or 'drop' (volatile only)
},
maxActiveTenants: 10_000,
maxTotalQueued: 50_000,
maxFlushConcurrency: 16,           // concurrent bulkInsert flushes
maxWalBytes: 64 * 1024 * 1024,     // WAL hard size (64 MiB; wal.hardMaxBytes)
maxDlqFiles: 10_000,               // refuse new DLQ when pending+processing >= cap
encryptionKey: process.env.LOGBUN_ENCRYPTION_KEY, // optional AES-GCM for local WAL/DLQ
integrityChain: true,              // optional prevHash/contentHash tamper evidence
```

Overflow with `dlq`: dump **largest** queue first (fair-share) → snapshot → **clear synchronously** → write DLQ → ack WAL. Concurrent enqueues during the write are not wiped. DLQ writes are refused when file count hits `maxDlqFiles`.

### Adapters (overview)

| Adapter | Import | Peers | Best for |
|---------|--------|-------|----------|
| **BunSQLiteAdapter** | `logbun/adapters/sqlite` | none | Dev / single instance |
| **TursoAdapter** | `logbun/adapters/turso` | `@libsql/client` | Multi-replica / per-tenant DBs |
| **ClickHouseAdapter** | `logbun/adapters/clickhouse` | `@clickhouse/client` | High-volume analytics |

All built-ins use **idempotent** inserts (`INSERT OR IGNORE` or ReplacingMergeTree). See [docs/adapters.md](./docs/adapters.md).

### Plugins

```typescript
// Elysia
import { auditPlugin } from 'logbun/plugins/elysia';
app.use(
  auditPlugin(audit, {
    trustedProxyCount: 1,
    getTenantId: ({ request }) => request.headers.get('x-tenant-id') ?? undefined,
  }),
);

// Hono
import { createAuditMiddleware, type LogbunHonoVariables } from 'logbun/plugins/hono';
app.use(
  '*',
  createAuditMiddleware(audit, {
    trustedProxyCount: 1,
    getTenantId: (c) => c.get('tenantId'),
  }),
);

// In handlers — fire (never throws) or fireAsync (may reject)
auditLog.fire('course.created', { actorId, entityId });
await auditLog.fireAsync('billing.charged', { actorId, entityId });
```

- `User-Agent` always attached when present.
- **Client IP is off by default** (`trustedProxyCount: 0`). Set to the number of trusted reverse proxies before trusting `X-Forwarded-For`.
- Plugins expose **`fire` and `fireAsync`** with request context applied.
- Optional **`getTenantId?: (ctx) => string | undefined`** fills `tenantId` when the handler omits it.

Full details: [docs/plugins.md](./docs/plugins.md).

### Querying

Cursor pagination, newest first (UUIDv7-friendly `id`):

```typescript
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

if (page1.nextCursor) {
  await audit.query({
    tenantId: 'tenant_123',
    pagination: { limit: 50, cursor: page1.nextCursor },
  });
}
```

`maxQueryLimit` (default 500) hard-caps page size.

### Retention

```typescript
retention: {
  days: 90,
  cronExpression: '0 0 * * *', // Bun.cron, default midnight
}
```

- SQLite/Turso: `DELETE` by `created_at`
- ClickHouse: drop old `YYYYMM` partitions + table TTL

### Redaction & payload limits

```typescript
redactPaths: ['password', 'metadata.ssn', 'newValues.token'],
maxPayloadBytes: 64_000,       // progressive: metadata → oldValues → newValues
maxStringFieldBytes: 2_048,    // never truncates tenantId
```

Bare keys are removed recursively from bags **including arrays of objects**.

### Encryption, integrity, instance lock

```typescript
new AuditLogger({
  ...ENTERPRISE_DEFAULTS,
  namespace: process.env.INSTANCE_ID!,
  adapter: /* … */,
  encryptionKey: process.env.LOGBUN_ENCRYPTION_KEY, // local WAL/DLQ only
  integrityChain: true,                             // seal prevHash/contentHash
  // instanceLock: true,                            // default when durable
});

// After query — verify oldest-first:
const page = await audit.query({ tenantId: 't1', pagination: { limit: 50 } });
const check = await audit.verifyIntegrity([...page.logs].reverse());
// { ok: boolean, failedAt: number, error?: string }
```

---

## Architecture (short)

```
fire / fireAsync
  → caps / redact / payload limit → [integrity seal?]
  → [pre-ready RAM buffer if fire() before ready]
  → durable? WAL append (encrypt?; fail → DLQ/drop)
  → admit (queue + global caps; fair-share dump)
  → per-tenant queue → flush → pool.withAdapter → bulkInsert
       ↓ fail                              ↓ success
     retries → DLQ → RetryEngine → poison    WAL.acknowledge
```

On disk (`{dataDir}/{namespace}/`):

```text
.instance.lock
wal/current.aof
wal/seg-000001.aof
wal/acked.ids
dlq/{safeTenantKey}_{ts}_{rand}.batch[.processing|.dead]
```

Deep dive: [docs/architecture.md](./docs/architecture.md).

---

## Error resilience

| Failure | Behavior |
|---------|----------|
| WAL append fails | DLQ or drop + `wal_fail` — **not** silent RAM-only success |
| Adapter `bulkInsert` fails | Up to `insertMaxRetries` total attempts → DLQ |
| Adapter + DLQ fail (durable) | **WAL stays unacked** → recovery on next boot |
| Adapter + DLQ fail (volatile) | In-flight RAM batch may be lost |
| `fire()` before `ready` | RAM pre-ready buffer only until bootstrap drains it |
| Bootstrap fails | **Degraded**: `fire` drops, `fireAsync`/`query` fail, `degraded === true` |
| Process crash mid-retry | `.processing` → `.batch` on next start |
| Permanent bad batch | Poison `.dead` after `maxScanAttempts` (default 10) |

Full matrix: [docs/production.md](./docs/production.md).

---

## Production SaaS checklist

**Preset:** `durable` + `requireTenantId` + unique `namespace` + local `dataDir` + remote adapter. Default mode is still **`volatile`** (back-compat) — enterprise **must** set `mode: 'durable'`.

```typescript
const audit = new AuditLogger<Actions>({
  namespace: process.env.INSTANCE_ID!,
  mode: 'durable', // required — default is volatile
  dataDir: process.env.LOGBUN_DATA_DIR, // local SSD per replica
  adapter: /* Turso or ClickHouse — not multi-writer SQLite */,
  requireTenantId: true,
  maxQueryLimit: 100,
  maxPayloadBytes: 32_000,
  maxActiveTenants: 10_000,
  maxTotalQueued: 50_000,
  maxFlushConcurrency: 16,
  maxWalBytes: 64 * 1024 * 1024,
  maxDlqFiles: 10_000,
  flushTimeoutMs: 30_000,
  dlqFsync: true,
  redactPaths: ['password', 'token', 'secret', 'ssn', 'authorization'],
  batching: {
    maxSize: 50,
    flushInterval: 2_000,
    maxQueueSize: 2_000,
    onQueueFull: 'dlq',
  },
  wal: { fsync: true },
  retry: { insertMaxRetries: 3, scanIntervalMs: 30_000, maxScanAttempts: 10 },
  retention: { days: 365 },
  onEvent: (e) => {
    metrics.record(e);
    if (
      e.type === 'wal_fail' ||
      e.type === 'degraded' ||
      e.type === 'poison' ||
      e.type === 'drop' ||
      e.type === 'bootstrap_fail'
    ) {
      alerts.notify(e);
    }
  },
});

await audit.ready;

await audit.fireAsync('billing.charged', {
  tenantId,
  actorId: user.id,
  entityId: invoice.id,
});

process.on('SIGTERM', async () => {
  await audit.shutdown();
  process.exit(0);
});
```

| Rule | Why |
|------|-----|
| `mode: 'durable'` (or `ENTERPRISE_DEFAULTS`) | Default is volatile; enterprise needs WAL |
| Unique `namespace` per process | Local WAL/DLQ single-writer |
| Local `dataDir` (not NFS) | Shared FS breaks durability files + instance lock |
| Remote adapter for multi-replica | Shared source of truth |
| `requireTenantId: true` | Stop cross-tenant leaks |
| `fireAsync` for critical mutations | Durability before client ACK |
| `encryptionKey` / disk encryption | Protect local WAL/DLQ at rest |
| `integrityChain` when you need tamper evidence | Detection via `verifyIntegrity` |
| Alert on `wal_fail` / `degraded` / `poison` / `drop` / unsafe `limit` | Ops signal |
| Watch `walApproxBytes` / DLQ counts | Approaching `maxWalBytes` / `maxDlqFiles` |
| Do not expose `listDlq` / `requeueDead` / `deleteDead` publicly | Privileged ops only (path-confined) |

---

## API snapshot

### `AuditLogger`

| Member | Description |
|--------|-------------|
| `ready` | Bootstrap promise (never rejects) |
| `degraded` | Bootstrap failed |
| `fire` / `fireAsync` | Write paths |
| `query` | Cursor query (limit clamped) |
| `verifyIntegrity` | Verify hash chain (oldest first; when `integrityChain`) |
| `getStats` | In-memory: `queued`, `tenants`, `degraded`, `recoveryBacklog`, `inflightFlushes` |
| `getStatsDetailed` | + best-effort `walApproxBytes`, `dlqPending` / `dlqProcessing` / `dlqDead` |
| `listDlq` / `requeueDead` / `deleteDead` / `retryDlqNow` | Ops DLQ tooling |
| `shutdown` | Drain + close + release instance lock |

### Main `LogbunConfig` options

| Option | Default | Notes |
|--------|---------|--------|
| `namespace` | required | `[a-zA-Z0-9_-]{1,64}` |
| `mode` | `volatile` | **Set `durable` for enterprise** (or use `ENTERPRISE_DEFAULTS`) |
| `adapter` | required | `IAdapter` |
| `dataDir` | `.logbun` | Local per replica |
| `requireTenantId` | `false` | **true for SaaS** |
| `instanceLock` | durable→true | Exclusive multi-process lock on data dir |
| `encryptionKey` | — | AES-256-GCM for local WAL/DLQ |
| `integrityChain` | `false` | Seal `prevHash`/`contentHash` |
| `maxActiveTenants` | `10000` | Queue key cap |
| `maxTotalQueued` | `50000` | Global RAM cap |
| `maxFlushConcurrency` | `16` | Concurrent bulkInsert flushes |
| `maxWalBytes` | `64MiB` | WAL size (`wal.hardMaxBytes` default hard; `walSoftLimitBytes` alias) |
| `maxDlqFiles` | `10000` | Refuse new DLQ when pending+processing ≥ cap |
| `flushTimeoutMs` | `30000` | Overall flushAll deadline |
| `dlqFsync` | durable→true | DLQ durability |
| `maxQueryLimit` | `500` | Page size hard cap |
| `maxPayloadBytes` | `64000` | Bag size limit |
| `maxStringFieldBytes` | `2048` | Scalar string cap |
| `maxPreReadyBuffer` | `10000` | Pre-ready RAM |
| `redactPaths` | — | Deep redact |
| `wal` / `retry` / `batching` / `tenancy` / `retention` / `pool` | see docs | |

Complete tables: [docs/configuration.md](./docs/configuration.md) · [docs/api-reference.md](./docs/api-reference.md).

### Events (`onEvent`)

Common types: `enqueue`, `flush_ok`, `flush_fail`, `dlq`, `drop`, `truncated`, `wal_fail`, `bootstrap_fail`, `prune_fail`, `poison`, `degraded`, `limit` (e.g. `unsafe_default_volatile`, `shutdown_deadline`).

---

## Custom adapter

```typescript
import type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from 'logbun';

class MyAdapter implements IAdapter {
  async init() {}
  async bulkInsert(tenantId: string | null, logs: LogbunLog[]): Promise<boolean> {
    return true; // false → soft fail / retries / DLQ; prefer throw for hard errors
  }
  async query(
    tenantId: string | null,
    filters: LogbunQueryFilters,
    pagination: { cursor?: string; limit: number },
  ): Promise<LogbunQueryResult> {
    return { logs: [], nextCursor: null };
  }
  async prune(days: number) {}
  async close() {}
}
```

Prefer **idempotent insert on `id`** for WAL replay safety.

---

## Requirements

- **Bun** ≥ 1.3.0 (`Bun.randomUUIDv7`, `Bun.cron`, `Bun.file`, `bun:sqlite`)
- **TypeScript** ≥ 5.0 recommended for consumers

## Scripts

```bash
bun test
bun run typecheck
bun run build
```

## License

MIT
