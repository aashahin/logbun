# Logbun

**Runtime-agnostic audit logging** for Node.js, Bun, Deno, and Cloudflare Workers.

Zero runtime dependencies on the core package. Type-safe actions. Fire-and-forget or awaitable durable enqueue. Pluggable reliability (memory, filesystem WAL/DLQ, Cloudflare Durable Object SQLite).

**Package version: 1.0.0** · ES2022 / Web APIs at the root (no `node:`, `bun:`, or `process` in the root graph).

---

## Documentation map

| Doc | Contents |
|-----|----------|
| **This README** | Install, quick start, runtimes, checklist |
| [docs/migration-0.2.1-to-1.0.md](./docs/migration-0.2.1-to-1.0.md) | **0.2.1 → 1.0 migration** |
| [docs/architecture.md](./docs/architecture.md) | Pipeline, reliability, pooling |
| [docs/configuration.md](./docs/configuration.md) | Config reference |
| [docs/api-reference.md](./docs/api-reference.md) | Public API |
| [docs/adapters.md](./docs/adapters.md) | Destination adapters |
| [docs/plugins.md](./docs/plugins.md) | Elysia & Hono |
| [docs/production.md](./docs/production.md) | Multi-replica ops |

---

## Features

- **`fire()`** — never throws; optional `context.waitUntil` for Workers
- **`fireAsync()`** — awaits full enqueue / journal (or DLQ escalation); may reject
- **`flush()` / `runMaintenance()`** — host-scheduled drain + DLQ retry + retention
- **Reliability adapters** — memory (volatile), filesystem, Cloudflare DO
- **Type-safe actions** — generic `AuditLogger<TActions>`
- **Multi-tenant** — shared DB or `database_per_tenant` + pool + `adapterFactory`
- **Backpressure** — per-tenant queues, global caps, fair-share dumps
- **Integrity chain** — optional `prevHash` / `contentHash`
- **Safety** — redaction, payload/string caps, query limits
- **Tree-shakable** — adapters, plugins, durability on subpaths

---

## Installation

```bash
npm install logbun
# or: bun add logbun / pnpm add logbun
# Deno: import from npm:logbun (grant FS permissions when using filesystem durability)
```

### Optional peers

```bash
npm install @libsql/client        # Turso
npm install @clickhouse/client    # ClickHouse
npm install elysia                # logbun/plugins/elysia
npm install hono                  # logbun/plugins/hono
# BunSQLiteAdapter: bun:sqlite only (logbun/adapters/bun-sqlite)
```

### Package exports

```text
logbun
logbun/durability/filesystem    # Node/Bun/Deno (node:fs)
logbun/durability/cloudflare    # Workers Durable Object SQLite (ESM)
logbun/adapters/bun-sqlite
logbun/adapters/turso
logbun/adapters/clickhouse
logbun/plugins/elysia
logbun/plugins/hono
```

---

## Quick start

### Volatile (default — in-memory reliability)

```typescript
import { AuditLogger } from 'logbun';
import { BunSQLiteAdapter } from 'logbun/adapters/bun-sqlite';

const audit = new AuditLogger({
  namespace: 'my-app',
  adapter: new BunSQLiteAdapter({ path: '.logbun/audit.db' }),
});

await audit.ready;
audit.fire('user.created', { actorId: 'u1', tenantId: 't1' });
await audit.fireAsync('user.updated', { actorId: 'u1', tenantId: 't1' });
// Request runtimes: await fireAsync + flush for delivery guarantees
await audit.flush();
await audit.shutdown();
```

### Durable filesystem (Node / Bun / Deno)

```typescript
import { AuditLogger, ENTERPRISE_DEFAULTS } from 'logbun';
import { FileReliabilityAdapter } from 'logbun/durability/filesystem';
import { BunSQLiteAdapter } from 'logbun/adapters/bun-sqlite';

const reliability = new FileReliabilityAdapter({
  namespace: process.env.INSTANCE_ID ?? 'my-app', // unique per replica
  dataDir: '.logbun',
  wal: { fsync: true },
  dlq: { fsync: true },
});

const audit = new AuditLogger({
  ...ENTERPRISE_DEFAULTS, // mode: 'durable', requireTenantId: true
  namespace: 'my-app',
  reliability,
  adapter: new BunSQLiteAdapter({ path: '.logbun/audit.db' }),
  redactPaths: ['password', 'token'],
  retention: { days: 90 },
});

await audit.ready;
await audit.fireAsync('course.created', {
  tenantId: 'tenant_123',
  actorId: user.id,
  entityId: course.id,
});

// Host schedule (cron / supervisor):
await audit.runMaintenance();
await audit.shutdown();
```

**Deno:** `deno run --allow-read --allow-write=./.logbun app.ts`

### Cloudflare Durable Objects

```typescript
import { AuditLogger } from 'logbun';
import { CloudflareReliabilityAdapter } from 'logbun/durability/cloudflare';

export class AuditDO {
  private audit: AuditLogger;

  constructor(private ctx: DurableObjectState) {
    this.audit = new AuditLogger({
      namespace: 'do',
      mode: 'durable',
      reliability: new CloudflareReliabilityAdapter({ state: ctx }),
      adapter: /* destination */,
    });
  }

  async alarm() {
    await this.audit.runMaintenance();
  }
}
```

Standard Workers should call a DO binding. Use `fireAsync` + journal for
admission that survives request end; do **not** treat detached volatile `fire()`
as durable in isolate-scoped runtimes.

### Hono `waitUntil`

```typescript
import { createAuditMiddleware } from 'logbun/plugins/hono';

app.use('*', createAuditMiddleware(audit, { trustedProxyCount: 1 }));
// When executionCtx.waitUntil exists (Workers), fire() registers admission tasks.
```

---

## Capability matrix

| | Volatile (root default) | File reliability | CF DO reliability |
|--|-------------------------|------------------|-------------------|
| Runtimes | Node, Bun, Deno, Workers | Node, Bun, Deno | Workers DO |
| Journal | no (optional memory) | WAL segments | DO SQLite |
| DLQ | memory | files + opaque IDs | DO SQLite |
| Survive process death | no | yes | yes |
| Host maintenance | yes (DLQ/retry/retention) | yes | DO alarm |

---

## Production checklist

1. `mode: 'durable'` + persistent reliability with **unique `namespace` per replica**
2. Prefer `fireAsync` when callers must know admission succeeded
3. Schedule `runMaintenance()` (or DO `alarm`)
4. On request-scoped volatile hosts: `await fireAsync(...); await flush()`
5. Set `requireTenantId: true` (or use `ENTERPRISE_DEFAULTS`) for multi-tenant SaaS
6. Observe via `onEvent`, `getStats()`, `getStatsDetailed()`

---

## Migrating from 0.2.1

See **[docs/migration-0.2.1-to-1.0.md](./docs/migration-0.2.1-to-1.0.md)** for
`FileReliabilityAdapter`, DLQ IDs, host maintenance, Bun SQLite path rename,
Deno permissions, and Cloudflare DO / `waitUntil` details.

---

## License

MIT
