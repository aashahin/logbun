# Migration guide: 0.2.1 → 1.0

Logbun 1.0 is **runtime-agnostic**: the root package is pure ES2022 / Web API code.
Filesystem and Cloudflare durability live on explicit subpaths.

## Breaking changes

### 1. Durable mode requires a persistent `reliability` adapter

**Before (0.2.x):**

```ts
new AuditLogger({
  namespace: 'app',
  mode: 'durable',
  dataDir: '.logbun',
  wal: { fsync: true },
  dlqFsync: true,
  encryptionKey: '...',
  instanceLock: true,
  adapter,
});
```

**After (1.0):**

```ts
import { FileReliabilityAdapter } from 'logbun/durability/filesystem';

new AuditLogger({
  namespace: 'app',
  mode: 'durable',
  reliability: new FileReliabilityAdapter({
    namespace: 'app',
    dataDir: '.logbun',
    wal: { fsync: true },
    dlq: { fsync: true },
    encryptionKey: '...',
    instanceLock: true,
  }),
  adapter,
});
```

Missing or non-persistent reliability (`MemoryReliabilityAdapter`) throws
**synchronously** in the constructor when `mode: 'durable'`.

Volatile mode (default) uses an in-memory reliability backend automatically.

### 2. Filesystem config moved off root `LogbunConfig`

Moved into `FileReliabilityAdapter` options:

| 0.2.x config | 1.0 location |
|--------------|--------------|
| `dataDir` | `FileReliabilityAdapter({ dataDir })` |
| `wal.*` | `FileReliabilityAdapter({ wal })` |
| `dlqFsync` | `FileReliabilityAdapter({ dlq: { fsync } })` |
| `maxWalBytes` / `walSoftLimitBytes` | `FileReliabilityAdapter({ maxWalBytes })` |
| `maxDlqFiles` | `FileReliabilityAdapter({ dlq: { maxEntries } })` |
| `encryptionKey` | `FileReliabilityAdapter({ encryptionKey })` |
| `instanceLock` | `FileReliabilityAdapter({ instanceLock })` |

### 3. DLQ uses opaque IDs (not paths)

`listDlq()` returns `DLQEntry[]` with stable `id` fields.

```ts
const dead = await audit.listDlq({ includeDead: true });
await audit.requeueDead(dead[0]!.id); // preserves id, resets attempts
await audit.deleteDead(dead[0]!.id);
```

Filesystem paths may appear in `metadata.path` for diagnostics only — never pass
paths as authority without confined ID lookup.

### 4. Host-scheduled maintenance (no cron / scan timers)

Removed:

- `RetentionConfig.cronExpression`
- `RetryConfig.scanIntervalMs` / `initialDelayMs`
- Internal `Bun.cron` retention and recurring DLQ scan timers

Added:

```ts
await audit.flush();           // drain in-memory queues
await audit.runMaintenance();  // flush + one DLQ scan + retention prune (single-flight)
await audit.retryDlqNow();     // DLQ scan only (compat)
```

Hosts schedule `runMaintenance()` (cron, DO alarm, etc.). Short batching
`flushInterval` timers remain.

### 5. Bun SQLite path rename

```diff
- import { BunSQLiteAdapter } from 'logbun/adapters/sqlite';
+ import { BunSQLiteAdapter } from 'logbun/adapters/bun-sqlite';
```

`logbun/adapters/sqlite` is removed in 1.0.

### 6. Path helpers and instance lock leave the root export

```ts
import {
  FileReliabilityAdapter,
  resolveLogbunDir,
  resolveDataDir,
  InstanceLock,
  InstanceLockError,
} from 'logbun/durability/filesystem';
```

Root still exports pure helpers: `sanitizeNamespace`, `sanitizeTenantKey`, crypto, UUIDv7.

### 7. Deno permissions (filesystem)

```bash
deno run --allow-read=./.logbun --allow-write=./.logbun --allow-sys=uid,gid your_app.ts
```

Grant read/write on your configured `dataDir` (default `.logbun`).
This is sufficient for live-owner exclusion. Add `--allow-run` when automatic
recovery of a crashed process's instance lock is required; otherwise recovery
fails closed and needs verified operator cleanup while the namespace is stopped.

### 8. Cloudflare Workers

```ts
import { CloudflareReliabilityAdapter } from 'logbun/durability/cloudflare';

// Inside a Durable Object:
const reliability = new CloudflareReliabilityAdapter({ state: this.ctx });
const audit = new AuditLogger({
  namespace: 'do',
  mode: 'durable',
  reliability,
  adapter: /* your destination adapter */,
});

// alarm handler:
async alarm() {
  await this.audit.runMaintenance();
}
```

- Standard Workers call a **DO binding**; do not put journal/DLQ on D1.
- Pass `waitUntil` via `LogbunRequestContext` (Hono middleware wires
  `executionCtx.waitUntil` structurally when present).
- **Volatile** request runtimes: use `await fireAsync(...); await flush()` —
  detached `fire()` alone is **not** durable across isolate exit.
- **Durable DO** journal admission survives request lifecycle termination.

## Non-breaking preservations

- `AuditLogger`, `IAdapter`, batching, tenancy, events, safety, integrity chain
- `fire` / `fireAsync` / query / retry semantics (minus timer scheduling)
- Optional peers: Turso, ClickHouse, Hono, Elysia
- npm package name `logbun`; Deno consumes the npm artifact

## Capability matrix

| Capability | Root (volatile) | File reliability | CF DO reliability |
|------------|-----------------|------------------|-------------------|
| Runtimes | Node, Bun, Deno, Workers | Node, Bun, Deno | Workers DO |
| Journal | in-memory optional | WAL segments + fsync | DO SQLite |
| DLQ | memory | files + opaque IDs | DO SQLite |
| Instance lock | n/a | optional PID lock | n/a (DO singleton) |
| Host maintenance | required for DLQ retry | required | DO `alarm` → `runMaintenance` |

## Test / CI checklist

```bash
bun test
bun run typecheck
bun run build
bun run assert:root-runtime
bun run smoke:node
bun run smoke:bun
# Deno / miniflare: optional; scripts document when tooling unavailable
```
