# API reference

Package entry: `import { … } from 'logbun'`.

Subpaths:

- `logbun/adapters/sqlite`
- `logbun/adapters/turso`
- `logbun/adapters/clickhouse`
- `logbun/plugins/elysia`
- `logbun/plugins/hono`

---

## `AuditLogger<TActions>`

```typescript
import { AuditLogger, ENTERPRISE_DEFAULTS } from 'logbun';
import type { LogbunConfig } from 'logbun';

const audit = new AuditLogger<Actions>({
  ...ENTERPRISE_DEFAULTS, // mode: 'durable', requireTenantId: true
  ...config,
});
```

### Lifecycle

| Member | Type | Description |
|--------|------|-------------|
| `ready` | `Promise<void>` | Resolves when bootstrap finishes. **Never rejects** — failures set `degraded`. |
| `degraded` | `boolean` | `true` after bootstrap failure. |
| `shutdown()` | `Promise<void>` | Begin shutdown, drain pre-ready, `flushAll`, stop retry/cron, close WAL & pool, release instance lock. Idempotent. |

### Writing

| Method | Returns | Throws? | Description |
|--------|---------|---------|-------------|
| `fire(action, input, context?)` | `void` | **Never** | Fire-and-forget. Does not await WAL. Drops silently when degraded / missing tenant (if required) / pre-ready full / post-shutdown. |
| `fireAsync(action, input, context?)` | `Promise<void>` | Yes | Awaits `ready`, then full enqueue (WAL when durable). Rejects on degraded, missing tenant (if required), or enqueue hard fail (`false`). |

**Input** (without `action` — passed as first arg):

```typescript
{
  tenantId?: string;
  actorId: string;
  entityId?: string;
  oldValues?: Record<string, unknown>;
  newValues?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
}
```

Library adds: `id` (UUIDv7), `createdAt` (ISO UTC), optional `ipAddress` / `userAgent` from context, and optional `prevHash` / `contentHash` when `integrityChain: true`.

**Processing order on write:** string field caps → `redactPaths` → payload size limit → (optional integrity seal) → enqueue.

### Integrity

| Method | Description |
|--------|-------------|
| `verifyIntegrity(logs, opts?)` | Verify a chain **oldest first**. Returns `{ ok, failedAt, error? }`. Requires logs sealed with `integrityChain: true`. Optional `opts.genesis` (default `INTEGRITY_GENESIS` — 64 zero hex). |

Tamper **detection only** — not WORM / legal-hold storage.

### Reading

```typescript
await audit.query({
  tenantId?: string;
  filters?: {
    action?: TActions;
    actorId?: string;
    entityId?: string;
    startDate?: string; // ISO 8601
    endDate?: string;
  };
  pagination?: { cursor?: string; limit?: number }; // default limit 50, max maxQueryLimit
});
// → { logs: LogbunLog[]; nextCursor: string | null }
```

- Newest first (`ORDER BY id DESC` / UUIDv7-friendly cursor `id < cursor`).
- Missing `tenantId` when required → throws.
- `database_per_tenant` resolve failure → throws (no base fallback).

### Stats

```typescript
// Sync — in-memory only (no disk I/O)
audit.getStats(): {
  queued: number;           // sum of in-memory queue lengths (pre-ready: buffer length)
  tenants: number;          // number of queue keys (pre-ready: 1 if buffer non-empty)
  degraded: boolean;
  recoveryBacklog: number;  // WAL recovery not yet injected
  inflightFlushes: number;  // concurrent bulkInsert paths in flight
}

// Async — same fields + best-effort disk metrics
await audit.getStatsDetailed(): Promise<{
  // …all getStats() fields…
  walApproxBytes?: number;  // WAL size; 0 if no WAL / not ready
  dlqPending?: number;
  dlqProcessing?: number;
  dlqDead?: number;
}>
```

| API | Cost | Notes |
|-----|------|--------|
| `getStats()` | In-memory | Safe when degraded or before ready. Pre-ready: `queued` reflects buffer so dashboards are not false idle. |
| `getStatsDetailed()` | Best-effort disk | Adds WAL approximate size and DLQ file counts by kind. |

Type: `AuditLoggerStats` (exported).

### DLQ ops (operator only — do not expose publicly)

| Method | Description |
|--------|-------------|
| `listDlq({ includePending?, includeProcessing?, includeDead? })` | List batch files + envelope metadata. Pending included by default; processing/dead opt-in. |
| `requeueDead(deadPath)` | Reset attempts to 0, write new `.batch`, delete `.dead`. Path must be under DLQ dir. |
| `deleteDead(deadPath)` | Unlink poison file. Path confined. |
| `retryDlqNow()` | One immediate RetryEngine scan. |

---

## Types (`logbun`)

### Log shapes

```typescript
interface LogbunLogInput<TAction> {
  tenantId?: string;
  actorId: string;
  action: TAction;
  entityId?: string;
  oldValues?: Record<string, unknown>;
  newValues?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
}

interface LogbunLog<TAction> extends LogbunLogInput<TAction> {
  id: string;
  createdAt: string;
  ipAddress?: string;
  userAgent?: string;
  /** Present when integrityChain sealed this log. */
  prevHash?: string;
  /** SHA-256 hex of prevHash + '\\n' + canonical payload. */
  contentHash?: string;
}
```

### Events

```typescript
type LogbunEventType =
  | 'enqueue'
  | 'flush_ok'
  | 'flush_fail'
  | 'dlq'
  | 'drop'
  | 'truncated'
  | 'wal_fail'
  | 'bootstrap_fail'
  | 'prune_fail'
  | 'poison'
  | 'degraded'
  | 'stats'   // reserved in type union
  | 'limit';

interface LogbunEvent {
  type: LogbunEventType;
  tenantId?: string | null;
  error?: string;
  count?: number;
  detail?: string;
}
```

**Emitted in practice today** (wire metrics on these):

| type | Typical `detail` |
|------|------------------|
| `enqueue` | — |
| `flush_ok` | `dlq_retry` on successful DLQ replay |
| `flush_fail` | `retries_exhausted`, `tenant_adapter`, `queue_room_fail`, `shutdown_flush` |
| `dlq` | `backpressure`, `wal_fail`, `shutdown`, `flush_*`, `queue_room_fail` |
| `drop` | `degraded`, `require_tenant_id`, `shutdown`, `pre_ready_buffer_full`, `queue_full`, `max_active_tenants`, `max_total_queued`, `wal_and_dlq_fail`, `pre_ready_enqueue_fail` |
| `truncated` | `max_string_field_bytes`, `max_payload_bytes` |
| `wal_fail` | `append`, `wal_full`, `shutdown_enqueue` |
| `bootstrap_fail` / `degraded` | bootstrap error message |
| `prune_fail` | tenant id / `knownTenantIds` / `base` |
| `poison` | scan attempts / reason |
| `limit` | `unsafe_default_volatile`, `unsafe_default_require_tenant` (once at construct), `shutdown_deadline` |

### Adapter interface

```typescript
interface IAdapter {
  init(): Promise<void>;
  bulkInsert(tenantId: string | null, logs: LogbunLog[]): Promise<boolean>;
  query(
    tenantId: string | null,
    filters: LogbunQueryFilters,
    pagination: { cursor?: string; limit: number }
  ): Promise<LogbunQueryResult>;
  prune(days: number): Promise<void>;
  close(): Promise<void>;
}
```

- Return `false` from `bulkInsert` for soft/retryable failure → engine retries then DLQ.
- Prefer **throw** on unexpected errors so `error` text is recorded on events.
- Built-in adapters persist optional `prev_hash` / `content_hash` when present on the log.

### DLQFileInfo

```typescript
interface DLQFileInfo {
  path: string;
  kind: 'pending' | 'processing' | 'dead';
  tenantId: string | null;
  attempts: number;
  logCount: number;
}
```

### Preset

```typescript
const ENTERPRISE_DEFAULTS = {
  mode: 'durable',
  requireTenantId: true,
} as const;
```

Library defaults stay looser for back-compat; spread this for multi-tenant / enterprise SaaS.

---

## Helpers exported from `logbun`

| Export | Description |
|--------|-------------|
| `ENTERPRISE_DEFAULTS` | `{ mode: 'durable', requireTenantId: true }` |
| `sanitizeNamespace(ns)` | Validate namespace; throws if invalid. |
| `resolveLogbunDir(namespace, dataDir?)` | `{dataDir}/{namespace}` with traversal checks. |
| `resolveDataDir` | Alias of `resolveLogbunDir`. |
| `sanitizeTenantKey(tenantId)` | Filename-safe key for DLQ paths. |
| `isTenantIdPresent(id)` | Non-empty trimmed string. |
| `INTEGRITY_GENESIS` | Genesis prevHash (64 zero hex). |
| `verifyIntegrityChain(logs, genesis?)` | Standalone chain verifier (same logic as `audit.verifyIntegrity`). |
| `normalizeEncryptionKey(key)` | Normalize `encryptionKey` to 32 bytes (hex / base64 / passphrase→SHA-256 / `Uint8Array`). |
| `InstanceLock` / `InstanceLockError` | Exclusive multi-process lock on `{dataDir}/{namespace}/.instance.lock` (used when `instanceLock` is on). |
| `AuditLoggerStats` | Type of `getStats()` / `getStatsDetailed()` return. |

---

## `IAdapter` implementors (subpath)

See [adapters.md](./adapters.md).
