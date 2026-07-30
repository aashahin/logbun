# Configuration reference

All options live on `LogbunConfig<TActions>` passed to `new AuditLogger(config)`.

## Required

| Option | Type | Description |
|--------|------|-------------|
| `namespace` | `string` | Isolates local WAL/DLQ. Must match `/^[a-zA-Z0-9_-]{1,64}$/`. **Unique per process** in multi-replica setups. |
| `adapter` | `IAdapter` | Storage adapter instance. |

## Core

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `mode` | `'volatile' \| 'durable'` | `'volatile'` | Local durability mode. |
| `dataDir` | `string` | `'.logbun'` | Root for `{dataDir}/{namespace}/wal|dlq`. Absolute OK; `..` segments rejected. |
| `requireTenantId` | `boolean` | `false` | Require non-empty `tenantId` on fire/query. Forced `true` for `database_per_tenant`. **Use `true` for multi-tenant SaaS.** |
| `onEvent` | `(e: LogbunEvent) => void` | — | Lifecycle events (never throws if listener throws). |
| `adapterFactory` | `(config) => IAdapter \| Promise<IAdapter>` | — | Preferred factory for per-tenant adapters. |
| `tenancy` | `TenancyConfig` | single DB | See [Tenancy](#tenancy). |
| `batching` | `Partial<BatchingConfig>` | see below | Batching & backpressure. |
| `pool` | `{ maxActiveConnections?: number }` | `50` | LRU pool size for `database_per_tenant`. |
| `wal` | `WalConfig` | durable defaults | Only used when `mode: 'durable'`. |
| `retry` | `RetryConfig` | see below | DLQ scan / insert attempts. |
| `retention` | `RetentionConfig` | — | Optional prune cron. |

## Capacity and safety limits

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxActiveTenants` | `number` | `10_000` | Max concurrent in-memory queue keys. Extra tenant keys → `drop` with `detail: max_active_tenants`. |
| `maxTotalQueued` | `number` | `50_000` | Global sum of queue lengths + in-flight reservations. |
| `maxFlushConcurrency` | `number` | `16` | Max concurrent `bulkInsert` flushes globally (semaphore around flush paths). |
| `maxWalBytes` | `number` | `64 * 1024 * 1024` (64 MiB) | Hard WAL size limit by default (`wal_full` → DLQ). Soft-only if `wal.hardMaxBytes: false`. |
| `walSoftLimitBytes` | `number` | — | Optional alias of `maxWalBytes`. Prefer `maxWalBytes`. |
| `maxDlqFiles` | `number` | `10_000` | Refuse new DLQ writes when pending+processing file count ≥ this. |
| `encryptionKey` | `string \| Uint8Array` | — | AES-256-GCM at-rest for local WAL lines + DLQ files (32-byte key, 64-hex, base64, or passphrase→SHA-256). |
| `integrityChain` | `boolean` | `false` | Seal each log with `prevHash`/`contentHash`; use `verifyIntegrity()`. Tamper **detection**, not WORM. |
| `instanceLock` | `boolean` | `true` if durable | Exclusive multi-process lock on namespace data dir (prevents shared WAL/DLQ). |
| `maxQueryLimit` | `number` | `500` | Hard cap on query page size. |
| `maxPayloadBytes` | `number` | `64_000` | Max JSON size of oldValues+newValues+metadata. Progressive shrink then `{ _truncated: true }`. |
| `maxStringFieldBytes` | `number` | `2_048` | Cap UTF-8 length of actorId, action, entityId, userAgent, ipAddress. **tenantId is never capped.** |
| `maxPreReadyBuffer` | `number` | `10_000` | Max `fire()` logs before `ready` (RAM-only). Excess → `drop` / `pre_ready_buffer_full`. |
| `maxRecoveryBatch` | `number` | `maxQueueSize` or `1_000` | WAL recovery inject batch size. Remaining backlog stays on disk; inject respects tenant/queue caps. |
| `flushTimeoutMs` | `number` | `30_000` | Overall deadline for `flushAll` / shutdown drain (waits for in-flight flushes until deadline). |
| `dlqFsync` | `boolean` | `true` if durable, else `false` | fsync DLQ files after write / attempt updates. |
| `redactPaths` | `string[]` | — | Bare keys and dotted paths; walks nested objects **and arrays**. |

**Default `mode` is `volatile`** (back-compat). Enterprise / multi-replica SaaS **must** set `mode: 'durable'`.

## BatchingConfig

Merged with defaults:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxSize` | `number` | `100` | Flush when queue length ≥ this. |
| `flushInterval` | `number` | `5_000` | ms timer flush. |
| `maxQueueSize` | `number` | `1_000` | Per-tenant backpressure threshold. |
| `onQueueFull` | `'dlq' \| 'drop'` | `'dlq'` | Overflow. **`drop` is invalid with `mode: 'durable'`** (constructor throws). |

## WalConfig

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `fsync` | `boolean` | `true` when durable | fsync after append and compact rewrite. |
| `compactAckThreshold` | `number` | `256` | Compact when this many ack ids accumulate. |
| `segmentBytes` | `number` | `16 * 1024 * 1024` | Rotate `current.aof` into sealed `seg-NNNNNN.aof` at this size. |
| `hardMaxBytes` | `boolean` | `true` | Refuse append when total WAL size ≥ `maxWalBytes` (`wal_full`). |

## RetryConfig

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `scanIntervalMs` | `number` | `60_000` | DLQ scan period. |
| `initialDelayMs` | `number` | `10_000` | Delay before first scan after bootstrap. |
| `maxScanAttempts` | `number` | `10` | Envelope attempts before poison `.dead`. |
| `insertMaxRetries` | `number` | `3` | **Total** bulkInsert attempts per flush/scan (not “retries after first”). |
| `insertBaseDelayMs` | `number` | `1_000` | Base delay; doubles each subsequent attempt. |

## RetentionConfig

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `days` | `number` | — | Required if `retention` set. |
| `cronExpression` | `string` | `'0 0 * * *'` | 5-field cron for `Bun.cron`. |

## Tenancy

```typescript
interface TenancyConfig {
  mode: 'single_database' | 'database_per_tenant';
  resolveConnection?: (tenantId: string) => Promise<Record<string, unknown> | null>;
  knownTenantIds?: () => string[] | Promise<string[]>;
}
```

- `resolveConnection` required for `database_per_tenant`.
- Returning `null` or throwing → tenant adapter unavailable → DLQ / query error (no base fallback).
- `knownTenantIds` used by retention prune so cold tenants are still pruned.

## Production template

**Preset:** `ENTERPRISE_DEFAULTS` (`mode: 'durable'` + `requireTenantId: true`) + unique `namespace` + local `dataDir` + remote adapter (Turso/ClickHouse). Default mode stays `volatile` for back-compat — do not ship enterprise without `durable`.

```typescript
import { AuditLogger, ENTERPRISE_DEFAULTS } from 'logbun';

const audit = new AuditLogger<Actions>({
  ...ENTERPRISE_DEFAULTS, // mode: 'durable', requireTenantId: true
  namespace: process.env.INSTANCE_ID!, // unique per process / replica
  dataDir: process.env.LOGBUN_DATA_DIR, // local SSD, not NFS
  adapter: /* TursoAdapter | ClickHouseAdapter — remote SoT */,
  instanceLock: true, // default when durable — two processes cannot share WAL/DLQ
  encryptionKey: process.env.LOGBUN_ENCRYPTION_KEY, // AES-GCM for local WAL/DLQ
  integrityChain: true, // optional tamper-evidence hash chain
  maxActiveTenants: 10_000,
  maxTotalQueued: 50_000,
  maxFlushConcurrency: 16,
  maxWalBytes: 64 * 1024 * 1024,
  maxDlqFiles: 10_000,
  maxQueryLimit: 100,
  maxPayloadBytes: 32_000,
  maxStringFieldBytes: 2_048,
  maxPreReadyBuffer: 10_000,
  flushTimeoutMs: 30_000,
  dlqFsync: true,
  redactPaths: ['password', 'token', 'secret', 'ssn', 'authorization'],
  batching: {
    maxSize: 50,
    flushInterval: 2_000,
    maxQueueSize: 2_000,
    onQueueFull: 'dlq',
  },
  wal: {
    fsync: true,
    compactAckThreshold: 256,
    segmentBytes: 16 * 1024 * 1024,
    hardMaxBytes: true, // wal_full → DLQ when at maxWalBytes
  },
  retry: {
    scanIntervalMs: 30_000,
    maxScanAttempts: 10,
    insertMaxRetries: 3,
    insertBaseDelayMs: 1_000,
  },
  retention: { days: 365, cronExpression: '0 0 * * *' },
  onEvent: (e) => {
    metrics.increment(`logbun.${e.type}`, { detail: e.detail });
    if (
      e.type === 'wal_fail' ||
      e.type === 'degraded' ||
      e.type === 'poison' ||
      e.type === 'drop' ||
      e.type === 'bootstrap_fail' ||
      (e.type === 'limit' && e.detail?.startsWith('unsafe_default'))
    ) {
      alerts.notify(e);
    }
  },
});

await audit.ready;
```

### Security options (detail)

| Option | Default | Notes |
|--------|---------|--------|
| `encryptionKey` | — | AES-256-GCM for **local** WAL lines + DLQ files only. Prefer 32-byte entropy (64-hex or raw bytes). Passphrases are SHA-256 hashed. |
| `integrityChain` | `false` | Seals `prevHash` / `contentHash` on each log; verify with `audit.verifyIntegrity(logs)` (oldest first). Detection, not WORM. |
| `instanceLock` | `true` if durable | Exclusive `{dataDir}/{namespace}/.instance.lock`. Fails bootstrap if another live PID holds it. Not safe on NFS. |
