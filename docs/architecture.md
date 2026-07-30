# Architecture

Logbun is an **audit-log pipeline** for Bun apps: type-safe actions, optional multi-tenant routing, RAM batching, crash-resilient local durability (WAL + DLQ), and pluggable storage adapters.

## High-level pipeline

```
fire() / fireAsync()
        │
        ▼
  build log (UUIDv7 id, createdAt, caps, redact, payload limit)
        │
        ├── integrityChain? ──► seal prevHash / contentHash (serialized total order)
        │
        ├── before ready ──► pre-ready RAM buffer (fire only; volatile)
        │
        ▼
  Batcher.enqueue (per-tenant queues)
        │
        ├── durable: WAL append (fsync optional; AES-GCM if encryptionKey)
        │     ├── hard max: wal_full → DLQ fallback (default)
        │     └── on WAL fail → DLQ single batch or drop (never silent RAM-only success)
        │
        ├── admit: per-queue maxQueueSize + maxActiveTenants + maxTotalQueued
        │     └── overflow → dump largest queue to DLQ (fair-share) or drop
        │
        ▼
  In-memory queue ──timer / maxSize──► flush (chunked: ≤ maxSize per bulkInsert)
        │                              fair-share: largest queues first
        ▼
  ConnectionPool.withAdapter (pins tenant adapters; no base fallback for tenants)
        │
        ▼
  adapter.bulkInsert  (insertMaxRetries total attempts + backoff; maxFlushConcurrency)
        │
        ├── success → WAL.acknowledge(ids)
        └── failure → DLQ envelope batch → RetryEngine scan → poison .dead
```

## Components

| Component | Responsibility |
|-----------|----------------|
| **AuditLogger** | Public API: fire/fireAsync, query, stats, integrity verify, DLQ ops, shutdown |
| **bootstrap** | Instance lock, encryption key normalize, init WAL/DLQ, adapter, pool, batcher, recovery, retry, retention cron |
| **Batcher** | Per-tenant queues, backpressure, fair-share flush/dump, recovery inject, flush semaphore |
| **ConnectionPool** | LRU + refCount/`withAdapter` for `database_per_tenant` |
| **WALStorage** | Segmented append-only NDJSON + ack sidecar; exclusive op chain; optional encrypt; compact |
| **DLQStorage** | One file per failed batch; v1 envelope; path confinement; optional fsync / encrypt |
| **InstanceLock** | Exclusive `.instance.lock` so two processes cannot share the same namespace data dir |
| **RetryEngine** | Periodic DLQ scan (concurrency 4); poison after maxScanAttempts |
| **Adapters** | Persist/query/prune in SQLite, Turso, or ClickHouse (incl. optional integrity columns) |

## Tenancy models

### `single_database` (default)

- One adapter instance (base).
- Rows store `tenant_id`.
- Query without `tenantId` returns **all tenants** unless `requireTenantId: true`.
- For multi-tenant SaaS on a shared DB: **always set `requireTenantId: true`**.

### `database_per_tenant`

- `requireTenantId` is forced on.
- `resolveConnection(tenantId)` returns connection config.
- Prefer `adapterFactory(config)` to build the adapter; otherwise the base adapter’s constructor is used.
- **Never** falls back to the base adapter for a real tenant id (writes go to DLQ on resolve failure).
- Pool evicts LRU entries with `refCount === 0`; in-use adapters are pinned via `withAdapter`.

## Durability modes

| Mode | Local disk | Crash before flush |
|------|------------|--------------------|
| `volatile` (default) | No WAL | Logs still only in RAM are lost |
| `durable` | WAL + optional DLQ fsync; instance lock default on | Unacked WAL entries recovered on next start |

### Important durability semantics

1. **`fire()` is async durability.** It never throws and does **not** wait for WAL append. Use **`fireAsync()`** when the client ACK must wait for enqueue/WAL (or DLQ escalation) to finish.
2. **WAL append failure** does not leave the log only in RAM as a success. The engine tries **DLQ**; if that fails, the enqueue returns failure (`fireAsync` rejects; `fire` emits `drop` / `wal_fail`).
3. **Pre-ready buffer** (`fire()` only, before `ready`): **RAM-only**, even if `mode: 'durable'`. `fireAsync` **awaits `ready`** first, so it does not use the pre-ready buffer. After ready, the buffer is drained through the real enqueue path (WAL when durable).
4. **Ack after success:** flush success or successful DLQ write appends ids to the ack sidecar; compact rewrites the WAL without acked lines.
5. **Idempotent adapters:** `INSERT OR IGNORE` (SQLite/Turso) and ClickHouse **ReplacingMergeTree** make WAL replay safe.
6. **Instance lock (default on in durable):** bootstrap acquires `{dataDir}/{namespace}/.instance.lock`. A second live process with the same namespace fails bootstrap → degraded. Not safe on NFS.

## Backpressure and caps

| Control | Scope | Default |
|---------|--------|---------|
| `batching.maxSize` | Max logs per bulkInsert chunk (flush never sends more) | 100 |
| `batching.maxQueueSize` | Per tenant queue (+ reservations); recovery inject respects this | 1_000 |
| `batching.onQueueFull` | `dlq` or `drop` (`drop` invalid with durable) | `dlq` |
| `maxActiveTenants` | Distinct queue keys | 10_000 |
| `maxTotalQueued` | Sum of all queue lengths + reservations | 50_000 |
| `maxFlushConcurrency` | Global concurrent bulkInsert paths | 16 |
| `maxWalBytes` | WAL size limit / guidance | 64 MiB |
| `wal.hardMaxBytes` | Refuse append at `maxWalBytes` (`wal_full` → DLQ) | `true` |
| `maxDlqFiles` | Refuse new DLQ writes (pending+processing) | 10_000 |

**Flush path:** each `flushKey` takes at most `maxSize` via sync `splice` (not the whole queue). Remainder is re-armed (immediate flush if still ≥ maxSize, else timer). Under the concurrency semaphore, **largest queues flush first** (fair-share). Recovery inject also refuses past per-key `maxQueueSize` (overflow stays in `recoveryBacklog`).

**Dump path:** fair-share victim = **largest** non-empty queue → snapshot → **clear length synchronously** → await DLQ write → ack WAL. On DLQ write failure, restore snapshot to the front of the queue (no ack).

## Retry and poison

- Failed flushes: up to **`insertMaxRetries` total** `bulkInsert` attempts (default **3**), with exponential backoff between tries.
- DLQ files: envelope `{ v:1, tenantId, attempts, logs }` (ciphertext when `encryptionKey` is set).
- RetryEngine scans pending `.batch` files (chunks of **4** concurrent), increments `attempts`, poisons to `.dead` when `attempts >= maxScanAttempts` (default **10**).
- Crash mid-scan: `.processing` renamed back to `.batch` on bootstrap (`recoverOrphans`).

## Retention

- Configured via `retention: { days, cronExpression? }` using **`Bun.cron`**.
- SQLite/Turso: `DELETE ... WHERE created_at < cutoff`.
- ClickHouse: drop month partitions older than cutoff (UTC YYYYMM) **plus** table TTL at create time.
- `database_per_tenant`: prune active pool tenants + optional `knownTenantIds()` for cold tenants.

## Integrity hash chain

When `integrityChain: true`:

- Each log is sealed with `prevHash` (previous `contentHash`, or `INTEGRITY_GENESIS`) and `contentHash` = SHA-256 of `prevHash + '\n' + canonical payload`.
- Seals are **serialized** process-wide so concurrent `fire()` keeps a total order.
- Fields are stored by built-in adapters (`prev_hash` / `content_hash`) and can be checked with `audit.verifyIntegrity(logs)` (oldest first).
- **Detection only** — not immutability or WORM.

## Encryption at rest (local artifacts)

When `encryptionKey` is set:

- WAL lines and DLQ batch file bodies are AES-256-GCM (`e1:` prefix).
- Key material: 32-byte `Uint8Array`, 64-char hex, base64 of 32 bytes, or passphrase (SHA-256 hashed — prefer full-entropy keys in production).
- Remote adapter storage is **not** encrypted by this option (use DB/disk encryption there).

## On-disk layout

`resolveLogbunDir(namespace, dataDir?)` → `join(dataDir ?? '.logbun', namespace)`.

```
{dataDir}/{namespace}/          # e.g. .logbun/my-app
├── .instance.lock              # exclusive multi-process lock (when instanceLock)
├── wal/
│   ├── current.aof             # active append target (NDJSON or ciphertext lines)
│   ├── seg-000001.aof          # sealed segments (rotation at wal.segmentBytes)
│   ├── seg-000002.aof
│   └── acked.ids               # one id per line until compact
└── dlq/
    ├── {safeTenantKey}_{ts}_{rand}.batch
    ├── {safeTenantKey}_{ts}_{rand}.batch.processing
    └── {safeTenantKey}_{ts}_{rand}.batch.dead
```

- `namespace` must match `/^[a-zA-Z0-9_-]{1,64}$/`.
- WAL rotates `current.aof` into `seg-NNNNNN.aof` at `wal.segmentBytes` (default 16 MiB) to bound compact/read peak memory.
- DLQ filename tenant key is **sanitized**; real `tenantId` is in the envelope.
- Ops paths for `requeueDead` / `deleteDead` are **confined** under the DLQ directory.

## Multi-replica

Local WAL/DLQ are **single-writer / single-process**. For horizontal scale:

1. Unique `namespace` per process (e.g. pod name).
2. Local `dataDir` (not shared NFS).
3. Leave `instanceLock` on (default in durable) or coordinate externally.
4. Remote shared store as source of truth (Turso / ClickHouse).

SQLite file adapters are single-instance; do not multi-write one file from many replicas.

## Security boundaries (library-owned)

| Concern | Behavior |
|---------|----------|
| Path traversal in namespace | Rejected by `sanitizeNamespace` |
| Path traversal in tenant for DLQ filenames | `sanitizeTenantKey` + envelope |
| Ops delete/requeue outside DLQ dir | `assertUnderDir` throws |
| Two processes, same namespace data dir | `InstanceLock` (durable default) |
| Cross-tenant query | Prevented only if `requireTenantId` or `database_per_tenant` |
| Client IP spoofing | `trustedProxyCount` default **0** (XFF ignored) |
| Secrets in payloads | Optional `redactPaths` (objects **and** arrays) |
| Local WAL/DLQ plaintext | Optional `encryptionKey` (AES-256-GCM) |
| Tamper evidence | Optional `integrityChain` + `verifyIntegrity` |

Application-level authz for `query` / DLQ ops is **your** responsibility.
