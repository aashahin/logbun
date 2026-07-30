# Capability notes (current library surface)

This page summarizes what the library includes **today** (package `0.2.1`), spanning early design plus production hardening. It is not a dated Git changelog; use `git log` for commit history.

## Core product

- Type-safe action generics on `AuditLogger<TActions>`
- Fire-and-forget `fire()` (never throws)
- Awaitable `fireAsync()` for durable critical paths
- Volatile vs durable modes
- Per-tenant RAM queues + timer/size flush
- Bootstrap recovery, graceful shutdown
- Cursor pagination on UUIDv7 ids
- Tree-shakable adapters and framework plugins
- `ENTERPRISE_DEFAULTS` preset (`mode: 'durable'`, `requireTenantId: true`)

## Multi-tenant

- `single_database` and `database_per_tenant`
- `requireTenantId` (forced for per-tenant DBs)
- `adapterFactory` + `resolveConnection`
- LRU pool with **refCount / `withAdapter`** (no mid-query close of pinned adapters)
- **No base-adapter fallback** for tenant writes/queries in per-tenant mode
- `knownTenantIds` for retention of cold tenants
- Global caps: `maxActiveTenants`, `maxTotalQueued`
- Fair-share flush and dump (largest queues first under pressure)

## Durability & reliability

- Segmented WAL NDJSON (`current.aof` + `seg-NNNNNN.aof`) + ack sidecar (not blind truncate of unacked data)
- WAL exclusive mutex (`runExclusive`)
- WAL fsync; DLQ fsync (`dlqFsync`)
- Hard WAL size cap (`maxWalBytes` + `wal.hardMaxBytes`, default refuse → `wal_full` → DLQ)
- Soft size alias `walSoftLimitBytes`
- WAL fail → DLQ or drop (not silent RAM-only success)
- Dump-to-DLQ clears queue **before** await (no lost concurrent enqueues)
- Stranded post-WAL room-fail → single-log DLQ
- Bounded recovery inject (`maxRecoveryBatch`); respects tenant/queue caps; excess stays on disk
- `flushAll` overall deadline + wait for in-flight flushes (`inflightFlushes`)
- Global flush concurrency semaphore (`maxFlushConcurrency`, default 16)
- Flush chunks ≤ `maxSize` (never whole oversized queue in one bulkInsert)
- DLQ v1 envelope with durable `attempts`
- Poison `.dead` + `requeueDead` / `deleteDead` / `listDlq` / `retryDlqNow`
- Path confinement on DLQ ops
- DLQ file write cap (`maxDlqFiles`; dead files do not count)
- Retry scan concurrency (4)
- `insertMaxRetries` = **total** attempts (default 3)
- Exclusive **instance lock** (default on in durable): `{namespace}/.instance.lock`

## Safety & SaaS hygiene

- Payload progressive truncation (`maxPayloadBytes`)
- String field caps (`maxStringFieldBytes`; never cap `tenantId`)
- `redactPaths` deep delete through objects **and arrays**
- Query limit clamp (`maxQueryLimit`)
- Pre-ready bounded buffer (volatile until ready; drained via real enqueue after bootstrap)
- Namespace / dataDir path sanitization
- Tenant key sanitization for filenames
- Degraded mode + `getStats()` / `getStatsDetailed()`
- Constructor `limit` events for unsafe defaults (`unsafe_default_volatile`, `unsafe_default_require_tenant`)
- Safe `onEvent` (listener errors swallowed)

## Encryption & integrity

- **AES-256-GCM at-rest** for local WAL lines and DLQ batch files (`encryptionKey`)
- Key normalize: 32-byte raw, 64-hex, base64, or passphrase→SHA-256 (`normalizeEncryptionKey`)
- Optional **hash chain** (`integrityChain`): `prevHash` / `contentHash` on each log
- `verifyIntegrity` / `verifyIntegrityChain` + `INTEGRITY_GENESIS`
- Adapters store `prev_hash` / `content_hash` with best-effort column migrate

## Observability

- `onEvent` lifecycle types including `wal_fail`, `poison`, `degraded`, `limit`, …
- `getStats()`: `queued`, `tenants`, `degraded`, `recoveryBacklog`, `inflightFlushes`
- `getStatsDetailed()`: + `walApproxBytes`, `dlqPending` / `dlqProcessing` / `dlqDead`
- Pre-ready buffer length surfaced in `getStats().queued`

## Adapters

- Bun SQLite: FULL sync default, busy timeout, composite tenant indexes, integrity columns + migrate
- Turso/libSQL batch writes + same index / integrity set
- ClickHouse ReplacingMergeTree, FINAL queries, partition prune, TTL, `queryFinal` flag, integrity columns + migrate

## Plugins

- Elysia derive plugin (`fire` + `fireAsync` + optional `getTenantId`)
  - **0.2.1:** derive uses `{ as: 'global' }` so parent apps see `auditLog` after `.use(auditPlugin(...))`
- Hono middleware + typed variables
- IP off by default; `trustedProxyCount` trust model

## Testing

Unit/integration coverage under `tests/` includes (non-exhaustive):

- WAL acknowledge, mutex, recovery bootstrap, segment hard max
- Dump-queue race, WAL-fail → DLQ
- DLQ envelope, path confinement, max files
- Pool refcount / single-flight
- Tenant isolation, caps, fair-share
- fireAsync / plugin fireAsync / getStatsDetailed
- Encryption at rest, integrity chain, instance lock
- Redaction arrays, retry attempt counts
- Query limit, path sanitize, client IP
- Flush concurrency / chunk size, recovery caps
- Production P0 / unsafe defaults
- **0.2.1:** heavy E2E suites (`tests/e2e-*.test.ts`) — lifecycle, multi-tenant, DLQ/failure, safety, plugins (Hono/Elysia HTTP), stress

## Intentional non-goals (still)

- Multi-process **shared** local WAL (use unique namespace + instance lock + remote SoT)
- Application authentication or tenant authorization
- True WORM / legal-hold object storage (integrity chain is **detection**, not immutability)
- Encryption of **remote** adapter stores (local WAL/DLQ only via `encryptionKey`)
- Automatic ClickHouse **engine** migration for pre-existing MergeTree tables (columns may migrate best-effort)
- Full Node.js runtime support (Bun-first: `bun:sqlite`, `Bun.cron`; core fs paths are node-compatible)
