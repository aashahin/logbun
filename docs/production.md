# Production guide (SaaS / enterprise)

## Enterprise preset (required)

Default `mode` is **`volatile`** for back-compat. **Enterprise / multi-tenant SaaS must set durable explicitly.**

| Must set | Why |
|----------|-----|
| `mode: 'durable'` | Local WAL + recovery; default is volatile |
| `requireTenantId: true` | Blocks cross-tenant query/write mistakes |
| Unique `namespace` per process | Local WAL/DLQ is single-writer |
| Local `dataDir` (SSD), not NFS | Shared FS corrupts WAL/DLQ semantics |
| Remote adapter (Turso / ClickHouse) | Multi-replica source of truth — not multi-writer SQLite |

Also: `await audit.ready` before traffic · `await audit.shutdown()` on SIGTERM · `fireAsync` for money/compliance · never expose DLQ ops publicly.

## Deployment topology

```
                    ┌─────────────────┐
   App replica A    │ namespace: pod-a │── local SSD ──► WAL/DLQ A
                    │ adapter ────────┼── network ───► Turso / ClickHouse
                    └─────────────────┘
                    ┌─────────────────┐
   App replica B    │ namespace: pod-b │── local SSD ──► WAL/DLQ B
                    │ adapter ────────┼── network ───► same remote SoT
                    └─────────────────┘
```

## Recommended config

See [configuration.md](./configuration.md#production-template).

Minimal durable multi-tenant:

```typescript
import { AuditLogger, ENTERPRISE_DEFAULTS } from 'logbun';
import { TursoAdapter } from 'logbun/adapters/turso';

const audit = new AuditLogger({
  ...ENTERPRISE_DEFAULTS, // mode: 'durable', requireTenantId: true
  namespace: process.env.INSTANCE_ID!,
  dataDir: process.env.LOGBUN_DATA_DIR,
  adapter: new TursoAdapter({
    url: process.env.TURSO_URL!,
    authToken: process.env.TURSO_TOKEN!,
  }),
  // instanceLock defaults true in durable — unique namespace per replica still required
  encryptionKey: process.env.LOGBUN_ENCRYPTION_KEY, // optional AES-GCM for local WAL/DLQ
  // integrityChain: true, // optional hash-chain tamper evidence
  maxFlushConcurrency: 16,
  maxWalBytes: 64 * 1024 * 1024,
  maxDlqFiles: 10_000,
  dlqFsync: true,
  wal: { fsync: true, hardMaxBytes: true },
  batching: { onQueueFull: 'dlq', maxSize: 50, flushInterval: 2_000 },
  redactPaths: ['password', 'token', 'secret', 'ssn'],
  onEvent: (e) => metrics.record(e),
});

await audit.ready;

process.on('SIGTERM', async () => {
  await audit.shutdown();
  process.exit(0);
});
```

## fire vs fireAsync

| Use case | API |
|----------|-----|
| High-volume, loss-tolerant until flush | `fire()` |
| Billing, permissions, deletions, compliance | `await fireAsync()` |
| Plugin middleware (Hono/Elysia) | `auditLog.fire` / `auditLog.fireAsync` |

`fire()` never throws. `fireAsync()` rejects when:

- Logger is degraded / not initialized
- `requireTenantId` and tenant missing
- Enqueue returns `false` (WAL+DLQ hard fail, caps, backpressure drop)

Plugins expose both `fire` and `fireAsync` with the same request context. Optional `getTenantId?: (ctx) => string | undefined` fills `tenantId` when the handler omits it.

## Observability

Wire `onEvent` to metrics and alerts:

| Event | Alert? | Meaning |
|-------|--------|---------|
| `bootstrap_fail` / `degraded` | **Page** | Logger unusable for writes |
| `wal_fail` | **Page** | Disk / WAL path problems |
| `poison` | **Ticket** | Batch permanently failing |
| `drop` | **Warn/Page** | Capacity or config drops (`max_active_tenants`, `queue_full`, DLQ cap, …) |
| `dlq` | Metric | Escalation volume |
| `flush_fail` | Metric/Warn | Adapter instability |
| `truncated` | Metric | Payload/field caps hitting |
| `limit` / `unsafe_default_volatile` | **Warn** | Constructor: `mode` omitted or `volatile` (back-compat default) |
| `limit` / `unsafe_default_require_tenant` | **Warn** | Constructor: `requireTenantId` not true (and not forced by `database_per_tenant`) |

On construct, Logbun emits those `limit` events **once** when unsafe defaults apply — alert on them in enterprise deploys; defaults themselves are unchanged for back-compat. Prefer `ENTERPRISE_DEFAULTS` (`mode: 'durable'`, `requireTenantId: true`).

### `getStats` vs `getStatsDetailed`

| API | Cost | Fields |
|-----|------|--------|
| `getStats()` | In-memory only | `queued`, `tenants`, `degraded`, `recoveryBacklog`, `inflightFlushes` |
| `getStatsDetailed()` | Best-effort disk (WAL size, DLQ counts) | Above + `walApproxBytes?`, `dlqPending?`, `dlqProcessing?`, `dlqDead?` |

- `walApproxBytes` is best-effort; `0` if no WAL / not ready.
- DLQ counts reflect pending / processing / dead file totals under the namespace.
- Safe when degraded (zeros + `degraded: true`). Before ready, `queued` includes the pre-ready buffer length so buffering does not look like idle healthy zeros.

Poll `getStats()` frequently; use `getStatsDetailed()` on a slower cadence for disk pressure.

## Recovery

On boot (durable mode):

1. WAL is read with a size guard (`maxWalBytes` / `walSoftLimitBytes` guidance).
2. Injected into RAM in waves (`maxRecoveryBatch`), respecting **`maxActiveTenants`** and **`maxTotalQueued`**.
3. **Backlog that does not fit stays on disk** (or in the recovery backlog) until flushes free capacity — recovery does not blow past queue caps.

`recoveryBacklog` / elevated `walApproxBytes` after restart means inject is still catching up or the adapter is slow.

## Failure matrix

| Failure | Durable behavior | Volatile behavior |
|---------|------------------|-------------------|
| WAL append fails | DLQ single log or drop + `wal_fail` — not silent RAM success | N/A (no WAL) |
| WAL at `maxWalBytes` (hard) | Append refused (`wal_full`) → DLQ fallback | N/A |
| Adapter down | Retries → DLQ → retry engine | Same (no WAL ack path) |
| Adapter + DLQ fail | **Unacked WAL retained** for next boot | May lose in-flight RAM batch |
| Process crash mid-flush | WAL recovery + orphan `.processing` → `.batch` | Lose unflushed RAM |
| Before `ready` (`fire`) | Pre-ready RAM only | Same |
| Bootstrap fail | Degraded: fire drops, query throws | Same |
| Instance lock held | Bootstrap fails → degraded | N/A if `instanceLock` false |
| Queue full (`dlq`) | Dump **largest** queue to DLQ (fair-share, sync clear), ack WAL | Same without WAL |
| Queue full (`drop`) | Invalid config if durable | Drop + event |
| DLQ file cap (`maxDlqFiles`) | New DLQ writes refused; escalate/drop path | Same |
| Tenant adapter resolve fail | DLQ; never base DB | Same |
| Retention cron error | `prune_fail` event; CH TTL still applies | Same |

## Ops runbook (DLQ)

Privileged only:

```typescript
const dead = await audit.listDlq({ includeDead: true, includePending: true });
for (const f of dead) {
  if (f.kind === 'dead' && f.attempts >= 10) {
    // inspect f.path, then either:
    // await audit.requeueDead(f.path);
    // await audit.deleteDead(f.path);
  }
}
await audit.retryDlqNow();
```

Paths outside the logger’s DLQ directory are rejected.

## Capacity planning

| Knob | Default | Symptom when too low / exceeded |
|------|---------|----------------------------------|
| `maxQueueSize` | 1_000 | Frequent DLQ dumps / latency spikes |
| `maxActiveTenants` | 10_000 | Drops with `max_active_tenants` |
| `maxTotalQueued` | 50_000 | Global drops / DLQ thrash |
| `maxFlushConcurrency` | 16 | Adapter overload, stalled flushes, growing `queued` / `inflightFlushes` |
| `maxWalBytes` | 64 MiB | `wal_full` refuse (hard default) → DLQ fallback; elevated `walApproxBytes` |
| `wal.segmentBytes` | 16 MiB | Fewer sealed segments if too high; more files if too low |
| `maxDlqFiles` | 10_000 | New DLQ writes refused; drops / hard fail when disk backlog is huge |
| `pool.maxActiveConnections` | 50 | `pool_exhausted` under churn with many pinned tenants |
| `maxRecoveryBatch` | `maxQueueSize` / 1_000 | Slow recovery or high RAM if set too high; long `recoveryBacklog` if too low |
| `wal.fsync` | true (durable) | Latency vs power-loss safety tradeoff |

Tune `maxFlushConcurrency` against remote adapter RPS. Global backpressure dumps the **largest** tenant queue first (fair-share). Long adapter outages grow the WAL — alert when `walApproxBytes` approaches `maxWalBytes`. Alert when `dlqPending + dlqProcessing` approaches `maxDlqFiles`.

## Security checklist

- [ ] `mode: 'durable'` (default is volatile)  
- [ ] `requireTenantId: true` for multi-tenant products  
- [ ] Unique `namespace` + local `dataDir` + remote adapter  
- [ ] `instanceLock` left on (default for durable) so two processes cannot share WAL/DLQ  
- [ ] `encryptionKey` for AES-GCM at-rest encryption of local WAL/DLQ (or OS disk encryption)  
- [ ] `integrityChain: true` when you need tamper-evidence hash chain + `verifyIntegrity`  
- [ ] `redactPaths` for secrets/PII fields  
- [ ] `trustedProxyCount` only if you own the proxy chain  
- [ ] DLQ ops not on public routes  
- [ ] Authz on `query` endpoints in your app  

## Integrity verification (ops)

When `integrityChain: true`, verify a page of logs **oldest first** after query (reverse the usual newest-first page if needed):

```typescript
const page = await audit.query({ tenantId, pagination: { limit: 100 } });
const chronological = [...page.logs].reverse();
const result = await audit.verifyIntegrity(chronological);
if (!result.ok) {
  alerts.notify({ type: 'integrity_fail', at: result.failedAt, error: result.error });
}
```

## What the library does **not** do

- Distributed consensus / multi-writer **shared** local WAL (use unique namespace + instance lock + remote SoT)  
- Application authentication or tenant authorization  
- True WORM / legal-hold object storage (integrity chain is **detection**, not immutability)  
- Encryption of **remote** adapter data (local WAL/DLQ only via `encryptionKey`)  
- Automatic **engine** migration for existing ClickHouse MergeTree tables (integrity columns may migrate best-effort)  
- Full Node.js runtime support (Bun-first: `bun:sqlite`, `Bun.cron`; core fs paths are node-compatible)
