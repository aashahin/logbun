# Configuration

## Core configuration

```ts
import { AuditLogger, ENTERPRISE_DEFAULTS } from 'logbun';
import { FileReliabilityAdapter } from 'logbun/durability/filesystem';

const audit = new AuditLogger({
  ...ENTERPRISE_DEFAULTS,
  namespace: 'api-replica-1',
  reliability: new FileReliabilityAdapter({
    namespace: 'api-replica-1',
    dataDir: '.logbun',
    wal: { fsync: true },
    dlq: { fsync: true, maxEntries: 10_000 },
    maxWalBytes: 64 * 1024 * 1024,
    encryptionKey: process.env.LOGBUN_ENCRYPTION_KEY,
  }),
  adapter: destination,
  requireTenantId: true,
  batching: { maxSize: 50, flushInterval: 2_000, maxQueueSize: 2_000, onQueueFull: 'dlq' },
  retry: { insertMaxRetries: 3, insertBaseDelayMs: 1_000, maxScanAttempts: 10 },
  retention: { days: 365 },
});
```

| Option | Default | Meaning |
|---|---:|---|
| `namespace` | required | `[a-zA-Z0-9_-]{1,64}`; isolate reliability storage / ownership |
| `mode` | `volatile` | `durable` requires `reliability.persistent === true` synchronously |
| `reliability` | memory in volatile mode | Journal + DLQ backend; use a subpath adapter for durability |
| `adapter` | required | Destination `IAdapter` |
| `batching.maxSize` | 100 | Maximum insert batch size |
| `batching.flushInterval` | 5,000 ms | Short-lived batching timer; it is not retry scheduling |
| `batching.maxQueueSize` | 1,000 | Per-tenant RAM queue cap |
| `batching.onQueueFull` | `dlq` | `drop` is rejected for durable mode |
| `retry.insertMaxRetries` | 3 | Total attempts per destination insertion |
| `retry.maxScanAttempts` | 10 | Failed DLQ scans before a batch becomes dead |
| `retention.days` | — | Pruned only by host calls to `runMaintenance()` |
| `maxRecoveryBatch` | max queue size | Bound on a journal recovery wave |
| `maxActiveTenants` / `maxTotalQueued` | 10,000 / 50,000 | RAM admission caps |
| `flushTimeoutMs` | 30,000 ms | Overall flush / shutdown deadline |
| `integrityChain` | false | SHA-256 chain fields for tamper detection |
| `redactPaths` | — | Deep paths removed before persistence |

## Reliability adapters

### Volatile: `MemoryReliabilityAdapter`

The root defaults to memory and is deliberately non-persistent. It is suitable
for long-lived processes that accept loss of unflushed work. In request-scoped
runtimes, use `await fireAsync(...); await flush()`; detached `fire()` is
not a delivery guarantee.

### Node, Bun, Deno: `FileReliabilityAdapter`

Import from `logbun/durability/filesystem`. All filesystem options live here,
not on `LogbunConfig`.

| Adapter option | Default | Meaning |
|---|---:|---|
| `dataDir` | `.logbun` | Parent directory; namespace paths are validated beneath it (see the production threat model) |
| `wal.fsync` | true | fsync journal appends / compaction |
| `wal.segmentBytes` | 16 MiB | WAL rotation threshold |
| `wal.hardMaxBytes` | true | Refuse over-limit append with `wal_full` |
| `maxWalBytes` | 64 MiB | Journal size cap |
| `dlq.fsync` | true | fsync DLQ writes / transitions |
| `dlq.maxEntries` | 10,000 | Pending + processing entry cap |
| `encryptionKey` | — | AES-256-GCM key material for WAL and DLQ |
| `instanceLock` | true | Exclusive namespace ownership |

Deno consumes the npm package and needs filesystem permissions, for example
`deno run --allow-read=./.logbun --allow-write=./.logbun --allow-sys=uid,gid app.ts`.
The configured directory may be absent on first run. When Deno denies metadata
access above that missing path, Logbun treats the denial as the runtime
capability boundary and validates the created in-scope paths afterwards.

### Cloudflare Workers: `CloudflareReliabilityAdapter`

Import from `logbun/durability/cloudflare` **inside a SQLite-backed Durable
Object**. Standard Workers call the owning DO binding. The DO's `alarm()`
must call `await audit.runMaintenance()`; the adapter schedules a pending-work
alarm when supported. It does not use D1, Node compatibility, filesystem APIs,
or a generic SQLite connection.

## Maintenance

There are no internal recurring DLQ or retention timers in 1.0:

```ts
await audit.flush();           // delivery attempt for RAM queues
await audit.retryDlqNow();     // one DLQ scan (convenience)
await audit.runMaintenance();  // flush + one DLQ scan + retention prune
```

Schedule `runMaintenance()` in the host (cron, queue worker, or DO alarm).
Concurrent calls are single-flight.

Removed from the public configuration: `RetentionConfig.cronExpression`,
`RetryConfig.scanIntervalMs`, and `RetryConfig.initialDelayMs`.
