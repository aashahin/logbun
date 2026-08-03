# API reference

Root import: `import { AuditLogger } from 'logbun'`.

Subpaths:

- `logbun/durability/filesystem` — ESM + CJS filesystem reliability
- `logbun/durability/cloudflare` — ESM-only Durable Object SQLite reliability
- `logbun/adapters/bun-sqlite` — Bun-only destination adapter
- `logbun/adapters/turso`, `logbun/adapters/clickhouse`
- `logbun/plugins/hono`, `logbun/plugins/elysia`

## `AuditLogger`

| Member | Contract |
|---|---|
| `ready` | Bootstrap promise. Bootstrap failure sets `degraded` and emits events. |
| `fire(action, input, context?)` | Returns `void`, never throws. If provided, `context.waitUntil` receives the admission task, including pre-ready draining. |
| `fireAsync(action, input, context?)` | Resolves after admission; in durable mode, journal commit occurs first. Rejects on hard admission failure. Cloudflare alarm scheduling can also reject after commit with `DurableAdmissionSchedulingError`. |
| `flush()` | Drains current RAM queues and compacts safely. |
| `runMaintenance()` | Single-flight: flushes, scans the DLQ once, and prunes configured retention. |
| `retryDlqNow()` | One DLQ scan without flush or retention. |
| `listDlq()` | Lists opaque `DLQEntry` values. Pending is included by default. |
| `requeueDead(id)` | Moves a dead entry to pending, resets attempts, and preserves its ID. |
| `deleteDead(id)` | Deletes a dead entry by opaque ID. |
| `getStats()` / `getStatsDetailed()` | RAM-only / adapter-backed journal and DLQ metrics. |
| `verifyIntegrity(logs)` | Verifies an oldest-first integrity chain. |
| `shutdown()` | Best-effort drain then closes destination and reliability resources. |

`LogbunRequestContext` is structural and runtime-neutral:

```ts
interface LogbunRequestContext {
  ipAddress?: string;
  userAgent?: string;
  waitUntil?: (task: Promise<unknown>) => void;
}
```

Hono passes an execution context's `waitUntil` without exporting Cloudflare
types from the root package. Elysia remains an isolated plugin subpath.

## `ReliabilityAdapter`

The public seam owns journal, DLQ lifecycle, recovery, statistics, lifecycle,
and exclusive ownership. Implementations must preserve:

1. durable journal append before durable admission resolves;
2. destination success before journal acknowledgement;
3. durable DLQ write before acknowledgement on destination failure; and
4. an unacknowledged journal when both destination and DLQ fail.

The minimal methods are `init`, `close`, `appendJournal`,
`acknowledgeJournal`, `recoverJournal`, `compactJournal`, `writeDlq`,
`listDlq`, `claimDlq`, settlement/poison/requeue/delete methods,
`readDlq`, `recoverOrphans`, and `getStats`.

Host-scheduled implementations may expose `pendingMaintenanceDelayMs()` and
`requestMaintenance()`. `AuditLogger.runMaintenance()` calls
`requestMaintenance()` after every pass so pending reliability work retains a
wake-up; the deprecated `rearmMaintenance()` hook remains a compatibility
fallback.

Cloudflare journal admission surfaces alarm-read/write failures after the
journal row is committed. The exported error and cross-entrypoint-safe guard
let callers distinguish this from a failed commit:

```ts
import { isDurableAdmissionSchedulingError } from 'logbun';

try {
  await audit.fireAsync('audit.event', input);
} catch (error) {
  if (isDurableAdmissionSchedulingError(error)) {
    // The event is durable already. Restore maintenance; do not resubmit it.
    await reliability.requestMaintenance();
  } else {
    throw error;
  }
}
```

```ts
interface DLQEntry {
  id: string; // stable opaque authority
  state: 'pending' | 'processing' | 'dead';
  kind: 'pending' | 'processing' | 'dead';
  tenantId: string | null;
  attempts: number;
  logCount: number;
  metadata?: Record<string, unknown>; // diagnostic only
}
```

Do not use filesystem paths as a mutation API. A file adapter may expose a
confined path in `metadata` for diagnostics, but `id` is the only authority
accepted by DLQ operations.

## Destination adapter

```ts
interface IAdapter {
  init(): Promise<void>;
  bulkInsert(tenantId: string | null, logs: LogbunLog[]): Promise<boolean>;
  query(tenantId: string | null, filters: LogbunQueryFilters,
    pagination: { cursor?: string; limit?: number }): Promise<LogbunQueryResult>;
  prune(days: number): Promise<void>;
  close(): Promise<void>;
}
```

Make inserts idempotent on `LogbunLog.id`: journal recovery and DLQ retries
can re-deliver a batch after a crash around acknowledgement.
