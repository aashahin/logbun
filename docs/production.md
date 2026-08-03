# Production operations

## Select the right reliability backend

| Deployment | Reliability | Notes |
|---|---|---|
| Long-lived process that can lose queued logs | root memory default | Volatile; monitor queue pressure |
| Node, Bun, or Deno replica | `FileReliabilityAdapter` | Unique namespace and local writable disk per replica |
| Cloudflare Workers | `CloudflareReliabilityAdapter` in a DO | Standard Worker calls the owning DO; DO alarm schedules maintenance |

Durable mode rejects missing or non-persistent reliability synchronously. Do
not rely on detached `fire()` in a request-scoped volatile runtime: use
`await fireAsync(...); await flush()` when delivery is required before the
isolate ends.

## Filesystem checklist

1. Store `dataDir` on durable local storage, not a shared multi-writer volume.
2. Use a unique `namespace` for each exclusive storage owner.
3. Keep the default instance lock unless an external exclusivity mechanism is
   known to be correct.
4. Set WAL/DLQ limits and alert on `walApproxBytes`, `dlqPending`, and
   `dlqProcessing`.
5. Use `encryptionKey` when local journal/DLQ files require at-rest
   encryption.
6. Use a destination with idempotent insert by `LogbunLog.id`.

## Maintenance and alerting

Schedule `runMaintenance()` in every host. It performs a bounded unit of work:
flush, one DLQ scan, and retention pruning. `retryDlqNow()` is useful for an
operator-triggered retry. Inspect and mutate dead letters only by their opaque
IDs; filesystem paths are diagnostics, not authority.

Alert on `bootstrap_fail`, `degraded`, `wal_fail`, `drop`, `poison`,
and sustained `flush_fail` events. A destination and DLQ failure deliberately
leaves the journal unacknowledged; recovery will retry it after restart.

## Cloudflare Durable Objects

Each reliable stream belongs to one SQLite-backed Durable Object. Bind standard
Workers to that object, use `fireAsync` for durable admission, and implement:

```ts
async alarm() {
  await this.audit.runMaintenance();
}
```

The adapter requests an alarm when work is pending. It is safe for a host to
call `runMaintenance()` concurrently because the logger makes it single-flight.

## Deno

Deno consumes the npm package; it does not need a separate source build:

```sh
deno run --allow-read --allow-write=./.logbun app.ts
```

Grant permissions narrowly to the configured data directory. Some Deno
Node-compatibility releases also require narrowly scoped `--allow-sys`
permissions for filesystem ownership metadata.
