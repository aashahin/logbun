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
   known to be correct. The lock prevents accidental multi-writer use; it is
   not a security boundary against a malicious process running as the same OS user.
4. Set WAL/DLQ limits and alert on `walApproxBytes`, `dlqPending`, and
   `dlqProcessing`.
5. Use `encryptionKey` when local journal/DLQ files require at-rest
   encryption.
6. Use a destination with idempotent insert by `LogbunLog.id`.

With filesystem `fsync` enabled, first-run WAL initialization publishes file
entries and newly created `wal`/namespace/`dataDir` entries child-before-parent.
A permission-denied parent outside a narrow Deno grant is best-effort; every
accessible level is still synced, and unexpected errors keep initialization
unready until a retry succeeds.

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
deno run --allow-read=./.logbun --allow-write=./.logbun --allow-sys=uid,gid app.ts
```

Grant permissions narrowly to the configured data directory. It may be absent
on first run. Deno reports the missing in-scope path, then can deny metadata
access to its parent; Logbun treats that denial as the capability boundary,
relies on Deno to prevent access outside the grant, and physically validates
the newly created data, namespace, WAL, and DLQ paths before use. The
`--allow-sys=uid,gid` grant covers ownership metadata used by Deno's Node
compatibility filesystem implementation. `--allow-run` is not required.

## Filesystem security model

The filesystem adapter validates namespaces and opaque IDs, rejects observed
symbolic-link path segments, revalidates storage directories and files around
operations, and uses `O_NOFOLLOW` where the portable Node-compatible interface
exposes it. These checks protect against traversal, accidental redirection, and
filesystem substitutions that are present when validation runs.

They do not provide malicious same-user isolation. Node, Bun, and Deno do not
expose portable `openat`/directory-handle-relative rename, link, and unlink
operations, so another process with write access can rename an already
validated ancestor in the interval before a path-based syscall. Put `dataDir`
under OS permissions, a dedicated user/container, or another isolation boundary
that excludes hostile writers. The instance lock coordinates cooperative
owners and catches accidental namespace sharing; a same-user attacker can
replace or remove it, including racing the final inode recheck and path-based
unlink because portable runtimes expose no compare-and-unlink operation.
Network filesystems may not provide the required exclusive-create or durability
semantics.
