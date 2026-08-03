# Capability notes — 1.0

Logbun 1.0 moves runtime-specific reliability out of the package root.

- Root: ES2022/Web APIs; UUIDv7, memory reliability, batching, retries,
  integrity, tenancy, events, and framework-neutral request context.
- `logbun/durability/filesystem`: persistent WAL/DLQ, encryption, fsync,
  caps, path confinement, and instance locking for Node/Bun/Deno.
- `logbun/durability/cloudflare`: SQLite-backed Durable Object journal and
  DLQ with atomic state transitions and alarm scheduling.
- `runMaintenance()` replaces recurring Bun cron and retry timers.
- DLQ operations use stable opaque IDs; any filesystem path is diagnostic only.

See [migration-0.2.1-to-1.0.md](./migration-0.2.1-to-1.0.md) for breaking
changes and [README.md](../README.md) for runtime support.
