# Architecture

The package root is ES2022/Web APIs only. Runtime-specific storage is reached
only through explicit exports:

```text
logbun (root) ── MemoryReliabilityAdapter (volatile)
              ├─ logbun/durability/filesystem ── Node/Bun/Deno WAL + files
              └─ logbun/durability/cloudflare ── Workers DO SQLite
```

## Delivery flow

```text
fire / fireAsync
  → redact, cap, UUIDv7, optional integrity seal
  → durable: ReliabilityAdapter.appendJournal
  → RAM tenant queue / short batching timer
  → destination bulkInsert
      success → acknowledgeJournal
      failure → writeDlq → acknowledgeJournal
      destination + DLQ failure → leave journal unacknowledged
```

The destination adapter must be idempotent by log ID. This allows recovery after
a process fails between a successful destination write and its journal ack.

The queue snapshots before asynchronous insertion. Concurrent enqueues receive a
fresh queue and cannot be erased by a DLQ spill. DLQ implementations serialize
write/claim transitions; claims are atomic `pending → processing`, and startup
returns orphaned processing entries to pending.

## Recovery and maintenance

Bootstrap order:

1. initialize reliability and acquire exclusive ownership when applicable;
2. initialize the destination;
3. read a bounded wave of unacknowledged journal records into queues;
4. recover orphaned DLQ processing claims; and
5. make one-shot retry machinery available.

`runMaintenance()` is intentionally host-driven and single-flight. It flushes
queues, scans the DLQ once, then runs configured retention pruning. It replaces
Bun cron and recurring scan timers. A Cloudflare Durable Object uses its
`alarm()` handler as the host scheduler.

## Filesystem layout

For `FileReliabilityAdapter({ dataDir, namespace })`:

```text
{dataDir}/{namespace}/
  .instance.lock
  wal/
    current.aof
    seg-000001.aof
    acked.ids
  dlq/
    {opaque-uuidv7}.batch
    {opaque-uuidv7}.batch.processing
    {opaque-uuidv7}.batch.dead
```

The namespace and opaque IDs are validated, and all lookup is confined below the
adapter-owned directory. WAL segmentation, bounded reading, acknowledgement
compaction, caps, encryption, fsync, and instance locking live exclusively in
the filesystem subpath.

## Cloudflare ownership

`CloudflareReliabilityAdapter` is a Durable Object-only SQLite adapter. Its
tables hold journal rows and DLQ rows with stable IDs and state transitions.
Table prefixes are sanitized SQL identifiers. Standard Workers do not own
reliability state; they call the appropriate DO binding. No D1, general SQLite
adapter, Node compatibility, or filesystem APIs are used.
