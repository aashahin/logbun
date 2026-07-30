# Logbun documentation

In-depth guides for the **logbun** package. Start with the root [README](../README.md) for install and quick start; use these pages for production architecture and full reference.

| Guide | Description |
|-------|-------------|
| [Architecture](./architecture.md) | Pipeline, WAL/DLQ, pooling, recovery |
| [Configuration](./configuration.md) | Every `LogbunConfig` option and defaults |
| [API reference](./api-reference.md) | Public classes, methods, types, events |
| [Adapters](./adapters.md) | SQLite, Turso, ClickHouse, custom adapters |
| [Production](./production.md) | Multi-replica SaaS checklist, ops, failure modes |
| [Plugins](./plugins.md) | Elysia & Hono, IP trust model |
| [Changelog notes](./changelog-notes.md) | Capability history (what the library includes today) |

**Runtime:** Bun ≥ 1.3.0 · **Package version:** see `package.json` (`0.2.0` at time of writing).
