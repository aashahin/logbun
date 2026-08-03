/**
 * logbun/durability/cloudflare
 *
 * Persistent reliability for Cloudflare Workers Durable Objects (SQLite storage).
 * Standard Workers should call a DO binding; the owning DO alarm handler invokes
 * `audit.runMaintenance()`.
 *
 * Does not use node:fs, D1, or generic SQLite outside DO storage.
 */
export {
  CloudflareReliabilityAdapter,
  type CloudflareReliabilityAdapterOptions,
  type DurableObjectSqlStorage,
  type DurableObjectStateLike,
} from './adapter';
