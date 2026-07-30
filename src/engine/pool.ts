import type { IAdapter, TenancyConfig } from '../types';

interface PoolEntry {
  adapter: IAdapter;
  lastUsed: number;
  /** Active withAdapter / pin holders — eviction skips refCount > 0. */
  refCount: number;
}

export type AdapterFactory = (
  config: Record<string, unknown>
) => IAdapter | Promise<IAdapter>;

/**
 * LRU Connection Pool — manages adapter connections per tenant.
 *
 * Strictly separated from the Batcher — the pool owns sockets/file
 * descriptors, the batcher owns data queues. No coupling.
 *
 * Uses Map insertion order for LRU tracking:
 *   - get() moves entries to the end (most recently used)
 *   - When at capacity, evicts the oldest entry with refCount === 0
 *   - If every entry is pinned, throws `pool_exhausted`
 *
 * Prefer {@link withAdapter} so refCount pin/unpin is automatic.
 *
 * Concurrent get() for the same missing tenant is single-flighted so only
 * one adapter is created (no connection leaks).
 *
 * database_per_tenant: resolve/create failures rethrow — callers must
 * NEVER fall back to the base adapter for a real tenant id.
 */
export class ConnectionPool {
  private readonly pool: Map<string, PoolEntry> = new Map();
  /** In-flight create promises — single-flight per tenantId. */
  private readonly inflight: Map<string, Promise<IAdapter>> = new Map();
  private readonly maxSize: number;
  private readonly baseAdapter: IAdapter;
  private readonly tenancy?: TenancyConfig;
  private readonly adapterFactory?: AdapterFactory;

  constructor(
    baseAdapter: IAdapter,
    maxSize: number = 50,
    tenancy?: TenancyConfig,
    adapterFactory?: AdapterFactory
  ) {
    this.baseAdapter = baseAdapter;
    this.maxSize = maxSize;
    this.tenancy = tenancy;
    this.adapterFactory = adapterFactory;
  }

  /**
   * Get an adapter connection for the given tenant.
   *
   * Does **not** pin the entry — callers that hold the adapter across an
   * await should use {@link withAdapter} or {@link pin}/{@link unpin}.
   *
   * For single_database mode: always returns the base adapter.
   * For database_per_tenant mode: creates per-tenant adapter instances
   * managed by the LRU pool. On resolve null/throw — rethrows; never
   * returns the base adapter for a tenant.
   */
  async get(tenantId: string): Promise<IAdapter> {
    if (!this.tenancy || this.tenancy.mode === 'single_database') {
      return this.baseAdapter;
    }

    const existing = this.pool.get(tenantId);
    if (existing) {
      this.touch(tenantId, existing);
      return existing.adapter;
    }

    const pending = this.inflight.get(tenantId);
    if (pending) {
      return pending;
    }

    const createPromise = this.createAndInsert(tenantId);
    this.inflight.set(tenantId, createPromise);
    try {
      return await createPromise;
    } finally {
      this.inflight.delete(tenantId);
    }
  }

  /**
   * Run `fn` with a pooled (or base) adapter, pinning the pool entry for the
   * duration so LRU eviction cannot close it mid-use.
   *
   * - `tenantId === null` → base adapter (no pool)
   * - single_database → base adapter
   * - database_per_tenant → get/create, refCount++, finally refCount--
   */
  async withAdapter<T>(
    tenantId: string | null,
    fn: (adapter: IAdapter) => Promise<T>
  ): Promise<T> {
    if (
      tenantId === null ||
      !this.tenancy ||
      this.tenancy.mode === 'single_database'
    ) {
      return await fn(this.baseAdapter);
    }

    await this.pin(tenantId);
    try {
      const entry = this.pool.get(tenantId);
      if (!entry) {
        throw new Error(
          `pool entry missing after pin for tenant "${tenantId}"`
        );
      }
      return await fn(entry.adapter);
    } finally {
      this.unpin(tenantId);
    }
  }

  /**
   * Ensure the tenant adapter exists and increment refCount.
   * Callers MUST pair with {@link unpin} (prefer {@link withAdapter}).
   *
   * After `await get()`, another create under pool pressure can LRU-evict
   * this entry while refCount is still 0. We retry until we hold a live
   * entry with refCount incremented (no await between map check and ++).
   */
  async pin(tenantId: string): Promise<IAdapter> {
    if (!this.tenancy || this.tenancy.mode === 'single_database') {
      return this.baseAdapter;
    }

    // Bound retries: pool_exhausted from get() still propagates immediately.
    for (let attempt = 0; attempt < 64; attempt++) {
      const adapter = await this.get(tenantId);
      const entry = this.pool.get(tenantId);
      // Same adapter instance must still be mapped (not closed+replaced).
      if (entry && entry.adapter === adapter) {
        entry.refCount++;
        this.touch(tenantId, entry);
        return adapter;
      }
      // Evicted between get() resume and pin — recreate and try again.
    }
    throw new Error(
      `pool entry missing after pin for tenant "${tenantId}"`
    );
  }

  /** Decrement refCount after {@link pin} / successful get+hold. */
  unpin(tenantId: string): void {
    const entry = this.pool.get(tenantId);
    if (!entry) return;
    if (entry.refCount > 0) {
      entry.refCount--;
    }
    entry.lastUsed = Date.now();
  }

  private async createAndInsert(tenantId: string): Promise<IAdapter> {
    // Re-check after any await race (another caller may have finished)
    const existing = this.pool.get(tenantId);
    if (existing) {
      this.touch(tenantId, existing);
      return existing.adapter;
    }

    if (this.pool.size >= this.maxSize) {
      await this.evictOne();
    }

    const adapter = await this.createTenantAdapter(tenantId);
    // Another concurrent path might have inserted — close ours if so
    const raced = this.pool.get(tenantId);
    if (raced) {
      try {
        await adapter.close();
      } catch {
        /* ignore */
      }
      this.touch(tenantId, raced);
      return raced.adapter;
    }

    // Capacity may have filled during create — try again
    if (this.pool.size >= this.maxSize) {
      await this.evictOne();
    }

    this.pool.set(tenantId, {
      adapter,
      lastUsed: Date.now(),
      refCount: 0,
    });
    return adapter;
  }

  /**
   * Evict the least-recently-used entry with refCount === 0.
   * @throws Error with message `pool_exhausted` when every entry is pinned
   */
  private async evictOne(): Promise<void> {
    let evictKey: string | undefined;
    for (const [key, entry] of this.pool) {
      if (entry.refCount === 0) {
        evictKey = key;
        break;
      }
    }

    if (evictKey === undefined) {
      throw new Error('pool_exhausted');
    }

    const evicted = this.pool.get(evictKey);
    this.pool.delete(evictKey);
    if (evicted) {
      try {
        await evicted.adapter.close();
      } catch {
        // ignore close errors on eviction
      }
    }
  }

  /** Move entry to most-recently-used (Map end). */
  private touch(tenantId: string, entry: PoolEntry): void {
    entry.lastUsed = Date.now();
    this.pool.delete(tenantId);
    this.pool.set(tenantId, entry);
  }

  /**
   * Tenant ids with an active pooled connection.
   * Used by retention prune to visit per-tenant adapters.
   */
  listActiveTenantIds(): string[] {
    return [...this.pool.keys()];
  }

  /**
   * Close and remove a single tenant's pooled adapter.
   * @throws Error with message `pool_close_in_use` when refCount > 0
   *   (adapter is pinned / mid-withAdapter — caller must wait or unpin first).
   *
   * Removes the entry from the map *before* awaiting `adapter.close()` so a
   * concurrent pin cannot observe a mid-close adapter (no await between
   * refCount check and delete — JS is single-threaded between awaits).
   */
  async close(tenantId: string): Promise<void> {
    const entry = this.pool.get(tenantId);
    if (!entry) return;
    if (entry.refCount > 0) {
      throw new Error('pool_close_in_use');
    }
    this.pool.delete(tenantId);
    await entry.adapter.close();
  }

  async closeAll(): Promise<void> {
    const closePromises: Promise<void>[] = [];
    for (const [, entry] of this.pool) {
      closePromises.push(entry.adapter.close());
    }
    await Promise.allSettled(closePromises);
    this.pool.clear();
    this.inflight.clear();
    await this.baseAdapter.close();
  }

  get size(): number {
    return this.pool.size;
  }

  private async createTenantAdapter(tenantId: string): Promise<IAdapter> {
    if (!this.tenancy?.resolveConnection) {
      throw new Error(
        `database_per_tenant mode requires a resolveConnection function, ` +
          `but none was provided for tenant "${tenantId}"`
      );
    }

    const connectionConfig = await this.tenancy.resolveConnection(tenantId);
    if (!connectionConfig) {
      throw new Error(
        `resolveConnection returned null for tenant "${tenantId}"`
      );
    }

    if (this.adapterFactory) {
      const adapter = await this.adapterFactory(connectionConfig);
      await adapter.init();
      return adapter;
    }

    const AdapterClass = this.baseAdapter.constructor as new (
      config: Record<string, unknown>
    ) => IAdapter;
    const adapter = new AdapterClass(connectionConfig);
    await adapter.init();
    return adapter;
  }
}
