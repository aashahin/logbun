import type { IAdapter, TenancyConfig } from '../types';

interface PoolEntry {
  adapter: IAdapter;
  lastUsed: number;
}

/**
 * LRU Connection Pool — manages adapter connections per tenant.
 *
 * Strictly separated from the Batcher — the pool owns sockets/file
 * descriptors, the batcher owns data queues. No coupling.
 *
 * Uses Map insertion order for LRU tracking:
 *   - get() moves entries to the end (most recently used)
 *   - When at capacity, evicts the first entry (least recently used)
 */
export class ConnectionPool {
  private readonly pool: Map<string, PoolEntry> = new Map();
  private readonly maxSize: number;
  private readonly baseAdapter: IAdapter;
  private readonly tenancy?: TenancyConfig;

  constructor(
    baseAdapter: IAdapter,
    maxSize: number = 50,
    tenancy?: TenancyConfig
  ) {
    this.baseAdapter = baseAdapter;
    this.maxSize = maxSize;
    this.tenancy = tenancy;
  }

  /**
   * Get an adapter connection for the given tenant.
   *
   * For single_database mode: always returns the base adapter.
   * For database_per_tenant mode: creates per-tenant adapter instances
   * managed by the LRU pool.
   */
  async get(tenantId: string): Promise<IAdapter> {
    // Single database mode — no pooling needed
    if (!this.tenancy || this.tenancy.mode === 'single_database') {
      return this.baseAdapter;
    }

    // Check if connection exists — move to end of Map (mark as recently used)
    const existing = this.pool.get(tenantId);
    if (existing) {
      this.pool.delete(tenantId);
      existing.lastUsed = Date.now();
      this.pool.set(tenantId, existing);
      return existing.adapter;
    }

    // At capacity — evict the least recently used (first entry in Map)
    if (this.pool.size >= this.maxSize) {
      const oldestKey = this.pool.keys().next().value;
      if (oldestKey !== undefined) {
        const evicted = this.pool.get(oldestKey);
        this.pool.delete(oldestKey);
        if (evicted) {
          await evicted.adapter.close();
        }
      }
    }

    // Create new connection via tenancy resolver
    const adapter = await this.createTenantAdapter(tenantId);
    this.pool.set(tenantId, { adapter, lastUsed: Date.now() });
    return adapter;
  }

  /** Close a specific tenant's connection */
  async close(tenantId: string): Promise<void> {
    const entry = this.pool.get(tenantId);
    if (entry) {
      await entry.adapter.close();
      this.pool.delete(tenantId);
    }
  }

  /** Close all connections — called during shutdown */
  async closeAll(): Promise<void> {
    const closePromises: Promise<void>[] = [];
    for (const [, entry] of this.pool) {
      closePromises.push(entry.adapter.close());
    }
    await Promise.allSettled(closePromises);
    this.pool.clear();

    // Also close the base adapter
    await this.baseAdapter.close();
  }

  /** Current number of active connections */
  get size(): number {
    return this.pool.size;
  }

  /**
   * Create a tenant-specific adapter instance using the tenancy resolver.
   * The base adapter's constructor pattern is used to create new instances.
   */
  private async createTenantAdapter(tenantId: string): Promise<IAdapter> {
    if (!this.tenancy?.resolveConnection) {
      throw new Error(
        `database_per_tenant mode requires a resolveConnection function, ` +
        `but none was provided for tenant "${tenantId}"`
      );
    }

    const connectionConfig = await this.tenancy.resolveConnection(tenantId);
    if (!connectionConfig) {
      throw new Error(`resolveConnection returned null for tenant "${tenantId}"`);
    }

    // The adapter needs to be reconstructable from connection config.
    // We use the base adapter's constructor pattern via a factory approach.
    // For now, we dynamically instantiate based on the base adapter type.
    const AdapterClass = this.baseAdapter.constructor as new (config: Record<string, unknown>) => IAdapter;
    const adapter = new AdapterClass(connectionConfig);
    await adapter.init();
    return adapter;
  }
}
