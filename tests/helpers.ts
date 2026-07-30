/**
 * Shared helpers for heavy integration / production-style tests.
 *
 * Call {@link installTestCleanup} once at the top of each test file so
 * `afterEach` registers in that file's suite (module cache would skip a
 * top-level afterEach on subsequent imports).
 */
import { afterEach } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import type {
  IAdapter,
  LogbunEvent,
  LogbunLog,
  LogbunQueryFilters,
  LogbunQueryResult,
} from '../src/types';

export type TempCleanup = {
  tempDataDir: (prefix?: string) => Promise<string>;
  trackPath: (path: string) => void;
};

/**
 * Register afterEach cleanup for this test file and return temp-dir helpers.
 * Must be called at module top-level of each test file that uses temp dirs.
 */
export function installTestCleanup(): TempCleanup {
  const cleanupPaths: string[] = [];

  afterEach(async () => {
    await Promise.all(
      cleanupPaths
        .splice(0)
        .map((path) => rm(path, { recursive: true, force: true })),
    );
  });

  return {
    async tempDataDir(prefix = 'logbun-e2e-'): Promise<string> {
      const dir = await mkdtemp(join(tmpdir(), prefix));
      cleanupPaths.push(dir);
      return dir;
    },
    trackPath(path: string): void {
      cleanupPaths.push(path);
    },
  };
}

export async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 3_000,
  intervalMs = 15,
): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export function makeLog(
  id: string,
  overrides: Partial<LogbunLog> = {},
): LogbunLog {
  return {
    id,
    actorId: 'actor-1',
    action: 'test.action',
    createdAt: new Date().toISOString(),
    ...overrides,
  };
}

/** In-memory adapter that records inserts; optionally fails. */
export function memoryAdapter(opts?: {
  failInsert?: boolean | (() => boolean);
  failAfter?: number;
  delayMs?: number;
  onInsert?: (tenantId: string | null, logs: LogbunLog[]) => void;
}): IAdapter & {
  inserted: LogbunLog[];
  insertCalls: number;
  byTenant: Map<string | null, LogbunLog[]>;
} {
  const inserted: LogbunLog[] = [];
  const byTenant = new Map<string | null, LogbunLog[]>();
  let insertCalls = 0;

  const adapter = {
    inserted,
    insertCalls: 0,
    byTenant,
    async init() {},
    async bulkInsert(tenantId: string | null, logs: LogbunLog[]) {
      insertCalls++;
      adapter.insertCalls = insertCalls;
      if (opts?.delayMs) await sleep(opts.delayMs);
      if (typeof opts?.failAfter === 'number' && insertCalls <= opts.failAfter) {
        throw new Error('adapter failAfter');
      }
      const shouldFail =
        typeof opts?.failInsert === 'function'
          ? opts.failInsert()
          : opts?.failInsert === true;
      if (shouldFail) throw new Error('adapter down');
      opts?.onInsert?.(tenantId, logs);
      inserted.push(...logs);
      const bucket = byTenant.get(tenantId) ?? [];
      bucket.push(...logs);
      byTenant.set(tenantId, bucket);
      return true;
    },
    async query(
      tenantId: string | null,
      filters: LogbunQueryFilters,
      pagination: { cursor?: string; limit: number },
    ): Promise<LogbunQueryResult> {
      let logs = tenantId
        ? inserted.filter((l) => l.tenantId === tenantId)
        : [...inserted];
      if (filters.action) logs = logs.filter((l) => l.action === filters.action);
      if (filters.actorId)
        logs = logs.filter((l) => l.actorId === filters.actorId);
      if (filters.entityId)
        logs = logs.filter((l) => l.entityId === filters.entityId);
      if (filters.startDate)
        logs = logs.filter((l) => l.createdAt >= filters.startDate!);
      if (filters.endDate)
        logs = logs.filter((l) => l.createdAt <= filters.endDate!);
      logs = [...logs].sort((a, b) => (a.id < b.id ? 1 : a.id > b.id ? -1 : 0));
      if (pagination.cursor) {
        logs = logs.filter((l) => l.id < pagination.cursor!);
      }
      const page = logs.slice(0, pagination.limit);
      const hasMore = logs.length > pagination.limit;
      return {
        logs: page,
        nextCursor: hasMore && page.length ? page[page.length - 1]!.id : null,
      };
    },
    async prune() {},
    async close() {},
  };

  return adapter;
}

/** Collect onEvent emissions in insertion order. */
export function eventCollector(): {
  events: LogbunEvent[];
  onEvent: (e: LogbunEvent) => void;
  ofType: (type: LogbunEvent['type']) => LogbunEvent[];
  has: (type: LogbunEvent['type'], detail?: string) => boolean;
} {
  const events: LogbunEvent[] = [];
  return {
    events,
    onEvent: (e) => events.push(e),
    ofType: (type) => events.filter((e) => e.type === type),
    has: (type, detail) =>
      events.some(
        (e) => e.type === type && (detail === undefined || e.detail === detail),
      ),
  };
}

/** Fast batching + retry defaults for tests (avoid long timers). */
export const FAST_BATCH = {
  maxSize: 50,
  flushInterval: 60_000,
  maxQueueSize: 500,
  onQueueFull: 'dlq' as const,
};

export const FAST_RETRY = {
  insertMaxRetries: 1,
  insertBaseDelayMs: 1,
  initialDelayMs: 60_000,
  scanIntervalMs: 60_000,
  maxScanAttempts: 3,
};
