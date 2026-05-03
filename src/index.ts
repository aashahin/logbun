// ─── Main Class ──────────────────────────────────────────────────────────────
export { AuditLogger } from './logger';

// ─── Types ───────────────────────────────────────────────────────────────────
export type {
  // Primitives
  DurabilityMode,
  QueueFullBehavior,
  TenancyMode,

  // Log shapes
  LogbunLogInput,
  LogbunLog,

  // Query
  LogbunQueryFilters,
  LogbunQueryResult,

  // Configuration
  LogbunConfig,
  BatchingConfig,
  TenancyConfig,
  RetentionConfig,

  // Adapter
  IAdapter,

  // Plugin context
  LogbunRequestContext,
} from './types';
