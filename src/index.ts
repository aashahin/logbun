// ─── Main Class ──────────────────────────────────────────────────────────────
export { AuditLogger } from './logger';
export type { AuditLoggerStats } from './logger';

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

  // Events
  LogbunEvent,
  LogbunEventType,

  // Configuration
  LogbunConfig,
  BatchingConfig,
  TenancyConfig,
  RetentionConfig,
  RetryConfig,

  // Reliability
  ReliabilityAdapter,
  DLQEntry,
  ClaimedDlqBatch,
  JournalRecoveryResult,
  ReliabilityStats,
  DlqState,

  // Adapter
  IAdapter,

  // Plugin context
  LogbunRequestContext,
} from './types';

export { ENTERPRISE_DEFAULTS } from './types';

// ─── In-memory reliability (volatile default) ────────────────────────────────
export { MemoryReliabilityAdapter } from './reliability/memory';
export type { MemoryReliabilityOptions } from './reliability/memory';

// ─── Pure helpers (no node:/bun:/process) ────────────────────────────────────
export { sanitizeNamespace } from './utils/namespace';
export { sanitizeTenantKey, isTenantIdPresent } from './utils/tenant';
export {
  INTEGRITY_GENESIS,
  verifyIntegrityChain,
  normalizeEncryptionKey,
} from './utils/crypto';
export {
  randomUUIDv7,
  uuidVersion,
  uuidVariantRfc,
  uuidv7TimestampMs,
} from './utils/uuidv7';
