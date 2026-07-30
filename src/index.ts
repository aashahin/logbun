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
  WalConfig,
  DLQFileInfo,

  // Adapter
  IAdapter,

  // Plugin context
  LogbunRequestContext,
} from './types';

export { ENTERPRISE_DEFAULTS } from './types';

// ─── Path / tenant helpers ───────────────────────────────────────────────────
export {
  sanitizeNamespace,
  resolveLogbunDir,
  resolveDataDir,
} from './utils/path';

export { sanitizeTenantKey, isTenantIdPresent } from './utils/tenant';

export {
  INTEGRITY_GENESIS,
  verifyIntegrityChain,
  normalizeEncryptionKey,
} from './utils/crypto';

export { InstanceLock, InstanceLockError } from './storage/instance-lock';
