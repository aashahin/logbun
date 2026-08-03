/**
 * logbun/durability/filesystem
 *
 * Persistent reliability for Node.js, Bun, and Deno (node:fs).
 *
 * Deno permissions:
 *   deno run --allow-read=./.logbun --allow-write=./.logbun --allow-sys=uid,gid your_app.ts
 * Grant read/write on whatever `dataDir` you configure (default `.logbun`).
 */
export {
  FileReliabilityAdapter,
  type FileReliabilityAdapterOptions,
  type FileReliabilityWalOptions,
  type FileReliabilityDlqOptions,
} from './adapter';

export {
  resolveLogbunDir,
  resolveDataDir,
  sanitizeNamespace,
} from './path';

export {
  InstanceLock,
  InstanceLockError,
  type InstanceLockOptions,
} from './instance-lock';

export {
  WALStorage,
  WAL_SIZE_SOFT_LIMIT_BYTES,
  WAL_SEGMENT_BYTES_DEFAULT,
  type WALStorageOptions,
  type WALReadBoundedOptions,
  type WALReadBoundedResult,
} from './wal';

export {
  DLQStorage,
  DLQ_MAX_FILES_DEFAULT,
  parseBatch,
  readBatch,
  idFromFilename,
  tenantIdFromFilename,
  type DLQStorageOptions,
  type DLQBatchEnvelope,
  type ParsedDLQBatch,
} from './dlq';
