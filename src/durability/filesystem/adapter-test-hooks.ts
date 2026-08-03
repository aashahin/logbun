import type { DLQStorageOptions } from './dlq';
import type { WALStorageOptions } from './wal';

/** @internal Source-test seams; this module is not a package export. */
export interface FileReliabilityAdapterTestHooks {
  walDirectorySync?: WALStorageOptions['directorySync'];
  dlqDirectorySync?: DLQStorageOptions['directorySync'];
  afterLockAcquire?: () => void | Promise<void>;
  beforeStorageClose?: () => void | Promise<void>;
}

const hooksByOptions = new WeakMap<object, FileReliabilityAdapterTestHooks>();

/** @internal Register hooks before constructing with the exact options object. */
export function setFileReliabilityAdapterTestHooks(
  options: object,
  hooks: FileReliabilityAdapterTestHooks,
): void {
  hooksByOptions.set(options, hooks);
}

/** @internal Production entrypoints import only this lookup; the setter is tree-shaken. */
export function fileReliabilityAdapterTestHooksFor(
  options: object,
): FileReliabilityAdapterTestHooks | undefined {
  return hooksByOptions.get(options);
}
