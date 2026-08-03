/**
 * Filesystem path helpers for FileReliabilityAdapter.
 * Requires node:path (Node / Bun / Deno node:compat).
 *
 * Deno: grant `--allow-read` and `--allow-write` for the data directory.
 */
import { join } from 'node:path';
import { sanitizeNamespace } from '../../utils/namespace';

export { sanitizeNamespace };

/**
 * Resolve the Logbun data directory root for a namespace.
 * Joins `dataDir` (default `.logbun`) with the sanitized namespace.
 * Namespace cannot escape the root (sanitize forbids `.` / `..` / `/`).
 * Absolute `dataDir` is allowed; `..` path segments in `dataDir` are rejected.
 */
export function resolveLogbunDir(namespace: string, dataDir?: string): string {
  const ns = sanitizeNamespace(namespace);
  const root = dataDir ?? '.logbun';

  if (dataDir != null) {
    const normalized = dataDir.replace(/\\/g, '/');
    const parts = normalized.split('/').filter((p) => p.length > 0);
    if (parts.includes('..')) {
      throw new Error(
        `Invalid dataDir ${JSON.stringify(dataDir)}: path traversal ("..") is not allowed`
      );
    }
  }

  return join(root, ns);
}

/** Shared-contract alias for {@link resolveLogbunDir}. */
export const resolveDataDir = resolveLogbunDir;
