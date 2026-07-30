import { join } from 'node:path';

const NAMESPACE_RE = /^[a-zA-Z0-9_-]{1,64}$/;

/**
 * Validate and return a storage namespace.
 * Only alphanumeric, underscore, and hyphen; 1–64 characters.
 * @throws Error if empty or invalid
 */
export function sanitizeNamespace(ns: string): string {
  if (typeof ns !== 'string' || !ns || !NAMESPACE_RE.test(ns)) {
    throw new Error(
      `Invalid namespace ${JSON.stringify(ns)}: must match /^[a-zA-Z0-9_-]{1,64}$/`
    );
  }
  return ns;
}

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
    // Reject path-traversal segments in dataDir (absolute paths are fine)
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
