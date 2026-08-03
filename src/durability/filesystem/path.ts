/**
 * Filesystem path helpers for FileReliabilityAdapter.
 * Requires node:path (Node / Bun / Deno node:compat).
 *
 * Deno: grant `--allow-read` and `--allow-write` for the data directory.
 */
import { lstat, realpath } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
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

/**
 * Validate the nearest existing ancestor before creating a filesystem root.
 * A realpath mismatch means some lexical path segment is a symbolic link.
 */
export async function assertNoSymlinkPath(
  targetPath: string,
  label = 'Logbun filesystem path',
): Promise<void> {
  let candidate = resolve(targetPath);
  for (;;) {
    try {
      const info = await lstat(candidate);
      if (info.isSymbolicLink()) {
        throw new Error(`${label} must not contain a symbolic link`);
      }
      const physical = await realpath(candidate);
      if (physical !== candidate) {
        throw new Error(`${label} must not contain a symbolic link`);
      }
      return;
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code === 'ENOENT') {
        const parent = dirname(candidate);
        if (parent === candidate) return;
        candidate = parent;
        continue;
      }
      throw error;
    }
  }
}

/** Shared-contract alias for {@link resolveLogbunDir}. */
export const resolveDataDir = resolveLogbunDir;
