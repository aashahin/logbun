/**
 * Namespace validation — pure ES2022, no node:/bun: imports.
 */

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
