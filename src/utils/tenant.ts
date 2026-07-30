/**
 * Filename-safe tenant key for DLQ / on-disk artifacts.
 * Original tenantId is preserved inside DLQ envelopes; only the path key is sanitized.
 *
 * Rejects path separators and `..` so join(dlqDir, key_…) cannot escape the directory.
 */
export function sanitizeTenantKey(tenantId: string | null | undefined): string {
  if (tenantId == null || tenantId === '') return '__global__';

  // Fast path: already safe
  if (/^[a-zA-Z0-9_-]{1,128}$/.test(tenantId) && tenantId !== '..' && tenantId !== '.') {
    return tenantId;
  }

  // Replace anything unsafe; collapse runs of underscores
  const replaced = tenantId
    .replace(/[^a-zA-Z0-9_-]+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .slice(0, 128);

  if (
    replaced &&
    replaced !== '.' &&
    replaced !== '..' &&
    !replaced.includes('/') &&
    !replaced.includes('\\')
  ) {
    return replaced;
  }

  // Deterministic short hash for pathological / empty-after-sanitize ids
  return `t_${fnv1aHex(tenantId)}`;
}

/** 32-bit FNV-1a as 8-char hex (no crypto dependency). */
function fnv1aHex(input: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, '0');
}

/**
 * Soft-validate tenant id for application use (not a security boundary for query).
 * Empty / whitespace-only is treated as missing.
 */
export function isTenantIdPresent(
  tenantId: string | null | undefined
): tenantId is string {
  return typeof tenantId === 'string' && tenantId.trim() !== '';
}
