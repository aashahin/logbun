/**
 * Resolve client IP from proxy headers.
 *
 * - `trustedProxyCount` 0 → do not trust XFF / X-Real-IP (return undefined).
 * - `trustedProxyCount` >= 1 → take XFF by skipping that many hops from the right;
 *   if XFF is missing, fall back to X-Real-IP.
 */
export function extractClientIp(
  getHeader: (name: string) => string | null | undefined,
  trustedProxyCount: number
): string | undefined {
  if (trustedProxyCount < 1) return undefined;

  const xff = getHeader('x-forwarded-for');
  if (xff) {
    const parts = xff
      .split(',')
      .map((part) => part.trim())
      .filter(Boolean);
    if (parts.length > 0) {
      // Skip N trusted proxies from the right → client at length-1-N.
      // If the list is shorter than N+1 entries, use the leftmost hop.
      const clientIdx = parts.length - 1 - trustedProxyCount;
      if (clientIdx >= 0) return parts[clientIdx];
      return parts[0];
    }
  }

  const realIp = getHeader('x-real-ip')?.trim();
  return realIp || undefined;
}
