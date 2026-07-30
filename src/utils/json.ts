/** Safe JSON.parse — returns undefined on failure instead of throwing. */
export function safeJsonParse<T = unknown>(raw: unknown): T | undefined {
  if (raw == null || raw === '') return undefined;
  if (typeof raw !== 'string') return undefined;
  try {
    return JSON.parse(raw) as T;
  } catch {
    return undefined;
  }
}

/** Cap a string field; returns truncated value (suffix … when cut). */
export function capString(
  value: string | undefined,
  maxBytes: number
): { value: string | undefined; truncated: boolean } {
  if (value === undefined) return { value: undefined, truncated: false };
  if (maxBytes <= 0) return { value, truncated: false };
  const encoded = new TextEncoder().encode(value);
  if (encoded.length <= maxBytes) return { value, truncated: false };
  // Decode a prefix that fits (avoid splitting multibyte sequences roughly)
  const decoder = new TextDecoder();
  let end = maxBytes;
  // Back up over incomplete UTF-8 tail
  while (end > 0 && (encoded[end - 1]! & 0xc0) === 0x80) end--;
  if (end <= 0) end = Math.min(maxBytes, encoded.length);
  return {
    value: decoder.decode(encoded.subarray(0, end)) + '…',
    truncated: true,
  };
}
