/**
 * RFC 9562 UUIDv7 — time-ordered, Web Crypto only (no Bun/Node APIs).
 *
 * Layout (big-endian):
 * - bytes 0–5:   unix timestamp ms (48 bits)
 * - byte 6:      version (0b0111xxxx) + rand_a high nibble
 * - byte 7:      rand_a low 8 bits
 * - byte 8:      variant (0b10xxxxxx) + rand_b high 6 bits
 * - bytes 9–15:  rand_b remainder
 *
 * Monotonic within the same millisecond via a 12-bit sequence counter
 * (rand_a) so consecutive ids in one process sort lexicographically.
 */

let lastTimestampMs = -1;
/** 12-bit sequence for same-ms monotonicity (0..4095). */
let seq = 0;

/**
 * Generate a UUIDv7 string (`xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx`).
 * Thread-safe enough for single-threaded JS; concurrent calls within the
 * same ms still advance the sequence counter atomically in the event loop.
 */
export function randomUUIDv7(nowMs: number = Date.now()): string {
  let ts = Math.max(0, Math.floor(nowMs));

  if (ts === lastTimestampMs) {
    seq = (seq + 1) & 0x0fff;
    if (seq === 0) {
      // Sequence overflow — wait for next millisecond
      ts = lastTimestampMs + 1;
      lastTimestampMs = ts;
    }
  } else if (ts > lastTimestampMs) {
    lastTimestampMs = ts;
    seq = 0;
  } else {
    // Clock went backwards — keep monotonic by advancing from last
    ts = lastTimestampMs;
    seq = (seq + 1) & 0x0fff;
    if (seq === 0) {
      ts = lastTimestampMs + 1;
      lastTimestampMs = ts;
    }
  }

  const bytes = new Uint8Array(16);
  // 48-bit timestamp
  bytes[0] = (ts / 2 ** 40) & 0xff;
  bytes[1] = (ts / 2 ** 32) & 0xff;
  bytes[2] = (ts / 2 ** 24) & 0xff;
  bytes[3] = (ts / 2 ** 16) & 0xff;
  bytes[4] = (ts / 2 ** 8) & 0xff;
  bytes[5] = ts & 0xff;

  // rand_a (12 bits) carries the sequence for same-ms order
  bytes[6] = 0x70 | ((seq >> 8) & 0x0f); // version 7
  bytes[7] = seq & 0xff;

  // Fill remaining with CSPRNG, then set variant on byte 8
  const rand = crypto.getRandomValues(new Uint8Array(8));
  bytes[8] = 0x80 | (rand[0]! & 0x3f); // variant 10
  for (let i = 1; i < 8; i++) {
    bytes[8 + i] = rand[i]!;
  }

  return formatUuid(bytes);
}

function formatUuid(bytes: Uint8Array): string {
  const hex = new Array<string>(16);
  for (let i = 0; i < 16; i++) {
    hex[i] = bytes[i]!.toString(16).padStart(2, '0');
  }
  return (
    hex[0]! +
    hex[1]! +
    hex[2]! +
    hex[3]! +
    '-' +
    hex[4]! +
    hex[5]! +
    '-' +
    hex[6]! +
    hex[7]! +
    '-' +
    hex[8]! +
    hex[9]! +
    '-' +
    hex[10]! +
    hex[11]! +
    hex[12]! +
    hex[13]! +
    hex[14]! +
    hex[15]!
  );
}

/** Parse version nibble from a UUID string (positions 14–15 of hex form). */
export function uuidVersion(id: string): number {
  const hex = id.replace(/-/g, '');
  if (hex.length !== 32) return -1;
  return parseInt(hex[12]!, 16);
}

/** RFC 4122/9562 variant: high 2 bits of clock_seq_hi_and_reserved = 0b10. */
export function uuidVariantRfc(id: string): boolean {
  const hex = id.replace(/-/g, '');
  if (hex.length !== 32) return false;
  const n = parseInt(hex[16]!, 16);
  return (n & 0xc) === 0x8;
}

/** Extract unix-ms timestamp from a UUIDv7 (for tests / diagnostics). */
export function uuidv7TimestampMs(id: string): number {
  const hex = id.replace(/-/g, '');
  if (hex.length !== 32) return -1;
  return parseInt(hex.slice(0, 12), 16);
}

/** Reset monotonic state — test-only. */
export function _resetUUIDv7StateForTests(): void {
  lastTimestampMs = -1;
  seq = 0;
}
