/**
 * At-rest crypto + integrity helpers (Web Crypto / SubtleCrypto).
 * Used by WAL/DLQ encryption and optional hash-chain tamper evidence.
 */

const TEXT = new TextEncoder();
const HEX = '0123456789abcdef';

/** AES-256-GCM encrypted line/blob prefix. */
export const ENC_PREFIX = 'e1:';

export type EncryptionKeyBytes = Uint8Array;

/**
 * Normalize config key material to 32 raw bytes.
 * Accepts: 32-byte Uint8Array, 64-char hex, or base64 of 32 bytes.
 * Longer passphrases are SHA-256 hashed to 32 bytes (documented tradeoff).
 */
export async function normalizeEncryptionKey(
  key: string | Uint8Array
): Promise<EncryptionKeyBytes> {
  if (key instanceof Uint8Array) {
    if (key.byteLength === 32) return key.slice();
    if (key.byteLength === 0) {
      throw new Error('encryptionKey must not be empty');
    }
    // Hash arbitrary bytes to 32
    const digest = await crypto.subtle.digest('SHA-256', new Uint8Array(key));
    return new Uint8Array(digest);
  }

  if (typeof key !== 'string' || key.length === 0) {
    throw new Error('encryptionKey must be a non-empty string or 32-byte Uint8Array');
  }

  // 64 hex chars → 32 bytes
  if (/^[0-9a-fA-F]{64}$/.test(key)) {
    const out = new Uint8Array(32);
    for (let i = 0; i < 32; i++) {
      out[i] = parseInt(key.slice(i * 2, i * 2 + 2), 16);
    }
    return out;
  }

  // base64 of 32 bytes
  try {
    const bin = Uint8Array.from(atob(key), (c) => c.charCodeAt(0));
    if (bin.byteLength === 32) return bin;
  } catch {
    /* fall through to passphrase hash */
  }

  // Passphrase → SHA-256 (enterprise: prefer raw 32-byte key via env)
  const digest = await crypto.subtle.digest('SHA-256', TEXT.encode(key));
  return new Uint8Array(digest);
}

/** Module-level AES CryptoKey cache keyed by hex of the 32-byte raw material. */
const aesKeyCache = new Map<string, CryptoKey>();

function rawKeyHex(raw: EncryptionKeyBytes): string {
  let out = '';
  for (let i = 0; i < raw.length; i++) {
    const b = raw[i]!;
    out += HEX[b >> 4]! + HEX[b & 0xf]!;
  }
  return out;
}

async function importAesKey(raw: EncryptionKeyBytes): Promise<CryptoKey> {
  const id = rawKeyHex(raw);
  const cached = aesKeyCache.get(id);
  if (cached) return cached;
  const key = await crypto.subtle.importKey(
    'raw',
    new Uint8Array(raw),
    { name: 'AES-GCM' },
    false,
    ['encrypt', 'decrypt']
  );
  aesKeyCache.set(id, key);
  return key;
}

function b64encode(bytes: Uint8Array): string {
  let s = '';
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]!);
  return btoa(s);
}

function b64decode(s: string): Uint8Array {
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

/**
 * Encrypt UTF-8 plaintext → `e1:<iv_b64>:<ct_b64>` (AES-256-GCM).
 */
export async function encryptUtf8(
  plaintext: string,
  rawKey: EncryptionKeyBytes
): Promise<string> {
  const key = await importAesKey(rawKey);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ct = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv },
    key,
    TEXT.encode(plaintext)
  );
  return `${ENC_PREFIX}${b64encode(iv)}:${b64encode(new Uint8Array(ct))}`;
}

/**
 * Decrypt a line produced by {@link encryptUtf8}.
 * Plain (non-prefixed) lines are returned as-is for migration.
 */
export async function decryptUtf8(
  line: string,
  rawKey: EncryptionKeyBytes
): Promise<string> {
  if (!line.startsWith(ENC_PREFIX)) return line;
  const rest = line.slice(ENC_PREFIX.length);
  const colon = rest.indexOf(':');
  if (colon <= 0) throw new Error('invalid encrypted payload');
  const iv = b64decode(rest.slice(0, colon));
  const ct = b64decode(rest.slice(colon + 1));
  const key = await importAesKey(rawKey);
  const pt = await crypto.subtle.decrypt(
    { name: 'AES-GCM', iv: new Uint8Array(iv) },
    key,
    new Uint8Array(ct)
  );
  return new TextDecoder().decode(pt);
}

export function isEncryptedLine(line: string): boolean {
  return line.startsWith(ENC_PREFIX);
}

function bytesToHex(bytes: ArrayBuffer | Uint8Array): string {
  const arr = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  let out = '';
  for (let i = 0; i < arr.length; i++) {
    const b = arr[i]!;
    out += HEX[b >> 4]! + HEX[b & 0xf]!;
  }
  return out;
}

/** Genesis prev hash for integrity chains. */
export const INTEGRITY_GENESIS =
  '0000000000000000000000000000000000000000000000000000000000000000';

/**
 * Canonical payload for integrity hashing — stable subset of log fields
 * excluding integrity itself and request decoration that may be re-derived.
 */
export function integrityPayload(log: {
  id: string;
  tenantId?: string;
  actorId: string;
  action: string;
  entityId?: string;
  oldValues?: unknown;
  newValues?: unknown;
  metadata?: unknown;
  createdAt: string;
}): string {
  return JSON.stringify({
    id: log.id,
    tenantId: log.tenantId ?? null,
    actorId: log.actorId,
    action: log.action,
    entityId: log.entityId ?? null,
    oldValues: log.oldValues ?? null,
    newValues: log.newValues ?? null,
    metadata: log.metadata ?? null,
    createdAt: log.createdAt,
  });
}

/** SHA-256 hex of `prevHash + '\\n' + payload`. */
export async function computeIntegrityHash(
  prevHash: string,
  payload: string
): Promise<string> {
  const data = TEXT.encode(`${prevHash}\n${payload}`);
  const digest = await crypto.subtle.digest('SHA-256', data);
  return bytesToHex(digest);
}

export interface IntegrityFields {
  prevHash: string;
  contentHash: string;
}

/**
 * Seal a log into the chain after `prevHash`.
 * Returns fields to attach on the log record.
 */
export async function sealIntegrity(
  log: {
    id: string;
    tenantId?: string;
    actorId: string;
    action: string;
    entityId?: string;
    oldValues?: unknown;
    newValues?: unknown;
    metadata?: unknown;
    createdAt: string;
  },
  prevHash: string
): Promise<IntegrityFields> {
  const payload = integrityPayload(log);
  const contentHash = await computeIntegrityHash(prevHash, payload);
  return { prevHash, contentHash };
}

/**
 * Verify a chain of logs in order (oldest first).
 * Returns first failure index or -1 if OK.
 */
export async function verifyIntegrityChain(
  logs: Array<{
    id: string;
    tenantId?: string;
    actorId: string;
    action: string;
    entityId?: string;
    oldValues?: unknown;
    newValues?: unknown;
    metadata?: unknown;
    createdAt: string;
    prevHash?: string;
    contentHash?: string;
  }>,
  genesis: string = INTEGRITY_GENESIS
): Promise<{ ok: boolean; failedAt: number; error?: string }> {
  let expectedPrev = genesis;
  for (let i = 0; i < logs.length; i++) {
    const log = logs[i]!;
    if (!log.prevHash || !log.contentHash) {
      return {
        ok: false,
        failedAt: i,
        error: 'missing integrity fields',
      };
    }
    if (log.prevHash !== expectedPrev) {
      return {
        ok: false,
        failedAt: i,
        error: `prevHash mismatch (expected ${expectedPrev.slice(0, 12)}…)`,
      };
    }
    const payload = integrityPayload(log);
    const expected = await computeIntegrityHash(log.prevHash, payload);
    if (expected !== log.contentHash) {
      return {
        ok: false,
        failedAt: i,
        error: 'contentHash mismatch (payload tampered)',
      };
    }
    expectedPrev = log.contentHash;
  }
  return { ok: true, failedAt: -1 };
}
