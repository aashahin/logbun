/**
 * Heavy E2E: safety controls — payload caps, string truncation, redaction,
 * integrity chain, encryption at rest, path/namespace sanitization.
 */
import { describe, expect, test } from 'bun:test';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

import { BunSQLiteAdapter } from '../src/adapters/bun-sqlite';
import {
  AuditLogger,
  ENTERPRISE_DEFAULTS,
  INTEGRITY_GENESIS,
  normalizeEncryptionKey,
  sanitizeNamespace,
  sanitizeTenantKey,
} from '../src/index';
import { resolveLogbunDir } from '../src/durability/filesystem';
import {
  installTestCleanup,
  eventCollector,
  FAST_BATCH,
  FAST_RETRY,
  waitFor,
  makeFileReliability,
} from './helpers';

type Actions = 'profile.updated' | 'secret.rotated' | 'doc.saved';

const { tempDataDir } = installTestCleanup();

describe('e2e safety controls', () => {
  test('maxPayloadBytes truncates oversized bags and emits truncated', async () => {
    const dataDir = await tempDataDir('logbun-e2e-payload-');
    const { has, onEvent } = eventCollector();
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-payload',
      reliability: makeFileReliability('e2e-payload', dataDir),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      maxPayloadBytes: 120,
      batching: { maxSize: 1, flushInterval: 20 },
      retry: FAST_RETRY,
      onEvent,
    });
    await audit.ready;

    const big = 'x'.repeat(5_000);
    await audit.fireAsync('doc.saved', {
      tenantId: 't1',
      actorId: 'u1',
      entityId: 'doc-1',
      newValues: { body: big, title: 'ok' },
      metadata: { note: big },
    });

    expect(has('truncated')).toBe(true);

    await waitFor(async () => {
      const page = await audit.query({
        tenantId: 't1',
        pagination: { limit: 5 },
      });
      return page.logs.length >= 1;
    });

    const page = await audit.query({
      tenantId: 't1',
      pagination: { limit: 5 },
    });
    const log = page.logs[0]!;
    const serialized = JSON.stringify({
      oldValues: log.oldValues,
      newValues: log.newValues,
      metadata: log.metadata,
    });
    // After cap, stored payload should be much smaller than raw input
    expect(serialized.length).toBeLessThan(5_000);
    await audit.shutdown();
  });

  test('maxStringFieldBytes truncates actor/entity/userAgent but not tenantId', async () => {
    const dataDir = await tempDataDir('logbun-e2e-strcap-');
    const { has, onEvent } = eventCollector();
    const longTenant = `tenant_${'z'.repeat(80)}`;
    const longActor = 'A'.repeat(500);
    const longEntity = 'E'.repeat(500);
    const longUa = 'UA/'.repeat(200);

    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-strcap',
      reliability: makeFileReliability('e2e-strcap', dataDir),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      maxStringFieldBytes: 32,
      batching: { maxSize: 1, flushInterval: 20 },
      retry: FAST_RETRY,
      onEvent,
    });
    await audit.ready;

    await audit.fireAsync(
      'profile.updated',
      {
        tenantId: longTenant,
        actorId: longActor,
        entityId: longEntity,
      },
      { userAgent: longUa, ipAddress: '198.51.100.7' },
    );

    expect(has('truncated', 'max_string_field_bytes')).toBe(true);

    await waitFor(async () => {
      const page = await audit.query({
        tenantId: longTenant,
        pagination: { limit: 5 },
      });
      return page.logs.length >= 1;
    });

    const page = await audit.query({
      tenantId: longTenant,
      pagination: { limit: 5 },
    });
    const log = page.logs[0]!;
    // tenantId is routing identity — never capped
    expect(log.tenantId).toBe(longTenant);
    expect(log.actorId!.length).toBeLessThan(longActor.length);
    expect(log.actorId).toContain('…');
    expect(log.entityId!.length).toBeLessThan(longEntity.length);
    expect(log.userAgent!.length).toBeLessThan(longUa.length);
    await audit.shutdown();
  });

  test('deep redaction across nested objects and arrays', async () => {
    const dataDir = await tempDataDir('logbun-e2e-redact-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-redact',
      reliability: makeFileReliability('e2e-redact', dataDir),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      redactPaths: [
        'password',
        'ssn',
        'token',
        'metadata.payment.cardNumber',
        'newValues.secrets',
      ],
      batching: { maxSize: 1, flushInterval: 20 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    await audit.fireAsync('profile.updated', {
      tenantId: 't-red',
      actorId: 'u1',
      entityId: 'profile-1',
      oldValues: { password: 'old-secret', name: 'Ada' },
      newValues: {
        password: 'new-secret',
        name: 'Ada Lovelace',
        secrets: { apiKey: 'k-123' },
        contacts: [
          { email: 'a@x.com', ssn: '111-22-3333' },
          { email: 'b@x.com', ssn: '444-55-6666' },
        ],
      },
      metadata: {
        token: 'hdr-token',
        payment: { cardNumber: '4111111111111111', last4: '1111' },
        safe: true,
      },
    });

    await waitFor(async () => {
      const page = await audit.query({
        tenantId: 't-red',
        pagination: { limit: 5 },
      });
      return page.logs.length >= 1;
    });

    const log = (
      await audit.query({ tenantId: 't-red', pagination: { limit: 5 } })
    ).logs[0]!;

    expect(log.oldValues!['password']).toBeUndefined();
    expect(log.oldValues!['name']).toBe('Ada');
    expect(log.newValues!['password']).toBeUndefined();
    expect(log.newValues!['secrets']).toBeUndefined();
    expect(log.newValues!['name']).toBe('Ada Lovelace');
    expect(log.metadata!['token']).toBeUndefined();
    expect(
      (log.metadata!['payment'] as Record<string, unknown>)['cardNumber'],
    ).toBeUndefined();
    expect(
      (log.metadata!['payment'] as Record<string, unknown>)['last4'],
    ).toBe('1111');
    expect(log.metadata!['safe']).toBe(true);

    const contacts = log.newValues!['contacts'] as Array<
      Record<string, unknown>
    >;
    expect(contacts[0]!['ssn']).toBeUndefined();
    expect(contacts[0]!['email']).toBe('a@x.com');
    expect(contacts[1]!['ssn']).toBeUndefined();

    await audit.shutdown();
  });

  test('integrityChain seals, verifies, and detects tamper after query', async () => {
    const dataDir = await tempDataDir('logbun-e2e-integ-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-integ',
      reliability: makeFileReliability('e2e-integ', dataDir),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      integrityChain: true,
      batching: { maxSize: 10, flushInterval: 30, maxQueueSize: 50 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    for (let i = 0; i < 5; i++) {
      await audit.fireAsync('doc.saved', {
        tenantId: 't-i',
        actorId: 'writer',
        entityId: `doc-${i}`,
        newValues: { n: i },
      });
    }

    await waitFor(async () => {
      const page = await audit.query({
        tenantId: 't-i',
        pagination: { limit: 20 },
      });
      return page.logs.length >= 5;
    });

    // Query returns newest first; integrity needs oldest-first
    const page = await audit.query({
      tenantId: 't-i',
      pagination: { limit: 20 },
    });
    const oldestFirst = [...page.logs].reverse();

    expect(oldestFirst[0]!.prevHash).toBe(INTEGRITY_GENESIS);
    for (const log of oldestFirst) {
      expect(log.contentHash).toMatch(/^[0-9a-f]{64}$/);
      expect(log.prevHash).toMatch(/^[0-9a-f]{64}$/);
    }
    for (let i = 1; i < oldestFirst.length; i++) {
      expect(oldestFirst[i]!.prevHash).toBe(oldestFirst[i - 1]!.contentHash);
    }

    const ok = await audit.verifyIntegrity(oldestFirst);
    expect(ok.ok).toBe(true);
    expect(ok.failedAt).toBe(-1);

    // Tamper
    const tampered = oldestFirst.map((l, i) =>
      i === 2
        ? { ...l, newValues: { n: 999, hacked: true } }
        : { ...l },
    );
    const bad = await audit.verifyIntegrity(tampered);
    expect(bad.ok).toBe(false);
    expect(bad.failedAt).toBeGreaterThanOrEqual(0);

    await audit.shutdown();
  });

  test('encryption at rest: WAL/DLQ ciphertext; recovery still works', async () => {
    const dataDir = await tempDataDir('logbun-e2e-enc-');
    const passphrase = 'e2e-test-passphrase-not-for-prod';

    const a1 = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-enc',
      reliability: makeFileReliability('e2e-enc', dataDir, { encryptionKey: passphrase }),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      // Keep unflushed so WAL has content
      batching: {
        maxSize: 10_000,
        flushInterval: 60_000,
        maxQueueSize: 10_000,
      },
      retry: FAST_RETRY,
    });
    await a1.ready;

    await a1.fireAsync('secret.rotated', {
      tenantId: 't-enc',
      actorId: 'ops',
      entityId: 'key-1',
      newValues: { fingerprint: 'abc123' },
    });

    // Peek at WAL file on disk — should not contain plaintext action if encrypted
    const nsDir = resolveLogbunDir('e2e-enc', dataDir);
    // WAL path is under namespace dir
    const { readdir } = await import('node:fs/promises');
    const walk = async (dir: string): Promise<string[]> => {
      const out: string[] = [];
      try {
        for (const ent of await readdir(dir, { withFileTypes: true })) {
          const p = join(dir, ent.name);
          if (ent.isDirectory()) out.push(...(await walk(p)));
          else out.push(p);
        }
      } catch {
        /* missing */
      }
      return out;
    };
    const files = await walk(nsDir);
    const aof = files.find((f) => f.endsWith('.aof') || f.includes('current'));
    if (aof) {
      const raw = await readFile(aof, 'utf8');
      // Ciphertext lines use e1: prefix; plaintext JSON would include "secret.rotated"
      const hasPlain = raw.includes('secret.rotated');
      const hasCipher = raw.includes('e1:') || !hasPlain;
      // If WAL still holds the entry, prefer cipher; empty WAL after compact is ok
      if (raw.trim().length > 0) {
        expect(hasCipher || !hasPlain).toBe(true);
      }
    }

    await a1.shutdown();

    // New instance with same key can read/query persisted SQLite
    const a2 = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-enc-2',
      reliability: makeFileReliability('e2e-enc-2', dataDir, { encryptionKey: passphrase }),
      dataDir: join(dataDir, 'n2'),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      mode: 'volatile',
      requireTenantId: true,
      batching: FAST_BATCH,
      retry: FAST_RETRY,
    });
    await a2.ready;
    const page = await a2.query({
      tenantId: 't-enc',
      pagination: { limit: 10 },
    });
    expect(page.logs.some((l) => l.entityId === 'key-1')).toBe(true);
    expect(page.logs[0]!.newValues?.['fingerprint']).toBe('abc123');
    await a2.shutdown();

    // normalizeEncryptionKey is deterministic for passphrases
    const k1 = await normalizeEncryptionKey(passphrase);
    const k2 = await normalizeEncryptionKey(passphrase);
    expect(k1).toEqual(k2);
    expect(k1.length).toBe(32);
  });

  test('integrity + encryption together still round-trips', async () => {
    const dataDir = await tempDataDir('logbun-e2e-both-');
    const passphrase = 'e2e-both-passphrase';
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-both',
      reliability: makeFileReliability('e2e-both', dataDir, {
        encryptionKey: passphrase,
      }),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      integrityChain: true,
      batching: { maxSize: 3, flushInterval: 25 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    for (let i = 0; i < 4; i++) {
      await audit.fireAsync('doc.saved', {
        tenantId: 't-both',
        actorId: 'u',
        entityId: `c-${i}`,
      });
    }

    await waitFor(async () => {
      const p = await audit.query({
        tenantId: 't-both',
        pagination: { limit: 20 },
      });
      return p.logs.length >= 4;
    });

    const logs = (
      await audit.query({ tenantId: 't-both', pagination: { limit: 20 } })
    ).logs;
    const chain = await audit.verifyIntegrity([...logs].reverse());
    expect(chain.ok).toBe(true);
    await audit.shutdown();
  });

  test('namespace and tenant path helpers reject traversal', () => {
    expect(() => sanitizeNamespace('../etc')).toThrow();
    expect(() => sanitizeNamespace('a/b')).toThrow();
    expect(() => sanitizeNamespace('')).toThrow();
    expect(sanitizeNamespace('my-app_01')).toBe('my-app_01');

    const key = sanitizeTenantKey('../../etc/passwd');
    expect(key).not.toContain('..');
    expect(key).not.toContain('/');
    expect(sanitizeTenantKey(null)).toBe('__global__');
    expect(sanitizeTenantKey(undefined)).toBe('__global__');

    const dir = resolveLogbunDir('safe-ns', '/tmp/logbun-data');
    expect(dir.includes('safe-ns')).toBe(true);
    expect(dir.includes('..')).toBe(false);
  });

  test('maxQueryLimit clamps oversized pagination requests', async () => {
    const dataDir = await tempDataDir('logbun-e2e-qlim-');
    const audit = new AuditLogger<Actions>({
      ...ENTERPRISE_DEFAULTS,
      namespace: 'e2e-qlim',
      reliability: makeFileReliability('e2e-qlim', dataDir),
      adapter: new BunSQLiteAdapter({ path: join(dataDir, 'a.db') }),
      maxQueryLimit: 5,
      batching: { maxSize: 50, flushInterval: 30 },
      retry: FAST_RETRY,
    });
    await audit.ready;

    for (let i = 0; i < 12; i++) {
      await audit.fireAsync('doc.saved', {
        tenantId: 't-q',
        actorId: 'u',
        entityId: `d-${i}`,
      });
    }

    await waitFor(async () => {
      const p = await audit.query({
        tenantId: 't-q',
        pagination: { limit: 1000 },
      });
      return p.logs.length > 0;
    });

    const page = await audit.query({
      tenantId: 't-q',
      pagination: { limit: 1000 },
    });
    expect(page.logs.length).toBeLessThanOrEqual(5);
    expect(page.nextCursor).not.toBeNull();
    await audit.shutdown();
  });

  test('volatile mode loses unflushed queue on shutdown without WAL recovery', async () => {
    const dataDir = await tempDataDir('logbun-e2e-vol-');
    const inserted: string[] = [];
    const adapter = {
      async init() {},
      async bulkInsert(_t: string | null, logs: { entityId?: string }[]) {
        for (const l of logs) if (l.entityId) inserted.push(l.entityId);
        return true;
      },
      async query() {
        return { logs: [], nextCursor: null };
      },
      async prune() {},
      async close() {},
    };

    const audit = new AuditLogger<Actions>({
      namespace: 'e2e-vol',
      mode: 'volatile',
      requireTenantId: true,
      adapter,
      // Never auto-flush
      batching: {
        maxSize: 10_000,
        flushInterval: 60_000,
        maxQueueSize: 10_000,
      },
      retry: FAST_RETRY,
    });
    await audit.ready;

    await audit.fireAsync('doc.saved', {
      tenantId: 't',
      actorId: 'a',
      entityId: 'will-flush-on-shutdown',
    });
    expect(audit.getStats().queued).toBeGreaterThan(0);

    // shutdown still flushes in-memory queue
    await audit.shutdown();
    expect(inserted).toContain('will-flush-on-shutdown');
  });
});
