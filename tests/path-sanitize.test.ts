import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';

import { resolveLogbunDir, sanitizeNamespace } from '../src/durability/filesystem';

describe('sanitizeNamespace', () => {
  test('accepts alphanumeric, underscore, and hyphen within 64 chars', () => {
    expect(sanitizeNamespace('app')).toBe('app');
    expect(sanitizeNamespace('my-app_1')).toBe('my-app_1');
    expect(sanitizeNamespace('A')).toBe('A');
    expect(sanitizeNamespace('a'.repeat(64))).toBe('a'.repeat(64));
    expect(sanitizeNamespace('Tenant_123-prod')).toBe('Tenant_123-prod');
  });

  test('rejects empty string', () => {
    expect(() => sanitizeNamespace('')).toThrow();
  });

  test('rejects namespaces longer than 64 chars', () => {
    expect(() => sanitizeNamespace('a'.repeat(65))).toThrow();
  });

  test('rejects path traversal and special characters', () => {
    expect(() => sanitizeNamespace('..')).toThrow();
    expect(() => sanitizeNamespace('../etc')).toThrow();
    expect(() => sanitizeNamespace('foo/bar')).toThrow();
    expect(() => sanitizeNamespace('foo\\bar')).toThrow();
    expect(() => sanitizeNamespace('foo.bar')).toThrow();
    expect(() => sanitizeNamespace('foo bar')).toThrow();
    expect(() => sanitizeNamespace('foo@bar')).toThrow();
    expect(() => sanitizeNamespace('foo:bar')).toThrow();
  });
});

describe('resolveLogbunDir', () => {
  test('defaults base to .logbun when dataDir is omitted', () => {
    const dir = resolveLogbunDir('my-app');
    expect(dir).toBe(join('.logbun', 'my-app'));
  });

  test('joins custom dataDir with sanitized namespace', () => {
    const dir = resolveLogbunDir('tenant-a', '/var/lib/logbun');
    expect(dir).toBe(join('/var/lib/logbun', 'tenant-a'));
  });

  test('works with relative dataDir', () => {
    const dir = resolveLogbunDir('ns1', 'data');
    expect(dir).toBe(join('data', 'ns1'));
  });

  test('throws on invalid namespace (does not resolve path)', () => {
    expect(() => resolveLogbunDir('../evil')).toThrow();
    expect(() => resolveLogbunDir('bad/name', '/tmp/logbun')).toThrow();
  });

  test('namespace cannot escape dataDir via path segments', () => {
    // sanitize rejects dots and slashes so escape is impossible
    expect(() => resolveLogbunDir('..', '/tmp')).toThrow();
    expect(() => resolveLogbunDir('a/../../x', '/tmp')).toThrow();
  });
});
