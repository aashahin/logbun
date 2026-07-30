import { expect, test } from 'bun:test';

import { extractClientIp } from '../src/utils/client-ip';

test('trustedProxyCount 0 ignores XFF and X-Real-IP', () => {
  const headers: Record<string, string> = {
    'x-forwarded-for': '1.1.1.1, 2.2.2.2',
    'x-real-ip': '3.3.3.3',
  };
  expect(extractClientIp((name) => headers[name], 0)).toBeUndefined();
});

test('trustedProxyCount 1 takes second-to-last XFF hop', () => {
  const headers: Record<string, string> = {
    'x-forwarded-for': 'client, proxy',
  };
  expect(extractClientIp((name) => headers[name], 1)).toBe('client');
});

test('trustedProxyCount falls back to X-Real-IP when XFF missing', () => {
  const headers: Record<string, string> = {
    'x-real-ip': '9.9.9.9',
  };
  expect(extractClientIp((name) => headers[name], 1)).toBe('9.9.9.9');
});
