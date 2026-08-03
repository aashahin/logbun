import { expect, test } from 'bun:test';
import { CloudflareReliabilityAdapter } from '../src/durability/cloudflare/adapter';

const state = {
  storage: {
    sql: {
      exec() {
        return {};
      },
    },
  },
};

test('CloudflareReliabilityAdapter rejects unsafe SQL table prefixes', () => {
  expect(() =>
    new CloudflareReliabilityAdapter({
      state,
      tablePrefix: 'audit; DROP TABLE journal',
    }),
  ).toThrow(/tablePrefix/);
});
