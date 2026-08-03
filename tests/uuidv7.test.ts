import { expect, test } from 'bun:test';
import {
  randomUUIDv7,
  uuidVersion,
  uuidVariantRfc,
  uuidv7TimestampMs,
  _resetUUIDv7StateForTests,
} from '../src/utils/uuidv7';

test('UUIDv7 has version 7 and RFC variant', () => {
  _resetUUIDv7StateForTests();
  const id = randomUUIDv7();
  expect(uuidVersion(id)).toBe(7);
  expect(uuidVariantRfc(id)).toBe(true);
  expect(id).toMatch(
    /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
  );
});

test('UUIDv7 is lexicographically ordered by timestamp', () => {
  _resetUUIDv7StateForTests();
  const a = randomUUIDv7(1_700_000_000_000);
  const b = randomUUIDv7(1_700_000_000_100);
  expect(a < b).toBe(true);
  expect(uuidv7TimestampMs(a)).toBe(1_700_000_000_000);
  expect(uuidv7TimestampMs(b)).toBe(1_700_000_000_100);
});

test('UUIDv7 is monotonic within the same millisecond', () => {
  _resetUUIDv7StateForTests();
  const ts = 1_700_000_111_000;
  const ids = Array.from({ length: 64 }, () => randomUUIDv7(ts));
  for (let i = 1; i < ids.length; i++) {
    expect(ids[i]! > ids[i - 1]!).toBe(true);
  }
});

test('UUIDv7 concurrent generation has no collisions', () => {
  _resetUUIDv7StateForTests();
  const set = new Set<string>();
  for (let i = 0; i < 5_000; i++) {
    set.add(randomUUIDv7());
  }
  expect(set.size).toBe(5_000);
});
