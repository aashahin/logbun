import { AuditLogger, MemoryReliabilityAdapter, randomUUIDv7 } from '../dist/index.js';
import { FileReliabilityAdapter } from '../dist/durability/filesystem/index.js';
import { BunSQLiteAdapter } from '../dist/adapters/bun-sqlite.js';

if (!AuditLogger || !MemoryReliabilityAdapter || !randomUUIDv7) {
  throw new Error('Bun ESM root missing exports');
}
if (!FileReliabilityAdapter) throw new Error('filesystem missing');
if (!BunSQLiteAdapter) throw new Error('bun-sqlite missing');

const id = randomUUIDv7();
if (!id.includes('-')) throw new Error('bad uuid');

// CJS
const cjs = require('../dist/index.cjs');
if (!cjs.AuditLogger) throw new Error('Bun CJS root missing');

console.log('Bun smoke OK (ESM+CJS + bun-sqlite)');
