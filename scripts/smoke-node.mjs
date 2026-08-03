import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);

// ESM
const esm = await import('../dist/index.js');
if (!esm.AuditLogger || !esm.MemoryReliabilityAdapter || !esm.randomUUIDv7) {
  throw new Error('ESM root missing exports');
}
const id = esm.randomUUIDv7();
if (typeof id !== 'string' || id.length < 36) throw new Error('bad uuid');

// CJS
const cjs = require('../dist/index.cjs');
if (!cjs.AuditLogger || !cjs.MemoryReliabilityAdapter) {
  throw new Error('CJS root missing exports');
}

const fsEsm = await import('../dist/durability/filesystem/index.js');
if (!fsEsm.FileReliabilityAdapter) throw new Error('filesystem ESM missing');
const fsCjs = require('../dist/durability/filesystem/index.cjs');
if (!fsCjs.FileReliabilityAdapter) throw new Error('filesystem CJS missing');

console.log('Node smoke OK (ESM+CJS root + filesystem)');
