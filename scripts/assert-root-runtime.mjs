#!/usr/bin/env node
/**
 * Assert built root artifact has no forbidden runtime-specific imports:
 * node:, bun:, Bun, Deno, process.*, cloudflare:.
 *
 * Traverses root ESM, CJS, and declaration entries and every relative chunk
 * they reach. Independently exported durability/adapters/plugins are not roots.
 */
import { existsSync, readFileSync } from 'node:fs';
import { dirname, extname, relative, resolve } from 'node:path';

const dist = new URL('../dist/', import.meta.url).pathname;

const FORBIDDEN = [
  { re: /\bfrom\s*['"]node:/, msg: 'node: import' },
  { re: /\bimport\s*['"]node:/, msg: 'node: import' },
  { re: /\brequire\s*\(\s*['"]node:/, msg: 'node: require' },
  { re: /\bfrom\s*['"]bun:/, msg: 'bun: import' },
  { re: /\bimport\s*['"]bun:/, msg: 'bun: import' },
  { re: /\bfrom\s*['"]cloudflare:/, msg: 'cloudflare: import' },
  { re: /\bBun\./, msg: 'Bun.* global' },
  { re: /\bDeno\./, msg: 'Deno.* global' },
  { re: /\bprocess\b/, msg: 'process global' },
];

function declarationCandidates(resolvedPath) {
  const ext = extname(resolvedPath);
  if (ext === '.js' || ext === '.mjs') {
    return [resolvedPath, resolvedPath.slice(0, -ext.length) + '.d.ts'];
  }
  if (ext === '.cjs') {
    return [resolvedPath, resolvedPath.slice(0, -ext.length) + '.d.cts'];
  }
  return [resolvedPath];
}

function localDependencies(file, source) {
  const dependencies = new Set();
  const pattern = /(?:\bfrom\s*|\bimport\s*|\brequire\s*\()(['"])(\.[^'"]+)\1/g;
  for (const match of source.matchAll(pattern)) {
    const specifier = match[2];
    if (!specifier) continue;
    for (const candidate of declarationCandidates(resolve(dirname(file), specifier))) {
      if (existsSync(candidate)) dependencies.add(candidate);
    }
  }
  return dependencies;
}

function rootReachableFiles() {
  const queue = [
    resolve(dist, 'index.js'),
    resolve(dist, 'index.cjs'),
    resolve(dist, 'index.d.ts'),
    resolve(dist, 'index.d.cts'),
  ].filter(existsSync);
  const visited = new Set();
  while (queue.length > 0) {
    const file = queue.pop();
    if (!file || visited.has(file)) continue;
    const rel = relative(dist, file);
    if (rel.startsWith('..')) throw new Error(`root graph escaped dist: ${file}`);
    visited.add(file);
    const source = readFileSync(file, 'utf8');
    for (const dependency of localDependencies(file, source)) {
      if (!visited.has(dependency)) queue.push(dependency);
    }
  }
  return visited;
}

if (!existsSync(dist)) {
  console.error('dist/ missing — run build first');
  process.exit(1);
}

const files = rootReachableFiles();
let failed = false;
for (const file of files) {
  const rel = relative(dist, file);
  let src = readFileSync(file, 'utf8');
  // Strip comments so documentation wording cannot produce a false positive.
  src = src
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/(^|[^:])\/\/.*$/gm, '$1');
  for (const { re, msg } of FORBIDDEN) {
    if (re.test(src)) {
      console.error(`FORBIDDEN ${msg} in ${rel}`);
      failed = true;
    }
  }
}

if (failed) {
  console.error('Root artifact runtime assertion failed');
  process.exit(1);
}
console.log(`Root runtime assertion OK (${files.size} root-reachable files scanned)`);
