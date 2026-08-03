import { defineConfig } from 'tsup'

/**
 * Universal package builds (Node/Bun/Deno consumers):
 * Core + plugins + filesystem durability + optional adapters: ESM + CJS
 *
 * Cloudflare durability is built separately (ESM only) via tsup.cloudflare.config.ts
 * so a parallel clean cannot wipe its outputs.
 */
export default defineConfig({
  entry: {
    index: 'src/index.ts',
    'durability/filesystem/index': 'src/durability/filesystem/index.ts',
    'adapters/bun-sqlite': 'src/adapters/bun-sqlite.ts',
    'adapters/turso': 'src/adapters/turso.ts',
    'adapters/clickhouse': 'src/adapters/clickhouse.ts',
    'plugins/elysia': 'src/plugins/elysia.ts',
    'plugins/hono': 'src/plugins/hono.ts',
  },
  format: ['esm', 'cjs'],
  dts: true,
  splitting: true,
  clean: true,
  treeshake: true,
  outDir: 'dist',
  target: 'es2022',
  minify: false,
  sourcemap: true,
  external: [
    'bun:sqlite',
    '@libsql/client',
    '@clickhouse/client',
    'elysia',
    'hono',
    'node:fs',
    'node:fs/promises',
    'node:path',
    'node:readline',
    'node:os',
  ],
})
