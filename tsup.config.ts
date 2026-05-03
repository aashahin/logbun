import { defineConfig } from 'tsup'

export default defineConfig({
  entry: [
    'src/index.ts',
    'src/adapters/sqlite.ts',
    'src/adapters/turso.ts',
    'src/adapters/clickhouse.ts',
    'src/plugins/elysia.ts',
    'src/plugins/hono.ts',
  ],
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
  ],
})
