import { defineConfig } from 'tsup'

/** Cloudflare durability — ESM only for Workers / Durable Objects. */
export default defineConfig({
  entry: {
    'durability/cloudflare/index': 'src/durability/cloudflare/index.ts',
  },
  format: ['esm'],
  dts: true,
  splitting: false,
  clean: false,
  treeshake: true,
  outDir: 'dist',
  target: 'es2022',
  minify: false,
  sourcemap: true,
})
