import { copyFile, readdir, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const dist = join(root, 'dist');

async function walk(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await walk(path)));
    else files.push(path);
  }
  return files;
}

for (const file of await walk(dist)) {
  if (/\.d\.(?:ts|mts|cts)(?:\.map)?$/.test(file)) await rm(file);
}

const tsc = join(root, 'node_modules', 'typescript', 'bin', 'tsc');
const result = spawnSync(process.execPath, [tsc, '-p', join(root, 'tsconfig.declarations.json')], {
  cwd: root,
  stdio: 'inherit',
});
if (result.status !== 0) process.exit(result.status ?? 1);

for (const file of await walk(dist)) {
  if (!file.endsWith('.d.ts')) continue;
  await copyFile(file, file.slice(0, -5) + '.d.cts');
}
