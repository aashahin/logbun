import { copyFile, mkdtemp, mkdir, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const root = new URL('..', import.meta.url).pathname;
const temp = await mkdtemp(join(tmpdir(), 'logbun-deno-'));
const packs = join(temp, 'packs');
const consumer = join(temp, 'consumer');
const dataDir = join(temp, 'data');

function run(command, args, cwd = root) {
  const result = spawnSync(command, args, { cwd, encoding: 'utf8' });
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed:\n${result.stdout}\n${result.stderr}`);
  }
  return result.stdout;
}

try {
  await mkdir(packs);
  const packed = JSON.parse(run('npm', ['pack', '--json', '--pack-destination', packs]));
  const tarball = join(packs, packed[0].filename);
  await mkdir(consumer);
  run('npm', ['install', '--ignore-scripts', '--no-package-lock', '--prefix', consumer, tarball]);
  await copyFile(join(root, 'scripts', 'deno-smoke.ts'), join(consumer, 'deno-smoke.ts'));

  const deno = join(root, 'node_modules', '.bin', 'deno');
  const output = run(deno, [
    'run',
    `--allow-read=${dataDir}`,
    `--allow-write=${dataDir}`,
    '--allow-sys=uid,gid',
    '--node-modules-dir=manual',
    'deno-smoke.ts',
    dataDir,
  ], consumer);
  process.stdout.write(output);
} finally {
  await rm(temp, { recursive: true, force: true });
}
