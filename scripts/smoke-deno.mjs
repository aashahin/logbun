import { copyFile, mkdtemp, mkdir, rm } from 'node:fs/promises';
import { spawn, spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const root = new URL('..', import.meta.url).pathname;
const temp = await mkdtemp(join(tmpdir(), 'logbun-deno-'));
const packs = join(temp, 'packs');
const consumer = join(temp, 'consumer');
const dataDir = join(temp, 'data');
let holder;

function run(command, args, cwd = root) {
  const result = spawnSync(command, args, { cwd, encoding: 'utf8' });
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed:\n${result.stdout}\n${result.stderr}`);
  }
  return result.stdout;
}

function denoArgs(script, args, allowRun = false) {
  return [
    'run',
    `--allow-read=${dataDir}`,
    `--allow-write=${dataDir}`,
    '--allow-sys=uid,gid',
    ...(allowRun ? ['--allow-run'] : []),
    '--node-modules-dir=manual',
    script,
    ...args,
  ];
}

function waitForHolder(child) {
  return new Promise((resolve, reject) => {
    let output = '';
    const timeout = setTimeout(() => reject(new Error(`Deno lock holder did not become ready:\n${output}`)), 10_000);
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', (chunk) => {
      output += chunk;
      if (!output.includes('DENO_LOCK_READY')) return;
      clearTimeout(timeout);
      resolve();
    });
    child.stderr.on('data', (chunk) => { output += chunk; });
    child.once('exit', (code, signal) => {
      clearTimeout(timeout);
      reject(new Error(`Deno lock holder exited before ready (${code ?? signal}):\n${output}`));
    });
    child.once('error', (error) => {
      clearTimeout(timeout);
      reject(error);
    });
  });
}

function waitForExit(child) {
  return new Promise((resolve) => child.once('exit', resolve));
}

try {
  await mkdir(packs);
  const packed = JSON.parse(run('npm', ['pack', '--json', '--pack-destination', packs]));
  const tarball = join(packs, packed[0].filename);
  await mkdir(consumer);
  run('npm', ['install', '--ignore-scripts', '--no-package-lock', '--prefix', consumer, tarball]);
  await copyFile(join(root, 'scripts', 'deno-smoke.ts'), join(consumer, 'deno-smoke.ts'));
  await copyFile(join(root, 'scripts', 'deno-lock-smoke.ts'), join(consumer, 'deno-lock-smoke.ts'));

  const deno = join(root, 'node_modules', '.bin', 'deno');
  const output = run(deno, denoArgs('deno-smoke.ts', [dataDir]), consumer);
  process.stdout.write(output);

  holder = spawn(
    deno,
    denoArgs('deno-lock-smoke.ts', ['hold', dataDir]),
    { cwd: consumer, stdio: ['ignore', 'pipe', 'pipe'] },
  );
  await waitForHolder(holder);
  run(
    deno,
    denoArgs('deno-lock-smoke.ts', ['expect-held', dataDir]),
    consumer,
  );

  holder.kill('SIGKILL');
  await waitForExit(holder);
  holder = undefined;

  // Without process-probe permission a dead owner remains potentially live.
  run(
    deno,
    denoArgs('deno-lock-smoke.ts', ['expect-held', dataDir]),
    consumer,
  );
  // Granting process-probe permission lets ESRCH prove the old owner is dead.
  run(
    deno,
    denoArgs('deno-lock-smoke.ts', ['acquire-release', dataDir], true),
    consumer,
  );
  console.log('Deno cross-process lock smoke OK (no-allow-run fail-closed, allow-run stale recovery)');
} finally {
  if (holder && holder.exitCode == null && holder.signalCode == null) {
    holder.kill('SIGKILL');
    await waitForExit(holder);
  }
  await rm(temp, { recursive: true, force: true });
}
