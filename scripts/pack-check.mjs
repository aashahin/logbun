import { copyFile, mkdtemp, mkdir, readFile, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const root = new URL('..', import.meta.url).pathname;
const temp = await mkdtemp(join(tmpdir(), 'logbun-pack-'));
const packs = join(temp, 'packs');
const consumer = join(temp, 'consumer');

function run(command, args, cwd = root) {
  const result = spawnSync(command, args, { cwd, encoding: 'utf8' });
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} failed:\n${result.stdout}\n${result.stderr}`);
  return result.stdout;
}

try {
  await mkdir(packs);
  const packed = JSON.parse(run('npm', ['pack', '--json', '--pack-destination', packs]));
  const tarball = join(packs, packed[0].filename);
  const listing = run('tar', ['-tf', tarball]);
  for (const required of [
    'package/dist/index.js',
    'package/dist/index.cjs',
    'package/dist/index.d.ts',
    'package/dist/durability/filesystem/index.js',
    'package/dist/durability/cloudflare/index.js',
  ]) {
    if (!listing.includes(required)) throw new Error(`packed artifact missing ${required}`);
  }

  await mkdir(consumer);
  run('npm', ['install', '--ignore-scripts', '--no-package-lock', '--prefix', consumer, tarball]);
  await copyFile(join(root, 'scripts', 'packed-node-smoke.mjs'), join(consumer, 'packed-node-smoke.mjs'));
  const packageJson = JSON.parse(await readFile(join(consumer, 'node_modules', 'logbun', 'package.json'), 'utf8'));
  if (packageJson.version !== '1.0.0') throw new Error('packed package version mismatch');
  const esm = run('node', ['--input-type=module', '-e', "import('logbun').then(({AuditLogger,MemoryReliabilityAdapter})=>{if(!AuditLogger||!MemoryReliabilityAdapter)throw new Error('missing root exports')})"], consumer);
  const cjs = run('node', ['-e', "const fs=require('node:fs');const os=require('node:os');const path=require('node:path');const p=require('logbun');const {FileReliabilityAdapter}=require('logbun/durability/filesystem');if(!p.AuditLogger||!FileReliabilityAdapter)throw new Error('missing CJS exports');(async()=>{const dir=fs.mkdtempSync(path.join(os.tmpdir(),'logbun-cjs-'));try{const r=new FileReliabilityAdapter({namespace:'cjs',dataDir:dir,instanceLock:false});await r.init();await r.appendJournal({id:'018f0000-0000-7000-8000-000000000003',actorId:'cjs',action:'cjs.smoke',createdAt:new Date().toISOString()});if((await r.recoverJournal()).logs.length!==1)throw new Error('CJS filesystem recovery failed');await r.close();}finally{fs.rmSync(dir,{recursive:true,force:true})}})().catch(e=>{console.error(e);process.exit(1)})"], consumer);
  const nodeSmoke = run('node', ['packed-node-smoke.mjs'], consumer);
  void esm;
  void cjs;
  process.stdout.write(nodeSmoke);
  console.log('npm pack validation OK (exports, ESM, CJS, declarations)');
} finally {
  await rm(temp, { recursive: true, force: true });
}
