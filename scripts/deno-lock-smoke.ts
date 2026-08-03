import { FileReliabilityAdapter } from 'logbun/durability/filesystem';

const [mode, dataDir] = Deno.args;
if (!mode || !dataDir) {
  throw new Error('usage: deno-lock-smoke.ts <hold|expect-held|acquire-release> <data-dir>');
}

const adapter = new FileReliabilityAdapter({
  namespace: 'deno-cross-process-lock',
  dataDir,
});

if (mode === 'hold') {
  await adapter.init();
  console.log('DENO_LOCK_READY');
  await new Promise<never>(() => undefined);
} else if (mode === 'expect-held') {
  let rejected = false;
  try {
    await adapter.init();
  } catch (error) {
    rejected = String(error).includes('instance_lock_held');
  }
  await adapter.close();
  if (!rejected) throw new Error('Deno lock acquisition did not fail closed');
} else if (mode === 'acquire-release') {
  await adapter.init();
  await adapter.close();
} else {
  throw new Error(`unknown Deno lock smoke mode: ${mode}`);
}
