import { InstanceLock } from '../../src/durability/filesystem/instance-lock';

const [phase, namespace, dataDir] = process.argv.slice(2);
if (!phase || !namespace || !dataDir) {
  throw new Error('usage: instance-lock-crash.ts <before|after> <namespace> <data-dir>');
}

const crash = () => {
  process.kill(process.pid, 'SIGKILL');
};
const lock = new InstanceLock(namespace, dataDir, {
  beforeMainPublish: phase === 'before' ? crash : undefined,
  afterMainPublish: phase === 'after' ? crash : undefined,
});
await lock.acquire();
throw new Error(`instance lock crash fixture survived ${phase} publication`);
