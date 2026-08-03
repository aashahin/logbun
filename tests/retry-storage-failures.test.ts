import { expect, test } from 'bun:test';

import { ConnectionPool } from '../src/engine/pool';
import { RetryEngine } from '../src/engine/retry';
import { MemoryReliabilityAdapter } from '../src/reliability/memory';
import { makeLog, memoryAdapter } from './helpers';

class FaultInjectingReliability extends MemoryReliabilityAdapter {
  listFailuresRemaining = 0;
  claimFailuresRemaining = 0;
  claimedReadFailuresRemaining = 0;
  failedSettlementFailuresRemaining = 0;

  override async listDlq(
    opts?: Parameters<MemoryReliabilityAdapter['listDlq']>[0],
  ): ReturnType<MemoryReliabilityAdapter['listDlq']> {
    if (this.listFailuresRemaining-- > 0) throw new Error('simulated list storage failure');
    return super.listDlq(opts);
  }

  override async claimDlq(
    id?: string,
  ): ReturnType<MemoryReliabilityAdapter['claimDlq']> {
    if (this.claimFailuresRemaining-- > 0) throw new Error(`simulated claim storage failure: ${id}`);
    const claimed = await super.claimDlq(id);
    if (claimed && this.claimedReadFailuresRemaining-- > 0) {
      throw new Error(`simulated claimed-batch read failure: ${id}`);
    }
    return claimed;
  }

  override async settleDlqFailure(id: string, nextAttempts: number): Promise<void> {
    if (this.failedSettlementFailuresRemaining-- > 0) {
      throw new Error(`simulated failed-settlement storage failure: ${id}`);
    }
    await super.settleDlqFailure(id, nextAttempts);
  }
}

function retryEngine(
  reliability: FaultInjectingReliability,
  adapter = memoryAdapter(),
): RetryEngine {
  return new RetryEngine({
    reliability,
    adapter,
    pool: new ConnectionPool(adapter, 5),
    retry: { insertMaxRetries: 1, insertBaseDelayMs: 0, maxScanAttempts: 10 },
  });
}

test('retry scan propagates list storage failures', async () => {
  const reliability = new FaultInjectingReliability();
  await reliability.init();
  reliability.listFailuresRemaining = 1;

  await expect(retryEngine(reliability).scan()).rejects.toThrow(/list storage failure/);
});

test('retry scan aggregates concurrent claim storage failures', async () => {
  const reliability = new FaultInjectingReliability();
  await reliability.init();
  await reliability.writeDlq(null, [makeLog('claim-failure-a')]);
  await reliability.writeDlq(null, [makeLog('claim-failure-b')]);
  reliability.claimFailuresRemaining = 2;

  const error = await retryEngine(reliability).scan().then(
    () => null,
    (failure: unknown) => failure,
  );
  expect(error).toBeInstanceOf(AggregateError);
  expect((error as AggregateError).errors).toHaveLength(2);
});

test('next retry scan recovers a claim stranded by a transient read failure', async () => {
  const reliability = new FaultInjectingReliability();
  await reliability.init();
  const id = await reliability.writeDlq(null, [makeLog('read-recovery')]);
  const destination = memoryAdapter();
  const engine = retryEngine(reliability, destination);
  reliability.claimedReadFailuresRemaining = 1;

  await expect(engine.scan()).rejects.toThrow(/claimed-batch read failure/);
  expect(await reliability.listDlq({ includeProcessing: true })).toEqual([
    expect.objectContaining({ id, state: 'processing' }),
  ]);

  await expect(engine.scan()).resolves.toBeUndefined();
  expect(destination.inserted.map((entry) => entry.id)).toEqual(['read-recovery']);
  expect(await reliability.readDlq(id)).toBeNull();
});

test('next retry scan recovers a transient failed settlement and delivers once', async () => {
  const reliability = new FaultInjectingReliability();
  await reliability.init();
  const id = await reliability.writeDlq(null, [makeLog('settlement-recovery')]);
  let destinationAttempt = 0;
  const destination = memoryAdapter({
    failInsert: () => ++destinationAttempt === 1,
  });
  const engine = retryEngine(reliability, destination);
  reliability.failedSettlementFailuresRemaining = 1;

  await expect(engine.scan()).rejects.toThrow(/failed-settlement storage failure/);
  expect(await reliability.listDlq({ includeProcessing: true })).toEqual([
    expect.objectContaining({ id, state: 'processing' }),
  ]);

  await expect(engine.scan()).resolves.toBeUndefined();
  expect(destination.inserted.map((entry) => entry.id)).toEqual(['settlement-recovery']);
  expect(await reliability.readDlq(id)).toBeNull();
});
