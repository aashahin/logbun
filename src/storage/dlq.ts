import type { LogbunLog } from '../types';
import { mkdir, readdir, rename, unlink } from 'node:fs/promises';

/**
 * Dead Letter Queue — directory-per-namespace, one file per failed batch.
 *
 * Filename convention: {tenantId}_{unixTimestampMs}.batch
 * Each .batch file contains a JSON array of LogbunLog objects.
 *
 * Atomic processing pattern:
 *   1. rename .batch → .batch.processing
 *   2. Parse → attempt bulkInsert
 *   3a. Success → unlink .processing
 *   3b. Failure → rename .processing → .batch
 */
export class DLQStorage {
  private readonly dir: string;

  constructor(namespace: string) {
    this.dir = `.logbun/${namespace}/dlq`;
  }

  /** Ensure the DLQ directory exists */
  async init(): Promise<void> {
    await mkdir(this.dir, { recursive: true });
  }

  /** Write a failed batch to the DLQ as a JSON array */
  async write(tenantId: string | null, logs: LogbunLog[]): Promise<void> {
    const key = tenantId ?? '__global__';
    const ts = Date.now();
    // Random suffix prevents filename collision when two backpressure
    // events fire within the same millisecond for the same tenant
    const rand = Math.random().toString(36).slice(2, 8);
    const filename = `${key}_${ts}_${rand}.batch`;
    const path = `${this.dir}/${filename}`;
    await Bun.write(path, JSON.stringify(logs));
  }

  /**
   * List all pending .batch files, sorted oldest-first.
   * @returns Full file paths of pending batch files
   */
  async listPending(): Promise<string[]> {
    let entries: string[];
    try {
      entries = await readdir(this.dir);
    } catch {
      return [];
    }

    return entries
      .filter((f) => f.endsWith('.batch') && !f.endsWith('.batch.processing'))
      .sort() // lexicographic — oldest first due to timestamp in filename
      .map((f) => `${this.dir}/${f}`);
  }

  /**
   * Atomically mark a batch file as being processed.
   * @returns The new .processing file path
   */
  async markProcessing(filePath: string): Promise<string> {
    const processingPath = `${filePath}.processing`;
    await rename(filePath, processingPath);
    return processingPath;
  }

  /** Remove a successfully processed batch file */
  async markDone(processingPath: string): Promise<void> {
    await unlink(processingPath);
  }

  /** Rename a failed .processing file back to .batch for retry */
  async markFailed(processingPath: string): Promise<void> {
    const originalPath = processingPath.replace(/\.processing$/, '');
    await rename(processingPath, originalPath);
  }

  /**
   * Move a permanently failing batch to .dead extension.
   * Prevents infinite retry loops on corrupted or schema-incompatible batches.
   */
  async markPoisoned(processingPath: string): Promise<void> {
    const deadPath = processingPath.replace(/\.processing$/, '.dead');
    await rename(processingPath, deadPath);
  }

  /**
   * Startup orphan cleanup: any .processing file found means the
   * previous process crashed mid-retry. Rename back to .batch.
   */
  async recoverOrphans(): Promise<void> {
    let entries: string[];
    try {
      entries = await readdir(this.dir);
    } catch {
      return;
    }

    const orphans = entries.filter((f) => f.endsWith('.batch.processing'));
    for (const orphan of orphans) {
      const processingPath = `${this.dir}/${orphan}`;
      const batchPath = processingPath.replace(/\.processing$/, '');
      await rename(processingPath, batchPath);
    }
  }
}
