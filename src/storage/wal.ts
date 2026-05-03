import type { LogbunLog } from '../types';
import { mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';

/**
 * Write-Ahead Log — Append-only NDJSON file.
 *
 * Format: one JSON object per line, terminated by \n.
 * Uses Bun.file().writer() for buffered append-only writes — fastest
 * possible I/O pattern (no seek, no read, no transaction).
 */
export class WALStorage {
  private readonly path: string;
  private writer: ReturnType<ReturnType<typeof Bun.file>['writer']> | null = null;

  constructor(namespace: string) {
    this.path = `.logbun/${namespace}/wal/current.aof`;
  }

  /** Ensure the WAL directory exists and open the writer */
  async init(): Promise<void> {
    await mkdir(dirname(this.path), { recursive: true });
    // Touch the file if it doesn't exist
    const file = Bun.file(this.path);
    if (!(await file.exists())) {
      await Bun.write(this.path, '');
    }
    this.writer = file.writer();
  }

  /** Append a single log entry as NDJSON */
  async append(log: LogbunLog): Promise<void> {
    if (!this.writer) {
      throw new Error('WAL not initialized — call init() first');
    }
    const line = JSON.stringify(log) + '\n';
    this.writer.write(line);
    await this.writer.flush();
  }

  /**
   * Read all log entries from the WAL.
   * Used only during bootstrap recovery. Silently discards malformed
   * partial writes from a previous crash.
   */
  async readAll(): Promise<LogbunLog[]> {
    const file = Bun.file(this.path);
    if (!(await file.exists())) return [];

    const content = await file.text();
    if (!content.trim()) return [];

    const lines = content.split('\n');
    const logs: LogbunLog[] = [];

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        logs.push(JSON.parse(trimmed) as LogbunLog);
      } catch {
        // Silently discard malformed partial writes
      }
    }

    return logs;
  }

  /**
   * Truncate the WAL to zero bytes after a successful recovery flush.
   * Reuses the file (does not delete it).
   */
  async truncate(): Promise<void> {
    // Close existing writer before truncating
    if (this.writer) {
      await this.writer.flush();
      await this.writer.end();
      this.writer = null;
    }
    await Bun.write(this.path, '');
    // Re-open writer
    this.writer = Bun.file(this.path).writer();
  }

  /** Close the writer — called during shutdown */
  async close(): Promise<void> {
    if (this.writer) {
      await this.writer.flush();
      await this.writer.end();
      this.writer = null;
    }
  }
}
