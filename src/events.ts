import type { LogbunEvent } from './types';

export type LogbunEventHandler = (event: LogbunEvent) => void;

/**
 * Invoke an optional onEvent listener without ever throwing.
 * Used by logger, batcher, retry engine, and bootstrap so listener
 * failures cannot break the audit pipeline.
 */
export function safeEmit(
  onEvent: LogbunEventHandler | undefined,
  event: LogbunEvent
): void {
  if (!onEvent) return;
  try {
    onEvent(event);
  } catch {
    // Listener errors must not break the engine
  }
}
