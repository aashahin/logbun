/**
 * A durable reliability mutation committed, but its host wake-up could not be
 * scheduled. Callers must surface this error without attempting a second
 * durable fallback, which could create duplicate journal/DLQ copies.
 */
export class DurableAdmissionSchedulingError extends Error {
  readonly durableAdmissionCommitted = true;

  constructor(cause: unknown) {
    const detail = cause instanceof Error ? cause.message : String(cause);
    super(`durable admission committed but maintenance scheduling failed: ${detail}`, {
      cause,
    });
    this.name = 'DurableAdmissionSchedulingError';
  }
}

export function isDurableAdmissionSchedulingError(
  error: unknown,
): error is DurableAdmissionSchedulingError {
  return error instanceof DurableAdmissionSchedulingError || (
    typeof error === 'object' &&
    error !== null &&
    'durableAdmissionCommitted' in error &&
    error.durableAdmissionCommitted === true
  );
}
