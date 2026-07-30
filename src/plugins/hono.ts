import type { Context, MiddlewareHandler } from 'hono';
import type { AuditLogger } from '../logger';
import type { LogbunLogInput } from '../types';
import { extractClientIp } from '../utils/client-ip';
import { isTenantIdPresent } from '../utils/tenant';

export interface AuditMiddlewareOptions {
  /**
   * Number of reverse proxies in front of the app that may append to
   * `X-Forwarded-For`. Default `0` — XFF is **not** trusted and `ipAddress`
   * stays undefined (unless you pass it yourself).
   *
   * When `>= 1`, the client IP is taken from XFF by skipping that many hops
   * from the right. If XFF is missing, falls back to `X-Real-IP`.
   */
  trustedProxyCount?: number;
  /**
   * Optional tenant resolver from the Hono request context. When provided,
   * `tenantId` is merged into `fire` / `fireAsync` input only if
   * `input.tenantId` is missing.
   */
  getTenantId?: (c: Context) => string | undefined;
}

/** Shape stored on Hono context as `auditLog`. */
export interface HonoAuditLog<T extends string = string> {
  /** Never-throws fire-and-forget enqueue. */
  fire: (action: T, input: Omit<LogbunLogInput<T>, 'action'>) => void;
  /** Await full enqueue (including WAL in durable mode); may reject. */
  fireAsync: (
    action: T,
    input: Omit<LogbunLogInput<T>, 'action'>
  ) => Promise<void>;
}

/** Augment Hono Variables for typed `c.get('auditLog')`. */
export type LogbunHonoVariables<T extends string = string> = {
  auditLog: HonoAuditLog<T>;
};

/**
 * Hono audit logging middleware.
 *
 * Sets `auditLog` on the context with `fire` and `fireAsync`. Auto-extracts
 * IP (when `trustedProxyCount` > 0) and User-Agent. Optional `getTenantId`
 * fills `tenantId` when the caller omits it.
 *
 * - `fire` is never-throws fire-and-forget.
 * - `fireAsync` returns a Promise and may reject (durable enqueue / degraded).
 * - `trustedProxyCount` defaults to `0` (XFF not trusted).
 *
 * Usage:
 * ```typescript
 * import { createAuditMiddleware, type LogbunHonoVariables } from 'logbun/plugins/hono';
 *
 * const app = new Hono<{ Variables: LogbunHonoVariables<Actions> }>();
 * app.use('*', createAuditMiddleware(audit, {
 *   trustedProxyCount: 1,
 *   getTenantId: (c) => c.req.header('x-tenant-id') ?? undefined,
 * }));
 *
 * app.post('/courses', async (c) => {
 *   c.get('auditLog').fire('course.created', { actorId: user.id });
 *   await c.get('auditLog').fireAsync('course.published', { actorId: user.id });
 * });
 * ```
 */
export const createAuditMiddleware = <T extends string>(
  logger: AuditLogger<T>,
  opts?: AuditMiddlewareOptions
): MiddlewareHandler => {
  const trustedProxyCount = opts?.trustedProxyCount ?? 0;
  const getTenantId = opts?.getTenantId;

  return async (c: Context, next: () => Promise<void>) => {
    const requestContext = {
      ipAddress: extractClientIp(
        (name) => c.req.header(name),
        trustedProxyCount
      ),
      userAgent: c.req.header('user-agent') ?? undefined,
    };

    const withTenant = (
      input: Omit<LogbunLogInput<T>, 'action'>
    ): Omit<LogbunLogInput<T>, 'action'> => {
      if (isTenantIdPresent(input.tenantId) || !getTenantId) return input;
      const tenantId = getTenantId(c);
      if (!isTenantIdPresent(tenantId)) return input;
      return { ...input, tenantId };
    };

    c.set('auditLog', {
      fire: (action: T, input: Omit<LogbunLogInput<T>, 'action'>) =>
        logger.fire(action, withTenant(input), requestContext),
      fireAsync: (action: T, input: Omit<LogbunLogInput<T>, 'action'>) =>
        logger.fireAsync(action, withTenant(input), requestContext),
    } satisfies HonoAuditLog<T>);
    await next();
  };
};
