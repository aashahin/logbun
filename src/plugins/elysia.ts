import { Elysia } from 'elysia';
import type { AuditLogger } from '../logger';
import type { LogbunLogInput } from '../types';
import { extractClientIp } from '../utils/client-ip';
import { isTenantIdPresent } from '../utils/tenant';

export interface AuditPluginOptions {
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
   * Optional tenant resolver from the request. When provided, `tenantId` is
   * merged into `fire` / `fireAsync` input only if `input.tenantId` is missing.
   */
  getTenantId?: (ctx: { request: Request }) => string | undefined;
}

/**
 * ElysiaJS audit logging plugin.
 *
 * Derives `auditLog.fire()` and `auditLog.fireAsync()` into the request context
 * with auto-extracted IP address (when `trustedProxyCount` > 0) and User-Agent.
 * Optional `getTenantId` fills `tenantId` when the caller omits it.
 *
 * - `fire` is never-throws fire-and-forget.
 * - `fireAsync` returns a Promise and may reject (durable enqueue / degraded).
 *
 * Usage:
 * ```typescript
 * import { auditPlugin } from 'logbun/plugins/elysia';
 *
 * const app = new Elysia()
 *   .use(auditPlugin(audit, {
 *     trustedProxyCount: 1,
 *     getTenantId: ({ request }) => request.headers.get('x-tenant-id') ?? undefined,
 *   }))
 *   .post('/courses', async ({ auditLog, body }) => {
 *     auditLog.fire('course.created', {
 *       actorId: user.id,
 *       entityId: course.id,
 *     });
 *     await auditLog.fireAsync('course.published', {
 *       actorId: user.id,
 *       entityId: course.id,
 *     });
 *   });
 * ```
 */
export const auditPlugin = <T extends string>(
  logger: AuditLogger<T>,
  opts?: AuditPluginOptions
) => {
  const trustedProxyCount = opts?.trustedProxyCount ?? 0;
  const getTenantId = opts?.getTenantId;

  // `as: 'global'` is required so parent apps that `.use(auditPlugin(...))`
  // see `auditLog` on the request context (Elysia local derive stays isolated).
  return new Elysia({ name: 'logbun' }).derive(
    { as: 'global' },
    ({ request }) => {
      const requestContext = {
        ipAddress: extractClientIp(
          (name) => request.headers.get(name),
          trustedProxyCount
        ),
        userAgent: request.headers.get('user-agent') ?? undefined,
      };

      const withTenant = (
        input: Omit<LogbunLogInput<T>, 'action'>
      ): Omit<LogbunLogInput<T>, 'action'> => {
        if (isTenantIdPresent(input.tenantId) || !getTenantId) return input;
        const tenantId = getTenantId({ request });
        if (!isTenantIdPresent(tenantId)) return input;
        return { ...input, tenantId };
      };

      return {
        auditLog: {
          fire: (action: T, input: Omit<LogbunLogInput<T>, 'action'>) =>
            logger.fire(action, withTenant(input), requestContext),
          fireAsync: (action: T, input: Omit<LogbunLogInput<T>, 'action'>) =>
            logger.fireAsync(action, withTenant(input), requestContext),
        },
      };
    }
  );
};
