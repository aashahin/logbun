import type { Context } from 'hono';
import type { AuditLogger } from '../logger';
import type { LogbunLogInput } from '../types';

/**
 * Hono audit logging middleware.
 *
 * Sets `auditLog.fire()` on the Hono context with auto-extracted
 * IP address and User-Agent from the request headers.
 *
 * Usage:
 * ```typescript
 * import { createAuditMiddleware } from 'logbun/plugins/hono';
 *
 * app.use('*', createAuditMiddleware(audit));
 *
 * app.post('/courses', (c) => {
 *   const auditLog = c.get('auditLog');
 *   auditLog.fire('course.created', {
 *     actorId: user.id,
 *     entityId: course.id,
 *   });
 * });
 * ```
 */
export const createAuditMiddleware = <T extends string>(logger: AuditLogger<T>) =>
  async (c: Context, next: () => Promise<void>) => {
    c.set('auditLog', {
      fire: (action: T, input: Omit<LogbunLogInput<T>, 'action'>) =>
        logger.fire(action, input, {
          // X-Forwarded-For may contain "client, proxy1, proxy2" — extract first (client) IP
          ipAddress: c.req.header('x-forwarded-for')?.split(',')[0]?.trim() ?? undefined,
          userAgent: c.req.header('user-agent') ?? undefined,
        }),
    });
    await next();
  };
