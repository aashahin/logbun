import { Elysia } from 'elysia';
import type { AuditLogger } from '../logger';
import type { LogbunLogInput } from '../types';

/**
 * ElysiaJS audit logging plugin.
 *
 * Derives `auditLog.fire()` into the request context with
 * auto-extracted IP address and User-Agent from the request headers.
 *
 * Usage:
 * ```typescript
 * import { auditPlugin } from 'logbun/plugins/elysia';
 *
 * const app = new Elysia()
 *   .use(auditPlugin(audit))
 *   .post('/courses', ({ auditLog, body }) => {
 *     auditLog.fire('course.created', {
 *       actorId: user.id,
 *       entityId: course.id,
 *     });
 *   });
 * ```
 */
export const auditPlugin = <T extends string>(logger: AuditLogger<T>) =>
  new Elysia({ name: 'logbun' })
    .derive(({ request }) => ({
      auditLog: {
        fire: (action: T, input: Omit<LogbunLogInput<T>, 'action'>) =>
          logger.fire(action, input, {
            // X-Forwarded-For may contain "client, proxy1, proxy2" — extract first (client) IP
            ipAddress: request.headers.get('x-forwarded-for')?.split(',')[0]?.trim() ?? undefined,
            userAgent: request.headers.get('user-agent') ?? undefined,
          }),
      },
    }));
