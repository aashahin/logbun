# Framework plugins

Plugins attach request context (`userAgent`, optional `ipAddress`) to `auditLog.fire` / `auditLog.fireAsync`.

They expose **both**:

| Method | Behavior |
|--------|----------|
| `auditLog.fire` | Never-throws fire-and-forget (same as `logger.fire`) |
| `auditLog.fireAsync` | Awaitable enqueue / WAL; may reject (same as `logger.fireAsync`) |

Optional **`getTenantId`** fills `tenantId` when the handler omits it (does not override an explicit `input.tenantId`).

## IP trust model

| `trustedProxyCount` | Behavior |
|---------------------|----------|
| `0` (default) | Ignore `X-Forwarded-For` and `X-Real-IP`. `ipAddress` is undefined. |
| `≥ 1` | Parse XFF (comma-separated, trimmed). Client index = `length - 1 - trustedProxyCount`. If too short, use leftmost hop. If no XFF, fall back to `X-Real-IP`. |

Set `trustedProxyCount` to the number of reverse proxies **you control** that append to XFF (e.g. `1` for a single load balancer). Do not trust XFF on the open internet without a trusted edge.

Implementation: `extractClientIp(getHeader, trustedProxyCount)` in `src/utils/client-ip.ts`.

---

## Elysia

```bash
bun add elysia
```

```typescript
import { Elysia } from 'elysia';
import { AuditLogger } from 'logbun';
import { auditPlugin } from 'logbun/plugins/elysia';
import { BunSQLiteAdapter } from 'logbun/adapters/sqlite';

type Actions = 'course.created' | 'course.deleted';

const audit = new AuditLogger<Actions>({
  namespace: 'api',
  mode: 'durable',
  adapter: new BunSQLiteAdapter(),
  requireTenantId: true,
});
await audit.ready;

const app = new Elysia()
  .use(
    auditPlugin(audit, {
      trustedProxyCount: 1,
      getTenantId: ({ request }) =>
        request.headers.get('x-tenant-id') ?? undefined,
    })
  )
  .post('/courses', async ({ auditLog, body }) => {
    // never-throws path
    auditLog.fire('course.created', {
      actorId: body.userId,
      entityId: body.courseId,
      newValues: body,
    });

    // critical: await durability before 200 (tenantId from getTenantId if omitted)
    await auditLog.fireAsync('course.created', {
      actorId: body.userId,
      entityId: body.courseId,
    });
  });
```

Options:

```typescript
{
  trustedProxyCount?: number; // default 0
  getTenantId?: (ctx: { request: Request }) => string | undefined;
}
```

---

## Hono

```bash
bun add hono
```

```typescript
import { Hono } from 'hono';
import { AuditLogger } from 'logbun';
import {
  createAuditMiddleware,
  type LogbunHonoVariables,
} from 'logbun/plugins/hono';
import { BunSQLiteAdapter } from 'logbun/adapters/sqlite';

type Actions = 'course.created';

const audit = new AuditLogger<Actions>({
  namespace: 'api',
  mode: 'durable',
  adapter: new BunSQLiteAdapter(),
  requireTenantId: true,
});
await audit.ready;

const app = new Hono<{ Variables: LogbunHonoVariables<Actions> }>();
app.use(
  '*',
  createAuditMiddleware(audit, {
    trustedProxyCount: 1,
    getTenantId: (c) => c.req.header('x-tenant-id') ?? undefined,
  })
);

app.post('/courses', async (c) => {
  const auditLog = c.get('auditLog');

  auditLog.fire('course.created', {
    actorId: 'user_1',
    entityId: 'course_1',
  });

  await auditLog.fireAsync('course.created', {
    actorId: 'user_1',
    entityId: 'course_1',
  });

  return c.json({ ok: true });
});
```

### Types

```typescript
type LogbunHonoVariables<T extends string> = {
  auditLog: {
    fire: (action: T, input: Omit<LogbunLogInput<T>, 'action'>) => void;
    fireAsync: (
      action: T,
      input: Omit<LogbunLogInput<T>, 'action'>
    ) => Promise<void>;
  };
};
```

Options:

```typescript
{
  trustedProxyCount?: number; // default 0
  getTenantId?: (c: Context) => string | undefined;
}
```
