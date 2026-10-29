import { z } from 'zod';

export const auditActorTypeSchema = z.enum(['DEPLOYMENT_ADMIN', 'SYSTEM', 'USER']);

export const auditEventSchema = z
  .object({
    action: z.string().min(1),
    actorType: auditActorTypeSchema,
    actorUserId: z.uuid().nullable(),
    details: z.record(z.string(), z.unknown()),
    eventId: z.uuid(),
    occurredAt: z.iso.datetime({ offset: true }),
    sequence: z.string().regex(/^[1-9]\d*$/u),
    subjectId: z.uuid().nullable(),
    subjectType: z.string().min(1),
  })
  .strict()
  .superRefine((event, context) => {
    const hasUserActor = event.actorUserId !== null;
    if ((event.actorType === 'USER') !== hasUserActor) {
      context.addIssue({
        code: 'custom',
        message: 'actorUserId is required only for USER actors',
        path: ['actorUserId'],
      });
    }
  });

export const auditEventListQuerySchema = z
  .object({
    action: z.string().trim().min(1).max(128).optional(),
    actorType: auditActorTypeSchema.optional(),
    cursor: z.string().min(1).max(512).optional(),
    limit: z.coerce.number().int().min(1).max(200).default(50),
    subjectId: z.uuid().optional(),
    subjectType: z.string().trim().min(1).max(128).optional(),
  })
  .strict();

export const auditEventListResponseSchema = z
  .object({
    events: z.array(auditEventSchema),
    nextCursor: z.string().min(1).nullable(),
  })
  .strict();

export type AuditActorType = z.infer<typeof auditActorTypeSchema>;
export type AuditEvent = z.infer<typeof auditEventSchema>;
export type AuditEventListQuery = z.infer<typeof auditEventListQuerySchema>;
export type AuditEventListResponse = z.infer<typeof auditEventListResponseSchema>;
