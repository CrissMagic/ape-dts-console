import { z } from 'zod';

export const emergencyRuntimeTaskSchema = z.object({
  runId: z.string().min(1),
  taskId: z.string().min(1),
  unitCount: z.number().int().nonnegative(),
});

export type EmergencyRuntimeTask = z.infer<typeof emergencyRuntimeTaskSchema>;

export const emergencyAccessSchema = z.object({
  emergencyCode: z.string().min(1).max(256),
});

export const emergencyStopTaskSchema = emergencyAccessSchema.extend({
  taskId: z.string().min(1),
});

export const emergencyForceKillTaskSchema = emergencyStopTaskSchema.extend({
  confirmed: z.literal(true),
  reason: z.string().trim().min(1).max(2_000),
});

export type EmergencyAccess = z.infer<typeof emergencyAccessSchema>;
export type EmergencyStopTask = z.infer<typeof emergencyStopTaskSchema>;
export type EmergencyForceKillTask = z.infer<typeof emergencyForceKillTaskSchema>;
