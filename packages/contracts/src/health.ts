import { z } from 'zod';

export const liveHealthResponseSchema = z.object({
  status: z.literal('ok'),
});

export const platformRuntimePhaseSchema = z.enum([
  'SEALED',
  'BOOTSTRAPPING',
  'READY',
  'METADATA_DEGRADED',
  'RECONCILING',
  'BLOCKED_CONCURRENT_INSTANCE',
]);

export const readyHealthResponseSchema = z.object({
  phase: z.literal('READY'),
  status: z.literal('ready'),
});

export const unavailableHealthResponseSchema = z.object({
  code: z.literal('PLATFORM_NOT_READY'),
  phase: platformRuntimePhaseSchema.exclude(['READY']),
  status: z.literal('not_ready'),
});

export type LiveHealthResponse = z.infer<typeof liveHealthResponseSchema>;
export type PlatformRuntimePhase = z.infer<typeof platformRuntimePhaseSchema>;
export type ReadyHealthResponse = z.infer<typeof readyHealthResponseSchema>;
export type UnavailableHealthResponse = z.infer<typeof unavailableHealthResponseSchema>;
