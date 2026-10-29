import { z } from 'zod';

export const artifactFileCandidateSchema = z
  .object({
    name: z
      .string()
      .min(1)
      .max(255)
      .refine(
        (name) =>
          !/[\\/]/u.test(name) &&
          [...name].every(
            (character) => character.charCodeAt(0) > 31 && character.charCodeAt(0) !== 127,
          ) &&
          name !== '.' &&
          name !== '..',
      ),
    sizeBytes: z
      .number()
      .int()
      .positive()
      .max(512 * 1024 * 1024),
    modifiedAt: z.iso.datetime(),
    kind: z.enum(['BINARY', 'TAR_GZ', 'TAR_XZ']),
  })
  .strict();
export const artifactFileListSchema = z.object({
  directory: z.string(),
  files: z.array(artifactFileCandidateSchema),
});
export const preparedArtifactFileSchema = z.object({
  fileName: z.string(),
  sha256: z.string().regex(/^[a-f0-9]{64}$/u),
  sizeBytes: z.number().int().positive(),
  suggestedVersion: z.string(),
});
export const registerArtifactFileRequestSchema = z
  .object({
    file: artifactFileCandidateSchema,
    expectedSha256: z.string().regex(/^[a-f0-9]{64}$/u),
    versionIdentifier: z.string().trim().min(1).max(128),
    declaredSource: z.string().trim().min(1).max(512),
  })
  .strict();
export const uploadArtifactFileResponseSchema = z.object({
  file: artifactFileCandidateSchema,
  prepared: preparedArtifactFileSchema,
});
export type UploadArtifactFileResponse = z.infer<typeof uploadArtifactFileResponseSchema>;
export type ArtifactFileCandidate = z.infer<typeof artifactFileCandidateSchema>;
export type PreparedArtifactFile = z.infer<typeof preparedArtifactFileSchema>;
export type RegisterArtifactFileRequest = z.infer<typeof registerArtifactFileRequestSchema>;

export const registerArtifactRequestSchema = z
  .object({
    versionIdentifier: z.string().trim().min(1).max(128),
    declaredSource: z.string().trim().min(1).max(512),
    objectKey: z.string().trim().min(1).max(512),
    sha256: z.string().regex(/^[a-f0-9]{64}$/u),
    sizeBytes: z.number().int().positive().max(Number.MAX_SAFE_INTEGER),
  })
  .strict();

export const artifactSummarySchema = z.object({
  id: z.uuid(),
  versionIdentifier: z.string(),
  declaredSource: z.string(),
  objectKey: z.string(),
  sha256: z.string(),
  sizeBytes: z.string(),
  state: z.enum(['ACTIVE', 'INACTIVE', 'QUARANTINED']),
  stateReason: z.string().nullable(),
  verified: z.boolean(),
  certifications: z.array(
    z.object({
      scenario: z.string(),
      platformVersion: z.string(),
      state: z.string(),
    }),
  ),
});
export const artifactListResponseSchema = z.object({ artifacts: z.array(artifactSummarySchema) });
export const deleteArtifactResponseSchema = z.object({ deleted: z.literal(true) });
export const artifactReferencesSchema = z.array(
  z.object({ id: z.uuid(), displayName: z.string() }),
);
export type ArtifactSummary = z.infer<typeof artifactSummarySchema>;
export type RegisterArtifactRequest = z.infer<typeof registerArtifactRequestSchema>;
