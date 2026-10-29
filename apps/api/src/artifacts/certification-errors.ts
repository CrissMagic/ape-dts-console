// Only these fixed, non-sensitive diagnostics may cross the deployment CLI boundary.
const executionFailures = new Set([
  'CERTIFICATION_CANDIDATE_SHA_MISMATCH',
  'CERTIFICATION_CANDIDATE_UNAVAILABLE',
  'CERTIFICATION_IMAGE_REQUIRED',
  'CERTIFICATION_IMAGE_UNAVAILABLE',
  'CERTIFICATION_EXECUTION_FAILED',
  'CERTIFICATION_REPORT_INVALID',
  'CERTIFICATION_ARCHIVE_CHANGED',
  'BEHAVIOR_SUITE_FAILED',
  'ARTIFACT_ARCHIVE_AMBIGUOUS',
  'ARTIFACT_ARCHIVE_BINARY_MISSING',
  'ARTIFACT_ARCHIVE_CLI_ONLY',
  'ARTIFACT_ARCHIVE_INVALID',
  'ARTIFACT_ARCHIVE_LIMIT',
  'ARTIFACT_ARCHIVE_TIMEOUT',
  'ARTIFACT_BINARY_INVALID',
  'ARTIFACT_FILE_CHANGED',
  'ARTIFACT_PATH_INVALID',
  'ARTIFACT_SIZE_LIMIT',
  'ARTIFACT_XZ_UNAVAILABLE',
]);

export function safeCertificationFailureCode(value: unknown): string {
  const code = value instanceof Error ? value.message : value;
  return typeof code === 'string' && executionFailures.has(code)
    ? code
    : 'CERTIFICATION_EXECUTION_FAILED';
}
