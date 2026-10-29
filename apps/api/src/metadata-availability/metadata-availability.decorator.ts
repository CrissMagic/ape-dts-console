import { SetMetadata } from '@nestjs/common';

export const METADATA_AVAILABILITY_BYPASS = Symbol('METADATA_AVAILABILITY_BYPASS');
export const METADATA_AVAILABILITY_RECOVERY_ACCESS = Symbol(
  'METADATA_AVAILABILITY_RECOVERY_ACCESS',
);

export const AllowWhenMetadataUnavailable = () => SetMetadata(METADATA_AVAILABILITY_BYPASS, true);
export const AllowDuringBootstrapRecovery = () =>
  SetMetadata(METADATA_AVAILABILITY_RECOVERY_ACCESS, true);
