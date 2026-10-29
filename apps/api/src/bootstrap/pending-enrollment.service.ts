import { Injectable } from '@nestjs/common';

export interface PendingEnrollment {
  readonly administratorId: string;
  readonly recoveryKey: string;
  readonly recoverySessionId: string;
  readonly totpSecret: Buffer;
  readonly totpUri: string;
}

@Injectable()
export class PendingEnrollmentService {
  private pending: PendingEnrollment | undefined;

  hasPending(): boolean {
    return this.pending !== undefined;
  }

  set(enrollment: PendingEnrollment): void {
    this.clear();
    this.pending = { ...enrollment, totpSecret: Buffer.from(enrollment.totpSecret) };
  }

  require(): PendingEnrollment {
    if (this.pending === undefined) {
      throw new Error('NO_PENDING_TOTP_ENROLLMENT');
    }
    return this.pending;
  }

  clear(): void {
    this.pending?.totpSecret.fill(0);
    this.pending = undefined;
  }
}
