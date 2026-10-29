import { Injectable, OnApplicationShutdown } from '@nestjs/common';

@Injectable()
export class UnlockedKeyService implements OnApplicationShutdown {
  private dek: Buffer | undefined;

  set(dek: Uint8Array): void {
    if (dek.byteLength !== 32) {
      throw new Error('INVALID_DEK_LENGTH');
    }
    this.clear();
    this.dek = Buffer.from(dek);
  }

  require(): Buffer {
    if (this.dek === undefined) {
      throw new Error('PLATFORM_KEYS_LOCKED');
    }
    return this.dek;
  }

  clear(): void {
    this.dek?.fill(0);
    this.dek = undefined;
  }

  onApplicationShutdown(): void {
    this.clear();
  }
}
