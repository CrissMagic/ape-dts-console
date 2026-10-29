import { Injectable } from '@nestjs/common';
import { loadMigrationBundle, type MigrationBundle } from '@scs/database';

@Injectable()
export class MigrationBundleService {
  private readonly bundle = loadMigrationBundle();

  async get(): Promise<MigrationBundle> {
    return await this.bundle;
  }
}
