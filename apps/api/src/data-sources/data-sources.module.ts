import { Module } from '@nestjs/common';

import { AuditModule } from '../audit/audit.module.js';
import { AuthModule } from '../auth/auth.module.js';
import { BootstrapModule } from '../bootstrap/bootstrap.module.js';
import { KafkaDataSourceAdapter } from './adapters/kafka-data-source.adapter.js';
import { MysqlCompatibleDataSourceAdapter } from './adapters/mysql-compatible-data-source.adapter.js';
import { PostgresDataSourceAdapter } from './adapters/postgres-data-source.adapter.js';
import { DataSourceAdapterRegistry } from './adapters/types.js';
import { DataSourcesController } from './data-sources.controller.js';
import { DataSourcesService } from './data-sources.service.js';

@Module({
  controllers: [DataSourcesController],
  exports: [DataSourcesService],
  imports: [AuditModule, AuthModule, BootstrapModule],
  providers: [
    DataSourcesService,
    {
      provide: DataSourceAdapterRegistry,
      useFactory: () =>
        new DataSourceAdapterRegistry([
          new PostgresDataSourceAdapter(),
          new KafkaDataSourceAdapter(),
          new MysqlCompatibleDataSourceAdapter('MYSQL'),
          new MysqlCompatibleDataSourceAdapter('DORIS'),
        ]),
    },
  ],
})
export class DataSourcesModule {}
