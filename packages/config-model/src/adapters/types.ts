import type { PlatformTaskConfig } from '../schema/platform-task.js';
import type { MysqlToKafkaTaskConfig } from '../schema/mysql-kafka-task.js';
import type { RuntimeText } from '../ini/runtime-text.js';

export type TaskScenario =
  'PG_SNAPSHOT_TO_KAFKA' | 'PG_CDC_TO_KAFKA' | 'KAFKA_RAW_FORWARD' | MysqlTaskScenario;
export type MysqlTaskScenario = 'MYSQL_SNAPSHOT_TO_KAFKA' | 'MYSQL_CDC_TO_KAFKA';

export interface MysqlCdcRuntimePosition {
  readonly binlogFilename: string;
  readonly binlogPosition: number;
  readonly serverId: number;
}

export interface ApeDtsRuntimeBindings {
  readonly log4rsPath: string;
  readonly logDirectory: string;
  readonly logLevel: 'debug' | 'error' | 'info' | 'trace' | 'warn';
  readonly metricsPort: number;
  readonly mysqlCdc?: MysqlCdcRuntimePosition;
  readonly mysqlTls?: {
    readonly mode: 'disable' | 'require' | 'verify_full';
    readonly caPath?: string;
  };
  readonly sourceConnection: RuntimeText;
  readonly sourcePartition?: number;
  readonly sourceTopic?: string;
  readonly targetConnection: RuntimeText;
}

export interface AdapterRenderRequest {
  readonly config: PlatformTaskConfig;
  readonly runtime: ApeDtsRuntimeBindings;
  readonly scenario: TaskScenario;
}

export interface MysqlAdapterRenderRequest {
  readonly config: MysqlToKafkaTaskConfig;
  readonly runtime: ApeDtsRuntimeBindings;
  readonly scenario: MysqlTaskScenario;
}

export interface RenderedApeDtsConfig {
  readonly log4rsYaml: string;
  readonly previewIni: string;
  readonly runtimeIni: string;
}

export interface ApeDtsConfigAdapter {
  readonly id: string;
  readonly version: string;
  render(request: AdapterRenderRequest): RenderedApeDtsConfig;
}
