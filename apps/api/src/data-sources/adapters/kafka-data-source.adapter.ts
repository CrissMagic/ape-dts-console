import type {
  CreateKafkaTopicRequest,
  CreateKafkaTopicResponse,
  DataSourceConfiguration,
  DataSourceIdentity,
  MetadataItem,
  MetadataQuery,
} from '@scs/contracts';
import { Kafka } from 'kafkajs';

import {
  DataSourceAdapterError,
  type DataSourceAdapter,
  type DataSourceAdapterInput,
  requireMetadataContext,
  toAdapterError,
} from './types.js';
import { fetchKafkaTopicMetadata } from './kafka-topic-metadata.js';

type KafkaConfiguration = Extract<DataSourceConfiguration, { type: 'KAFKA' }>;

function requireKafkaConfiguration(configuration: DataSourceConfiguration): KafkaConfiguration {
  if (configuration.type !== 'KAFKA') {
    throw new Error('KAFKA_ADAPTER_CONFIGURATION_TYPE_MISMATCH');
  }
  return configuration;
}

export class KafkaDataSourceAdapter implements DataSourceAdapter {
  readonly type = 'KAFKA' as const;
  async readProgress(input: DataSourceAdapterInput, request: { topic?: string; groupId?: string }) {
    if (!request.topic || !request.groupId) throw new Error('PROGRESS_GROUP_REQUIRED');
    const topic = request.topic;
    const groupId = request.groupId;
    return this.withAdmin(input, async (admin) => {
      const ends = await admin.fetchTopicOffsets(topic);
      const groups = await admin.fetchOffsets({ groupId, topics: [topic] });
      const committed = groups.find((item) => item.topic === topic)?.partitions ?? [];
      const lag = kafkaOffsetLag(ends, committed);
      return {
        slotStatus: null,
        unconfirmedWalBytes: null,
        retainedWalBytes: null,
        offsetLag: lag,
      };
    });
  }

  async createKafkaTopic(
    input: DataSourceAdapterInput,
    request: CreateKafkaTopicRequest,
  ): Promise<CreateKafkaTopicResponse> {
    return await this.withAdmin(input, async (admin) => {
      try {
        // CreateTopics is supported by Kafka 2.1 and does not require topic IDs.
        // It is atomic at the broker: an existing topic is never altered.
        const created = await admin.createTopics({
          topics: [
            {
              topic: request.topic,
              numPartitions: request.numPartitions,
              replicationFactor: request.replicationFactor,
            },
          ],
          waitForLeaders: false,
          timeout: 10_000,
        });
        return { topic: request.topic, created };
      } catch (error) {
        // KafkaJS wraps per-topic protocol failures in an aggregate error.
        const failure =
          typeof error === 'object' &&
          error !== null &&
          'errors' in error &&
          Array.isArray(error.errors) &&
          error.errors.length === 1
            ? (error.errors[0] as unknown)
            : error;
        const type =
          typeof failure === 'object' && failure !== null && 'type' in failure
            ? failure.type
            : null;
        if (type === 'TOPIC_ALREADY_EXISTS') return { topic: request.topic, created: false };
        if (
          type === 'INVALID_PARTITIONS' ||
          type === 'INVALID_REPLICATION_FACTOR' ||
          type === 'INVALID_TOPIC_EXCEPTION' ||
          type === 'INVALID_CONFIG'
        ) {
          throw new DataSourceAdapterError(
            'KAFKA_TOPIC_PARAMETERS_INVALID',
            'Topic 参数无效，请检查名称、分区数和副本数；副本数不得超过可用 Broker 数。',
            { cause: error },
          );
        }
        if (type === 'REQUEST_TIMED_OUT') {
          throw new DataSourceAdapterError(
            'TIMEOUT',
            'Topic 创建响应超时，结果尚未确认，请刷新 Topic 列表核实。',
            { cause: error },
          );
        }
        throw toAdapterError(failure);
      }
    });
  }

  private async withAdmin<T>(
    input: DataSourceAdapterInput,
    operation: (admin: ReturnType<Kafka['admin']>) => Promise<T>,
  ): Promise<T> {
    const configuration = requireKafkaConfiguration(input.configuration);
    const kafka = new Kafka({
      brokers: [...configuration.bootstrapServers],
      clientId: 'scs-data-sync-platform-data-source-test',
      connectionTimeout: 10_000,
      requestTimeout: 10_000,
      retry: { retries: 0 },
    });
    const admin = kafka.admin();
    try {
      await admin.connect();
      return await operation(admin);
    } catch (error) {
      throw toAdapterError(error);
    } finally {
      await admin.disconnect().catch(() => undefined);
    }
  }

  async test(input: DataSourceAdapterInput): Promise<DataSourceIdentity> {
    return await this.withAdmin(input, async (admin) => {
      const cluster = await admin.describeCluster();
      return cluster.clusterId === undefined || cluster.clusterId.length === 0
        ? { kind: 'UNAVAILABLE', serverVersion: null, value: null }
        : { kind: 'KAFKA_CLUSTER', serverVersion: null, value: cluster.clusterId };
    });
  }

  async metadata(
    input: DataSourceAdapterInput,
    query: MetadataQuery,
  ): Promise<readonly MetadataItem[]> {
    const configuration = requireKafkaConfiguration(input.configuration);
    try {
      const metadata = await fetchKafkaTopicMetadata(configuration.bootstrapServers);
      if (query.resource === 'TOPICS') {
        return metadata.map((topic) => ({
          attributes: { partitionCount: String(topic.partitions.length) },
          identity: topic.topicId,
          name: topic.name,
        }));
      }
      if (query.resource === 'PARTITIONS') {
        const topicName = requireMetadataContext(query.topic, 'topic');
        const topic = metadata.find((candidate) => candidate.name === topicName);
        if (topic === undefined) {
          throw new DataSourceAdapterError('INVALID_METADATA_QUERY', '目标 Topic 当前不存在。');
        }
        return topic.partitions.map((partition) => ({
          attributes: { leader: String(partition.leader) },
          identity: null,
          name: String(partition.partitionId),
        }));
      }
      throw new DataSourceAdapterError('METADATA_UNSUPPORTED', 'Kafka 数据源不支持该元数据资源。');
    } catch (error) {
      throw toAdapterError(error);
    }
  }
}

export function kafkaOffsetLag(
  ends: readonly { partition: number; high: string; low: string }[],
  committed: readonly { partition: number; offset: string }[],
): string | null {
  if (ends.length === 0) return null;
  let lag = 0n;
  for (const end of ends) {
    const offset = committed.find((item) => item.partition === end.partition)?.offset;
    if (offset === undefined || !/^\d+$/u.test(offset)) return null;
    const value = BigInt(offset);
    // A deleted/expired offset cannot be represented as a healthy zero lag.
    if (value < BigInt(end.low) || value > BigInt(end.high)) return null;
    lag += BigInt(end.high) - value;
  }
  return lag.toString();
}
