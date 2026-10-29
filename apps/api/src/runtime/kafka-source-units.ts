import { kafkaTaskMappings, type KafkaRawForwardTaskConfig } from '@scs/config-model';

export function kafkaUnitKey(
  config: KafkaRawForwardTaskConfig,
  topic: string,
  partition: number,
): string {
  return config.schemaVersion === 1
    ? `partition:${partition}`
    : `topic:${topic}:partition:${partition}`;
}

export function kafkaSourceBinding(
  config: KafkaRawForwardTaskConfig,
  unitKey: string,
): {
  readonly sourceTopic: string;
  readonly sourcePartition: number;
} | null {
  const match =
    config.schemaVersion === 1
      ? /^partition:(0|[1-9]\d*)$/u.exec(unitKey)
      : /^topic:([A-Za-z0-9._-]+):partition:(0|[1-9]\d*)$/u.exec(unitKey);
  if (match === null) return null;
  const sourceTopic = config.schemaVersion === 1 ? config.source.topic : match[1];
  const sourcePartition = Number(config.schemaVersion === 1 ? match[1] : match[2]);
  if (
    sourceTopic === undefined ||
    !Number.isSafeInteger(sourcePartition) ||
    sourcePartition < 0 ||
    !kafkaTaskMappings(config).some((item) => item.source === sourceTopic)
  )
    return null;
  return { sourceTopic, sourcePartition };
}
