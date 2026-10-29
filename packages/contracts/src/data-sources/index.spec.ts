import { describe, expect, it } from 'vitest';

import { dataSourceConfigurationSchema } from './index.js';

describe('data source contracts', () => {
  it('rejects Kafka bootstrap ports outside the TCP port range', () => {
    const result = dataSourceConfigurationSchema.safeParse({
      bootstrapServers: ['kafka.internal:99999'],
      plaintextRiskAcknowledged: true,
      type: 'KAFKA',
    });

    expect(result.success).toBe(false);
  });
});
