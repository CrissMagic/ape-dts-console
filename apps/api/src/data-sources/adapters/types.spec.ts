import { describe, expect, it } from 'vitest';

import { toAdapterError } from './types.js';

function kafkaAuthorizationError(type: string, code: number): Error {
  return Object.assign(new Error(`Kafka protocol error: ${type}`), { code, type });
}

describe('toAdapterError', () => {
  it.each([
    { code: 29, type: 'TOPIC_AUTHORIZATION_FAILED' },
    { code: 30, type: 'GROUP_AUTHORIZATION_FAILED' },
    { code: 31, type: 'CLUSTER_AUTHORIZATION_FAILED' },
    { code: 53, type: 'TRANSACTIONAL_ID_AUTHORIZATION_FAILED' },
    { code: 65, type: 'DELEGATION_TOKEN_AUTHORIZATION_FAILED' },
  ])('classifies Kafka $type as a permission failure', ({ code, type }) => {
    expect(toAdapterError(kafkaAuthorizationError(type, code))).toMatchObject({
      code: 'PERMISSION_DENIED',
    });
  });
});
