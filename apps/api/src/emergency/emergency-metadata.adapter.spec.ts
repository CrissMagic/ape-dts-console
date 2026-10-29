import { describe, expect, it, vi } from 'vitest';

import { EmergencyMetadataAdapter } from './emergency-metadata.adapter.js';

describe('EmergencyMetadataAdapter', () => {
  it('uses the live metadata availability state', () => {
    const availability = { isMetadataUnavailable: vi.fn(() => true) };
    const adapter = new EmergencyMetadataAdapter(availability as never);

    expect(adapter.isMetadataUnavailable()).toBe(true);
    expect(availability.isMetadataUnavailable).toHaveBeenCalledOnce();
  });
});
