import { describe, expect, it } from 'vitest';

import { formatDateTime, shanghaiDateTimeLocalToIso } from './date-time';

describe('Asia/Shanghai date time formatting', () => {
  it('renders UTC input as a complete Shanghai date without ISO separators or timezone text', () => {
    expect(formatDateTime('2026-09-30T00:01:02.000Z')).toBe('2026年09月30日 08:01:02');
  });

  it('keeps missing or invalid timestamps explicit', () => {
    expect(formatDateTime(null)).toBe('—');
    expect(formatDateTime('not-a-time')).toBe('—');
  });

  it('converts a datetime-local value as Asia/Shanghai instead of the browser timezone', () => {
    expect(shanghaiDateTimeLocalToIso('2026-09-30T08:01')).toBe('2026-09-30T00:01:00.000Z');
  });
});
