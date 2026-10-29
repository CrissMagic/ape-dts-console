import { describe, expect, it } from 'vitest';
import { formatWalBytes } from './source-progress';

describe('WAL byte presentation', () => {
  it('distinguishes unavailable samples, a real zero and binary units', () => {
    expect(formatWalBytes(null)).toBe('未知');
    expect(formatWalBytes('0')).toBe('0 B');
    expect(formatWalBytes('1024')).toBe('1 KiB');
    expect(formatWalBytes('106576296')).toBe('101.63 MiB');
  });
  it('formats large counters without parsing them as an imprecise number first', () => {
    expect(formatWalBytes('90071992547409999')).toBe('80 PiB');
  });
});
