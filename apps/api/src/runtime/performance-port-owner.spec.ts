import { describe, expect, it } from 'vitest';
import { assertMetricsPortOwner } from './performance-port-owner.js';

describe('metrics TCP listener ownership', () => {
  const binding = { pid: 123, port: 19001 };
  const tcp =
    ' sl local_address rem_address st tx_queue rx_queue tr uid timeout inode\n0: 0100007F:4A39 00000000:0000 0A 00000000:00000000 00:00000000 00000000 1000 0 555 1';
  it('accepts the loopback listener only when its socket belongs to the selected child', async () => {
    await expect(
      assertMetricsPortOwner(binding, {
        readTcp: async (pid) => {
          expect(pid).toBe(123);
          return tcp;
        },
        listDescriptors: async () => ['3'],
        readDescriptor: async () => 'socket:[555]',
      }),
    ).resolves.toBeUndefined();
  });
  it.each(['socket:[556]', '/dev/null'])(
    'rejects a listener belonging to a different process: %s',
    async (target) => {
      await expect(
        assertMetricsPortOwner(binding, {
          readTcp: async () => tcp,
          listDescriptors: async () => ['3'],
          readDescriptor: async () => target,
        }),
      ).rejects.toThrow('METRICS_PORT_OWNERSHIP_UNVERIFIED');
    },
  );
  it('rejects wildcard listeners and unreadable process evidence', async () => {
    await expect(
      assertMetricsPortOwner(binding, {
        readTcp: async () => tcp.replace('0100007F', '00000000'),
        listDescriptors: async () => ['3'],
        readDescriptor: async () => 'socket:[555]',
      }),
    ).rejects.toThrow('METRICS_PORT_OWNERSHIP_UNVERIFIED');
    await expect(
      assertMetricsPortOwner(binding, {
        readTcp: async () => {
          throw new Error('ENOENT');
        },
        listDescriptors: async () => [],
        readDescriptor: async () => '',
      }),
    ).rejects.toThrow('METRICS_PORT_OWNERSHIP_UNVERIFIED');
  });
});
