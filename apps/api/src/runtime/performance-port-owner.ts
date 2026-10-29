import { readFile, readdir, readlink } from 'node:fs/promises';

interface MetricsProcessFilesystem {
  readTcp(pid: number): Promise<string>;
  listDescriptors(pid: number): Promise<string[]>;
  readDescriptor(pid: number, descriptor: string): Promise<string>;
}

const processFilesystem: MetricsProcessFilesystem = {
  readTcp: (pid) => readFile(`/proc/${pid}/net/tcp`, 'utf8'),
  listDescriptors: (pid) => readdir(`/proc/${pid}/fd`),
  readDescriptor: (pid, descriptor) => readlink(`/proc/${pid}/fd/${descriptor}`),
};

/** The runtime is Linux-only; unavailable /proc evidence degrades monitoring, not the task. */
export async function assertMetricsPortOwner(
  binding: { pid: number; port: number },
  filesystem: MetricsProcessFilesystem = processFilesystem,
): Promise<void> {
  try {
    if (
      !Number.isSafeInteger(binding.pid) ||
      binding.pid <= 0 ||
      !Number.isInteger(binding.port) ||
      binding.port < 1 ||
      binding.port > 65535
    )
      throw new Error();
    const address = `0100007F:${binding.port.toString(16).padStart(4, '0').toUpperCase()}`;
    const sockets = new Set(
      (await filesystem.readTcp(binding.pid))
        .split('\n')
        .map((line) => line.trim().split(/\s+/u))
        .filter((columns) => columns[1] === address && columns[3] === '0A')
        .map((columns) => `socket:[${columns[9]}]`),
    );
    if (sockets.size !== 1) throw new Error();
    const descriptors = await filesystem.listDescriptors(binding.pid);
    for (const descriptor of descriptors) {
      if (!/^\d+$/u.test(descriptor)) continue;
      // Descriptors can legitimately close between readdir and readlink.
      const target = await filesystem.readDescriptor(binding.pid, descriptor).catch(() => null);
      if (target !== null && sockets.has(target)) return;
    }
  } catch {
    throw new Error('METRICS_PORT_OWNERSHIP_UNVERIFIED');
  }
  throw new Error('METRICS_PORT_OWNERSHIP_UNVERIFIED');
}
