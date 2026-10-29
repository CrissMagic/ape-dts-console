import { bootstrapStatusResponseSchema } from '@scs/contracts';

export class BootstrapApiError extends Error {
  constructor(
    readonly code: string,
    readonly status: number,
    options?: ErrorOptions,
  ) {
    super(code, options);
    this.name = 'BootstrapApiError';
  }
}

export const bootstrapApi = {
  async getStatus() {
    let response: Response;
    try {
      response = await fetch('/api/bootstrap/status', {
        cache: 'no-store',
        credentials: 'same-origin',
      });
    } catch (cause) {
      throw new BootstrapApiError('NETWORK_ERROR', 0, { cause });
    }
    if (!response.ok) throw new BootstrapApiError(`HTTP_${response.status}`, response.status);
    const parsed = bootstrapStatusResponseSchema.safeParse(await response.json());
    if (!parsed.success) throw new BootstrapApiError('INVALID_SERVER_RESPONSE', response.status);
    return parsed.data;
  },
};
