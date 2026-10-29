export interface AlertSender {
  send(
    destination: string,
    text: string,
  ): Promise<{ status: 'SENT' | 'FAILED'; failureCode: string | null }>;
}

export function isWecomWebhook(value: string): boolean {
  try {
    const url = new URL(value);
    return (
      url.protocol === 'https:' &&
      url.hostname === 'qyapi.weixin.qq.com' &&
      url.port === '' &&
      url.pathname === '/cgi-bin/webhook/send' &&
      url.username === '' &&
      url.password === '' &&
      url.hash === '' &&
      [...url.searchParams.keys()].length === 1 &&
      /^[\w-]{8,256}$/u.test(url.searchParams.get('key') ?? '')
    );
  } catch {
    return false;
  }
}

export class WecomAlertSender implements AlertSender {
  constructor(private readonly request: typeof fetch = fetch) {}

  async send(destination: string, text: string) {
    if (!isWecomWebhook(destination))
      return { status: 'FAILED' as const, failureCode: 'INVALID_WEBHOOK' };
    try {
      const response = await this.request(destination, {
        method: 'POST',
        redirect: 'error',
        signal: AbortSignal.timeout(5000),
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ msgtype: 'text', text: { content: text.slice(0, 1000) } }),
      });
      if (!response.ok) return { status: 'FAILED' as const, failureCode: 'CHANNEL_HTTP_ERROR' };
      const body: unknown = await response.json();
      if (typeof body !== 'object' || body === null || !('errcode' in body) || body.errcode !== 0)
        return { status: 'FAILED' as const, failureCode: 'CHANNEL_REJECTED' };
      return { status: 'SENT' as const, failureCode: null };
    } catch {
      // Never persist transport errors: they may contain the credential-bearing URL.
      return { status: 'FAILED' as const, failureCode: 'CHANNEL_UNREACHABLE' };
    }
  }
}
