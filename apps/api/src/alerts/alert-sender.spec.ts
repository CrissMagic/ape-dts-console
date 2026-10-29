import { describe, expect, it, vi } from 'vitest';
import { isWecomWebhook, WecomAlertSender } from './alert-sender.js';
const url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abcdefgh';
describe('enterprise WeChat delivery', () => {
  it.each([
    'http://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abcdefgh',
    'https://localhost/x',
    'https://qyapi.weixin.qq.com.evil.test/cgi-bin/webhook/send?key=abcdefgh',
    `${url}&redirect=evil`,
    `${url}&key=second-key`,
  ])('rejects unauthorized destinations without sending %s', async (value) => {
    const request = vi.fn<typeof fetch>();
    expect(isWecomWebhook(value)).toBe(false);
    expect((await new WecomAlertSender(request).send(value, 'test')).status).toBe('FAILED');
    expect(request).not.toHaveBeenCalled();
  });
  it('uses text payload and disables redirects with a deadline', async () => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(new Response('{"errcode":0}'));
    expect(await new WecomAlertSender(request).send(url, 'test')).toEqual({
      status: 'SENT',
      failureCode: null,
    });
    const options = request.mock.calls[0]![1]!;
    expect(options.redirect).toBe('error');
    expect(options.signal).toBeInstanceOf(AbortSignal);
    expect(JSON.parse(options.body as string)).toEqual({
      msgtype: 'text',
      text: { content: 'test' },
    });
  });
  it('records channel rejection without returning remote error text', async () => {
    const request = vi
      .fn<typeof fetch>()
      .mockResolvedValue(new Response('{"errcode":1,"errmsg":"secret"}'));
    expect(await new WecomAlertSender(request).send(url, 'test')).toEqual({
      status: 'FAILED',
      failureCode: 'CHANNEL_REJECTED',
    });
  });
  it('records timeout or network failures without exposing credentials', async () => {
    const request = vi.fn<typeof fetch>().mockRejectedValue(new Error(url));
    expect(await new WecomAlertSender(request).send(url, 'test')).toEqual({
      status: 'FAILED',
      failureCode: 'CHANNEL_UNREACHABLE',
    });
  });
});
