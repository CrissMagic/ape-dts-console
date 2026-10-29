import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, it, vi } from 'vitest';

import { registerStaticWeb } from './static-web.js';

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map(async (directory) => {
      await rm(directory, { force: true, recursive: true });
    }),
  );
});

describe('static Web security headers', () => {
  it('sets a strict CSP and content-sniffing protection on every static response', async () => {
    const webDistPath = await mkdtemp(path.join(tmpdir(), 'scs-static-web-'));
    temporaryDirectories.push(webDistPath);
    let setHeaders: ((reply: { header(name: string, value: string): void }) => void) | undefined;
    const app = {
      register: vi.fn(
        async (
          _plugin: unknown,
          candidate: {
            setHeaders?: (reply: { header(name: string, value: string): void }) => void;
          },
        ) => {
          setHeaders = candidate.setHeaders;
        },
      ),
    };

    await registerStaticWeb(app as never, {
      host: '127.0.0.1',
      port: 3000,
      requireWebDist: true,
      webDistPath,
    });

    const headers = new Map<string, string>();
    const applyHeaders = setHeaders;
    if (applyHeaders === undefined) {
      throw new Error('STATIC_HEADER_CALLBACK_MISSING');
    }
    applyHeaders({ header: (name, value) => void headers.set(name, value) });

    expect(headers.get('Content-Security-Policy')).toContain("default-src 'self'");
    expect(headers.get('Content-Security-Policy')).toContain("object-src 'none'");
    expect(headers.get('Content-Security-Policy')).toContain("frame-ancestors 'none'");
    expect(headers.get('Content-Security-Policy')).toContain("style-src 'self' 'unsafe-inline'");
    expect(headers.get('Content-Security-Policy')).toContain("script-src 'self'");
    expect(headers.get('Referrer-Policy')).toBe('no-referrer');
    expect(headers.get('X-Content-Type-Options')).toBe('nosniff');
  });
});
