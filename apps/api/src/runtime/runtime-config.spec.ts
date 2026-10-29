import path from 'node:path';

import { describe, expect, it } from 'vitest';

import { readRuntimeConfig } from './runtime-config.js';

describe('readRuntimeConfig', () => {
  it('uses explicit non-secret development defaults only when values are absent', () => {
    expect(readRuntimeConfig({})).toEqual({
      host: '0.0.0.0',
      port: 3000,
      requireWebDist: false,
      webDistPath: path.resolve(process.cwd(), 'apps/web/dist'),
    });
  });

  it.each([
    { PORT: '' },
    { PORT: '0' },
    { PORT: '65536' },
    { PORT: '3.5' },
    { PORT: 'not-a-port' },
  ])('rejects an invalid configured port: %o', (environment) => {
    expect(() => readRuntimeConfig(environment)).toThrow(
      'PORT must be an integer between 1 and 65535',
    );
  });

  it('does not treat an explicitly empty host or web path as absent', () => {
    expect(() => readRuntimeConfig({ HOST: ' ' })).toThrow(
      'HOST must not be empty when it is configured',
    );
    expect(() => readRuntimeConfig({ WEB_DIST_PATH: '' })).toThrow(
      'WEB_DIST_PATH must not be empty when it is configured',
    );
  });

  it('requires the web build in production or when a path is explicitly configured', () => {
    expect(readRuntimeConfig({ NODE_ENV: 'production' }).requireWebDist).toBe(true);
    expect(readRuntimeConfig({ WEB_DIST_PATH: './public' }).requireWebDist).toBe(true);
  });
});
