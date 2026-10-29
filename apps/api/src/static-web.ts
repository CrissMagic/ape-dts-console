import { stat } from 'node:fs/promises';

import fastifyStatic from '@fastify/static';
import { Logger } from '@nestjs/common';
import type { NestFastifyApplication } from '@nestjs/platform-fastify';

import type { RuntimeConfig } from './runtime/runtime-config.js';

const WEB_CONTENT_SECURITY_POLICY = [
  "default-src 'self'",
  "base-uri 'none'",
  "connect-src 'self'",
  "font-src 'self'",
  "form-action 'self'",
  "frame-ancestors 'none'",
  "img-src 'self' data:",
  "object-src 'none'",
  "script-src 'self'",
  "style-src 'self' 'unsafe-inline'",
].join('; ');

export async function registerStaticWeb(
  app: NestFastifyApplication,
  config: RuntimeConfig,
): Promise<void> {
  try {
    const webDist = await stat(config.webDistPath);
    if (!webDist.isDirectory()) {
      throw new Error('configured path is not a directory');
    }
  } catch (error) {
    if (config.requireWebDist) {
      const reason = error instanceof Error ? error.message : 'unknown filesystem error';
      throw new Error(`Web build is unavailable at ${config.webDistPath}: ${reason}`, {
        cause: error,
      });
    }

    Logger.warn(
      `Web build not found at ${config.webDistPath}; development API will start without static assets`,
      'StaticWeb',
    );
    return;
  }

  await app.register(fastifyStatic, {
    index: ['index.html'],
    prefix: '/',
    root: config.webDistPath,
    setHeaders: (reply) => {
      reply.header('Content-Security-Policy', WEB_CONTENT_SECURITY_POLICY);
      reply.header('Referrer-Policy', 'no-referrer');
      reply.header('X-Content-Type-Options', 'nosniff');
    },
  });
}
