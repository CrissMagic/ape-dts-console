import 'reflect-metadata';

import { pathToFileURL } from 'node:url';

import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';

import { AppModule } from './app.module.js';
import { readRuntimeConfig } from './runtime/runtime-config.js';
import { registerStaticWeb } from './static-web.js';

function installGracefulShutdown(app: NestFastifyApplication): void {
  let shutdownInProgress = false;

  const shutdown = (signal: NodeJS.Signals): void => {
    if (shutdownInProgress) {
      return;
    }

    shutdownInProgress = true;
    void app.close().catch((error: unknown) => {
      const reason = error instanceof Error ? error.message : 'unknown shutdown failure';
      Logger.error(`${signal} shutdown failed: ${reason}`, undefined, 'Bootstrap');
      process.exitCode = 1;
    });
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

export async function bootstrap(
  environment: NodeJS.ProcessEnv = process.env,
): Promise<NestFastifyApplication> {
  const config = readRuntimeConfig(environment);
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter({ logger: true }),
    { bufferLogs: true },
  );

  app.setGlobalPrefix('api');
  installGracefulShutdown(app);
  app.useLogger(new Logger('Platform'));

  await registerStaticWeb(app, config);
  await app.listen(config.port, config.host);

  Logger.log(`HTTP server listening on ${config.host}:${config.port}`, 'Bootstrap');
  return app;
}

const executedFile = process.argv[1];
const isMainModule =
  executedFile !== undefined && import.meta.url === pathToFileURL(executedFile).href;

if (isMainModule) {
  void bootstrap().catch((error: unknown) => {
    const reason = error instanceof Error ? error.message : 'unknown startup failure';
    Logger.error(reason, undefined, 'Bootstrap');
    process.exitCode = 1;
  });
}
