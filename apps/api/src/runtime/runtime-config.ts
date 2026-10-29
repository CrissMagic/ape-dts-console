import path from 'node:path';

export interface RuntimeConfig {
  host: string;
  port: number;
  requireWebDist: boolean;
  webDistPath: string;
}

function readNonEmptyValue(environment: NodeJS.ProcessEnv, key: string, fallback: string): string {
  const value = environment[key];

  if (value === undefined) {
    return fallback;
  }

  if (value.trim().length === 0) {
    throw new Error(`${key} must not be empty when it is configured`);
  }

  return value;
}

function readPort(environment: NodeJS.ProcessEnv): number {
  const value = environment.PORT;

  if (value === undefined) {
    return 3000;
  }

  if (!/^\d+$/.test(value)) {
    throw new Error('PORT must be an integer between 1 and 65535');
  }

  const port = Number(value);
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
    throw new Error('PORT must be an integer between 1 and 65535');
  }

  return port;
}

export function readRuntimeConfig(environment: NodeJS.ProcessEnv = process.env): RuntimeConfig {
  const webDistWasConfigured = environment.WEB_DIST_PATH !== undefined;
  const webDistPath = path.resolve(
    readNonEmptyValue(environment, 'WEB_DIST_PATH', path.resolve(process.cwd(), 'apps/web/dist')),
  );

  return {
    host: readNonEmptyValue(environment, 'HOST', '0.0.0.0'),
    port: readPort(environment),
    requireWebDist: environment.NODE_ENV === 'production' || webDistWasConfigured,
    webDistPath,
  };
}
