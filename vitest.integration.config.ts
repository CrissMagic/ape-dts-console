import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    hookTimeout: 600_000,
    testTimeout: 120_000,
  },
});
