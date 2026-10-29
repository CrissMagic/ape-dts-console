import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/api': {
        target: process.env.SCS_DEV_API_URL ?? 'http://127.0.0.1:3000',
      },
    },
    strictPort: true,
  },
  test: {
    maxWorkers: 2,
    testTimeout: 15_000,
    environment: 'jsdom',
    include: ['src/**/*.spec.ts', 'src/**/*.spec.tsx'],
    setupFiles: ['./src/test/setup.ts'],
  },
});
