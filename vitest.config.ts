import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['spec/conformance/**/*.test.ts', 'src/**/*.test.ts'],
    testTimeout: 30_000,
  },
});
