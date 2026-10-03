import { afterAll, describe, expect, it } from 'vitest';
import { enqueue, pool, uniqueQueue, waitForState } from './helpers.js';

afterAll(() => pool.end());

// Requires the API and at least one worker to be running.
describe('flaky jobs', () => {
  it('retries with backoff and lands in dead after max_attempts', async () => {
    const queue = uniqueQueue();
    const maxAttempts = 3;

    // p = 1 means the handler fails on every attempt.
    const startedAt = Date.now();
    const { status, job } = await enqueue({
      queue,
      type: 'flaky',
      payload: { p: 1 },
      max_attempts: maxAttempts,
    });
    expect(status).toBe(201);

    const dead = await waitForState(job.id, ['dead'], 30_000);

    expect(dead.attempts).toBe(maxAttempts);
    expect(dead.last_error).toBeTruthy();

    // Backoff is base 1s * 2^(attempt-1) plus jitter, so waiting between
    // 3 attempts takes at least 1s + 2s = 3s. Without backoff this would be instant.
    const elapsed = Date.now() - startedAt;
    expect(elapsed).toBeGreaterThanOrEqual(3000);
  }, 40_000);
});
