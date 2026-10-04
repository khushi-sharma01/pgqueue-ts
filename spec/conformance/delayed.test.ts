import { afterAll, describe, expect, it } from 'vitest';
import { enqueue, getJob, pool, sleep, uniqueQueue, waitForState } from './helpers.js';

afterAll(() => pool.end());

// Requires the API and at least one worker to be running.
describe('delayed jobs', () => {
  it('does not run before run_at, then runs', async () => {
    const queue = uniqueQueue();
    const delayMs = 3000;

    const { status, job } = await enqueue({
      queue,
      type: 'noop',
      payload: {},
      delay_ms: delayMs,
    });
    expect(status).toBe(201);

    // run_at is set from the database clock, delay_ms after creation.
    const runAt = new Date(job.run_at).getTime();
    const createdAt = new Date(job.created_at).getTime();
    expect(runAt - createdAt).toBeGreaterThanOrEqual(delayMs);

    // Well before run_at, the job must still be untouched.
    await sleep(1000);
    const early = await getJob(job.id);
    expect(early.state).toBe('queued');
    expect(early.attempts).toBe(0);

    // After run_at, a worker picks it up and completes it.
    const done = await waitForState(job.id, ['completed'], 15_000);
    expect(new Date(done.finished_at!).getTime()).toBeGreaterThanOrEqual(runAt);
  }, 30_000);
});
