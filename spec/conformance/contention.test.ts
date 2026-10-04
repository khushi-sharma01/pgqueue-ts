import { afterAll, describe, expect, it } from 'vitest';
import { pool, startWorker, stopAllWorkers, uniqueQueue, waitForAllFinished } from './helpers.js';

afterAll(async () => {
  stopAllWorkers();
  await pool.end();
});

const TOTAL_JOBS = 10_000;
const WORKERS = 16;

describe('claiming under contention', () => {
  it('runs every job exactly once and never in two workers at once', async () => {
    const queue = uniqueQueue('contention');

    // Preload straight into the table so the test measures claiming, not HTTP.
    // `probe` jobs record their start and end in job_runs, sleeping 5 ms each.
    await pool.query(
      `INSERT INTO jobs (id, queue, type, payload, state)
       SELECT gen_random_uuid(), $1, 'probe', '{"ms": 5}', 'queued'
       FROM generate_series(1, $2)`,
      [queue, TOTAL_JOBS],
    );

    // One process with 16 concurrent claim loops, each with its own worker id.
    startWorker({ queue, concurrency: WORKERS });
    await waitForAllFinished(queue, TOTAL_JOBS, 180_000);

    // 1. Every job completed, none went dead.
    const states = await pool.query(
      `SELECT state, count(*)::int AS n FROM jobs WHERE queue = $1 GROUP BY state`,
      [queue],
    );
    expect(Object.fromEntries(states.rows.map((r) => [r.state, r.n]))).toEqual({
      completed: TOTAL_JOBS,
    });

    // 2. Each job ran exactly once, and all 16 workers took part
    //    (otherwise there was no real contention to test).
    const runs = await pool.query(
      `SELECT count(*)::int AS runs,
              count(DISTINCT r.job_id)::int AS jobs,
              count(DISTINCT r.worker_id)::int AS workers
       FROM job_runs r JOIN jobs j ON j.id = r.job_id
       WHERE j.queue = $1`,
      [queue],
    );
    expect(runs.rows[0].runs).toBe(TOTAL_JOBS);
    expect(runs.rows[0].jobs).toBe(TOTAL_JOBS);
    expect(runs.rows[0].workers).toBeGreaterThanOrEqual(WORKERS);

    // 3. No two runs of the same job overlap in time.
    const overlaps = await pool.query(
      `SELECT count(*)::int AS n
       FROM job_runs a
       JOIN job_runs b ON a.job_id = b.job_id AND a.id < b.id
       JOIN jobs j ON j.id = a.job_id
       WHERE j.queue = $1
         AND a.started_at < COALESCE(b.finished_at, 'infinity')
         AND b.started_at < COALESCE(a.finished_at, 'infinity')`,
      [queue],
    );
    expect(overlaps.rows[0].n).toBe(0);
  }, 240_000);
});
