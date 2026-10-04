import { afterAll, describe, expect, it } from 'vitest';
import {
  enqueue,
  getJob,
  pool,
  sleep,
  startWorker,
  stopAllWorkers,
  uniqueQueue,
  waitForState,
} from './helpers.js';

afterAll(async () => {
  stopAllWorkers();
  await pool.end();
});

const LEASE_SECONDS = 3;

// Worker ids look like `<hostname>-<pid>-<n>`, so the test can find the process to kill.
function pidFromWorkerId(workerId: string): number {
  const match = /-(\d+)-\d+$/.exec(workerId);
  if (!match) throw new Error(`Cannot read a pid from worker id "${workerId}"`);
  return Number(match[1]);
}

async function waitForFirstRun(jobId: string, timeoutMs: number): Promise<string> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const { rows } = await pool.query(
      'SELECT worker_id FROM job_runs WHERE job_id = $1 ORDER BY id LIMIT 1',
      [jobId],
    );
    if (rows[0]) return rows[0].worker_id as string;
    await sleep(100);
  }
  throw new Error(`Job ${jobId} never started running`);
}

describe('crash recovery', () => {
  it('re-runs a job after its worker is killed with SIGKILL mid-job', async () => {
    const queue = uniqueQueue('crash');
    const workerOpts = {
      queue,
      concurrency: 1,
      leaseSeconds: LEASE_SECONDS,
      reaperIntervalSeconds: 1,
    };

    // A 6 s job: long enough to kill the worker while it is running.
    const { status, job } = await enqueue({ queue, type: 'probe', payload: { ms: 6000 } });
    expect(status).toBe(201);

    startWorker(workerOpts);
    const workerId = await waitForFirstRun(job.id, 15_000);

    // kill -9: no cleanup, no ack, no lease release.
    process.kill(pidFromWorkerId(workerId), 'SIGKILL');

    // The job is not lost and not finished: it is stuck until the lease expires.
    const during = await getJob(job.id);
    expect(during.state).not.toBe('completed');

    // A replacement worker picks it up once the lease expires.
    startWorker(workerOpts);
    const done = await waitForState(job.id, ['completed'], 45_000);
    expect(done.attempts).toBe(2);

    const { rows } = await pool.query<{
      worker_id: string;
      started_at: Date;
      finished_at: Date | null;
    }>('SELECT worker_id, started_at, finished_at FROM job_runs WHERE job_id = $1 ORDER BY id', [
      job.id,
    ]);
    expect(rows).toHaveLength(2);
    const [killedRun, rerun] = rows;

    // The killed run never finished; the rerun did, on a different worker.
    expect(killedRun!.finished_at).toBeNull();
    expect(rerun!.finished_at).not.toBeNull();
    expect(rerun!.worker_id).not.toBe(killedRun!.worker_id);

    // The job could not restart before its lease expired (small tolerance for timing).
    const gapMs = rerun!.started_at.getTime() - killedRun!.started_at.getTime();
    expect(gapMs).toBeGreaterThanOrEqual(LEASE_SECONDS * 1000 - 500);
  }, 60_000);
});
