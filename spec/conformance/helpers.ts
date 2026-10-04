import { spawn, type ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import pg from 'pg';

// Conformance tests are black-box: they need a base URL, a database,
// and a command that starts one worker process (see spec/worker-contract.md).
// Not called BASE_URL: Vite/Vitest reserve that name and set it to "/".
export const API_URL = process.env.API_URL ?? 'http://localhost:3000';
export const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://pgqueue:pgqueue@localhost:5432/pgqueue';
export const WORKER_CMD = process.env.WORKER_CMD ?? 'node dist/src/worker/main.js';

export const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 4 });

export interface Job {
  id: string;
  queue: string;
  type: string;
  state: 'queued' | 'active' | 'completed' | 'dead';
  attempts: number;
  max_attempts: number;
  run_at: string;
  last_error: string | null;
  created_at: string;
  finished_at: string | null;
}

// A fresh queue name per test keeps tests isolated from each other.
export const uniqueQueue = (prefix = 'conf') => `${prefix}-${randomUUID()}`;

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export async function enqueue(
  body: Record<string, unknown>,
  idempotencyKey?: string,
): Promise<{ status: number; job: Job }> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (idempotencyKey) headers['Idempotency-Key'] = idempotencyKey;

  const res = await fetch(`${API_URL}/v1/jobs`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  return { status: res.status, job: (await res.json()) as Job };
}

export async function getJob(id: string): Promise<Job> {
  const res = await fetch(`${API_URL}/v1/jobs/${id}`);
  if (res.status !== 200) throw new Error(`GET /v1/jobs/${id} returned ${res.status}`);
  return (await res.json()) as Job;
}

// Polls until the job reaches one of the given states, or throws on timeout.
export async function waitForState(
  id: string,
  states: Job['state'][],
  timeoutMs: number,
  pollMs = 100,
): Promise<Job> {
  const deadline = Date.now() + timeoutMs;
  let last: Job | undefined;
  while (Date.now() < deadline) {
    last = await getJob(id);
    if (states.includes(last.state)) return last;
    await sleep(pollMs);
  }
  throw new Error(
    `Job ${id} did not reach [${states.join(', ')}] within ${timeoutMs}ms (last state: ${last?.state})`,
  );
}

// Waits until every job in a queue is completed or dead.
export async function waitForAllFinished(queue: string, total: number, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  let finished = 0;
  while (Date.now() < deadline) {
    assertNoWorkerFailed();
    const { rows } = await pool.query(
      `SELECT count(*)::int AS n FROM jobs
       WHERE queue = $1 AND state IN ('completed', 'dead')`,
      [queue],
    );
    finished = rows[0].n;
    if (finished >= total) return;
    await sleep(500);
  }
  throw new Error(`Only ${finished}/${total} jobs finished in queue ${queue} within ${timeoutMs}ms`);
}

// ---- Worker launcher -------------------------------------------------------

export interface WorkerOptions {
  queue: string;
  concurrency?: number; // concurrent claim loops inside the process
  leaseSeconds?: number;
  reaperIntervalSeconds?: number;
}

const workers = new Set<ChildProcess>();
let stopping = false;
let workerFailure: string | undefined;

// Throws if a worker process failed to start or exited with an error.
// A worker killed by a signal (the crash test does this on purpose) does not count.
export function assertNoWorkerFailed(): void {
  if (workerFailure) throw new Error(workerFailure);
}

// Starts one worker process that serves only the given queue.
// Set WORKER_LOGS=1 to see worker output while debugging.
export function startWorker(opts: WorkerOptions): ChildProcess {
  const [file, ...args] = WORKER_CMD.split(' ');
  const child = spawn(file!, args, {
    stdio: process.env.WORKER_LOGS ? 'inherit' : 'ignore',
    env: {
      ...process.env,
      DATABASE_URL,
      QUEUES: opts.queue,
      WORKER_CONCURRENCY: String(opts.concurrency ?? 4),
      LEASE_SECONDS: String(opts.leaseSeconds ?? 30),
      REAPER_INTERVAL_SECONDS: String(opts.reaperIntervalSeconds ?? 5),
    },
  });
  workers.add(child);
  child.on('error', (err) => {
    workerFailure = `Worker failed to start (${WORKER_CMD}): ${err.message}`;
  });
  child.on('exit', (code) => {
    workers.delete(child);
    if (!stopping && code !== null && code !== 0) {
      workerFailure =
        `Worker exited with code ${code} (command: ${WORKER_CMD}). ` +
        'Did you run "npm run build"? Set WORKER_LOGS=1 to see its output.';
    }
  });
  return child;
}

export function stopAllWorkers(): void {
  stopping = true;
  for (const w of workers) w.kill('SIGKILL');
  workers.clear();
}
