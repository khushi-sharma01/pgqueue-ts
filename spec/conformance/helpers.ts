import { randomUUID } from 'node:crypto';
import pg from 'pg';

// Conformance tests are black-box: they only need a base URL and a database.
export const BASE_URL = process.env.BASE_URL ?? 'http://localhost:3000';
export const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://pgqueue:pgqueue@localhost:5432/pgqueue';

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

  const res = await fetch(`${BASE_URL}/v1/jobs`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  return { status: res.status, job: (await res.json()) as Job };
}

export async function getJob(id: string): Promise<Job> {
  const res = await fetch(`${BASE_URL}/v1/jobs/${id}`);
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
