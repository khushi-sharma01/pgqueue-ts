import { randomUUID } from 'node:crypto';
import { afterAll, describe, expect, it } from 'vitest';
import { enqueue, pool, uniqueQueue } from './helpers.js';

afterAll(() => pool.end());

describe('idempotency keys', () => {
  it('returns the same job for a duplicate key and creates only one row', async () => {
    const queue = uniqueQueue();
    const key = randomUUID();
    const body = { queue, type: 'noop', payload: {} };

    const first = await enqueue(body, key);
    const second = await enqueue(body, key);

    expect(first.status).toBe(201);
    expect(second.status).toBe(200);
    expect(second.job.id).toBe(first.job.id);

    const { rows } = await pool.query(
      'SELECT count(*)::int AS n FROM jobs WHERE queue = $1 AND idempotency_key = $2',
      [queue, key],
    );
    expect(rows[0].n).toBe(1);
  });

  it('treats the same key in a different queue as a different job', async () => {
    const key = randomUUID();
    const a = await enqueue({ queue: uniqueQueue(), type: 'noop', payload: {} }, key);
    const b = await enqueue({ queue: uniqueQueue(), type: 'noop', payload: {} }, key);

    expect(a.status).toBe(201);
    expect(b.status).toBe(201);
    expect(b.job.id).not.toBe(a.job.id);
  });
});
