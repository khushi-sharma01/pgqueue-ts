# Worker contract

The conformance tests start their own worker processes and rely on this contract.
Any implementation of the queue must follow it.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | none (required) | Postgres connection string |
| `QUEUES` | none (required) | Comma-separated queue names this worker claims from |
| `WORKER_CONCURRENCY` | 4 | Number of concurrent claim loops in the process |
| `LEASE_SECONDS` | 30 | Lease length given to a claimed job |
| `REAPER_INTERVAL_SECONDS` | 5 | How often the process scans for expired leases |

Heartbeat interval is `LEASE_SECONDS / 3`.

## Worker ids

Each claim loop has its own id, used as `locked_by` and as `job_runs.worker_id`:

```
<hostname>-<pid>-<loop index>      e.g. my-laptop-48213-7
```

The crash test reads the pid from this format to `kill -9` the process.

## Reaper

Every worker process runs a reaper loop. Expired leases (`state = 'active' AND locked_until < now()`)
go back to `queued`, or to `dead` if `attempts >= max_attempts`.

## Job type `probe` (test only)

Payload: `{ "ms": <integer> }`. Handler:

1. `INSERT INTO job_runs (job_id, worker_id) VALUES ($1, $2)` and remember the row id.
2. Sleep `ms` milliseconds.
3. `UPDATE job_runs SET finished_at = clock_timestamp() WHERE id = $3`.

A run that is killed mid-job keeps `finished_at = NULL`.
`probe` exists so the correctness tests can prove what happened without adding writes to `noop`,
which the benchmarks use.

## Table `job_runs` (test only)

```sql
CREATE TABLE job_runs (
  id          bigserial PRIMARY KEY,
  job_id      uuid NOT NULL,
  worker_id   text NOT NULL,
  started_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz
);
CREATE INDEX job_runs_job_idx ON job_runs (job_id);
```

`clock_timestamp()` is used instead of `now()` because `now()` is frozen at transaction start.
