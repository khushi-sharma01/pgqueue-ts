# TwinQueue (solo edition): one job queue, built properly in TypeScript

A 5-week build-and-benchmark plan for one backend engineer, about 4 hours a week (roughly 20 hours total).
Build a distributed job queue on Node.js + Postgres, break it on purpose, prove it stays correct, and measure where it bottlenecks.

**Stack:** TypeScript, Fastify, node-postgres (`pg`), pino, Vitest, k6, Docker Compose.

The old plan compared Node and Go. That is dropped. The story now is:
**"I built a queue that stays correct when workers crash, and I can show where it breaks under load and why."**

---

## Goal and success criteria

Clients enqueue work over HTTP, and a pool of workers processes it reliably. Small system, hard problems: concurrent claiming, crash recovery, retries, idempotency, backpressure. These are the topics senior backend interviews probe.

Done when:

- The conformance test suite passes in CI.
- Killing a worker with `kill -9` mid-job never loses the job.
- With 16 concurrent workers, no job is ever active in two workers at once.
- `make bench` reproduces every benchmark number from a clean environment.
- The README shows architecture, results and lessons, and at least two posts are published.

## Scope

**Core (build this)**

- Enqueue and status API
- Workers claiming with `FOR UPDATE SKIP LOCKED`
- Job types: `noop`, `hash`, `flaky`
- Retries with backoff and jitter, dead-letter, idempotency keys
- Leases, heartbeats, reaper, and the `kill -9` test
- Benchmarks S1, S2, S3 with k6 and `docker stats`
- README plus 2 posts

**Stretch (only if ahead)**

- Graceful shutdown (SIGTERM)
- Priorities and the requeue endpoint
- `llm_call` job type (mocked LLM, see below)
- Prometheus and Grafana
- Benchmarks S4 (worker scaling) and S7 (Postgres restart under load)
- A tuning round
- Third post

If you fall behind, drop stretch items first, then the tuning round. Protect the conformance tests, the crash test, the benchmark and the write-up.

## Where the ~20 hours go

| Area | Hours |
|---|---|
| Spec, schema, compose, first tests | 3 |
| API and worker with claiming | 4 |
| Retries, dead-letter, idempotency | 3 |
| Leases, reaper, crash test | 4 |
| Benchmarks (harness, runs, results) | 4 |
| README and posts | 2 |

## Weekly rhythm

| Slot | Time | What to do |
|---|---|---|
| Mon to Fri | 20 to 30 min | One small task: one endpoint, one test, one query. When time is up, write a one-line "next step" note and stop. |
| Saturday | 1 to 2 h | The week's milestone: put pieces together, run tests, debug. |
| Sunday | 15 min | Update `notes.md` with bugs, surprises and numbers. On post weeks, draft the post. |

- Keep every task under 30 minutes. Split anything bigger.
- Use AI or templates for boilerplate: Dockerfile, CI YAML, k6 setup, Vitest config.
- **Write the SQL and concurrency logic yourself.** That is what interviewers ask about.

---

## Architecture

PostgreSQL is the source of truth. Workers claim jobs with `FOR UPDATE SKIP LOCKED`, so many workers can pull from one table without blocking each other. The system runs as two processes: an HTTP API and a worker pool.

```
              HTTP                     SQL
   k6  ─────────────►  Fastify API  ─────────────►  ┌──────────────┐
 (load gen)            enqueue · status · stats     │  PostgreSQL  │
                                                     │  jobs table  │
                       Worker pool  ◄───────────────►│  partial idx │
                       claim · run · ack             │  leases      │
                       heartbeat · reaper            └──────────────┘
```

Workers talk to Postgres directly (not over HTTP), so the claim path is the real hot path.

## Built-in job types

Workers do not run arbitrary user code. A fixed set of job types keeps the benchmark reproducible.

| Type | What it does | Stresses |
|---|---|---|
| `noop` | Returns immediately | Queue overhead only: claim and ack speed |
| `hash` | SHA-256 in a loop, `n` iterations | CPU-bound work (run in `worker_threads`, e.g. Piscina, never on the event loop) |
| `flaky` | Fails with probability `p` | Retry, backoff and dead-letter paths |
| `llm_call` (stretch) | Calls a **mock** LLM endpoint with random latency and occasional 429/500 | Retries, rate limiting, dead-lettering for AI-style workloads |
| `sleep` (stretch) | Waits `ms` milliseconds | Concurrency and I/O-like waiting |

## Job lifecycle

Every job is in exactly one state: `queued`, `active`, `completed` or `dead`. A delayed job is a queued job whose `run_at` is in the future.

```
queued ──claim (SKIP LOCKED)──► active ──ack──► completed
  ▲                               │
  │  handler failed:              │ attempts exhausted
  │  run_at = now + backoff       ▼
  └───────────────────────────  dead   (manual requeue via API = stretch)
  ▲
  └── lease expired (worker died): reaper requeues, or marks dead if attempts exhausted
```

**Delivery guarantee: at-least-once.** If a worker dies after doing the work but before acking, the job runs again, so handlers must be idempotent. Exactly-once only holds in the failure-free case. Say this plainly in the README; it is the honest answer interviewers want.

---

## The spec

Write this before any implementation code. It lives in `/spec`.

### HTTP API

| Endpoint | Purpose |
|---|---|
| `POST /v1/jobs` | Enqueue: `queue`, `type`, `payload`, `delay`, `max_attempts`, optional `Idempotency-Key` header. Returns 201, or 200 with the original job on a duplicate key. |
| `GET /v1/jobs/{id}` | Status, attempts, last error, timestamps |
| `GET /v1/queues/{name}/stats` | Counts by state, oldest queued job age |
| `GET /healthz` | Liveness check |
| `POST /v1/jobs/{id}/requeue` | Stretch: move a dead job back to queued |
| `GET /metrics` | Stretch: Prometheus metrics |

### Schema

```sql
CREATE TABLE jobs (
  id              uuid PRIMARY KEY,
  queue           text NOT NULL,
  type            text NOT NULL,
  payload         jsonb NOT NULL,
  state           text NOT NULL CHECK (state IN ('queued','active','completed','dead')),
  priority        int NOT NULL DEFAULT 0,
  attempts        int NOT NULL DEFAULT 0,
  max_attempts    int NOT NULL DEFAULT 5,
  run_at          timestamptz NOT NULL DEFAULT now(),
  locked_by       text,
  locked_until    timestamptz,
  idempotency_key text,
  last_error      text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  finished_at     timestamptz
);

CREATE UNIQUE INDEX jobs_idem_idx ON jobs (queue, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX jobs_claim_idx ON jobs (queue, priority DESC, run_at)
  WHERE state = 'queued';

CREATE INDEX jobs_lease_idx ON jobs (locked_until)
  WHERE state = 'active';
```

### The claim query

```sql
WITH next AS (
  SELECT id FROM jobs
  WHERE queue = $1 AND state = 'queued' AND run_at <= now()
  ORDER BY priority DESC, run_at
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
UPDATE jobs j
SET state = 'active', locked_by = $3,
    locked_until = now() + $4::interval, attempts = attempts + 1
FROM next WHERE j.id = next.id
RETURNING j.*;
```

### Correctness details to get right (these are the interview gold)

- **Fenced acks.** `ack`, `fail` and `heartbeat` must include `WHERE id = $1 AND state = 'active' AND locked_by = $2`. If it updates 0 rows, the worker lost its lease (the reaper gave the job to someone else), so it must stop and discard its result.
- **Reaper handles exhaustion.** When a lease expires and `attempts >= max_attempts`, mark the job `dead` instead of requeueing. Otherwise a job that crashes workers loops forever.
- **Use database time.** Compute `now()` in SQL, never in the app, so worker clock skew cannot break leases.
- **Idempotent enqueue.** Use `INSERT ... ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`, then select the existing row if nothing was inserted.

### Behavior rules

| Rule | Value |
|---|---|
| Retry delay | `min(cap, base × 2^(attempt−1))` plus up to 20% random jitter. Base 1 s, cap 1 h. |
| Lease and heartbeat | Lease 30 s, heartbeat every 10 s, reaper scans every 5 s |
| Ordering | Oldest `run_at` first, best effort under concurrency (priorities are stretch) |
| Dead-letter | After `max_attempts` failures the job becomes `dead` and keeps its last error |
| Graceful shutdown (stretch) | On SIGTERM stop claiming, finish in-flight jobs for up to 30 s, then release leases |

### Conformance tests (write in week 1, failing on purpose)

1. 10,000 jobs and 16 workers: every job completes once and none is ever active twice.
2. Kill a worker mid-job: the job is re-run after lease expiry.
3. Same idempotency key twice: one job, same id returned.
4. Delayed job does not run before `run_at`.
5. A flaky job retries with growing delays and lands in `dead` after max attempts.
6. Stretch: SIGTERM lets in-flight jobs finish and leaves no leases dangling.
7. Stretch: Postgres restart during load, with no lost or duplicated completions.

Keep the tests black-box: they run against a base URL and a database connection string and know nothing about the implementation. That makes them reusable if you ever rewrite the queue in another language. For test 1, have each job record its start and end in a separate table so you can *prove* no overlap with a SQL query rather than trusting counters.

### Repository layout

```
twinqueue/
  spec/         openapi.yaml, schema.sql, conformance/ (black-box tests)
  src/
    api/        Fastify server, routes
    worker/     claim loop, heartbeat, reaper, handlers/
    db/         pool, queries (all SQL lives here)
  bench/        k6 scripts, scenario configs, results/ (raw data)
  infra/        docker-compose (Postgres; Prometheus/Grafana optional)
  docs/         decisions/ (short ADRs), tuning-log.md, posts/ (drafts), plan.md
  notes.md      running log of bugs, surprises, numbers
  Makefile      make up | test | bench | report
```

---

## 5-week roadmap

| Week | Milestone | Done when |
|---|---|---|
| 1 | Spec, schema, compose | OpenAPI and SQL committed. Three conformance tests written and failing. `docker-compose` starts Postgres. CI runs lint, typecheck and tests. |
| 2 | API and worker | `POST /v1/jobs` and `GET /v1/jobs/{id}` work. Worker claims with SKIP LOCKED and runs `noop` and `hash`. Architecture diagram drawn. **Post 1 published.** |
| 3 | Reliability | Retries with backoff and jitter, dead-letter, idempotency keys. The flaky-job test is green. |
| 4 | Leases | Heartbeats, fenced acks and reaper. The `kill -9` test passes. No double-active jobs with 16 workers. Draft post 2. |
| 5 | Benchmark and wrap-up | `make bench` runs S1, S2, S3, 3 runs each. Results compared with your written predictions. README finished. **Post 2 published.** |

If you have spare time in week 5 or after: graceful shutdown, S4, one tuning round (2 hours, logged in `docs/tuning-log.md`), and post 3.

---

## Benchmark plan

**Write your predictions down before running anything**, then report where you were wrong. Wrong predictions make the best posts.

### Predictions to write down first

- Enqueue throughput is limited by Postgres inserts and the connection pool, not by Fastify.
- In S2 (drain), Postgres becomes the bottleneck before Node does. Batch claiming (`LIMIT` > 1) will help a lot.
- `hash` jobs on the event loop would wreck API latency; `worker_threads` fixes it, with a ceiling around the CPU core count.
- The biggest gains from tuning come from SQL, batching and pool sizing, not from JavaScript micro-optimizations.

### Reproducibility rules (replacing the old "fair comparison" rules)

- Pinned Node LTS version and pinned Postgres version, in Docker.
- Identical Docker CPU and memory limits every run (for example 2 CPUs and 1 GB for the API, the same for workers, 4 CPUs and 4 to 8 GB for Postgres).
- **Freeze before the baseline.** Tag the repo, then run. No code changes between baseline runs.
- Do not change durability settings such as `synchronous_commit` without disclosing it.
- Run on a laptop that is plugged in, with other apps closed, and say so in the write-up.
- Warm up first, run each scenario 3 times in a fixed order, and report the median with min and max.
- Profile with `clinic.js` or `0x` to *prove* what the bottleneck is. Also check Postgres with `pg_stat_activity` and `EXPLAIN ANALYZE` on the claim query.
- Run the loop twice if you tune: once for the baseline and once after, and publish before and after.

### Scenarios

| Scenario | What you do | What it shows |
|---|---|---|
| S1 Enqueue ramp | Open-loop ramp of `POST /v1/jobs` from 200 to 10k req/s until saturation | API throughput, latency percentiles, breaking point |
| S2 Drain | Preload 100k `noop` jobs, start workers, measure jobs per second until empty | Claim and ack efficiency, database contention |
| S3 CPU-bound | `hash` jobs at fixed concurrency, with and without `worker_threads` | Event loop blocking, CPU efficiency |
| S4 Worker scaling (stretch) | 4, 8, 16, 32, 64 workers on the same backlog | Where throughput stops scaling (usually a database limit) |
| S7 Failure injection (stretch) | Kill workers and restart Postgres under load | Recovery time, lost or duplicated jobs (must be zero lost) |

### Metrics to record

Throughput, p50 / p95 / p99 / max latency, error rate, CPU %, RSS memory, and queue age at the end of each run. Report medians, never averages.

### k6 script for S1

Use an arrival-rate executor. It keeps sending at the target rate even when the server slows down, which avoids coordinated omission (closed-loop tests quietly hide latency).

```js
import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    enqueue_ramp: {
      executor: 'ramping-arrival-rate',
      startRate: 200, timeUnit: '1s',
      preAllocatedVUs: 200, maxVUs: 2000,
      stages: [
        { target: 1000,  duration: '2m' },
        { target: 5000,  duration: '3m' },
        { target: 10000, duration: '3m' },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(99)<250'],
  },
};

export default function () {
  const body = JSON.stringify({ queue: 'bench', type: 'noop', payload: { i: __ITER } });
  const res = http.post(`${__ENV.TARGET}/v1/jobs`, body,
    { headers: { 'Content-Type': 'application/json' } });
  check(res, { 'created': (r) => r.status === 201 });
}
```

### Results template

| Metric | Baseline | After tuning | Change | Notes / bottleneck |
|---|---|---|---|---|
| Max enqueue rate at p99 < 250 ms (S1) | | | | |
| Drain rate, jobs/s (S2) | | | | |
| CPU-bound jobs/s, with worker_threads (S3) | | | | |
| Image size, cold start | | | | |

**Pitfalls:** averages instead of percentiles, one lucky run, a busy laptop, and claims that are not scoped. Always say "for this workload on this setup."

---

## Optional: the `llm_call` job type (AI angle)

Only after the core is done. This ties the queue to the kind of work AI backends actually do.

- A small mock LLM server with random latency (200 ms to 5 s) and occasional 429/500 responses.
- The `llm_call` handler calls it with a timeout, so slow calls exercise the lease and heartbeat logic.
- Add a simple concurrency cap or token-bucket limit per queue (Redis is not required; an in-process limiter or a Postgres counter is fine).
- Show in the README: retries on 429 with backoff, dead-letter for permanent failures, idempotency keys so a retried call is not billed twice.

One short section in the README on this is enough to make the project relevant to AI roles without turning it into a chatbot project.

---

## Writing plan (2 posts, optional third)

One idea and one visual per post. Lead with the problem, not the tech. Show real numbers and real failures; a bug story earns more trust than a victory lap. Keep the repo as the destination: pin it, write a strong README, and put the link in the first comment.

| Post | Week | Angle and hook | Visual | Question to ask |
|---|---|---|---|---|
| 1 | 2 | "I'm building a distributed job queue on Postgres and I'll try to break it on purpose." | Architecture diagram | What failure would you want me to test? |
| 2 | 5 | "How do two workers never claim the same job?" SKIP LOCKED, leases, the `kill -9` test, and what the benchmark showed. | Claim-query carousel plus one chart | SKIP LOCKED, advisory locks or Redis Lua? |
| 3 (optional) | after | Tuning round: what I predicted, what was actually the bottleneck. | Before/after chart | What would you have tried first? |

Post template:

> **Two workers. One job. Who wins?**
>
> I'm building a job queue on Postgres. The first hard problem: making sure two workers never process the same job at the same time.
>
> My answer is `FOR UPDATE SKIP LOCKED` plus a lease with heartbeats. Under [N] concurrent workers and [N] jobs I saw [N] double claims.
>
> What would you do differently: advisory locks, or Redis Lua?
>
> *First comment: repo link and the SQL.*

Fill the bracketed numbers only after you have measured them. Draft in one 30-minute Sunday slot and publish mid-week. Correct mistakes in public when someone finds one. Do not share proprietary code or anything from your employer.

## Replacing your missing partner: accountability and review

- **Public build notes.** Posting on schedule is your deadline.
- **One reviewer.** Ask one experienced backend engineer (a colleague, a friend, a Discord or Reddit community) to review `spec/` and the claim and reaper logic in week 4. Design review, not syntax.
- **ADRs.** For each big decision (lease length, retry policy, why Postgres and not Redis), write a five-line note in `docs/decisions/`: context, decision, trade-off. This replaces the "spec changes need two people" rule.
- **Weekly check.** Every Sunday, one line in `notes.md`: did I hit the milestone? If not, cut something.

---

## Interview questions this project prepares you for

- Why at-least-once and not exactly-once? What makes handlers idempotent?
- What happens when a worker crashes mid-job? How long until recovery, and how did you pick the lease length?
- How do you stop two workers claiming one job? Compare SKIP LOCKED, advisory locks and Redis.
- What is a fenced ack, and what goes wrong without one?
- What happens when Postgres is down or slow? Where do you apply backpressure?
- How would you scale this to 100× the load? What breaks first: the table, the indexes, the connection pool?
- How do you avoid a hot table full of completed jobs? (Partitioning and retention.)
- What was your bottleneck in the benchmark, and how did you prove it?
- Why did CPU-bound jobs need `worker_threads`? What does the event loop do when blocked?

---

## First week

Every number in this plan (rates, sizes, timeouts, hours) is a starting point. Adjust to your hardware and schedule.

- **Monday (30 min):** create the repo, set up TypeScript, ESLint, Vitest, and the folder layout. Copy this plan to `docs/plan.md`.
- **Tuesday (30 min):** draft `openapi.yaml` for enqueue and status.
- **Wednesday (30 min):** write `schema.sql` and the claim query. Try the claim query by hand in `psql` from two terminals.
- **Thursday (30 min):** write the first three conformance tests. Commit them failing.
- **Friday (30 min):** write `docker-compose` with Postgres.
- **Weekend (1 to 2 h):** add two more tests and get CI green (lint, typecheck, and the tests running against the compose Postgres, even if they fail on purpose).
