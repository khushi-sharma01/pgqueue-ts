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
