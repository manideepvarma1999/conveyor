create table conveyor_jobs (
  id               bigint generated always as identity primary key,
  queue            text not null check (queue ~ '^[a-z0-9][a-z0-9_.-]{0,62}$'),
  kind             text not null check (length(kind) between 1 and 128),
  payload          jsonb not null default '{}',
  status           text not null default 'queued'
                   check (status in ('queued', 'running', 'succeeded', 'dead', 'cancelled')),
  priority         smallint not null default 0 check (priority between -100 and 100),
  attempts         integer not null default 0,
  max_attempts     integer not null default 5 check (max_attempts between 1 and 100),
  run_at           timestamptz not null default now(),
  lease_token      uuid,
  lease_expires_at timestamptz,
  idempotency_key  text check (length(idempotency_key) <= 200),
  last_error       text,
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now(),
  finished_at      timestamptz,
  check ((status = 'running') = (lease_token is not null))
);

-- The claim query: oldest ready job per queue, highest priority first.
create index conveyor_jobs_ready_idx on conveyor_jobs (queue, priority desc, run_at, id) where status = 'queued';
-- The reaper: running jobs whose lease expired.
create index conveyor_jobs_lease_idx on conveyor_jobs (lease_expires_at) where status = 'running';
-- Idempotent enqueue: at most one job per (queue, key).
create unique index conveyor_jobs_idempotency_idx on conveyor_jobs (queue, idempotency_key) where idempotency_key is not null;
create index conveyor_jobs_status_idx on conveyor_jobs (queue, status, updated_at desc);

create table conveyor_api_keys (
  id          bigint generated always as identity primary key,
  name        text not null check (length(name) between 1 and 100),
  key_hash    text not null unique, -- SHA-256 of the key; the key itself is shown once and never stored
  scopes      text[] not null check (scopes <@ array['enqueue', 'work', 'admin']::text[] and cardinality(scopes) > 0),
  created_at  timestamptz not null default now(),
  last_used_at timestamptz,
  revoked_at  timestamptz
);
