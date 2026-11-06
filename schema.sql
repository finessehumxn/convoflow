DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app') THEN
    CREATE ROLE app LOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END $$;
GRANT USAGE ON SCHEMA public TO app;

CREATE TABLE tenants (
  id text PRIMARY KEY,
  daily_limit bigint NOT NULL,
  channels text[] NOT NULL DEFAULT '{voice,chat,email,sms}',
  api_key_hash text UNIQUE
);

CREATE TABLE tenant_usage (
  tenant_id text NOT NULL REFERENCES tenants,
  day date NOT NULL,
  stored bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant_id, day)
);

CREATE TABLE inbox (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant_hint text NOT NULL DEFAULT '',
  payload text NOT NULL,
  attempts int NOT NULL DEFAULT 0,
  next_attempt_at timestamptz,
  enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX inbox_by_tenant ON inbox (tenant_hint, id);
CREATE INDEX inbox_retries ON inbox (next_attempt_at) WHERE attempts > 0;

CREATE TABLE messages (
  tenant_id text NOT NULL,
  event_key text NOT NULL,
  conversation_id text NOT NULL,
  channel text NOT NULL,
  text text NOT NULL,
  trace_id text,
  enqueued_at timestamptz NOT NULL,
  ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, event_key)
) PARTITION BY HASH (tenant_id);
DO $$ BEGIN
  FOR i IN 0..7 LOOP
    EXECUTE format('CREATE TABLE messages_p%s PARTITION OF messages FOR VALUES WITH (MODULUS 8, REMAINDER %s)', i, i);
  END LOOP;
END $$;
CREATE INDEX messages_ingested_at ON messages (ingested_at);

ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON messages
  USING (tenant_id = current_setting('app.tenant_id', true))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

CREATE TABLE dead_letters (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payload text NOT NULL,
  reason text NOT NULL,
  failed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  replayed_at timestamptz
);
CREATE INDEX dead_letters_open ON dead_letters (reason, id) WHERE replayed_at IS NULL;

CREATE TABLE worker_counters (
  worker_id text PRIMARY KEY,
  claimed bigint NOT NULL DEFAULT 0,
  stored bigint NOT NULL DEFAULT 0,
  duplicates bigint NOT NULL DEFAULT 0,
  dead bigint NOT NULL DEFAULT 0
);

CREATE TABLE scan_runs (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  window_start timestamptz NOT NULL,
  window_end timestamptz NOT NULL,
  scanned bigint NOT NULL,
  findings bigint NOT NULL,
  seconds double precision NOT NULL
);

CREATE TABLE findings (
  tenant_id text NOT NULL,
  event_key text NOT NULL,
  rule text NOT NULL,
  severity text NOT NULL,
  evidence text NOT NULL,
  found_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant_id, event_key, rule)
);

GRANT SELECT ON tenants TO app;
GRANT SELECT, INSERT, UPDATE ON tenant_usage, worker_counters TO app;
GRANT SELECT, INSERT, UPDATE, DELETE ON inbox TO app;
GRANT SELECT, INSERT ON messages, dead_letters TO app;
GRANT SELECT ON scan_runs TO app;
