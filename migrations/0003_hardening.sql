-- Several keys per tenant, so a key can be rotated without downtime.
-- tenants.api_key_hash stays for rollback compatibility but is no longer read.
CREATE TABLE tenant_keys (
  key_hash text PRIMARY KEY,
  tenant_id text NOT NULL REFERENCES tenants ON DELETE CASCADE,
  label text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  revoked_at timestamptz
);
INSERT INTO tenant_keys (key_hash, tenant_id, label)
  SELECT api_key_hash, id, 'migrated' FROM tenants WHERE api_key_hash IS NOT NULL;
GRANT SELECT ON tenant_keys TO app;

-- Shared secrets for signed provider webhooks (Twilio auth token, Zendesk signing secret).
CREATE TABLE tenant_webhooks (
  tenant_id text NOT NULL REFERENCES tenants ON DELETE CASCADE,
  provider text NOT NULL CHECK (provider IN ('twilio', 'zendesk')),
  secret text NOT NULL,
  PRIMARY KEY (tenant_id, provider)
);
GRANT SELECT ON tenant_webhooks TO app;

-- Rate limits shared by every API instance.
CREATE TABLE rate_hits (
  bucket text NOT NULL,
  window_start timestamptz NOT NULL,
  hits int NOT NULL DEFAULT 0,
  PRIMARY KEY (bucket, window_start)
);
GRANT SELECT, INSERT, UPDATE ON rate_hits TO app;

-- The inbox is insert-then-delete. Vacuum it on a fixed row count, not a fraction,
-- so dead rows from a burst are reclaimed quickly instead of bloating the table.
ALTER TABLE inbox SET (
  autovacuum_vacuum_scale_factor = 0,
  autovacuum_vacuum_threshold = 5000,
  autovacuum_vacuum_cost_delay = 0
);

-- Per-tenant queue state, so one slow tenant can be found without reading messages.
CREATE FUNCTION tenant_metrics() RETURNS TABLE (
  tenant_id text, queued bigint, oldest_seconds double precision
) LANGUAGE sql SECURITY DEFINER SET search_path = public AS $$
  SELECT tenant_hint, count(*),
         extract(epoch FROM clock_timestamp() - min(enqueued_at))::double precision
  FROM inbox GROUP BY tenant_hint ORDER BY count(*) DESC LIMIT 50
$$;
REVOKE ALL ON FUNCTION tenant_metrics() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION tenant_metrics() TO app;

CREATE FUNCTION inbox_dead_tuples() RETURNS bigint
LANGUAGE sql SECURITY DEFINER SET search_path = public AS $$
  SELECT COALESCE((SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'inbox'), 0)
$$;
REVOKE ALL ON FUNCTION inbox_dead_tuples() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION inbox_dead_tuples() TO app;

-- Reads the tenant out of a stored payload without failing on rows that are not JSON.
CREATE FUNCTION payload_tenant(payload text) RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  RETURN payload::jsonb ->> 'tenant_id';
EXCEPTION WHEN others THEN
  RETURN NULL;
END $$;

-- A record that a tenant's data was erased, kept after the data is gone.
CREATE TABLE tenant_deletions (
  tenant_id text NOT NULL,
  deleted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  messages bigint NOT NULL,
  findings bigint NOT NULL,
  dead_letters bigint NOT NULL,
  queued bigint NOT NULL
);
