-- Mailgun inbound email signs its requests, so it joins the signed providers.
ALTER TABLE tenant_webhooks DROP CONSTRAINT tenant_webhooks_provider_check;
ALTER TABLE tenant_webhooks ADD CONSTRAINT tenant_webhooks_provider_check
  CHECK (provider IN ('twilio', 'zendesk', 'mailgun'));

-- Customers read their own findings through the API. Row level security scopes
-- every read to the tenant set for the transaction, as it does for messages.
ALTER TABLE findings ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON findings
  USING (tenant_id = current_setting('app.tenant_id', true));
GRANT SELECT ON findings TO app;
CREATE INDEX findings_by_tenant ON findings (tenant_id, found_at, event_key, rule);

-- Where to send a customer's alerts, and which findings have been sent.
CREATE TABLE tenant_alerts (
  tenant_id text PRIMARY KEY REFERENCES tenants ON DELETE CASCADE,
  url text NOT NULL,
  secret text NOT NULL,
  min_severity text NOT NULL DEFAULT 'critical' CHECK (min_severity IN ('low', 'medium', 'high', 'critical'))
);
ALTER TABLE findings ADD COLUMN notified_at timestamptz;
CREATE INDEX findings_unnotified ON findings (tenant_id) WHERE notified_at IS NULL;

-- Who did what. Rows are only ever added.
CREATE TABLE audit_log (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  at timestamptz NOT NULL DEFAULT clock_timestamp(),
  actor text NOT NULL,
  tenant_id text,
  action text NOT NULL,
  detail jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX audit_by_tenant ON audit_log (tenant_id, at);
GRANT INSERT ON audit_log TO app;
GRANT USAGE ON SEQUENCE audit_log_id_seq TO app;
