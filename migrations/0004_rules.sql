-- Which rule set produced a finding, so a rule change can be traced and re-run.
ALTER TABLE findings ADD COLUMN rules_version text NOT NULL DEFAULT '1';
ALTER TABLE scan_runs ADD COLUMN rules_version text NOT NULL DEFAULT '1';
-- A tenant can switch off rules that do not apply to it (for example a card processor).
ALTER TABLE tenants ADD COLUMN disabled_rules text[] NOT NULL DEFAULT '{}';
