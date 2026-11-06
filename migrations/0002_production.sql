ALTER TABLE tenants ADD COLUMN retention_days int NOT NULL DEFAULT 365;

CREATE FUNCTION pipeline_metrics() RETURNS TABLE (
  queue_depth bigint,
  queue_lag_seconds double precision,
  dead_5m bigint,
  stored_5m bigint,
  stored_total numeric,
  dead_total numeric,
  duplicates_total numeric,
  scan_overdue_seconds double precision
) LANGUAGE sql SECURITY DEFINER SET search_path = public AS $$
  SELECT (SELECT count(*) FROM inbox),
         COALESCE((SELECT extract(epoch FROM clock_timestamp() - min(enqueued_at)) FROM inbox), 0)::double precision,
         (SELECT count(*) FROM dead_letters WHERE failed_at > clock_timestamp() - interval '5 minutes'),
         (SELECT count(*) FROM messages WHERE ingested_at > clock_timestamp() - interval '5 minutes'),
         COALESCE((SELECT sum(stored) FROM worker_counters), 0),
         COALESCE((SELECT sum(dead) FROM worker_counters), 0),
         COALESCE((SELECT sum(duplicates) FROM worker_counters), 0),
         (SELECT extract(epoch FROM clock_timestamp() - max(window_end)) FROM scan_runs)::double precision
$$;
REVOKE ALL ON FUNCTION pipeline_metrics() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pipeline_metrics() TO app;
