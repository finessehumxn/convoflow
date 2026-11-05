"""HTTP ingest plus health and metrics, using only the standard library."""
import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

from adapters import BadPayload, normalize
from convoflow import ADMIN_DSN, APP_DSN, enqueue

MAX_BODY = 1_000_000
ALERTS = {"queue_lag_seconds": 60, "dead_letter_ratio_5m": 0.05, "scan_overdue_seconds": 7200}


def hash_key(key):
    return hashlib.sha256(key.encode()).hexdigest()


def measure(db):
    row = db.execute("""
        SELECT (SELECT count(*) FROM inbox),
               COALESCE((SELECT extract(epoch FROM clock_timestamp() - min(enqueued_at)) FROM inbox), 0),
               (SELECT count(*) FROM dead_letters WHERE failed_at > clock_timestamp() - interval '5 minutes'),
               (SELECT count(*) FROM messages WHERE ingested_at > clock_timestamp() - interval '5 minutes'),
               COALESCE((SELECT sum(stored) FROM worker_counters), 0),
               COALESCE((SELECT sum(dead) FROM worker_counters), 0),
               COALESCE((SELECT sum(duplicates) FROM worker_counters), 0),
               (SELECT extract(epoch FROM clock_timestamp() - max(window_end)) FROM scan_runs)
    """).fetchone()
    db.rollback()
    depth, lag, dead5, stored5, stored, dead, dups, since_scan = row
    total5 = dead5 + stored5
    return {
        "queue_depth": depth,
        "queue_lag_seconds": float(lag),
        "dead_letter_ratio_5m": dead5 / total5 if total5 else 0.0,
        "stored_total": int(stored),
        "dead_total": int(dead),
        "duplicates_total": int(dups),
        "scan_overdue_seconds": float(since_scan) if since_scan is not None else 0.0,
    }


def firing(metrics):
    return sorted(name for name, limit in ALERTS.items() if metrics[name] > limit)


def render(metrics):
    lines = [f"convoflow_{name} {value}" for name, value in metrics.items()]
    lines += [f'convoflow_alert{{name="{name}"}} 1' for name in firing(metrics)]
    return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, code, body, content_type="application/json"):
        data = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path not in ("/health", "/metrics"):
            return self.reply(404, '{"error":"not_found"}')
        with psycopg.connect(ADMIN_DSN) as db:
            metrics = measure(db)
        if self.path == "/metrics":
            return self.reply(200, render(metrics), "text/plain")
        alerts = firing(metrics)
        self.reply(503 if alerts else 200, json.dumps({"ok": not alerts, "alerts": alerts}))

    def do_POST(self):
        parts = self.path.strip("/").split("/")
        if len(parts) != 3 or parts[:2] != ["v1", "events"]:
            return self.reply(404, '{"error":"not_found"}')
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_BODY:
            return self.reply(413, '{"error":"too_large"}')
        with psycopg.connect(ADMIN_DSN) as db:
            found = db.execute(
                "SELECT id FROM tenants WHERE api_key_hash = %s",
                (hash_key(self.headers.get("X-Api-Key", "")),),
            ).fetchone()
        if not found:
            return self.reply(401, '{"error":"bad_key"}')
        try:
            payload = json.loads(self.rfile.read(length))
            events, trace_id = normalize(parts[2], found[0], payload)
        except (ValueError, BadPayload) as err:
            return self.reply(400, json.dumps({"error": str(err)}))
        with psycopg.connect(APP_DSN) as db:
            enqueue(db, [json.dumps(e) for e in events])
        self.reply(202, json.dumps({"accepted": len(events), "trace_id": trace_id}))


def serve(port=8080):
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    serve()
