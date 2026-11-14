"""HTTP ingest plus health and metrics, using only the standard library."""
import collections
import hashlib
import json
import os
import signal
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from psycopg_pool import ConnectionPool

from adapters import BadPayload, normalize
from convoflow import APP_DSN, CHANNELS, RULES, enqueue, log

MAX_BODY = 1_000_000
ALERTS = {"queue_lag_seconds": 60, "dead_letter_ratio_5m": 0.05, "scan_overdue_seconds": 7200}


# The public demo is off unless a tenant is named for it, so production never exposes it.
DEMO_TENANT = os.environ.get("CONVOFLOW_DEMO_TENANT", "")
DEMO_MAX_TEXT = 500
DEMO_PER_MINUTE = 10
_demo_hits = collections.defaultdict(collections.deque)
_demo_lock = threading.Lock()
PAGE = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "page.html"), encoding="utf-8").read()


def demo_allowed(ip, now=None):
    now = time.monotonic() if now is None else now
    with _demo_lock:
        hits = _demo_hits[ip]
        while hits and now - hits[0] > 60:
            hits.popleft()
        if len(hits) >= DEMO_PER_MINUTE:
            return False
        hits.append(now)
        return True


def would_flag(db, text):
    """Runs the scanner's own rules, in the database's regex engine, on one text."""
    rows = db.execute(
        "SELECT rule, severity FROM unnest(%s::text[], %s::text[], %s::text[]) r(rule, severity, pattern) WHERE %s ~ pattern",
        ([r[0] for r in RULES], [r[1] for r in RULES], [r[2] for r in RULES], text),
    ).fetchall()
    db.rollback()
    return [{"rule": r, "severity": sev} for r, sev in rows]


_pool = ConnectionPool(APP_DSN, min_size=1, max_size=10, open=False, kwargs={"prepare_threshold": None})


def get_pool():
    if _pool.closed:
        _pool.open(wait=True)
    return _pool


def hash_key(key):
    return hashlib.sha256(key.encode()).hexdigest()


def measure(db):
    depth, lag, dead5, stored5, stored, dead, dups, since_scan = db.execute(
        "SELECT * FROM pipeline_metrics()"
    ).fetchone()
    db.rollback()
    total5 = dead5 + stored5
    return {
        "queue_depth": depth,
        "queue_lag_seconds": lag,
        "dead_letter_ratio_5m": dead5 / total5 if total5 else 0.0,
        "stored_total": int(stored),
        "dead_total": int(dead),
        "duplicates_total": int(dups),
        "scan_overdue_seconds": since_scan or 0.0,
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

    def client_ip(self):
        forwarded = self.headers.get("X-Forwarded-For", "")
        return forwarded.split(",")[0].strip() or self.client_address[0]

    def reply(self, code, body, content_type="application/json"):
        data = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/" and "text/html" in self.headers.get("Accept", ""):
            return self.reply(200, PAGE, "text/html; charset=utf-8")
        if self.path == "/stats":
            with get_pool().connection() as db:
                metrics = measure(db)
            return self.reply(200, json.dumps({"metrics": metrics, "alerts": firing(metrics), "demo": bool(DEMO_TENANT)}))
        if self.path == "/":
            return self.reply(200, json.dumps({
                "service": "convoflow",
                "about": "Multi-tenant ingestion for voice, chat, email and SMS with dedupe, dead letters, quotas and compliance scanning",
                "routes": {
                    "GET /health": "database reachable",
                    "GET /metrics": "live pipeline counters",
                    "GET /alerts": "alerts currently firing",
                    "POST /v1/events/{voice|chat|email|sms}": "ingest, needs X-Api-Key",
                },
            }, indent=2))
        if self.path == "/health":
            with get_pool().connection() as db:
                db.execute("SELECT 1")
            return self.reply(200, '{"ok":true}')
        if self.path not in ("/alerts", "/metrics"):
            return self.reply(404, '{"error":"not_found"}')
        with get_pool().connection() as db:
            metrics = measure(db)
        if self.path == "/metrics":
            return self.reply(200, render(metrics), "text/plain")
        alerts = firing(metrics)
        self.reply(503 if alerts else 200, json.dumps({"ok": not alerts, "alerts": alerts}))

    def do_POST(self):
        if self.path == "/demo":
            return self.demo()
        parts = self.path.strip("/").split("/")
        if len(parts) != 3 or parts[:2] != ["v1", "events"]:
            return self.reply(404, '{"error":"not_found"}')
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            return self.reply(400, '{"error":"bad_length"}')
        if length > MAX_BODY:
            return self.reply(413, '{"error":"too_large"}')
        with get_pool().connection() as db:
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
        with get_pool().connection() as db:
            enqueue(db, [json.dumps(e) for e in events])
        log("events_accepted", tenant_id=found[0], source=parts[2], count=len(events), trace_id=trace_id)
        self.reply(202, json.dumps({"accepted": len(events), "trace_id": trace_id}))


    def demo(self):
        if not DEMO_TENANT:
            return self.reply(404, '{"error":"not_found"}')
        if not demo_allowed(self.client_ip()):
            return self.reply(429, '{"error":"slow_down"}')
        try:
            length = int(self.headers.get("Content-Length") or 0)
            if length > 4096:
                return self.reply(413, '{"error":"too_large"}')
            body = json.loads(self.rfile.read(length))
            channel, text = body["channel"], str(body["text"]).strip()
        except (ValueError, KeyError, TypeError):
            return self.reply(400, '{"error":"send channel and text"}')
        if channel not in CHANNELS or not text or len(text) > DEMO_MAX_TEXT:
            return self.reply(400, json.dumps({"error": f"channel must be one of {list(CHANNELS)} and text 1-{DEMO_MAX_TEXT} characters"}))
        trace_id = uuid.uuid4().hex
        event = {"tenant_id": DEMO_TENANT, "conversation_id": f"demo-{trace_id[:12]}", "channel": channel,
                 "text": text, "trace_id": trace_id}
        with get_pool().connection() as db:
            enqueue(db, [json.dumps(event)])
            flags = would_flag(db, text)
        log("demo_accepted", tenant_id=DEMO_TENANT, channel=channel, trace_id=trace_id, flags=len(flags))
        self.reply(202, json.dumps({"accepted": 1, "trace_id": trace_id, "would_flag": flags}))


def serve(port=8080):
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    signal.signal(signal.SIGTERM, lambda *_: threading.Thread(target=server.shutdown).start())
    log("api_started", port=port)
    server.serve_forever()
    get_pool().close()
    log("api_stopped")


if __name__ == "__main__":
    serve()
