"""HTTP ingest, signed provider webhooks, health and metrics.

Built on the standard library server on purpose (see docs/adr/0002-stdlib-http.md).
What a framework would otherwise give us is done here explicitly: socket timeouts,
a cap on requests in flight, a body size limit, and backpressure from the queue.
"""
import base64
import hashlib
import hmac
import json
import os
import signal
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qsl, urlparse

from psycopg_pool import ConnectionPool

import pii
import secretbox
import tracing
import webhooks
from adapters import BadPayload, normalize
from convoflow import APP_DSN, CHANNELS, RULES, enqueue, log

MAX_BODY = 1_000_000
REQUEST_TIMEOUT_SECONDS = 15
MAX_IN_FLIGHT = int(os.environ.get("CONVOFLOW_MAX_IN_FLIGHT", "64"))
MAX_QUEUE = int(os.environ.get("CONVOFLOW_MAX_QUEUE", "200000"))
QUEUE_CHECK_SECONDS = 1.0
ALERTS = {"queue_lag_seconds": 60, "dead_letter_ratio_5m": 0.05, "scan_overdue_seconds": 7200}

# The public demo is off unless a tenant is named for it, so production never exposes it.
DEMO_TENANT = os.environ.get("CONVOFLOW_DEMO_TENANT", "")
DEMO_MAX_TEXT = 500
DEMO_PER_MINUTE = 10
# Set this when a proxy rewrites the host, so Twilio signatures are checked against the URL Twilio called.
PUBLIC_URL = os.environ.get("CONVOFLOW_PUBLIC_URL", "").rstrip("/")
# Per-tenant metrics name tenants, so they are shown only with this operator token.
METRICS_TOKEN = os.environ.get("CONVOFLOW_METRICS_TOKEN", "")
PAGE = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "page.html"), encoding="utf-8").read()

_pool = ConnectionPool(APP_DSN, min_size=1, max_size=10, open=False, kwargs={"prepare_threshold": None})
_in_flight = threading.BoundedSemaphore(MAX_IN_FLIGHT)
_queue = {"depth": 0, "checked": 0.0}
_queue_lock = threading.Lock()


def get_pool():
    if _pool.closed:
        _pool.open(wait=True)
    return _pool


def hash_key(key):
    return hashlib.sha256(key.encode()).hexdigest()


def queue_full():
    """Counts the inbox at most once a second per process, so the check stays cheap under load."""
    now = time.monotonic()
    with _queue_lock:
        if now - _queue["checked"] >= QUEUE_CHECK_SECONDS:
            with get_pool().connection() as db:
                _queue["depth"] = db.execute("SELECT count(*) FROM inbox").fetchone()[0]
                db.rollback()
            _queue["checked"] = now
        return _queue["depth"] >= MAX_QUEUE


def rate_allowed(db, bucket, per_minute):
    """A per-minute counter in Postgres, so the limit holds across every API instance and restart."""
    hits = db.execute(
        """INSERT INTO rate_hits (bucket, window_start, hits) VALUES (%s, date_trunc('minute', now()), 1)
           ON CONFLICT (bucket, window_start) DO UPDATE SET hits = rate_hits.hits + 1 RETURNING hits""",
        (bucket,),
    ).fetchone()[0]
    db.commit()
    return hits <= per_minute


def would_flag(db, text):
    """Runs the scanner's own rules, in the database's regex engine, on one text."""
    rows = db.execute(
        "SELECT rule, severity FROM unnest(%s::text[], %s::text[], %s::text[]) r(rule, severity, pattern) WHERE %s ~ pattern",
        ([r[0] for r in RULES], [r[1] for r in RULES], [r[2] for r in RULES], text),
    ).fetchall()
    db.rollback()
    return [{"rule": r, "severity": sev} for r, sev in rows]


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


def measure_tenants(db):
    rows = db.execute("SELECT * FROM tenant_metrics()").fetchall()
    dead_tuples = db.execute("SELECT inbox_dead_tuples()").fetchone()[0]
    db.rollback()
    return rows, dead_tuples


def firing(metrics):
    return sorted(name for name, limit in ALERTS.items() if metrics[name] > limit)


def _label(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def render(metrics, tenants=(), dead_tuples=None):
    lines = [f"convoflow_{name} {value}" for name, value in metrics.items()]
    lines += [f'convoflow_alert{{name="{name}"}} 1' for name in firing(metrics)]
    for tenant, queued, oldest in tenants:
        lines.append(f'convoflow_tenant_queued{{tenant="{_label(tenant)}"}} {queued}')
        lines.append(f'convoflow_tenant_oldest_seconds{{tenant="{_label(tenant)}"}} {oldest}')
    if dead_tuples is not None:
        lines.append(f"convoflow_inbox_dead_tuples {dead_tuples}")
    return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    timeout = REQUEST_TIMEOUT_SECONDS  # a client that stalls mid-request loses its socket

    def log_message(self, *args):
        pass

    def client_ip(self):
        forwarded = self.headers.get("X-Forwarded-For", "")
        return forwarded.split(",")[0].strip() or self.client_address[0]

    def is_operator(self):
        given = self.headers.get("Authorization", "")
        return bool(METRICS_TOKEN) and hmac.compare_digest(given, "Bearer " + METRICS_TOKEN)

    def reply(self, code, body, content_type="application/json", headers=None):
        data = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(data)

    def read_body(self, limit=MAX_BODY):
        """Returns the body, or None after replying with an error."""
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            self.reply(400, '{"error":"bad_length"}')
            return None
        if length > limit:
            self.reply(413, '{"error":"too_large"}')
            return None
        return self.rfile.read(length)

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
                    "GET /metrics": "pipeline counters, per-tenant queue state",
                    "GET /alerts": "alerts currently firing",
                    "POST /v1/events/{voice|chat|email|sms}": "ingest, needs X-Api-Key",
                    "POST /v1/webhooks/twilio/{tenant}": "Twilio SMS and voice transcriptions, verified by X-Twilio-Signature",
                    "POST /v1/webhooks/zendesk/{tenant}": "Zendesk comments, verified by X-Zendesk-Webhook-Signature",
                    "POST /v1/webhooks/mailgun/{tenant}": "Mailgun inbound email, verified by its signature fields",
                    "GET /v1/findings?limit=&cursor=&severity=": "your findings, needs X-Api-Key",
                },
            }, indent=2))
        if self.path.split("?")[0] == "/v1/findings":
            return self.findings()
        if self.path == "/health":
            with get_pool().connection() as db:
                db.execute("SELECT 1")
            return self.reply(200, '{"ok":true}')
        if self.path not in ("/alerts", "/metrics"):
            return self.reply(404, '{"error":"not_found"}')
        with get_pool().connection() as db:
            metrics = measure(db)
            if self.path == "/metrics":
                tenants, dead_tuples = measure_tenants(db)
                if not self.is_operator():
                    tenants = ()  # totals only: tenant names stay private
                return self.reply(200, render(metrics, tenants, dead_tuples), "text/plain")
        alerts = firing(metrics)
        self.reply(503 if alerts else 200, json.dumps({"ok": not alerts, "alerts": alerts}))

    def do_POST(self):
        if not _in_flight.acquire(blocking=False):
            return self.reply(503, '{"error":"busy"}', headers={"Retry-After": "1"})
        try:
            self.route_post()
        finally:
            _in_flight.release()

    def route_post(self):
        if self.path == "/demo":
            return self.demo()
        parts = self.path.split("?")[0].strip("/").split("/")
        if len(parts) == 3 and parts[:2] == ["v1", "events"]:
            return self.ingest(parts[2])
        if len(parts) == 4 and parts[:2] == ["v1", "webhooks"] and parts[2] in ("twilio", "zendesk", "mailgun"):
            return self.webhook(parts[2], parts[3])
        self.reply(404, '{"error":"not_found"}')

    def backpressure(self):
        """Replies 429 and returns True when workers are too far behind to take more."""
        if queue_full():
            self.reply(429, '{"error":"queue_full"}', headers={"Retry-After": "5"})
            return True
        return False

    def accept(self, tenant_id, source, payload):
        with tracing.request_span("ingest", tenant=tenant_id, source=source) as ids:
            try:
                events, trace_id = normalize(source, tenant_id, payload, ids.trace_id, ids.span_id)
            except BadPayload as err:
                self.reply(400, json.dumps({"error": str(err)}))
                return None
            with get_pool().connection() as db:
                enqueue(db, [json.dumps(e) for e in events])
        log("events_accepted", tenant_id=tenant_id, source=source, count=len(events), trace_id=trace_id)
        return len(events), trace_id

    def ingest(self, source):
        body = self.read_body()
        if body is None:
            return
        with get_pool().connection() as db:
            found = db.execute(
                "SELECT tenant_id FROM tenant_keys WHERE key_hash = %s AND revoked_at IS NULL",
                (hash_key(self.headers.get("X-Api-Key", "")),),
            ).fetchone()
        if not found:
            return self.reply(401, '{"error":"bad_key"}')
        if self.backpressure():
            return
        try:
            payload = json.loads(body)
        except ValueError as err:
            return self.reply(400, json.dumps({"error": str(err)}))
        result = self.accept(found[0], source, payload)
        if result:
            self.reply(202, json.dumps({"accepted": result[0], "trace_id": result[1]}))

    def public_url(self):
        if PUBLIC_URL:
            return PUBLIC_URL + self.path
        proto = self.headers.get("X-Forwarded-Proto", "http").split(",")[0].strip()
        return f"{proto}://{self.headers.get('Host', '')}{self.path}"

    def webhook(self, provider, tenant_id):
        body = self.read_body()
        if body is None:
            return
        with get_pool().connection() as db:
            row = db.execute(
                "SELECT secret FROM tenant_webhooks WHERE tenant_id = %s AND provider = %s",
                (tenant_id, provider),
            ).fetchone()
        if not row:
            return self.reply(404, '{"error":"not_found"}')  # same answer whether the tenant exists or not
        secret = secretbox.open_(row[0])
        if provider == "twilio":
            ok, fields = webhooks.verify_twilio(secret, self.public_url(), body, self.headers.get("X-Twilio-Signature"))
            if not ok:
                return self.reply(401, '{"error":"bad_signature"}')
            if "TranscriptionText" in fields:
                source, payload = "voice", webhooks.twilio_transcription_payload(fields)
            elif "MessageSid" in fields:
                source, payload = "sms", webhooks.twilio_sms_payload(fields)
            else:
                return self.reply(400, '{"error":"not an inbound message or transcription"}')
        elif provider == "mailgun":
            ok, fields = webhooks.verify_mailgun(secret, body, self.headers.get("Content-Type", ""))
            if not ok:
                return self.reply(401, '{"error":"bad_signature"}')
            try:
                source, payload = "email", webhooks.mailgun_email_payload(fields)
            except KeyError as err:
                return self.reply(400, json.dumps({"error": f"missing {err}"}))
        else:
            if not webhooks.verify_zendesk(secret, self.headers.get("X-Zendesk-Webhook-Signature-Timestamp"),
                                           body, self.headers.get("X-Zendesk-Webhook-Signature")):
                return self.reply(401, '{"error":"bad_signature"}')
            try:
                source, payload = "chat", json.loads(body)
            except ValueError as err:
                return self.reply(400, json.dumps({"error": str(err)}))
        if self.backpressure():
            return
        result = self.accept(tenant_id, source, payload)
        if not result:
            return
        if provider == "twilio":
            return self.reply(200, "<Response/>", "text/xml")  # empty TwiML: store it, send no reply
        self.reply(202, json.dumps({"accepted": result[0], "trace_id": result[1]}))

    def tenant_for_key(self, db):
        key_hash = hash_key(self.headers.get("X-Api-Key", ""))
        row = db.execute("SELECT tenant_id FROM tenant_keys WHERE key_hash = %s AND revoked_at IS NULL",
                         (key_hash,)).fetchone()
        return (row[0], key_hash[:12]) if row else (None, None)

    def findings(self):
        """A tenant's findings, oldest first, with an opaque cursor for the next page."""
        query = dict(parse_qsl(urlparse(self.path).query))
        try:
            limit = max(1, min(int(query.get("limit", 100)), 500))
            after = json.loads(base64.urlsafe_b64decode(query["cursor"])) if query.get("cursor") else None
        except (ValueError, TypeError):
            return self.reply(400, '{"error":"bad limit or cursor"}')
        with get_pool().connection() as db:
            tenant_id, kid = self.tenant_for_key(db)
            if not tenant_id:
                db.rollback()
                return self.reply(401, '{"error":"bad_key"}')
            db.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant_id,))
            rows = db.execute(
                """SELECT found_at, event_key, rule, severity, evidence, rules_version FROM findings
                   WHERE (%s::timestamptz IS NULL OR (found_at, event_key, rule) > (%s::timestamptz, %s, %s))
                     AND (%s::text IS NULL OR severity = %s)
                   ORDER BY found_at, event_key, rule LIMIT %s""",
                (after and after[0], after and after[0], after and after[1], after and after[2],
                 query.get("severity"), query.get("severity"), limit),
            ).fetchall()
            db.execute("INSERT INTO audit_log (actor, tenant_id, action, detail) VALUES (%s, %s, 'findings_read', %s)",
                       ("key:" + kid, tenant_id, json.dumps({"returned": len(rows)})))
            db.commit()
        items = [{"found_at": r[0].isoformat(), "event_key": r[1], "rule": r[2], "severity": r[3],
                  "evidence": r[4], "rules_version": r[5]} for r in rows]
        cursor = None
        if len(rows) == limit:
            last = rows[-1]
            cursor = base64.urlsafe_b64encode(json.dumps([last[0].isoformat(), last[1], last[2]]).encode()).decode()
        self.reply(200, json.dumps({"findings": items, "next_cursor": cursor}))

    def demo(self):
        if not DEMO_TENANT:
            return self.reply(404, '{"error":"not_found"}')
        with get_pool().connection() as db:
            if not rate_allowed(db, "demo:" + self.client_ip(), DEMO_PER_MINUTE):
                return self.reply(429, '{"error":"slow_down"}', headers={"Retry-After": "60"})
        body = self.read_body(4096)
        if body is None:
            return
        try:
            data = json.loads(body)
            channel, text = data["channel"], str(data["text"]).strip()
        except (ValueError, KeyError, TypeError):
            return self.reply(400, '{"error":"send channel and text"}')
        if channel not in CHANNELS or not text or len(text) > DEMO_MAX_TEXT:
            return self.reply(400, json.dumps({"error": f"channel must be one of {list(CHANNELS)} and text 1-{DEMO_MAX_TEXT} characters"}))
        if self.backpressure():
            return
        text = pii.mask(text)
        with tracing.request_span("demo", tenant=DEMO_TENANT, source=channel) as ids:
            event = {"tenant_id": DEMO_TENANT, "conversation_id": f"demo-{uuid.uuid4().hex[:12]}", "channel": channel,
                     "text": text, "trace_id": ids.trace_id, **({"span_id": ids.span_id} if ids.span_id else {})}
            with get_pool().connection() as db:
                enqueue(db, [json.dumps(event)])
                flags = would_flag(db, text)
        log("demo_accepted", tenant_id=DEMO_TENANT, channel=channel, trace_id=ids.trace_id, flags=len(flags))
        self.reply(202, json.dumps({"accepted": 1, "trace_id": ids.trace_id, "would_flag": flags, "stored_as": text}))


def serve(port=8080):
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    signal.signal(signal.SIGTERM, lambda *_: threading.Thread(target=server.shutdown).start())
    log("api_started", port=port, max_in_flight=MAX_IN_FLIGHT, max_queue=MAX_QUEUE)
    server.serve_forever()
    get_pool().close()
    log("api_stopped")


if __name__ == "__main__":
    serve()
