"""Multi-tenant conversation ingestion on Postgres.

Workers claim inbox rows with FOR UPDATE SKIP LOCKED and write results in the
same transaction as the claim, so a crash puts the batch back in the queue.
"""
import hashlib
import json
import os
import random
import sys
import time
import uuid
from datetime import date

import psycopg

ADMIN_DSN = "host=127.0.0.1 port=54329 dbname=convoflow user=convoflow"
APP_DSN = "host=127.0.0.1 port=54329 dbname=convoflow user=app"
CHANNELS = ("voice", "chat", "email", "sms")
REQUIRED = ("tenant_id", "conversation_id", "channel", "text")
BATCH_SIZE = 500
SCAN_LAG_SECONDS = 60

# Rules run in the database so a scan never pulls messages into Python.
RULES = (
    ("card_number_exposed", "critical", r"\m\d{4}[ -]?\d{4}[ -]?\d{4}[ -]?\d{4}\M"),
    ("ssn_exposed", "critical", r"\m\d{3}-\d{2}-\d{4}\M"),
    ("denied_being_ai", "high", r"(?i)i am (a )?(real )?human"),
)


def reset():
    with psycopg.connect(ADMIN_DSN, autocommit=True) as db:
        db.execute(open(os.path.join(os.path.dirname(__file__), "schema.sql")).read())


def add_tenants(limits):
    with psycopg.connect(ADMIN_DSN, autocommit=True) as db:
        with db.cursor() as cur:
            cur.executemany("INSERT INTO tenants VALUES (%s, %s)", list(limits.items()))


def enqueue(db, payloads):
    with db.cursor() as cur, cur.copy("COPY inbox (payload) FROM STDIN") as copy:
        for payload in payloads:
            copy.write_row((payload,))
    db.commit()


def parse(raw):
    try:
        e = json.loads(raw)
    except ValueError:
        return None, "invalid_json"
    if not isinstance(e, dict):
        return None, "not_an_object"
    for field in REQUIRED:
        if not e.get(field):
            return None, f"missing_{field}"
    if e["channel"] not in CHANNELS:
        return None, "unknown_channel"
    return e, None


def event_key(e):
    body = f"{e['conversation_id']}|{e['channel']}|{e['text']}"
    return hashlib.sha256(body.encode()).hexdigest()


def store_for_tenant(cur, tenant_id, rows):
    """Returns (stored, duplicates, rows over quota). Caller locks tenants in sorted order."""
    today = date.today()
    cur.execute("SELECT daily_limit FROM tenants WHERE id = %s", (tenant_id,))
    tenant = cur.fetchone()
    if tenant is None:
        return 0, 0, rows, "unknown_tenant"
    cur.execute(
        "INSERT INTO tenant_usage (tenant_id, day) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (tenant_id, today),
    )
    cur.execute(
        "SELECT stored FROM tenant_usage WHERE tenant_id = %s AND day = %s FOR UPDATE",
        (tenant_id, today),
    )
    room = max(tenant[0] - cur.fetchone()[0], 0)
    allowed, over = rows[:room], rows[room:]
    cur.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant_id,))
    cur.execute(
        """INSERT INTO messages (tenant_id, event_key, conversation_id, channel, text, enqueued_at)
           SELECT * FROM unnest(%s::text[], %s::text[], %s::text[], %s::text[], %s::text[], %s::timestamptz[])
           ON CONFLICT DO NOTHING""",
        [list(c) for c in zip(*allowed)] if allowed else [[], [], [], [], [], []],
    )
    stored = cur.rowcount
    cur.execute(
        "UPDATE tenant_usage SET stored = stored + %s WHERE tenant_id = %s AND day = %s",
        (stored, tenant_id, today),
    )
    return stored, len(allowed) - stored, over, "over_quota"


def process_batch(db, worker_id, before_commit=None):
    """Claims and stores one batch in a single transaction. Returns rows claimed."""
    with db.cursor() as cur:
        cur.execute(
            """DELETE FROM inbox WHERE id IN (
                   SELECT id FROM inbox ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED
               ) RETURNING payload, enqueued_at""",
            (BATCH_SIZE,),
        )
        claimed = cur.fetchall()
        if not claimed:
            db.rollback()
            return 0
        by_tenant, dead = {}, []
        for raw, enqueued_at in claimed:
            e, reason = parse(raw)
            if reason:
                dead.append((raw, reason))
            else:
                by_tenant.setdefault(e["tenant_id"], []).append((
                    e["tenant_id"], event_key(e), e["conversation_id"],
                    e["channel"], e["text"], enqueued_at,
                ))
        stored = duplicates = 0
        for tenant_id in sorted(by_tenant):
            s, d, over, reason = store_for_tenant(cur, tenant_id, by_tenant[tenant_id])
            stored += s
            duplicates += d
            dead += [(json.dumps({"tenant_id": r[0], "conversation_id": r[2]}), reason) for r in over]
        if dead:
            cur.executemany("INSERT INTO dead_letters (payload, reason) VALUES (%s, %s)", dead)
        cur.execute(
            """INSERT INTO worker_counters VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (worker_id) DO UPDATE SET
                 claimed = worker_counters.claimed + EXCLUDED.claimed,
                 stored = worker_counters.stored + EXCLUDED.stored,
                 duplicates = worker_counters.duplicates + EXCLUDED.duplicates,
                 dead = worker_counters.dead + EXCLUDED.dead""",
            (worker_id, len(claimed), stored, duplicates, len(dead)),
        )
        if before_commit:
            before_commit()
        db.commit()
        return len(claimed)


def run_worker(idle_exit=None):
    worker_id = uuid.uuid4().hex[:8]
    idle_since = time.monotonic()
    db = psycopg.connect(APP_DSN)
    while True:
        try:
            claimed = process_batch(db, worker_id)
        except psycopg.OperationalError as err:
            print(f"worker {worker_id} retrying: {err}", file=sys.stderr)
            db.close()
            time.sleep(1)
            db = psycopg.connect(APP_DSN)
            continue
        except psycopg.errors.DeadlockDetected:
            db.rollback()
            continue
        if claimed:
            idle_since = time.monotonic()
        elif idle_exit and time.monotonic() - idle_since > idle_exit:
            return
        else:
            time.sleep(0.2)


def scan(lag_seconds=SCAN_LAG_SECONDS):
    """Scans messages since the last run. Lag leaves room for in-flight commits."""
    start = time.perf_counter()
    with psycopg.connect(ADMIN_DSN) as db:
        window_start, window_end = db.execute(
            """SELECT COALESCE((SELECT max(window_end) FROM scan_runs), 'epoch'::timestamptz),
                      clock_timestamp() - make_interval(secs => %s)""",
            (lag_seconds,),
        ).fetchone()
        scanned = db.execute(
            "SELECT count(*) FROM messages WHERE ingested_at >= %s AND ingested_at < %s",
            (window_start, window_end),
        ).fetchone()[0]
        found = 0
        for rule, severity, pattern in RULES:
            found += db.execute(
                """INSERT INTO findings (tenant_id, event_key, rule, severity, evidence)
                   SELECT tenant_id, event_key, %s, %s, substring(text from %s)
                   FROM messages
                   WHERE ingested_at >= %s AND ingested_at < %s AND text ~ %s
                   ON CONFLICT DO NOTHING""",
                (rule, severity, pattern, window_start, window_end, pattern),
            ).rowcount
        db.execute(
            """INSERT INTO scan_runs (window_start, window_end, scanned, findings, seconds)
               VALUES (%s, %s, %s, %s, %s)""",
            (window_start, window_end, scanned, found, time.perf_counter() - start),
        )
    return scanned, found


class Simulator:
    """Generates traffic with known duplicates, bad rows and planted violations."""

    PLANTED = (
        "Your card 4111 1111 1111 1111 is on file.",
        "I have your SSN as 123-45-6789 here.",
        "Yes, I am a real human agent.",
    )

    def __init__(self, tenants, seed=7, dup_rate=0.02, bad_rate=0.001, plant_rate=0.005):
        self.rng = random.Random(seed)
        self.tenants = tenants
        self.dup_rate, self.bad_rate, self.plant_rate = dup_rate, bad_rate, plant_rate
        self.recent, self.planted, self.n = [], 0, 0
        self.prefix = uuid.uuid4().hex[:8]

    def next(self):
        self.n += 1
        roll = self.rng.random()
        if roll < self.bad_rate:
            return "{not json" if self.n % 2 else json.dumps({"tenant_id": "t1", "channel": "fax"})
        if roll < self.bad_rate + self.dup_rate and self.recent:
            return self.rng.choice(self.recent)
        tenant = "t0" if self.rng.random() < 0.4 else self.rng.choice(self.tenants)
        text = f"Thanks for reaching out about order {self.n}. Let me look into that for you today."
        if tenant != "capped" and self.rng.random() < self.plant_rate:
            text = self.rng.choice(self.PLANTED) + f" Ref {self.n}."
            self.planted += 1
        payload = json.dumps({
            "tenant_id": tenant, "conversation_id": f"{self.prefix}-{self.n}",
            "channel": self.rng.choice(CHANNELS), "text": text,
        })
        self.recent = (self.recent + [payload])[-1000:]
        return payload
