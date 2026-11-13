"""Multi-tenant conversation ingestion on Postgres.

Workers claim inbox rows with FOR UPDATE SKIP LOCKED and write results in the
same transaction as the claim, so a crash puts the batch back in the queue.
"""
import hashlib
import json
import os
import random
import signal
import sys
import time
import uuid

import psycopg
from psycopg import sql

ADMIN_DSN = os.environ.get("CONVOFLOW_ADMIN_DSN", "host=127.0.0.1 port=54329 dbname=convoflow_dev user=convoflow")
APP_DSN = os.environ.get("CONVOFLOW_APP_DSN", "host=127.0.0.1 port=54329 dbname=convoflow_dev user=app")
CHANNELS = ("voice", "chat", "email", "sms")
REQUIRED = ("tenant_id", "conversation_id", "channel", "text")
BATCH_SIZE = 500
SCAN_LAG_SECONDS = 60
FAIR_SHARE = 50
MAX_ATTEMPTS = 3
RETRY_BASE_SECONDS = 2
DEAD_LETTER_DAYS = 90
FAIR_CLAIMS = os.environ.get("CONVOFLOW_FAIR", "1") == "1"

# Rules run in the database so a scan never pulls messages into Python.
RULES = (
    ("card_number_exposed", "critical", r"\m\d{4}[ -]?\d{4}[ -]?\d{4}[ -]?\d{4}\M"),
    ("ssn_exposed", "critical", r"\m\d{3}-\d{2}-\d{4}\M"),
    ("denied_being_ai", "high", r"(?i)i am (a )?(real )?human"),
)


def log(event, **fields):
    print(json.dumps({"event": event, **fields}, default=str), flush=True)


def reset():
    """Drops everything. Refuses to run anywhere but a local database."""
    if "host=127.0.0.1" not in ADMIN_DSN:
        raise RuntimeError("reset is only allowed on a local database")
    with psycopg.connect(ADMIN_DSN, autocommit=True) as db:
        db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    migrate()


def migrate():
    """Applies each file in migrations/ once, in order, in its own transaction."""
    folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations")
    with psycopg.connect(ADMIN_DSN, autocommit=True) as db:
        db.execute("CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())")
        db.execute("SELECT pg_advisory_lock(7342)")
        done = {r[0] for r in db.execute("SELECT name FROM schema_migrations")}
        for name in sorted(f for f in os.listdir(folder) if f.endswith(".sql")):
            if name in done:
                continue
            with db.transaction():
                db.execute(open(os.path.join(folder, name)).read())
                db.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (name,))
            log("migration_applied", name=name)
        password = os.environ.get("CONVOFLOW_APP_PASSWORD")
        if password:
            db.execute(sql.SQL("ALTER ROLE app PASSWORD {}").format(sql.Literal(password)))
        db.execute("SELECT pg_advisory_unlock(7342)")


def add_tenants(limits):
    with psycopg.connect(ADMIN_DSN, autocommit=True) as db:
        with db.cursor() as cur:
            cur.executemany("INSERT INTO tenants (id, daily_limit) VALUES (%s, %s)", list(limits.items()))


def enqueue(db, payloads):
    with db.cursor() as cur, cur.copy("COPY inbox (tenant_hint, payload) FROM STDIN") as copy:
        for payload in payloads:
            copy.write_row((tenant_hint(payload), payload))
    db.commit()


def tenant_hint(raw):
    """Routing only. A wrong hint changes the order, never the tenant a row is stored under."""
    e, reason = parse(raw)
    return "" if reason else str(e["tenant_id"])


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
    """Returns (stored, duplicates, rows over quota, blocked rows, reason). Caller locks tenants in sorted order.
    The quota day is the database's UTC date, so workers in different time zones agree on it."""
    cur.execute("SELECT (now() AT TIME ZONE 'UTC')::date")
    today = cur.fetchone()[0]
    cur.execute("SELECT daily_limit, channels FROM tenants WHERE id = %s", (tenant_id,))
    tenant = cur.fetchone()
    if tenant is None:
        return 0, 0, rows, {}, "unknown_tenant"
    blocked = [r for r in rows if r[3] not in tenant[1]]
    rows = [r for r in rows if r[3] in tenant[1]]
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
        """INSERT INTO messages (tenant_id, event_key, conversation_id, channel, text, enqueued_at, trace_id)
           SELECT * FROM unnest(%s::text[], %s::text[], %s::text[], %s::text[], %s::text[], %s::timestamptz[], %s::text[])
           ON CONFLICT DO NOTHING""",
        [list(c) for c in zip(*[r[:7] for r in allowed])] if allowed else [[], [], [], [], [], [], []],
    )
    stored = cur.rowcount
    cur.execute(
        "UPDATE tenant_usage SET stored = stored + %s WHERE tenant_id = %s AND day = %s",
        (stored, tenant_id, today),
    )
    return stored, len(allowed) - stored, over, blocked, "over_quota"


def claim(cur):
    """Retries one suspect row alone if any are due. Otherwise takes a fair share
    from every tenant, then fills the batch oldest-first."""
    cur.execute(
        """DELETE FROM inbox WHERE id IN (
               SELECT id FROM inbox WHERE attempts > 0 AND next_attempt_at <= clock_timestamp()
               ORDER BY next_attempt_at LIMIT 1 FOR UPDATE SKIP LOCKED
           ) RETURNING id, payload, enqueued_at, attempts"""
    )
    claimed = cur.fetchall()
    if claimed:
        return claimed
    if FAIR_CLAIMS:
        cur.execute(
            """DELETE FROM inbox WHERE id IN (
                   SELECT i.id FROM (SELECT id FROM tenants UNION ALL SELECT '') t,
                   LATERAL (SELECT id FROM inbox WHERE tenant_hint = t.id AND attempts = 0
                            ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED) i
               ) RETURNING id, payload, enqueued_at, attempts""",
            (FAIR_SHARE,),
        )
        claimed = cur.fetchall()
    if len(claimed) < BATCH_SIZE:
        cur.execute(
            """DELETE FROM inbox WHERE id IN (
                   SELECT id FROM inbox WHERE attempts = 0 ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED
               ) RETURNING id, payload, enqueued_at, attempts""",
            (BATCH_SIZE - len(claimed),),
        )
        claimed += cur.fetchall()
    return claimed


def record_failure(db, worker_id, rows, err):
    """A failed batch marks every row suspect, so each retries alone and a good row
    can never be dead-lettered for sharing a batch with a bad one."""
    db.rollback()
    with db.cursor() as cur:
        for row_id, payload, _, attempts in rows:
            if len(rows) == 1 and attempts + 1 >= MAX_ATTEMPTS:
                cur.execute("DELETE FROM inbox WHERE id = %s", (row_id,))
                cur.execute(
                    "INSERT INTO dead_letters (payload, reason) VALUES (%s, %s)",
                    (payload.replace("\x00", ""), f"max_attempts: {type(err).__name__}"),
                )
                cur.execute(
                    """INSERT INTO worker_counters (worker_id, claimed, dead) VALUES (%s, 1, 1)
                       ON CONFLICT (worker_id) DO UPDATE SET
                         claimed = worker_counters.claimed + 1, dead = worker_counters.dead + 1""",
                    (worker_id,),
                )
                continue
            cur.execute(
                """UPDATE inbox SET attempts = attempts + 1,
                       next_attempt_at = clock_timestamp() + make_interval(secs => %s)
                   WHERE id = %s""",
                (RETRY_BASE_SECONDS * 2 ** attempts, row_id),
            )
    db.commit()


def process_batch(db, worker_id, before_commit=None):
    """Claims and stores one batch in a single transaction. Returns rows claimed."""
    with db.cursor() as cur:
        claimed = claim(cur)
    if not claimed:
        db.rollback()
        return 0
    try:
        store_batch(db, worker_id, claimed, before_commit)
    except (psycopg.DataError, psycopg.IntegrityError) as err:
        log("batch_failed", worker_id=worker_id, rows=len(claimed), error=type(err).__name__)
        record_failure(db, worker_id, claimed, err)
    return len(claimed)


def store_batch(db, worker_id, claimed, before_commit):
    with db.cursor() as cur:
        by_tenant, dead = {}, []
        for _, raw, enqueued_at, _ in claimed:
            e, reason = parse(raw)
            if reason:
                dead.append((raw, reason))
            else:
                by_tenant.setdefault(e["tenant_id"], []).append((
                    e["tenant_id"], event_key(e), e["conversation_id"],
                    e["channel"], e["text"], enqueued_at, e.get("trace_id"), raw,
                ))
        stored = duplicates = 0
        for tenant_id in sorted(by_tenant):
            s, d, over, blocked, reason = store_for_tenant(cur, tenant_id, by_tenant[tenant_id])
            stored += s
            duplicates += d
            dead += [(r[7], reason) for r in over]
            dead += [(r[7], "channel_not_entitled") for r in blocked]
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


def replay(reason, limit=10_000, repair=None):
    """Sends dead letters back through the queue, each row once.

    repair(payload) can fix a row first and returns None to leave it alone.
    Duplicates are safe because stored messages are keyed by content.
    """
    with psycopg.connect(ADMIN_DSN) as db, db.cursor() as cur:
        cur.execute(
            """SELECT id, payload FROM dead_letters
               WHERE reason = %s AND replayed_at IS NULL
               ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED""",
            (reason, limit),
        )
        chosen = [(i, repair(p) if repair else p) for i, p in cur.fetchall()]
        chosen = [(i, p) for i, p in chosen if p is not None]
        if chosen:
            cur.executemany(
                "INSERT INTO inbox (tenant_hint, payload) VALUES (%s, %s)",
                [(tenant_hint(p), p) for _, p in chosen],
            )
            cur.execute(
                "UPDATE dead_letters SET replayed_at = clock_timestamp() WHERE id = ANY(%s)",
                ([i for i, _ in chosen],),
            )
    return len(chosen)


def purge():
    """Deletes data past each tenant's retention period, and old dead letters."""
    with psycopg.connect(ADMIN_DSN) as db:
        messages = db.execute(
            """DELETE FROM messages m USING tenants t WHERE m.tenant_id = t.id
               AND m.ingested_at < clock_timestamp() - make_interval(days => t.retention_days)"""
        ).rowcount
        findings = db.execute(
            """DELETE FROM findings f USING tenants t WHERE f.tenant_id = t.id
               AND f.found_at < clock_timestamp() - make_interval(days => t.retention_days)"""
        ).rowcount
        dead = db.execute(
            "DELETE FROM dead_letters WHERE failed_at < clock_timestamp() - make_interval(days => %s)",
            (DEAD_LETTER_DAYS,),
        ).rowcount
    log("purged", messages=messages, findings=findings, dead_letters=dead)
    return messages, findings, dead


def run_worker(idle_exit=None):
    worker_id = uuid.uuid4().hex[:8]
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(True))
    log("worker_started", worker_id=worker_id)
    idle_since = time.monotonic()
    db = psycopg.connect(APP_DSN)
    while not stop:
        try:
            claimed = process_batch(db, worker_id)
        except psycopg.OperationalError as err:
            log("worker_retrying", worker_id=worker_id, error=str(err))
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
    db.close()
    log("worker_stopped", worker_id=worker_id)


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
