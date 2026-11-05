"""Multi-tenant conversation ingestion: day 1 of the build.

Takes events from four channels, drops duplicates per tenant, and sends
malformed events to a dead-letter table instead of stopping the run.
"""
import hashlib
import json
import random
import sqlite3
import sys
import time

CHANNELS = ("voice", "chat", "email", "sms")
REQUIRED = ("tenant_id", "conversation_id", "channel", "text")


def connect(path=":memory:"):
    db = sqlite3.connect(path)
    db.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        CREATE TABLE IF NOT EXISTS events (
            tenant_id TEXT NOT NULL,
            event_key TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            text TEXT NOT NULL,
            PRIMARY KEY (tenant_id, event_key)
        );
        CREATE TABLE IF NOT EXISTS dead_letters (raw TEXT NOT NULL, reason TEXT NOT NULL);
    """)
    return db


def event_key(e):
    body = f"{e['conversation_id']}|{e['channel']}|{e['text']}"
    return hashlib.sha256(body.encode()).hexdigest()


def problem(e):
    if not isinstance(e, dict):
        return "not_an_object"
    for field in REQUIRED:
        if not e.get(field):
            return f"missing_{field}"
    if e["channel"] not in CHANNELS:
        return "unknown_channel"
    return None


def ingest(db, events, batch_size=1000):
    stats = {"seen": 0, "stored": 0, "duplicates": 0, "dead": 0}
    good, bad = [], []

    def flush():
        before = db.total_changes
        db.executemany("INSERT OR IGNORE INTO events VALUES (?,?,?,?,?)", good)
        stored = db.total_changes - before
        stats["stored"] += stored
        stats["duplicates"] += len(good) - stored
        db.executemany("INSERT INTO dead_letters VALUES (?,?)", bad)
        stats["dead"] += len(bad)
        db.commit()
        good.clear()
        bad.clear()

    for e in events:
        stats["seen"] += 1
        reason = problem(e)
        if reason:
            bad.append((json.dumps(e, default=str), reason))
        else:
            good.append((e["tenant_id"], event_key(e), e["conversation_id"], e["channel"], e["text"]))
        if len(good) + len(bad) >= batch_size:
            flush()
    flush()
    return stats


def simulate(n, tenants=20, dup_rate=0.02, bad_rate=0.001, seed=7):
    rng = random.Random(seed)
    recent = []
    for i in range(n):
        roll = rng.random()
        if roll < bad_rate:
            yield {"tenant_id": f"t{i % tenants}", "channel": "fax", "conversation_id": str(i), "text": "x"}
        elif roll < bad_rate + dup_rate and recent:
            yield rng.choice(recent)
        else:
            e = {
                "tenant_id": f"t{rng.randrange(tenants)}",
                "conversation_id": f"c{i}",
                "channel": rng.choice(CHANNELS),
                "text": f"message {i}",
            }
            recent = (recent + [e])[-1000:]
            yield e


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
    db = connect("convoflow.db")
    start = time.perf_counter()
    stats = ingest(db, simulate(n))
    secs = time.perf_counter() - start
    stats["seconds"] = round(secs, 2)
    stats["items_per_sec"] = round(n / secs)
    stats["projected_items_per_day"] = round(n / secs * 86400)
    print(json.dumps(stats, indent=2))
