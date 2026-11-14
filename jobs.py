"""One-off and scheduled tasks.

    python jobs.py migrate | hourly | monitor | every-hour | seed-key
    python jobs.py issue-key TENANT [DAILY_LIMIT] [LABEL]
    python jobs.py list-keys TENANT
    python jobs.py revoke-key KEY_ID
    python jobs.py set-webhook TENANT twilio|zendesk SECRET
    python jobs.py delete-tenant TENANT --confirm
"""
import os
import secrets
import sys
import time

import psycopg

from api import firing, hash_key, measure
from convoflow import ADMIN_DSN, APP_DSN, log, migrate, purge, scan

METRICS = {
    "queue_depth": "QueueDepth",
    "queue_lag_seconds": "QueueLagSeconds",
    "dead_letter_ratio_5m": "DeadLetterRatio5m",
    "scan_overdue_seconds": "ScanOverdueSeconds",
}


def publish(client, metrics):
    client.put_metric_data(
        Namespace="Convoflow",
        MetricData=[{"MetricName": name, "Value": float(metrics[key])} for key, name in METRICS.items()],
    )


def monitor():
    import boto3

    client = boto3.client("cloudwatch")
    while True:
        with psycopg.connect(APP_DSN) as db:
            metrics = measure(db)
        publish(client, metrics)
        log("metrics_published", alerts=firing(metrics), **metrics)
        time.sleep(60)


def hourly():
    scanned, found = scan()
    log("scan_finished", scanned=scanned, findings=found)
    purge()


def issue_key(tenant_id, daily_limit=1_000_000, label="", key=None):
    """Creates the tenant if needed and adds an API key. Existing keys keep working
    until revoked, so a key can be rotated without downtime. Only the hash is stored."""
    key = key or secrets.token_urlsafe(32)
    with psycopg.connect(ADMIN_DSN) as db:
        db.execute("INSERT INTO tenants (id, daily_limit) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                   (tenant_id, daily_limit))
        db.execute("INSERT INTO tenant_keys (key_hash, tenant_id, label) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                   (hash_key(key), tenant_id, label))
    return key


def key_id(key_hash):
    return key_hash[:12]


def list_keys(tenant_id):
    with psycopg.connect(ADMIN_DSN) as db:
        rows = db.execute(
            "SELECT key_hash, label, created_at, revoked_at FROM tenant_keys WHERE tenant_id = %s ORDER BY created_at",
            (tenant_id,),
        ).fetchall()
    return [{"id": key_id(h), "label": l, "created_at": c, "revoked_at": r} for h, l, c, r in rows]


def revoke_key(kid):
    with psycopg.connect(ADMIN_DSN) as db:
        n = db.execute(
            "UPDATE tenant_keys SET revoked_at = clock_timestamp() WHERE left(key_hash, 12) = %s AND revoked_at IS NULL",
            (kid,),
        ).rowcount
    log("key_revoked", key_id=kid, count=n)
    return n


def set_webhook(tenant_id, provider, secret):
    with psycopg.connect(ADMIN_DSN) as db:
        db.execute(
            """INSERT INTO tenant_webhooks (tenant_id, provider, secret) VALUES (%s, %s, %s)
               ON CONFLICT (tenant_id, provider) DO UPDATE SET secret = EXCLUDED.secret""",
            (tenant_id, provider, secret),
        )


def backfill_mask(batch=1000):
    """Masks rows stored before masking existed. Runs once; schema_migrations records it."""
    import pii
    marker = "backfill:mask-v1"
    with psycopg.connect(ADMIN_DSN) as db:
        if db.execute("SELECT 1 FROM schema_migrations WHERE name = %s", (marker,)).fetchone():
            return 0
        changed = 0
        for table, key, column in (("messages", "tenant_id, event_key", "text"),
                                   ("findings", "tenant_id, event_key, rule", "evidence"),
                                   ("dead_letters", "id", "payload")):
            with db.cursor(name=f"scan_{table}") as cur:
                cur.itersize = batch
                cur.execute(f"SELECT {key}, {column} FROM {table} WHERE {column} ~ '[0-9]{{3}}'")
                rows = [(r[:-1], r[-1]) for r in cur]
            cols = [c.strip() for c in key.split(",")]
            where = " AND ".join(f"{c} = %s" for c in cols)
            for ids, value in rows:
                masked = pii.mask(value)
                if masked != value:
                    db.execute(f"UPDATE {table} SET {column} = %s WHERE {where}", (masked, *ids))
                    changed += 1
        db.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (marker,))
    log("backfill_mask_done", rows=changed)
    return changed


def delete_tenant(tenant_id):
    """Erases one tenant's data in a single transaction and records that it happened."""
    with psycopg.connect(ADMIN_DSN) as db:
        if not db.execute("SELECT 1 FROM tenants WHERE id = %s", (tenant_id,)).fetchone():
            raise SystemExit(f"no tenant {tenant_id}")
        counts = {
            "messages": db.execute("DELETE FROM messages WHERE tenant_id = %s", (tenant_id,)).rowcount,
            "findings": db.execute("DELETE FROM findings WHERE tenant_id = %s", (tenant_id,)).rowcount,
            "dead_letters": db.execute("DELETE FROM dead_letters WHERE payload_tenant(payload) = %s",
                                       (tenant_id,)).rowcount,
            "queued": db.execute("DELETE FROM inbox WHERE tenant_hint = %s", (tenant_id,)).rowcount,
        }
        db.execute("DELETE FROM tenant_usage WHERE tenant_id = %s", (tenant_id,))
        db.execute("DELETE FROM tenants WHERE id = %s", (tenant_id,))
        db.execute(
            """INSERT INTO tenant_deletions (tenant_id, messages, findings, dead_letters, queued)
               VALUES (%s, %s, %s, %s, %s)""",
            (tenant_id, counts["messages"], counts["findings"], counts["dead_letters"], counts["queued"]),
        )
    log("tenant_deleted", tenant_id=tenant_id, **counts)
    return counts


if __name__ == "__main__":
    cmd, args = sys.argv[1], sys.argv[2:]
    if cmd == "issue-key":
        key = issue_key(args[0], int(args[1]) if len(args) > 1 else 1_000_000, args[2] if len(args) > 2 else "")
        print(f"key: {key}\nid:  {key_id(hash_key(key))}  (shown once; only the hash is stored)")
    elif cmd == "list-keys":
        for k in list_keys(args[0]):
            print(k["id"], k["label"] or "-", k["created_at"], "revoked " + str(k["revoked_at"]) if k["revoked_at"] else "active")
    elif cmd == "revoke-key":
        print(revoke_key(args[0]), "revoked")
    elif cmd == "set-webhook":
        set_webhook(*args[:3])
        print("saved")
    elif cmd == "backfill-mask":
        print(backfill_mask(), "rows masked")
    elif cmd == "delete-tenant":
        if "--confirm" not in args:
            raise SystemExit("this erases the tenant's data; add --confirm")
        print(delete_tenant(args[0]))
    elif cmd == "seed-key":
        # For hosts without a shell: CONVOFLOW_SEED_TENANT and CONVOFLOW_SEED_KEY set one tenant's key at boot.
        if os.environ.get("CONVOFLOW_SEED_KEY"):
            issue_key(os.environ.get("CONVOFLOW_SEED_TENANT", "demo"), 100_000, "seed", os.environ["CONVOFLOW_SEED_KEY"])
            log("seed_key_applied", tenant_id=os.environ.get("CONVOFLOW_SEED_TENANT", "demo"))
    elif cmd == "every-hour":
        # For hosts without a scheduler (Railway). On AWS, EventBridge runs "hourly" instead.
        while True:
            hourly()
            time.sleep(3600)
    else:
        {"migrate": migrate, "hourly": hourly, "monitor": monitor}[cmd]()
