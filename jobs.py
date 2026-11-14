"""One-off and scheduled tasks.

    python jobs.py migrate | hourly | monitor | every-hour | seed-key
    python jobs.py issue-key TENANT [DAILY_LIMIT] [LABEL]
    python jobs.py list-keys TENANT
    python jobs.py revoke-key KEY_ID
    python jobs.py set-webhook TENANT twilio|zendesk|mailgun SECRET
    python jobs.py set-alert TENANT HTTPS_URL SECRET [low|medium|high|critical]
    python jobs.py rotate-secrets
    python jobs.py backup [DIR] | restore DIR
    python jobs.py delete-tenant TENANT --confirm
"""
import json
import os
import secrets
import sys
import time
import urllib.request

import psycopg

import alerts
import llm_rules
import backup as backups
import secretbox
from api import firing, hash_key, measure
from convoflow import ADMIN_DSN, APP_DSN, log, migrate, purge, scan

BACKUP_DIR = os.environ.get("CONVOFLOW_BACKUP_DIR", "")


def audit(db, action, tenant_id=None, actor="operator", **detail):
    db.execute("INSERT INTO audit_log (actor, tenant_id, action, detail) VALUES (%s, %s, %s, %s)",
               (actor, tenant_id, action, json.dumps(detail, default=str)))

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


OPS_WEBHOOK = os.environ.get("CONVOFLOW_OPS_WEBHOOK", "")


def ops_transitions(previous, current):
    """Alerts that started and stopped firing since the last check."""
    return sorted(set(current) - set(previous)), sorted(set(previous) - set(current))


def page_ops(started, resolved, metrics):
    """Posts to a Slack-compatible webhook, so a firing alert reaches a person."""
    if not OPS_WEBHOOK or not (started or resolved):
        return False
    lines = [f":rotating_light: convoflow alert firing: {name} = {metrics[name]:.2f}" for name in started]
    lines += [f":white_check_mark: convoflow alert resolved: {name}" for name in resolved]
    alerts.check_destination(OPS_WEBHOOK)
    req = urllib.request.Request(OPS_WEBHOOK, json.dumps({"text": "\n".join(lines)}).encode(),
                                 {"Content-Type": "application/json"}, method="POST")
    with alerts._opener.open(req, timeout=10) as res:
        return 200 <= res.status < 300


def scheduler(tick=60, hour=3600):
    """For hosts without a scheduler (Railway): checks alerts every minute and runs hourly() each hour.
    On AWS, CloudWatch alarms page through SNS and EventBridge runs hourly instead."""
    firing_before, last_hourly = [], 0.0
    while True:
        try:
            if time.monotonic() - last_hourly >= hour:
                hourly()
                last_hourly = time.monotonic()
            with psycopg.connect(APP_DSN) as db:
                metrics = measure(db)
        except psycopg.Error as err:
            # Keep the loop alive: a missing migration or a database restart must not stop scans for good.
            log("scheduler_error", error=f"{type(err).__name__}: {err}")
            time.sleep(tick)
            continue
        now_firing = firing(metrics)
        started, resolved = ops_transitions(firing_before, now_firing)
        try:
            if page_ops(started, resolved, metrics):
                log("ops_paged", started=started, resolved=resolved)
        except (OSError, alerts.BadDestination) as err:
            log("ops_page_failed", error=str(err))
        else:
            firing_before = now_firing
        time.sleep(tick)


def hourly():
    scanned, found = scan()
    log("scan_finished", scanned=scanned, findings=found)
    llm_rules.scan()
    alerts.notify()
    purge()
    if BACKUP_DIR:
        last = backups.latest(BACKUP_DIR)
        if not last or time.time() - os.path.getmtime(last) > 23 * 3600:
            backups.backup(BACKUP_DIR)


def issue_key(tenant_id, daily_limit=1_000_000, label="", key=None):
    """Creates the tenant if needed and adds an API key. Existing keys keep working
    until revoked, so a key can be rotated without downtime. Only the hash is stored."""
    key = key or secrets.token_urlsafe(32)
    with psycopg.connect(ADMIN_DSN) as db:
        db.execute("INSERT INTO tenants (id, daily_limit) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
                   (tenant_id, daily_limit))
        added = db.execute("INSERT INTO tenant_keys (key_hash, tenant_id, label) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                           (hash_key(key), tenant_id, label)).rowcount
        if added:
            audit(db, "key_issued", tenant_id, key_id=key_id(hash_key(key)), label=label)
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
        revoked = db.execute(
            """UPDATE tenant_keys SET revoked_at = clock_timestamp() WHERE left(key_hash, 12) = %s AND revoked_at IS NULL
               RETURNING tenant_id""",
            (kid,),
        ).fetchall()
        n = len(revoked)
        for (tenant_id,) in revoked:
            audit(db, "key_revoked", tenant_id, key_id=kid)
    log("key_revoked", key_id=kid, count=n)
    return n


def set_webhook(tenant_id, provider, secret):
    with psycopg.connect(ADMIN_DSN) as db:
        db.execute(
            """INSERT INTO tenant_webhooks (tenant_id, provider, secret) VALUES (%s, %s, %s)
               ON CONFLICT (tenant_id, provider) DO UPDATE SET secret = EXCLUDED.secret""",
            (tenant_id, provider, secretbox.seal(secret)),
        )
        audit(db, "webhook_secret_set", tenant_id, provider=provider)


def set_alert(tenant_id, url, secret, min_severity="critical"):
    alerts.check_destination(url)
    with psycopg.connect(ADMIN_DSN) as db:
        db.execute(
            """INSERT INTO tenant_alerts (tenant_id, url, secret, min_severity) VALUES (%s, %s, %s, %s)
               ON CONFLICT (tenant_id) DO UPDATE SET url = EXCLUDED.url, secret = EXCLUDED.secret,
                 min_severity = EXCLUDED.min_severity""",
            (tenant_id, url, secretbox.seal(secret), min_severity),
        )
        audit(db, "alert_destination_set", tenant_id, host=alerts.urlparse(url).hostname, min_severity=min_severity)


def rotate_secrets():
    """Re-encrypts every stored secret with the newest key, and encrypts any plain ones."""
    n = 0
    with psycopg.connect(ADMIN_DSN) as db:
        for table, keys in (("tenant_webhooks", ("tenant_id", "provider")), ("tenant_alerts", ("tenant_id",))):
            for row in db.execute(f"SELECT {', '.join(keys)}, secret FROM {table}").fetchall():
                *ids, stored = row
                db.execute(f"UPDATE {table} SET secret = %s WHERE {' AND '.join(k + ' = %s' for k in keys)}",
                           (secretbox.reseal(stored), *ids))
                n += 1
        audit(db, "secrets_rotated", count=n)
    return n


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
        audit(db, "tenant_deleted", tenant_id, **counts)
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
    elif cmd == "set-alert":
        set_alert(*args[:4])
        print("saved")
    elif cmd == "rotate-secrets":
        print(rotate_secrets(), "secrets re-encrypted")
    elif cmd == "backup":
        print(backups.backup(args[0] if args else BACKUP_DIR))
    elif cmd == "restore":
        print(backups.restore(args[0]))
    elif cmd == "backfill-mask":
        print(backfill_mask(), "rows masked")
    elif cmd == "delete-tenant":
        if "--confirm" not in args:
            raise SystemExit("this erases the tenant's data; add --confirm")
        print(delete_tenant(args[0]))
    elif cmd == "seed-key":
        # For hosts without a shell: CONVOFLOW_SEED_TENANT and CONVOFLOW_SEED_KEY set one tenant's key at boot.
        if os.environ.get("CONVOFLOW_SEED_KEY"):
            tenant = os.environ.get("CONVOFLOW_SEED_TENANT", "demo")
            limit = int(os.environ.get("CONVOFLOW_SEED_LIMIT", "100000"))
            issue_key(tenant, limit, "seed", os.environ["CONVOFLOW_SEED_KEY"])
            with psycopg.connect(ADMIN_DSN) as db:
                db.execute("UPDATE tenants SET daily_limit = %s WHERE id = %s", (limit, tenant))
            log("seed_key_applied", tenant_id=os.environ.get("CONVOFLOW_SEED_TENANT", "demo"))
    elif cmd in ("every-hour", "scheduler"):
        scheduler()
    else:
        {"migrate": migrate, "hourly": hourly, "monitor": monitor}[cmd]()
