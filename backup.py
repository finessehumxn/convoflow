"""Logical backups with COPY, and a restore that checks every row count.

A backup is a folder holding one gzipped CSV per table plus manifest.json with
row counts and the migrations it was taken at. Restore needs an empty database
migrated to the same version, loads every table in one transaction, resets the
id sequences, and refuses to commit if any count differs from the manifest.

This is a logical backup for a small deployment, not point-in-time recovery.
On AWS, RDS automated backups and snapshots are the primary copy.
"""
import gzip
import json
import os
import shutil
from datetime import datetime, timezone

import psycopg

from convoflow import ADMIN_DSN, log

# Parents before children, so foreign keys hold during restore.
TABLES = ["tenants", "tenant_keys", "tenant_webhooks", "tenant_alerts", "tenant_usage", "messages", "findings",
          "dead_letters", "scan_runs", "worker_counters", "inbox", "audit_log", "tenant_deletions"]
IDENTITY = {"dead_letters": "id", "scan_runs": "id", "inbox": "id", "audit_log": "id"}
KEEP = 7


def backup(root):
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    folder = os.path.join(root, f"convoflow-{stamp}")
    os.makedirs(folder)
    counts = {}
    with psycopg.connect(ADMIN_DSN) as db:
        db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")  # one consistent snapshot
        migrations = [r[0] for r in db.execute("SELECT name FROM schema_migrations ORDER BY name")]
        for table in TABLES:
            with gzip.open(os.path.join(folder, f"{table}.csv.gz"), "wb") as out, \
                    db.cursor().copy(f"COPY (SELECT * FROM {table}) TO STDOUT (FORMAT csv)") as copy:
                for chunk in copy:
                    out.write(chunk)
            counts[table] = db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    with open(os.path.join(folder, "manifest.json"), "w") as f:
        json.dump({"taken_at": stamp, "migrations": migrations, "counts": counts}, f, indent=2)
    _prune(root)
    log("backup_done", folder=folder, **{f"rows_{t}": n for t, n in counts.items() if n})
    return folder


def _prune(root):
    old = sorted(d for d in os.listdir(root) if d.startswith("convoflow-"))[:-KEEP]
    for d in old:
        shutil.rmtree(os.path.join(root, d))


def latest(root):
    found = sorted(d for d in os.listdir(root) if d.startswith("convoflow-")) if os.path.isdir(root) else []
    return os.path.join(root, found[-1]) if found else None


def restore(folder):
    with open(os.path.join(folder, "manifest.json")) as f:
        manifest = json.load(f)
    with psycopg.connect(ADMIN_DSN) as db:
        migrations = [r[0] for r in db.execute("SELECT name FROM schema_migrations ORDER BY name")]
        if migrations != manifest["migrations"]:
            raise RuntimeError(f"schema is at {migrations}, backup was taken at {manifest['migrations']}")
        for table in TABLES:
            if db.execute(f"SELECT EXISTS (SELECT 1 FROM {table})").fetchone()[0]:
                raise RuntimeError(f"{table} is not empty; restore only into a fresh database")
        for table in TABLES:
            with gzip.open(os.path.join(folder, f"{table}.csv.gz"), "rb") as src, \
                    db.cursor().copy(f"COPY {table} FROM STDIN (FORMAT csv)") as copy:
                while chunk := src.read(1 << 20):
                    copy.write(chunk)
        for table, column in IDENTITY.items():
            db.execute(f"SELECT setval(pg_get_serial_sequence('{table}', '{column}'), "
                       f"COALESCE((SELECT max({column}) FROM {table}), 0) + 1, false)")
        restored = {t: db.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in TABLES}
        if restored != manifest["counts"]:
            raise RuntimeError(f"row counts differ, nothing committed: {restored} vs {manifest['counts']}")
    log("restore_done", folder=folder)
    return restored
