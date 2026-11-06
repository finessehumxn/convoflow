"""One-off and scheduled tasks: python jobs.py migrate | hourly | monitor | issue-key TENANT [DAILY_LIMIT]"""
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


def issue_key(tenant_id, daily_limit=1_000_000):
    """Creates the tenant if needed and replaces its API key. Only the hash is stored."""
    key = secrets.token_urlsafe(32)
    with psycopg.connect(ADMIN_DSN) as db:
        db.execute(
            """INSERT INTO tenants (id, daily_limit, api_key_hash) VALUES (%s, %s, %s)
               ON CONFLICT (id) DO UPDATE SET api_key_hash = EXCLUDED.api_key_hash""",
            (tenant_id, daily_limit, hash_key(key)),
        )
    return key


if __name__ == "__main__":
    if sys.argv[1] == "issue-key":
        print(issue_key(sys.argv[2], *map(int, sys.argv[3:4])))
    else:
        {"migrate": migrate, "hourly": hourly, "monitor": monitor}[sys.argv[1]]()
