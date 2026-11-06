import json
import os
import signal
import subprocess
import sys
import time

import psycopg
import pytest

from api import measure
from convoflow import ADMIN_DSN, APP_DSN, add_tenants, enqueue, migrate, process_batch, purge, reset
from api import hash_key
from jobs import issue_key, publish


@pytest.fixture
def db():
    reset()
    add_tenants({"a": 1000})
    with psycopg.connect(APP_DSN) as conn:
        yield conn


def event(conv="c1"):
    return json.dumps({"tenant_id": "a", "conversation_id": conv, "channel": "chat", "text": "hello"})


def test_migrate_twice_keeps_existing_data(db):
    enqueue(db, [event()])
    process_batch(db, "w")
    migrate()
    with psycopg.connect(ADMIN_DSN) as conn:
        assert conn.execute("SELECT count(*) FROM messages").fetchone() == (1,)
        assert conn.execute("SELECT count(*) FROM schema_migrations").fetchone() == (2,)


def test_retention_deletes_only_expired_data(db):
    enqueue(db, [event("old"), event("new")])
    process_batch(db, "w")
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        conn.execute("UPDATE messages SET ingested_at = now() - interval '400 days' WHERE conversation_id = 'old'")
    assert purge()[0] == 1
    with psycopg.connect(ADMIN_DSN) as conn:
        assert conn.execute("SELECT conversation_id FROM messages").fetchall() == [("new",)]


def test_worker_finishes_cleanly_on_sigterm(db):
    enqueue(db, [event()])
    code = "import convoflow; convoflow.run_worker()"
    worker = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
    for _ in range(50):
        with psycopg.connect(ADMIN_DSN) as conn:
            if conn.execute("SELECT count(*) FROM messages").fetchone()[0]:
                break
        time.sleep(0.1)
    worker.send_signal(signal.SIGTERM)
    out = worker.communicate(timeout=10)[0]
    assert worker.returncode == 0 and "worker_stopped" in out


def test_metrics_reach_cloudwatch_client(db):
    class Client:
        def put_metric_data(self, Namespace, MetricData):
            self.sent = (Namespace, {m["MetricName"] for m in MetricData})

    client = Client()
    publish(client, measure(db))
    assert client.sent == ("Convoflow", {"QueueDepth", "QueueLagSeconds", "DeadLetterRatio5m", "ScanOverdueSeconds"})


def test_issuing_a_new_key_replaces_the_old_one_and_stores_only_a_hash(db):
    first = issue_key("new-tenant")
    second = issue_key("new-tenant")
    with psycopg.connect(ADMIN_DSN) as conn:
        stored = conn.execute("SELECT api_key_hash FROM tenants WHERE id = 'new-tenant'").fetchone()[0]
    assert stored == hash_key(second) != hash_key(first)
    assert second not in stored
