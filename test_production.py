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
from jobs import delete_tenant, issue_key, key_id, list_keys, publish, revoke_key


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
        assert conn.execute("SELECT count(*) FROM schema_migrations").fetchone() == (4,)


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


def test_keys_rotate_without_downtime_and_store_only_a_hash(db):
    first = issue_key("new-tenant", label="old")
    second = issue_key("new-tenant", label="new")
    with psycopg.connect(ADMIN_DSN) as conn:
        active = lambda: {r[0] for r in conn.execute(
            "SELECT key_hash FROM tenant_keys WHERE tenant_id = 'new-tenant' AND revoked_at IS NULL")}
        assert active() == {hash_key(first), hash_key(second)}
        conn.commit()
        assert revoke_key(key_id(hash_key(first))) == 1
        assert active() == {hash_key(second)}
        dump = str(conn.execute("SELECT * FROM tenant_keys").fetchall())
    assert first not in dump and second not in dump
    assert [k["label"] for k in list_keys("new-tenant")] == ["old", "new"]


def test_delete_tenant_erases_its_data_everywhere_and_leaves_others(db):
    add_tenants({"b": 1000})
    issue_key("a")
    enqueue(db, [event("c1"), json.dumps({"tenant_id": "b", "conversation_id": "x", "channel": "chat", "text": "hi"})])
    process_batch(db, "w")
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        conn.execute("INSERT INTO dead_letters (payload, reason) VALUES (%s, 'test'), ('{not json', 'test')",
                     (json.dumps({"tenant_id": "a", "text": "x"}),))
    counts = delete_tenant("a")
    assert counts == {"messages": 1, "findings": 0, "dead_letters": 1, "queued": 0}
    with psycopg.connect(ADMIN_DSN) as conn:
        q = lambda sql: conn.execute(sql).fetchall()
        assert q("SELECT DISTINCT tenant_id FROM messages") == [("b",)]
        assert q("SELECT count(*) FROM tenant_keys WHERE tenant_id = 'a'") == [(0,)]
        assert q("SELECT count(*) FROM dead_letters") == [(1,)]
        assert q("SELECT tenant_id, messages FROM tenant_deletions") == [("a", 1)]