import json

import psycopg
import pytest

from convoflow import ADMIN_DSN, APP_DSN, add_tenants, enqueue, process_batch, reset, scan


def event(tenant="a", conv="c1", text="hello", channel="chat"):
    return json.dumps({"tenant_id": tenant, "conversation_id": conv, "channel": channel, "text": text})


@pytest.fixture
def db():
    reset()
    add_tenants({"a": 1000, "b": 1000, "capped": 2})
    with psycopg.connect(APP_DSN) as conn:
        yield conn


def admin(sql):
    with psycopg.connect(ADMIN_DSN) as conn:
        return conn.execute(sql).fetchall()


def test_replay_stores_nothing_new(db):
    enqueue(db, [event(), event()])
    process_batch(db, "w")
    assert admin("SELECT count(*) FROM messages") == [(1,)]
    assert admin("SELECT stored, duplicates FROM worker_counters") == [(1, 1)]


def test_tenant_cannot_read_another_tenants_messages(db):
    enqueue(db, [event("a"), event("b")])
    process_batch(db, "w")
    db.execute("SELECT set_config('app.tenant_id', 'a', true)")
    assert db.execute("SELECT DISTINCT tenant_id FROM messages").fetchall() == [("a",)]
    db.rollback()


def test_no_tenant_context_sees_nothing(db):
    enqueue(db, [event("a")])
    process_batch(db, "w")
    assert db.execute("SELECT count(*) FROM messages").fetchone() == (0,)
    db.rollback()


def test_tenant_cannot_write_as_another_tenant(db):
    db.execute("SELECT set_config('app.tenant_id', 'a', true)")
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        db.execute("INSERT INTO messages (tenant_id, event_key, conversation_id, channel, text, enqueued_at) "
                   "VALUES ('b', 'k', 'c', 'chat', 't', now())")
    db.rollback()


def test_crash_before_commit_returns_batch_to_queue(db):
    enqueue(db, [event()])

    def crash():
        raise RuntimeError("crash")

    with pytest.raises(RuntimeError):
        process_batch(db, "w", before_commit=crash)
    db.rollback()
    assert admin("SELECT count(*) FROM inbox") == [(1,)]
    assert admin("SELECT count(*) FROM messages") == [(0,)]
    process_batch(db, "w")
    assert admin("SELECT count(*) FROM messages") == [(1,)]


def test_bad_rows_go_to_dead_letters_and_good_rows_still_store(db):
    enqueue(db, ["{not json", event(channel="fax"), event(tenant="nobody"), event()])
    process_batch(db, "w")
    assert sorted(r[0] for r in admin("SELECT reason FROM dead_letters")) == [
        "invalid_json", "unknown_channel", "unknown_tenant"]
    assert admin("SELECT count(*) FROM messages") == [(1,)]


def test_tenant_over_daily_limit_is_held_back_without_hurting_others(db):
    enqueue(db, [event("capped", f"c{i}") for i in range(5)] + [event("a")])
    process_batch(db, "w")
    assert admin("SELECT tenant_id, count(*) FROM messages GROUP BY 1 ORDER BY 1") == [("a", 1), ("capped", 2)]
    assert admin("SELECT reason, count(*) FROM dead_letters GROUP BY 1") == [("over_quota", 3)]


def test_scanner_finds_violations_with_evidence_once(db):
    enqueue(db, [event(text="Your card 4111 1111 1111 1111 is on file."), event(conv="c2", text="Fine.")])
    process_batch(db, "w")
    assert scan(lag_seconds=0) == (2, 1)
    assert admin("SELECT rule, severity, evidence FROM findings") == [
        ("card_number_exposed", "critical", "4111 1111 1111 1111")]
    assert scan(lag_seconds=0)[1] == 0


def test_scanner_covers_new_messages_only(db):
    enqueue(db, [event()])
    process_batch(db, "w")
    scan(lag_seconds=0)
    enqueue(db, [event(conv="c2", text="SSN 123-45-6789")])
    process_batch(db, "w")
    assert scan(lag_seconds=0) == (1, 1)
