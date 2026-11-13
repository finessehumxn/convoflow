import json

import psycopg
import pytest

import convoflow
from convoflow import ADMIN_DSN, APP_DSN, add_tenants, enqueue, process_batch, replay, reset, scan


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


def test_quota_resets_each_utc_day_and_yesterday_does_not_count(db):
    with psycopg.connect(ADMIN_DSN) as conn:
        conn.execute("""INSERT INTO tenant_usage (tenant_id, day, stored)
                        VALUES ('capped', (now() AT TIME ZONE 'UTC')::date - 1, 2)""")
    enqueue(db, [event("capped", f"c{i}") for i in range(5)])
    process_batch(db, "w")
    assert admin("SELECT count(*) FROM messages WHERE tenant_id = 'capped'") == [(2,)]
    assert admin("""SELECT stored FROM tenant_usage WHERE tenant_id = 'capped'
                    AND day = (now() AT TIME ZONE 'UTC')::date""") == [(2,)]


def test_quota_day_comes_from_the_database_not_the_worker_clock(db, monkeypatch):
    import os, time as _time
    monkeypatch.setenv("TZ", "Pacific/Kiritimati")
    _time.tzset()
    try:
        enqueue(db, [event("capped", f"c{i}") for i in range(3)])
        process_batch(db, "w")
    finally:
        monkeypatch.delenv("TZ")
        _time.tzset()
    assert admin("""SELECT count(*) FROM tenant_usage WHERE tenant_id = 'capped'
                    AND day <> (now() AT TIME ZONE 'UTC')::date""") == [(0,)]


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


def test_tenant_only_gets_channels_it_pays_for(db):
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        conn.execute("UPDATE tenants SET channels = '{chat}' WHERE id = 'a'")
    enqueue(db, [event(channel="chat"), event(conv="c2", channel="voice")])
    process_batch(db, "w")
    assert admin("SELECT channel FROM messages") == [("chat",)]
    assert admin("SELECT reason FROM dead_letters") == [("channel_not_entitled",)]


def test_quiet_tenant_is_served_ahead_of_a_backlog(db):
    enqueue(db, [event("a", f"c{i}") for i in range(1000)])
    enqueue(db, [event("b", "late")])
    process_batch(db, "w")
    assert admin("SELECT count(*) FROM messages WHERE tenant_id = 'b'") == [(1,)]


def test_held_back_rows_keep_their_full_payload_and_can_be_replayed(db):
    enqueue(db, [event("capped", f"c{i}") for i in range(5)])
    process_batch(db, "w")
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        conn.execute("UPDATE tenants SET daily_limit = 100 WHERE id = 'capped'")
    assert replay("over_quota") == 3
    process_batch(db, "w")
    assert admin("SELECT count(*) FROM messages WHERE tenant_id = 'capped'") == [(5,)]


def test_a_dead_letter_is_only_replayed_once(db):
    enqueue(db, [event("capped", f"c{i}") for i in range(3)])
    process_batch(db, "w")
    assert replay("over_quota") == 1
    assert replay("over_quota") == 0


def test_replaying_an_already_stored_event_creates_no_duplicate(db):
    enqueue(db, [event()])
    process_batch(db, "w")
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        conn.execute("INSERT INTO dead_letters (payload, reason) VALUES (%s, 'manual')", (event(),))
    replay("manual")
    process_batch(db, "w")
    assert admin("SELECT count(*) FROM messages") == [(1,)]


def test_repair_fixes_a_bad_row_and_skips_what_it_cannot_fix(db):
    enqueue(db, [event(channel="fax"), "{not json"])
    process_batch(db, "w")
    fixed = replay("unknown_channel", repair=lambda p: p.replace("fax", "sms"))
    skipped = replay("invalid_json", repair=lambda p: None)
    process_batch(db, "w")
    assert (fixed, skipped) == (1, 0)
    assert admin("SELECT channel FROM messages") == [("sms",)]
    assert admin("SELECT count(*) FROM dead_letters WHERE replayed_at IS NULL") == [(1,)]


POISON = '{"tenant_id": "a", "conversation_id": "bad", "channel": "chat", "text": "nul \\u0000 byte"}'


def drain(db):
    for _ in range(50):
        process_batch(db, "w")
        if admin("SELECT count(*) FROM inbox") == [(0,)]:
            return


def test_poison_row_cannot_block_the_rows_around_it(db, monkeypatch):
    monkeypatch.setattr(convoflow, "RETRY_BASE_SECONDS", 0)
    enqueue(db, [event(conv="c1"), POISON, event(conv="c2")])
    drain(db)
    assert admin("SELECT conversation_id FROM messages ORDER BY 1") == [("c1",), ("c2",)]
    assert admin("SELECT reason FROM dead_letters") == [("max_attempts: DataError",)]


def test_every_item_is_counted_after_a_poison_row(db, monkeypatch):
    monkeypatch.setattr(convoflow, "RETRY_BASE_SECONDS", 0)
    enqueue(db, [event(conv="c1"), POISON])
    drain(db)
    assert admin("SELECT sum(stored), sum(dead) FROM worker_counters") == [(1, 1)]


def test_failed_rows_wait_for_their_backoff(db, monkeypatch):
    monkeypatch.setattr(convoflow, "RETRY_BASE_SECONDS", 3600)
    enqueue(db, [POISON])
    process_batch(db, "w")
    assert process_batch(db, "w") == 0
    assert admin("SELECT attempts FROM inbox") == [(1,)]
