import json
import re

import psycopg

from convoflow import ADMIN_DSN, APP_DSN, add_tenants, enqueue, process_batch, reset, scan
from pii import CARD_TOKEN, SSN_TOKEN, luhn_valid, mask


def test_valid_card_is_masked_with_last_four_and_fingerprint():
    out = mask("card 4111 1111 1111 1111 thanks")
    assert "4111 1111" not in out
    assert re.search(CARD_TOKEN, out) and "****1111" in out


def test_number_that_fails_luhn_is_left_alone():
    assert not luhn_valid("1234567812345678")
    assert mask("order 1234 5678 1234 5678") == "order 1234 5678 1234 5678"


def test_ssn_is_masked_but_never_issued_numbers_are_not():
    assert re.search(SSN_TOKEN, mask("SSN 123-45-6789"))
    for fake in ("000-12-3456", "666-12-3456", "912-34-5678", "123-00-4567", "123-45-0000"):
        assert mask(fake) == fake


def test_same_value_gets_same_fingerprint_and_masking_twice_changes_nothing():
    a, b = mask("4111-1111-1111-1111"), mask("4111111111111111")
    assert a == b and mask(a) == a


def test_raw_values_never_reach_any_table():
    reset()
    add_tenants({"a": 1000, "capped": 0})
    secret_card, secret_ssn = "4111 1111 1111 1111", "123-45-6789"
    with psycopg.connect(APP_DSN) as db:
        enqueue(db, [
            json.dumps({"tenant_id": "a", "conversation_id": "c1", "channel": "chat", "text": f"card {secret_card}"}),
            json.dumps({"tenant_id": "capped", "conversation_id": "c2", "channel": "sms", "text": f"ssn {secret_ssn}"}),
        ])
        with psycopg.connect(ADMIN_DSN) as admin:
            assert secret_card not in str(admin.execute("SELECT payload FROM inbox").fetchall())
        process_batch(db, "w")
    assert scan(lag_seconds=0) == (1, 1)
    with psycopg.connect(ADMIN_DSN) as admin:
        dump = str(admin.execute("SELECT text FROM messages").fetchall())
        dump += str(admin.execute("SELECT payload FROM dead_letters").fetchall())
        dump += str(admin.execute("SELECT evidence FROM findings").fetchall())
        assert secret_card not in dump and secret_ssn not in dump
        assert "****1111" in dump and "***-**-6789" in dump


def test_backfill_masks_rows_stored_before_masking_and_runs_once():
    from jobs import backfill_mask
    reset()
    add_tenants({"a": 1000})
    with psycopg.connect(ADMIN_DSN, autocommit=True) as admin:
        admin.execute("""INSERT INTO messages (tenant_id, event_key, conversation_id, channel, text, enqueued_at)
                         VALUES ('a', 'k1', 'c', 'chat', 'old 4111 1111 1111 1111', now())""")
        admin.execute("INSERT INTO dead_letters (payload, reason) VALUES ('ssn 123-45-6789', 'x')")
        admin.execute("""INSERT INTO findings (tenant_id, event_key, rule, severity, evidence)
                         VALUES ('a', 'k1', 'card_number_exposed', 'critical', '4111 1111 1111 1111')""")
    assert backfill_mask() == 3
    assert backfill_mask() == 0
    with psycopg.connect(ADMIN_DSN) as admin:
        dump = str(admin.execute("SELECT text FROM messages").fetchall())
        dump += str(admin.execute("SELECT payload FROM dead_letters").fetchall())
        dump += str(admin.execute("SELECT evidence FROM findings").fetchall())
    assert "4111 1111" not in dump and "123-45-6789" not in dump
