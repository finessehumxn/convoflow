import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import psycopg
import pytest

from adapters import BadPayload, normalize
from api import Handler, firing, hash_key, measure
from convoflow import ADMIN_DSN, APP_DSN, add_tenants, process_batch, reset, scan


@pytest.fixture
def base():
    reset()
    add_tenants({"a": 1000})
    with psycopg.connect(ADMIN_DSN, autocommit=True) as db:
        db.execute("UPDATE tenants SET api_key_hash = %s WHERE id = 'a'", (hash_key("secret"),))
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def post(url, body, key="secret"):
    req = urllib.request.Request(url, json.dumps(body).encode(), {"X-Api-Key": key})
    try:
        with urllib.request.urlopen(req) as res:
            return res.status, json.load(res)
    except urllib.error.HTTPError as err:
        return err.code, json.load(err)


def test_voice_call_becomes_one_event_per_segment():
    events, trace = normalize("voice", "a", {"call_id": "c", "segments": [
        {"speaker": "agent", "text": "hi"}, {"speaker": "customer", "text": "hello"}]})
    assert [e["text"] for e in events] == ["agent: hi", "customer: hello"]
    assert {e["trace_id"] for e in events} == {trace}


def test_bad_payload_is_rejected():
    with pytest.raises(BadPayload):
        normalize("sms", "a", {"body": "no sid"})


def test_bad_key_is_refused(base):
    assert post(f"{base}/v1/events/sms", {"sid": "1", "body": "x"}, key="wrong")[0] == 401


def test_event_flows_from_http_to_finding_with_its_trace(base):
    status, body = post(f"{base}/v1/events/sms", {"sid": "s1", "body": "Your SSN is 123-45-6789"})
    assert status == 202
    with psycopg.connect(APP_DSN) as db:
        process_batch(db, "w")
    scan(lag_seconds=0)
    with psycopg.connect(ADMIN_DSN) as db:
        row = db.execute("""SELECT f.rule, m.trace_id FROM findings f
                            JOIN messages m USING (tenant_id, event_key)""").fetchone()
    assert row == ("ssn_exposed", body["trace_id"])


def test_stale_queue_fires_an_alert(base):
    with psycopg.connect(ADMIN_DSN, autocommit=True) as db:
        db.execute("INSERT INTO inbox (payload, enqueued_at) VALUES ('{}', now() - interval '5 minutes')")
    with psycopg.connect(APP_DSN) as db:
        assert firing(measure(db)) == ["queue_lag_seconds"]
    with pytest.raises(urllib.error.HTTPError) as err:
        urllib.request.urlopen(f"{base}/alerts")
    assert err.value.code == 503


def test_health_stays_up_when_the_queue_is_behind(base):
    with psycopg.connect(ADMIN_DSN, autocommit=True) as db:
        db.execute("INSERT INTO inbox (payload, enqueued_at) VALUES ('{}', now() - interval '5 minutes')")
    assert urllib.request.urlopen(f"{base}/health").status == 200


def test_api_role_cannot_read_other_tenants_messages(base):
    with psycopg.connect(APP_DSN) as db:
        assert db.execute("SELECT count(*) FROM messages").fetchone() == (0,)
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            db.execute("SELECT * FROM findings")
