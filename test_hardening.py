import json
import threading
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from urllib.parse import urlencode

import psycopg
import pytest

import api
import tracing
import webhooks
from convoflow import ADMIN_DSN, APP_DSN, add_tenants, enqueue, process_batch, reset
from jobs import issue_key, set_webhook


@pytest.fixture
def base():
    reset()
    add_tenants({"a": 1000})
    issue_key("a", key="secret")
    set_webhook("a", "twilio", "twilio-token")
    set_webhook("a", "zendesk", "zendesk-secret")
    api._queue.update(depth=0, checked=0.0)
    server = ThreadingHTTPServer(("127.0.0.1", 0), api.Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def call(url, body=b"", headers=None):
    req = urllib.request.Request(url, body, headers or {}, method="POST")
    try:
        with urllib.request.urlopen(req) as res:
            return res.status, res.read().decode(), dict(res.headers)
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode(), dict(err.headers)


def stored_texts():
    with psycopg.connect(APP_DSN) as db:
        process_batch(db, "w")
    with psycopg.connect(ADMIN_DSN) as db:
        return [r[0] for r in db.execute("SELECT text FROM messages")]


def twilio_post(base, fields, token="twilio-token"):
    url = f"{base}/v1/webhooks/twilio/a"
    body = urlencode(fields).encode()
    sig = webhooks.twilio_signature(token, url, sorted(fields.items()))
    return call(url, body, {"X-Twilio-Signature": sig, "Content-Type": "application/x-www-form-urlencoded"})


def test_signed_twilio_sms_is_stored_masked_and_answered_with_twiml(base):
    status, body, headers = twilio_post(base, {"MessageSid": "SM1", "From": "+15550001111", "Body": "card 4111 1111 1111 1111"})
    assert status == 200 and body == "<Response/>" and "xml" in headers["Content-Type"]
    [text] = stored_texts()
    assert text.startswith("card [card ****1111 fp:") and "4111 1111" not in text


def test_twilio_with_wrong_token_or_tampered_body_is_refused(base):
    assert twilio_post(base, {"MessageSid": "SM1", "Body": "hi"}, token="wrong")[0] == 401
    url = f"{base}/v1/webhooks/twilio/a"
    sig = webhooks.twilio_signature("twilio-token", url, [("Body", "hi"), ("MessageSid", "SM1")])
    tampered = urlencode({"MessageSid": "SM1", "Body": "changed"}).encode()
    assert call(url, tampered, {"X-Twilio-Signature": sig})[0] == 401
    assert stored_texts() == []


def test_unknown_tenant_looks_the_same_as_unconfigured(base):
    assert twilio_post(base.replace("/a", "/a"), {"MessageSid": "S", "Body": "x"})[0] == 200
    assert call(f"{base}/v1/webhooks/twilio/nobody", b"x")[0] == 404


def zendesk_post(base, payload, secret="zendesk-secret", when=None):
    body = json.dumps(payload).encode()
    ts = (when or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")
    sig = webhooks.zendesk_signature(secret, ts, body)
    return call(f"{base}/v1/webhooks/zendesk/a", body, {
        "X-Zendesk-Webhook-Signature": sig, "X-Zendesk-Webhook-Signature-Timestamp": ts})


def test_signed_zendesk_comment_is_stored(base):
    status, body, _ = zendesk_post(base, {"ticket": {"id": 42}, "comment": {"body": "Thanks!"}})
    assert status == 202 and json.loads(body)["accepted"] == 1
    assert stored_texts() == ["Thanks!"]


def test_zendesk_bad_signature_and_old_timestamp_are_refused(base):
    payload = {"ticket": {"id": 42}, "comment": {"body": "x"}}
    assert zendesk_post(base, payload, secret="wrong")[0] == 401
    assert zendesk_post(base, payload, when=datetime.now(timezone.utc) - timedelta(minutes=10))[0] == 401


def test_full_queue_gets_429_with_retry_after(base, monkeypatch):
    monkeypatch.setattr(api, "MAX_QUEUE", 2)
    with psycopg.connect(APP_DSN) as db:
        enqueue(db, [json.dumps({"tenant_id": "a", "conversation_id": f"c{i}", "channel": "chat", "text": "x"}) for i in range(2)])
    status, body, headers = call(f"{base}/v1/events/sms", json.dumps({"sid": "s", "body": "x"}).encode(), {"X-Api-Key": "secret"})
    assert status == 429 and headers["Retry-After"] == "5"


def test_requests_over_the_in_flight_cap_get_503(base, monkeypatch):
    monkeypatch.setattr(api, "_in_flight", threading.BoundedSemaphore(1))
    api._in_flight.acquire()
    try:
        status, _, headers = call(f"{base}/v1/events/sms", b"{}", {"X-Api-Key": "secret"})
    finally:
        api._in_flight.release()
    assert status == 503 and headers["Retry-After"] == "1"


def test_metrics_show_each_tenants_backlog(base):
    with psycopg.connect(APP_DSN) as db:
        enqueue(db, [json.dumps({"tenant_id": "a", "conversation_id": "c", "channel": "chat", "text": "x"})])
    with urllib.request.urlopen(base + "/metrics") as res:
        text = res.read().decode()
    assert 'convoflow_tenant_queued{tenant="a"} 1' in text and "convoflow_inbox_dead_tuples" in text


def test_demo_rate_limit_is_shared_through_the_database(base, monkeypatch):
    monkeypatch.setattr(api, "DEMO_TENANT", "a")
    with psycopg.connect(APP_DSN) as db:
        assert all(api.rate_allowed(db, "demo:1.2.3.4", 10) for _ in range(10))
    with psycopg.connect(APP_DSN) as other_instance:
        assert not api.rate_allowed(other_instance, "demo:1.2.3.4", 10)


def test_a_message_can_be_followed_from_request_to_worker_batch(base):
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    exporter = InMemorySpanExporter()
    tracing.configure(exporter)
    try:
        status, body, _ = call(f"{base}/v1/events/sms", json.dumps({"sid": "s", "body": "hi"}).encode(), {"X-Api-Key": "secret"})
        trace_id = json.loads(body)["trace_id"]
        with psycopg.connect(APP_DSN) as db:
            process_batch(db, "w")
    finally:
        tracing.disable()
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert f"{spans['ingest'].context.trace_id:032x}" == trace_id
    assert [f"{l.context.trace_id:032x}" for l in spans["store_batch"].links] == [trace_id]
    assert spans["store_batch"].links[0].context.span_id == spans["ingest"].context.span_id
