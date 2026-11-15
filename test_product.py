import json
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlencode

import psycopg
import pytest
from cryptography.fernet import Fernet

import alerts
import api
import backup
import secretbox
import jobs
import webhooks
from convoflow import ADMIN_DSN, APP_DSN, add_tenants, enqueue, migrate, process_batch, reset, scan
from jobs import delete_tenant, issue_key, revoke_key, rotate_secrets, set_alert, set_webhook, key_id
from api import hash_key

CARD = "card 4111 1111 1111 1111"


def admin(sql, *args):
    with psycopg.connect(ADMIN_DSN) as db:
        return db.execute(sql, args).fetchall()


def store(tenant, *texts):
    with psycopg.connect(APP_DSN) as db:
        enqueue(db, [json.dumps({"tenant_id": tenant, "conversation_id": f"{tenant}-{i}", "channel": "chat", "text": t})
                     for i, t in enumerate(texts)])
        process_batch(db, "w")
    scan(lag_seconds=0)


@pytest.fixture
def base():
    reset()
    add_tenants({"a": 1000, "b": 1000})
    issue_key("a", key="key-a")
    issue_key("b", key="key-b")
    api._queue.update(depth=0, checked=0.0)
    server = ThreadingHTTPServer(("127.0.0.1", 0), api.Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def get(url, key):
    req = urllib.request.Request(url, headers={"X-Api-Key": key})
    try:
        with urllib.request.urlopen(req) as res:
            return res.status, json.load(res)
    except urllib.error.HTTPError as err:
        return err.code, json.load(err)


# Findings API

def test_findings_api_pages_through_only_the_callers_findings(base):
    store("a", *[f"{CARD} n{i}" for i in range(5)])
    store("b", CARD)
    status, page1 = get(f"{base}/v1/findings?limit=2", "key-a")
    assert status == 200 and len(page1["findings"]) == 2 and page1["next_cursor"]
    seen = page1["findings"]
    cursor = page1["next_cursor"]
    while cursor:
        _, page = get(f"{base}/v1/findings?limit=2&cursor={cursor}", "key-a")
        seen += page["findings"]
        cursor = page["next_cursor"]
    assert len(seen) == 5 and len({f["event_key"] for f in seen}) == 5
    assert all(f["evidence"].startswith("[card ****1111") for f in seen)
    assert len(get(f"{base}/v1/findings", "key-b")[1]["findings"]) == 1


def test_findings_api_filters_refuses_bad_keys_and_is_audited(base):
    store("a", CARD, "I am a real human")
    assert [f["rule"] for f in get(f"{base}/v1/findings?severity=high", "key-a")[1]["findings"]] == ["denied_being_ai"]
    assert get(f"{base}/v1/findings", "nope")[0] == 401
    assert get(f"{base}/v1/findings?cursor=%%%", "key-a")[0] == 400
    assert admin("SELECT actor, action FROM audit_log WHERE action = 'findings_read'") == [
        (f"key:{hash_key('key-a')[:12]}", "findings_read")]


def test_app_role_cannot_read_another_tenants_findings_even_without_a_filter(base):
    store("a", CARD)
    store("b", CARD)
    with psycopg.connect(APP_DSN) as db:
        db.execute("SELECT set_config('app.tenant_id', 'a', true)")
        assert db.execute("SELECT DISTINCT tenant_id FROM findings").fetchall() == [("a",)]


# Alerts

class Receiver:
    def __init__(self, status=200):
        self.got, self.status = [], status
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                outer.got.append((dict(self.headers), body))
                self.send_response(outer.status)
                self.end_headers()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/hook"


@pytest.fixture
def local_alerts(monkeypatch):
    monkeypatch.setattr(alerts, "ALLOW_PRIVATE", True)


def test_new_critical_findings_are_sent_signed_once(base, local_alerts):
    rx = Receiver()
    set_alert("a", rx.url, "hook-secret")
    store("a", CARD, "I am a real human")
    assert alerts.notify() == 1
    assert alerts.notify() == 0
    headers, body = rx.got[0]
    assert headers["X-Convoflow-Signature"] == alerts.sign("hook-secret", headers["X-Convoflow-Timestamp"], body)
    assert [f["rule"] for f in json.loads(body)["findings"]] == ["card_number_exposed"]


def test_failed_delivery_is_retried_next_run(base, local_alerts):
    rx = Receiver(status=500)
    set_alert("a", rx.url, "s", "high")
    store("a", CARD)
    assert alerts.notify() == 0
    rx.status = 200
    assert alerts.notify() == 1 and len(rx.got) == 2


def test_alert_urls_must_be_public_https():
    for bad in ("http://example.com/x", "https://127.0.0.1/x", "https://10.0.0.5/x", "https://169.254.169.254/latest"):
        with pytest.raises(alerts.BadDestination):
            alerts.check_destination(bad)


def test_alerts_do_not_follow_redirects(base, local_alerts):
    class R(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            self.send_response(307)
            self.send_header("Location", "http://169.254.169.254/latest/meta-data")
            self.end_headers()

    srv = ThreadingHTTPServer(("127.0.0.1", 0), R)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    set_alert("a", f"http://127.0.0.1:{srv.server_address[1]}/", "s")
    store("a", CARD)
    assert alerts.notify() == 0
    assert admin("SELECT count(*) FROM findings WHERE notified_at IS NOT NULL") == [(0,)]


# Secrets

def test_secrets_are_encrypted_at_rest_and_rotate(base, monkeypatch, local_alerts):
    set_webhook("a", "zendesk", "zendesk-plain-secret")
    set_alert("a", "http://127.0.0.1:1/x", "alert-plain-secret")
    dump = str(admin("SELECT secret FROM tenant_webhooks")) + str(admin("SELECT secret FROM tenant_alerts"))
    assert "plain-secret" not in dump and dump.count("enc:v1:") == 2
    old = secretbox._DEV_KEY
    new = Fernet.generate_key().decode()
    monkeypatch.setenv("CONVOFLOW_SECRETS_KEYS", f"{new},{old}")
    assert rotate_secrets() == 2
    monkeypatch.setenv("CONVOFLOW_SECRETS_KEYS", new)
    assert secretbox.open_(admin("SELECT secret FROM tenant_webhooks")[0][0]) == "zendesk-plain-secret"


def test_plain_secrets_from_before_encryption_still_work_and_get_encrypted(base):
    with psycopg.connect(ADMIN_DSN, autocommit=True) as db:
        db.execute("INSERT INTO tenant_webhooks VALUES ('a', 'zendesk', 'legacy')")
    assert secretbox.open_(admin("SELECT secret FROM tenant_webhooks")[0][0]) == "legacy"
    rotate_secrets()
    assert admin("SELECT secret FROM tenant_webhooks")[0][0].startswith("enc:v1:")


# Mailgun and Twilio voice

def multipart(fields):
    boundary = "----convoflowtest"
    parts = [f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n' for k, v in fields.items()]
    return ("".join(parts) + f"--{boundary}--\r\n").encode(), f"multipart/form-data; boundary={boundary}"


def mailgun_fields(key, ts=None, **extra):
    ts = str(int(ts or time.time()))
    return {"timestamp": ts, "token": "tok123", "signature": webhooks.mailgun_signature(key, ts, "tok123"),
            "Message-Id": "<m1@example.com>", "subject": "Refund", "body-plain": f"My {CARD}", **extra}


def post(url, body, headers):
    req = urllib.request.Request(url, body, headers, method="POST")
    try:
        with urllib.request.urlopen(req) as res:
            return res.status
    except urllib.error.HTTPError as err:
        return err.code


def test_signed_mailgun_email_is_stored_masked(base):
    set_webhook("a", "mailgun", "mg-key")
    body, ctype = multipart(mailgun_fields("mg-key"))
    assert post(f"{base}/v1/webhooks/mailgun/a", body, {"Content-Type": ctype}) == 202
    with psycopg.connect(APP_DSN) as db:
        process_batch(db, "w")
    [(text, channel)] = admin("SELECT text, channel FROM messages")
    assert channel == "email" and text.startswith("Refund\nMy card [card ****1111")


def test_mailgun_bad_signature_or_old_timestamp_is_refused(base):
    set_webhook("a", "mailgun", "mg-key")
    for fields in (mailgun_fields("wrong-key"), mailgun_fields("mg-key", ts=time.time() - 900)):
        body, ctype = multipart(fields)
        assert post(f"{base}/v1/webhooks/mailgun/a", body, {"Content-Type": ctype}) == 401


def test_twilio_voice_transcription_becomes_a_voice_event(base):
    set_webhook("a", "twilio", "tw")
    url = f"{base}/v1/webhooks/twilio/a"
    fields = {"CallSid": "CA1", "RecordingSid": "RE1", "TranscriptionText": "please charge 4111 1111 1111 1111",
              "TranscriptionStatus": "completed"}
    sig = webhooks.twilio_signature("tw", url, sorted(fields.items()))
    assert post(url, urlencode(fields).encode(), {"X-Twilio-Signature": sig}) == 200
    with psycopg.connect(APP_DSN) as db:
        process_batch(db, "w")
    [(text, channel)] = admin("SELECT text, channel FROM messages")
    assert channel == "voice" and text.startswith("recording: please charge [card ****1111")


# Audit

def test_key_and_tenant_operations_are_audited(base):
    k = issue_key("a", label="rotation")
    revoke_key(key_id(hash_key(k)))
    delete_tenant("b")
    actions = [r[0] for r in admin("SELECT action FROM audit_log ORDER BY id")]
    assert actions[-3:] == ["key_issued", "key_revoked", "tenant_deleted"]


# Backups

def test_backup_and_restore_drill_round_trips_every_table(base, tmp_path):
    store("a", CARD, "hello", "I am a real human")
    store("b", "fine")
    with psycopg.connect(ADMIN_DSN, autocommit=True) as db:
        db.execute("INSERT INTO dead_letters (payload, reason) VALUES ('x', 'test')")
    before = {t: admin(f"SELECT count(*) FROM {t}")[0][0] for t in backup.TABLES}
    folder = backup.backup(str(tmp_path))
    reset()
    assert backup.restore(folder) == before
    assert admin("SELECT count(*) FROM findings WHERE evidence LIKE '[card ****1111%%'") == [(1,)]
    with psycopg.connect(ADMIN_DSN, autocommit=True) as db:
        db.execute("INSERT INTO dead_letters (payload, reason) VALUES ('after restore', 'test')")
    assert admin("SELECT count(*) FROM dead_letters") == [(2,)]


def test_restore_refuses_a_database_that_is_not_empty(base, tmp_path):
    store("a", "hello")
    folder = backup.backup(str(tmp_path))
    with pytest.raises(RuntimeError, match="not empty"):
        backup.restore(folder)


def test_ops_are_paged_when_an_alert_starts_and_when_it_resolves(base, local_alerts, monkeypatch):
    import jobs
    rx = Receiver()
    monkeypatch.setattr(jobs, "OPS_WEBHOOK", rx.url)
    started, resolved = jobs.ops_transitions([], ["queue_lag_seconds"])
    assert jobs.page_ops(started, resolved, {"queue_lag_seconds": 75.0})
    assert jobs.ops_transitions(["queue_lag_seconds"], ["queue_lag_seconds"]) == ([], [])
    assert jobs.page_ops(*jobs.ops_transitions(["queue_lag_seconds"], []), {})
    texts = [json.loads(b)["text"] for _, b in rx.got]
    assert "firing: queue_lag_seconds = 75.00" in texts[0] and "resolved: queue_lag_seconds" in texts[1]


def test_scheduler_survives_a_database_error_and_tries_again(monkeypatch):
    import jobs
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise psycopg.errors.UndefinedTable("relation does not exist")
        raise SystemExit  # stop the loop on the retry

    monkeypatch.setattr(jobs, "hourly", flaky)
    monkeypatch.setattr(jobs.time, "sleep", lambda s: None)
    with pytest.raises(SystemExit):
        jobs.scheduler(tick=0, hour=0)
    assert len(calls) == 2


def test_one_failing_hourly_step_does_not_stop_the_others(monkeypatch):
    ran = []

    def boom():
        raise RuntimeError("model outage")

    steps = [("scan", lambda: ran.append("scan")), ("llm_scan", boom),
             ("notify", lambda: ran.append("notify")), ("purge", lambda: ran.append("purge"))]
    monkeypatch.setattr(jobs, "HOURLY_STEPS", steps)
    assert jobs.hourly() == ["llm_scan"]
    assert ran == ["scan", "notify", "purge"]


def test_model_rules_switched_on_without_a_key_skip_instead_of_crashing(monkeypatch):
    import llm_rules

    monkeypatch.setattr(llm_rules, "ENABLED", True)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert llm_rules.scan() == (0, 0)


def test_worker_health_reports_a_stalled_loop():
    import json as _json
    import urllib.error
    import urllib.request

    import convoflow

    server = convoflow.serve_health(0, max_age=60)
    url = f"http://127.0.0.1:{server.server_address[1]}/health"
    try:
        convoflow._last_batch[0] = time.monotonic()
        assert _json.loads(urllib.request.urlopen(url).read())["status"] == "ok"
        convoflow._last_batch[0] = time.monotonic() - 120
        try:
            urllib.request.urlopen(url)
            raise AssertionError("expected 503")
        except urllib.error.HTTPError as err:
            assert err.code == 503
    finally:
        server.shutdown()
