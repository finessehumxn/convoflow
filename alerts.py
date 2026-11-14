"""Sends each tenant its new findings: a signed JSON POST, or a Slack message for Slack URLs.

Delivery is at least once. A finding is marked sent only after a 2xx reply, so a
failed send is retried on the next run, for up to MAX_RETRY_HOURS.
Receivers check X-Convoflow-Signature: sha256=hex(HMAC(secret, timestamp + "." + body)).
"""
import hashlib
import hmac
import ipaddress
import json
import socket
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse

import psycopg

import secretbox
from convoflow import ADMIN_DSN, log

SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2, "critical": 3}
MAX_PER_MESSAGE = 50
MAX_RETRY_HOURS = 24
ALLOW_PRIVATE = False  # tests only: lets alerts reach a local receiver


class BadDestination(ValueError):
    pass


def check_destination(url):
    """Refuses anything but https to a public address, so a tenant cannot aim alerts at internal services."""
    u = urlparse(url)
    if ALLOW_PRIVATE and u.scheme in ("http", "https"):
        return
    if u.scheme != "https" or not u.hostname:
        raise BadDestination("alert URL must be https")
    for info in socket.getaddrinfo(u.hostname, u.port or 443, proto=socket.IPPROTO_TCP):
        ip = ipaddress.ip_address(info[4][0])
        if not ip.is_global:
            raise BadDestination(f"alert URL resolves to a non-public address {ip}")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None  # a redirect could point somewhere check_destination never saw


_opener = urllib.request.build_opener(_NoRedirect)


def sign(secret, timestamp, body):
    return "sha256=" + hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()


def body_for(url, tenant_id, findings):
    if urlparse(url).hostname == "hooks.slack.com":
        lines = [f"*convoflow*: {len(findings)} new finding(s) for `{tenant_id}`"]
        lines += [f"• {f['severity']}: {f['rule']} ({f['evidence']})" for f in findings[:10]]
        return json.dumps({"text": "\n".join(lines)}).encode()
    return json.dumps({"tenant_id": tenant_id, "findings": findings}, default=str).encode()


def send(url, secret, tenant_id, findings):
    check_destination(url)
    body = body_for(url, tenant_id, findings)
    ts = str(int(time.time()))
    req = urllib.request.Request(url, body, {
        "Content-Type": "application/json", "User-Agent": "convoflow-alerts",
        "X-Convoflow-Timestamp": ts, "X-Convoflow-Signature": sign(secret, ts, body)}, method="POST")
    with _opener.open(req, timeout=10) as res:
        return 200 <= res.status < 300


def notify():
    """Sends unsent findings at or above each tenant's minimum severity. Returns findings sent."""
    sent = 0
    with psycopg.connect(ADMIN_DSN) as db:
        targets = db.execute("SELECT tenant_id, url, secret, min_severity FROM tenant_alerts").fetchall()
        for tenant_id, url, sealed, min_severity in targets:
            allowed = [s for s, r in SEVERITY_RANK.items() if r >= SEVERITY_RANK[min_severity]]
            rows = db.execute(
                """SELECT event_key, rule, severity, evidence, found_at, rules_version FROM findings
                   WHERE tenant_id = %s AND notified_at IS NULL AND severity = ANY(%s)
                     AND found_at > clock_timestamp() - make_interval(hours => %s)
                   ORDER BY found_at LIMIT %s""",
                (tenant_id, allowed, MAX_RETRY_HOURS, MAX_PER_MESSAGE),
            ).fetchall()
            if not rows:
                continue
            findings = [dict(zip(("event_key", "rule", "severity", "evidence", "found_at", "rules_version"), r)) for r in rows]
            try:
                ok = send(url, secretbox.open_(sealed), tenant_id, findings)
            except (urllib.error.URLError, OSError, BadDestination) as err:
                log("alert_failed", tenant_id=tenant_id, error=str(err))
                continue
            if ok:
                db.execute(
                    """UPDATE findings f SET notified_at = clock_timestamp()
                       FROM unnest(%s::text[], %s::text[]) AS s(event_key, rule)
                       WHERE f.tenant_id = %s AND f.event_key = s.event_key AND f.rule = s.rule""",
                    ([f["event_key"] for f in findings], [f["rule"] for f in findings], tenant_id),
                )
                db.commit()
                sent += len(findings)
                log("alert_sent", tenant_id=tenant_id, findings=len(findings))
    return sent
