"""Signed provider webhooks: Twilio SMS and Zendesk ticket comments.

Each tenant stores its own secret. A request is accepted only when its signature
matches, so knowing the URL is not enough to inject events.
"""
import base64
import hashlib
import hmac
from datetime import datetime, timezone
from urllib.parse import parse_qsl

ZENDESK_MAX_SKEW_SECONDS = 300


def twilio_signature(auth_token, url, params):
    """Twilio signs the full URL plus every POST field, sorted by name, with HMAC-SHA1."""
    data = url + "".join(k + v for k, v in sorted(params))
    return base64.b64encode(hmac.new(auth_token.encode(), data.encode(), hashlib.sha1).digest()).decode()


def verify_twilio(auth_token, url, body, signature):
    params = parse_qsl(body.decode(), keep_blank_values=True)
    ok = hmac.compare_digest(twilio_signature(auth_token, url, params), signature or "")
    return ok, dict(params)


def twilio_sms_payload(fields):
    """Maps Twilio's inbound message fields onto the sms adapter."""
    return {"sid": fields["MessageSid"], "body": fields.get("Body", "")}


def zendesk_signature(secret, timestamp, body):
    """Zendesk signs the timestamp header followed by the raw body with HMAC-SHA256."""
    return base64.b64encode(hmac.new(secret.encode(), timestamp.encode() + body, hashlib.sha256).digest()).decode()


def verify_zendesk(secret, timestamp, body, signature, now=None):
    """Also rejects old timestamps, so a captured request cannot be replayed later."""
    try:
        sent = datetime.fromisoformat((timestamp or "").replace("Z", "+00:00"))
    except ValueError:
        return False
    now = now or datetime.now(timezone.utc)
    if sent.tzinfo is None or abs((now - sent).total_seconds()) > ZENDESK_MAX_SKEW_SECONDS:
        return False
    return hmac.compare_digest(zendesk_signature(secret, timestamp, body), signature or "")
