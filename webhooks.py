"""Signed provider webhooks: Twilio SMS and voice transcriptions, Zendesk comments, Mailgun email.

Each tenant stores its own secret. A request is accepted only when its signature
matches, so knowing the URL is not enough to inject events.
"""
import base64
import email
import email.policy
import hashlib
import hmac
from datetime import datetime, timezone
from urllib.parse import parse_qsl

ZENDESK_MAX_SKEW_SECONDS = 300  # also used for Mailgun


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


def twilio_transcription_payload(fields):
    """Twilio posts a recording's transcript to transcribeCallback; it becomes one voice segment."""
    return {"call_id": fields.get("CallSid") or fields["RecordingSid"],
            "segments": [{"speaker": "recording", "text": fields["TranscriptionText"]}]}


def parse_form(body, content_type):
    """Reads urlencoded or multipart/form-data fields. File parts are skipped."""
    if content_type.startswith("multipart/form-data"):
        msg = email.message_from_bytes(b"Content-Type: " + content_type.encode() + b"\r\n\r\n" + body,
                                       policy=email.policy.HTTP)
        fields = {}
        for part in msg.iter_parts():
            name = part.get_param("name", header="content-disposition")
            if name and not part.get_filename():
                fields[name] = part.get_content() if part.get_content_maintype() == "text" else part.get_payload(decode=True).decode()
        return fields
    return dict(parse_qsl(body.decode(), keep_blank_values=True))


def mailgun_signature(signing_key, timestamp, token):
    return hmac.new(signing_key.encode(), (timestamp + token).encode(), hashlib.sha256).hexdigest()


def verify_mailgun(signing_key, body, content_type, now=None):
    """Mailgun signs timestamp + token with HMAC-SHA256. Old timestamps are refused to stop replays."""
    fields = parse_form(body, content_type)
    ts, token, sig = fields.get("timestamp", ""), fields.get("token", ""), fields.get("signature", "")
    try:
        age = abs((now or datetime.now(timezone.utc).timestamp()) - int(ts))
    except ValueError:
        return False, fields
    if age > ZENDESK_MAX_SKEW_SECONDS:
        return False, fields
    return hmac.compare_digest(mailgun_signature(signing_key, ts, token), sig), fields


def mailgun_email_payload(fields):
    return {"message_id": fields["Message-Id"], "subject": fields.get("subject", ""), "body": fields["body-plain"]}
