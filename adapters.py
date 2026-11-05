"""Turns each source's payload into internal events. One call can become many."""
import uuid


class BadPayload(ValueError):
    pass


def _voice(p):
    return [
        {"conversation_id": p["call_id"], "channel": "voice", "text": f"{s['speaker']}: {s['text']}"}
        for s in p["segments"]
    ]


def _chat(p):
    return [{"conversation_id": str(p["ticket"]["id"]), "channel": "chat", "text": p["comment"]["body"]}]


def _email(p):
    return [{"conversation_id": p["message_id"], "channel": "email", "text": f"{p['subject']}\n{p['body']}"}]


def _sms(p):
    return [{"conversation_id": p["sid"], "channel": "sms", "text": p["body"]}]


SOURCES = {"voice": _voice, "chat": _chat, "email": _email, "sms": _sms}


def normalize(source, tenant_id, payload):
    if source not in SOURCES:
        raise BadPayload(f"unknown source {source}")
    try:
        events = SOURCES[source](payload)
    except (KeyError, TypeError) as err:
        raise BadPayload(f"bad {source} payload: {err}") from err
    trace_id = uuid.uuid4().hex
    return [{**e, "tenant_id": tenant_id, "trace_id": trace_id} for e in events], trace_id
