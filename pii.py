"""Masks card numbers and SSNs before anything is written to the database.

A match is replaced with a token such as ``[card ****1111 fp:1a2b3c4d]``. The
last four digits help a reviewer, and the fingerprint is a keyed hash of the
full value, so the same card can be linked across messages without storing it.
The scanner's rules then match these tokens, never raw values.
"""
import hashlib
import hmac
import os
import re

_KEY = os.environ.get("CONVOFLOW_PII_KEY", "dev-only-pii-key").encode()

_CARD = re.compile(r"(?<![\d-])(?:\d[ -]?){12,18}\d(?![\d-])")
_SSN = re.compile(r"(?<![\d-])(\d{3})-(\d{2})-(\d{4})(?![\d-])")

CARD_TOKEN = r"\[card \*{4}\d{4} fp:[0-9a-f]{8}\]"
SSN_TOKEN = r"\[ssn \*{3}-\*{2}-\d{4} fp:[0-9a-f]{8}\]"


def luhn_valid(digits):
    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch)
        if i % 2:
            n = n * 2 - 9 if n > 4 else n * 2
        total += n
    return total % 10 == 0


def ssn_valid(area, group, serial):
    """Rejects numbers the SSA never issues, which are common in order and phone numbers."""
    return area not in ("000", "666") and not area.startswith("9") and group != "00" and serial != "0000"


def fingerprint(kind, value):
    return hmac.new(_KEY, f"{kind}:{value}".encode(), hashlib.sha256).hexdigest()[:8]


def _card(m):
    digits = re.sub(r"\D", "", m.group(0))
    if not 13 <= len(digits) <= 19 or not luhn_valid(digits):
        return m.group(0)
    return f"[card ****{digits[-4:]} fp:{fingerprint('card', digits)}]"


def _ssn(m):
    area, group, serial = m.groups()
    if not ssn_valid(area, group, serial):
        return m.group(0)
    return f"[ssn ***-**-{serial} fp:{fingerprint('ssn', area + group + serial)}]"


def mask(text):
    """Returns text with valid card numbers and SSNs replaced by tokens. Safe to run twice."""
    return _CARD.sub(_card, _SSN.sub(_ssn, text))
