"""Checks the signature code against implementations that are not ours.

Twilio: the official twilio-python RequestValidator. Zendesk publishes no
library, so the expected value comes from openssl, following Zendesk's
documented recipe: base64(HMAC-SHA256(secret, timestamp + raw body)).
"""
import base64
import subprocess
from urllib.parse import urlencode

import pytest
from twilio.request_validator import RequestValidator

import webhooks

URLS = ["https://example.com/v1/webhooks/twilio/acme", "https://example.com/v1/webhooks/twilio/acme?x=1&y=two"]
PARAMS = {"MessageSid": "SM0123", "From": "+15551230000", "To": "+15559870000", "Body": "Hello & welcome = yes"}


@pytest.mark.parametrize("url", URLS)
def test_our_twilio_signature_matches_twilios_library(url):
    official = RequestValidator("auth-token-123")
    ours = webhooks.twilio_signature("auth-token-123", url, sorted(PARAMS.items()))
    assert ours == official.compute_signature(url, PARAMS)


@pytest.mark.parametrize("url", URLS)
def test_we_accept_what_twilio_signs_and_twilio_accepts_what_we_sign(url):
    official = RequestValidator("auth-token-123")
    body = urlencode(PARAMS).encode()
    ok, fields = webhooks.verify_twilio("auth-token-123", url, body, official.compute_signature(url, PARAMS))
    assert ok and fields["Body"] == PARAMS["Body"]
    assert official.validate(url, PARAMS, webhooks.twilio_signature("auth-token-123", url, sorted(PARAMS.items())))


def test_zendesk_signature_matches_openssl():
    secret, ts, body = "dGhpcyBpcyBhIHNlY3JldA==", "2026-11-14T16:00:00Z", b'{"ticket":{"id":7},"comment":{"body":"hi"}}'
    raw = subprocess.run(["openssl", "dgst", "-sha256", "-hmac", secret, "-binary"],
                         input=ts.encode() + body, capture_output=True, check=True).stdout
    assert webhooks.zendesk_signature(secret, ts, body) == base64.b64encode(raw).decode()
