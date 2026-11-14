"""Encrypts webhook and alert secrets before they are stored.

CONVOFLOW_SECRETS_KEYS holds one or more Fernet keys, comma separated, newest
first. New values are encrypted with the first key and any listed key can
decrypt, so rotation is: put a new key first, run `jobs.py rotate-secrets`,
then drop the old key. Values stored before encryption existed are read as
plain text until `rotate-secrets` encrypts them.
"""
import os

from cryptography.fernet import Fernet, InvalidToken, MultiFernet

PREFIX = "enc:v1:"
_DEV_KEY = "Q29udm9mbG93LWRldi1vbmx5LXNlY3JldHMta2V5ISE="  # local and tests only


def _box():
    keys = [k.strip() for k in os.environ.get("CONVOFLOW_SECRETS_KEYS", _DEV_KEY).split(",") if k.strip()]
    return MultiFernet([Fernet(k) for k in keys])


def seal(value):
    return PREFIX + _box().encrypt(value.encode()).decode()


def open_(stored):
    if not stored.startswith(PREFIX):
        return stored
    try:
        return _box().decrypt(stored[len(PREFIX):].encode()).decode()
    except InvalidToken as err:
        raise RuntimeError("secret cannot be decrypted with CONVOFLOW_SECRETS_KEYS") from err


def reseal(stored):
    """Re-encrypts with the newest key. Plain values get encrypted."""
    if not stored.startswith(PREFIX):
        return seal(stored)
    return PREFIX + _box().rotate(stored[len(PREFIX):].encode()).decode()
