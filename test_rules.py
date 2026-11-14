"""A labelled corpus for every rule. Adding a rule means adding cases here first."""
import psycopg
import pytest

from convoflow import ADMIN_DSN, RULES
from pii import mask

CASES = {
    "card_number_exposed": {
        "hit": ["card 4111 1111 1111 1111", "use 5500-0000-0000-0004 please", "amex 378282246310005"],
        "miss": ["order 1234 5678 1234 5678", "call 555-123-4567", "tracking 1Z999AA10123456784", "4111"],
    },
    "ssn_exposed": {
        "hit": ["SSN 123-45-6789", "my social is 078-05-1121"],
        "miss": ["000-12-3456", "666-12-3456", "912-34-5678", "123-00-4567", "123-45-0000", "date 2024-11-13"],
    },
    "denied_being_ai": {
        "hit": ["I am a real human", "Yes, I am human.", "i am a human agent"],
        "miss": ["I am an AI assistant", "a human will call you"],
    },
}


def test_every_rule_has_a_corpus():
    assert {r[0] for r in RULES} == set(CASES)


@pytest.mark.parametrize("rule,pattern", [(r[0], r[2]) for r in RULES])
def test_rule_matches_hits_and_skips_misses(rule, pattern):
    with psycopg.connect(ADMIN_DSN) as db:
        matches = lambda text: db.execute("SELECT %s ~ %s", (mask(text), pattern)).fetchone()[0]
        assert [t for t in CASES[rule]["hit"] if not matches(t)] == []
        assert [t for t in CASES[rule]["miss"] if matches(t)] == []
