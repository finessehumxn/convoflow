import json
from types import SimpleNamespace

import psycopg

import llm_rules
from convoflow import ADMIN_DSN, APP_DSN, add_tenants, enqueue, process_batch, reset


class FakeClient:
    """Stands in for anthropic.Anthropic(); records what was sent and answers from a script."""

    def __init__(self, answer, stop_reason="end_turn"):
        self.sent, self.answer, self.stop_reason = [], answer, stop_reason
        self.beta = SimpleNamespace(messages=SimpleNamespace(create=self.create))

    def create(self, **kwargs):
        self.sent.append(kwargs)
        items = json.loads(kwargs["messages"][0]["content"].split("\n", 1)[1])
        results = self.answer(items)
        return SimpleNamespace(stop_reason=self.stop_reason, stop_details=None,
                               content=[SimpleNamespace(type="text", text=json.dumps({"results": results}))])


def setup(*texts, tenants=None):
    reset()
    add_tenants(tenants or {"a": 1000})
    with psycopg.connect(APP_DSN) as db:
        enqueue(db, [json.dumps({"tenant_id": "a", "conversation_id": f"c{i}", "channel": "chat", "text": t})
                     for i, t in enumerate(texts)])
        process_batch(db, "w")


def findings():
    with psycopg.connect(ADMIN_DSN) as db:
        return db.execute("SELECT rule, severity, evidence, rules_version FROM findings ORDER BY rule").fetchall()


def flag_promises(items):
    return [{"id": i["id"], "rule": "llm_unauthorized_promise", "evidence": "full refund, guaranteed"}
            for i in items if "refund" in i["text"]]


def test_model_flags_are_stored_with_quote_and_model_version():
    setup("You'll get a full refund, guaranteed.", "Thanks for waiting.")
    assert llm_rules.scan(FakeClient(flag_promises)) == (2, 1)
    [(rule, severity, evidence, version)] = findings()
    assert (rule, severity, evidence) == ("llm_unauthorized_promise", "high", "full refund, guaranteed")
    assert version == f"llm-1:{llm_rules.MODEL}"


def test_each_message_is_checked_once():
    setup("full refund, guaranteed")
    client = FakeClient(flag_promises)
    llm_rules.scan(client)
    llm_rules.scan(client)
    assert len(client.sent) == 1


def test_only_masked_text_is_sent_and_request_is_constrained():
    setup("refund to card 4111 1111 1111 1111")
    client = FakeClient(lambda items: [])
    llm_rules.scan(client)
    req = client.sent[0]
    assert "4111 1111" not in json.dumps(req) and "[card ****1111" in json.dumps(req)
    assert req["output_config"]["format"]["type"] == "json_schema" and req["fallbacks"] == "default"


def test_answers_about_ids_outside_the_batch_or_unknown_rules_are_ignored():
    setup("hello")
    client = FakeClient(lambda items: [{"id": "99", "rule": "llm_threat_or_abuse", "evidence": "x"},
                                       {"id": "0", "rule": "made_up_rule", "evidence": "hello"}])
    llm_rules.scan(client)
    assert findings() == []


def test_invented_evidence_is_replaced_with_the_real_text():
    setup("We will refund you.")
    llm_rules.scan(FakeClient(lambda items: [{"id": "0", "rule": "llm_unauthorized_promise", "evidence": "not in message"}]))
    assert findings()[0][2] == "We will refund you."


def test_refusal_or_error_leaves_messages_for_the_next_run():
    setup("full refund, guaranteed")
    assert llm_rules.scan(FakeClient(flag_promises, stop_reason="refusal")) == (1, 0)

    def boom(items):
        raise TimeoutError("api down")

    setup("full refund, guaranteed")
    assert llm_rules.scan(FakeClient(boom)) == (0, 0)
    assert llm_rules.scan(FakeClient(flag_promises)) == (1, 1)


def test_tenant_can_switch_model_rules_off():
    setup("full refund, guaranteed")
    with psycopg.connect(ADMIN_DSN, autocommit=True) as db:
        db.execute("UPDATE tenants SET disabled_rules = '{llm}' WHERE id = 'a'")
    client = FakeClient(flag_promises)
    assert llm_rules.scan(client) == (0, 0) and client.sent == []


def test_off_by_default_without_a_client():
    assert llm_rules.scan() == (0, 0)
