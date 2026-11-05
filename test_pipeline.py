from pipeline import connect, ingest, simulate

GOOD = {"tenant_id": "a", "conversation_id": "c1", "channel": "chat", "text": "hi"}


def test_replay_stores_nothing_new():
    db = connect()
    ingest(db, [GOOD])
    assert ingest(db, [GOOD]) == {"seen": 1, "stored": 0, "duplicates": 1, "dead": 0}


def test_same_event_stays_separate_per_tenant():
    db = connect()
    assert ingest(db, [GOOD, {**GOOD, "tenant_id": "b"}])["stored"] == 2


def test_bad_event_goes_to_dead_letters_and_run_continues():
    db = connect()
    stats = ingest(db, [{**GOOD, "channel": "fax"}, "not json", GOOD])
    assert (stats["stored"], stats["dead"]) == (1, 2)
    assert db.execute("SELECT count(*) FROM dead_letters").fetchone()[0] == 2


def test_every_item_is_accounted_for():
    stats = ingest(connect(), simulate(5000))
    assert stats["stored"] + stats["duplicates"] + stats["dead"] == stats["seen"] == 5000
