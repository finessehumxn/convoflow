"""Measures what one flooding tenant does to a quiet tenant's latency.

  python noisy.py          fair claiming
  CONVOFLOW_FAIR=0 python noisy.py   oldest-first claiming
"""
import json
import statistics
import subprocess
import sys
import time

import psycopg

from convoflow import ADMIN_DSN, FAIR_CLAIMS, add_tenants, enqueue, reset

FLOOD = 300_000
QUIET_EVENTS = 300
WORKERS = 4


def event(tenant, n):
    return json.dumps({"tenant_id": tenant, "conversation_id": f"{tenant}-{n}", "channel": "chat", "text": f"message {n}"})


reset()
add_tenants({"noisy": 10**12, "quiet": 10**12})
with psycopg.connect(ADMIN_DSN) as db:
    enqueue(db, (event("noisy", i) for i in range(FLOOD)))
    pool = [subprocess.Popen([sys.executable, "-c", "import convoflow; convoflow.run_worker()"]) for _ in range(WORKERS)]
    start = time.monotonic()
    for i in range(QUIET_EVENTS):
        enqueue(db, [event("quiet", i)])
        time.sleep(0.005)
    while db.execute("SELECT count(*) FROM inbox").fetchone()[0]:
        db.commit()
        time.sleep(0.25)
    db.commit()
    elapsed = time.monotonic() - start
    for p in pool:
        p.terminate()
    for p in pool:
        p.wait()
    time.sleep(0.5)
    rows = [float(r[0]) for r in db.execute(
        "SELECT extract(epoch FROM ingested_at - enqueued_at) FROM messages WHERE tenant_id = 'quiet'")]
    rows.sort()
print(json.dumps({
    "fair_claims": FAIR_CLAIMS, "flood": FLOOD, "quiet_events": len(rows),
    "quiet_p50_s": round(statistics.median(rows), 3),
    "quiet_p95_s": round(rows[int(len(rows) * 0.95)], 3),
    "quiet_max_s": round(rows[-1], 3),
    "total_seconds": round(elapsed, 1),
    "items_per_second": round((FLOOD + QUIET_EVENTS) / elapsed),
}))
