"""Runs real load through real workers, kills one on purpose, then audits the books.

  burst:  python loadtest.py burst --items 1000000
  soak:   python loadtest.py soak --rate 15 --hours 24
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone

import psycopg

from convoflow import ADMIN_DSN, Simulator, add_tenants, enqueue, reset, scan

TENANTS = [f"t{i}" for i in range(19)] + ["capped"]
MAX_QUEUE = 200_000
CHUNK = 5000


def start_worker():
    code = "import convoflow; convoflow.run_worker()"
    return subprocess.Popen([sys.executable, "-c", code])


def setup():
    reset()
    add_tenants({**{t: 10**12 for t in TENANTS[:-1]}, "capped": 5000})


def audit(db, produced, planted, started):
    q = lambda sql: db.execute(sql).fetchone()
    claimed, stored, dups, dead = q("""SELECT COALESCE(sum(claimed),0), COALESCE(sum(stored),0),
        COALESCE(sum(duplicates),0), COALESCE(sum(dead),0) FROM worker_counters""")
    p50, p95, p99, worst = q("""SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY s),
        percentile_cont(0.95) WITHIN GROUP (ORDER BY s),
        percentile_cont(0.99) WITHIN GROUP (ORDER BY s), max(s)
        FROM (SELECT extract(epoch FROM ingested_at - enqueued_at) s FROM messages) x""")
    report = {
        "produced": produced, "claimed": int(claimed), "stored": int(stored),
        "duplicates": int(dups), "dead": int(dead),
        "rows_in_messages": q("SELECT count(*) FROM messages")[0],
        "rows_in_dead_letters": q("SELECT count(*) FROM dead_letters")[0],
        "left_in_queue": q("SELECT count(*) FROM inbox")[0],
        "planted_violations": planted,
        "findings": q("SELECT count(*) FROM findings")[0],
        "scan_runs": q("SELECT count(*) FROM scan_runs")[0],
        "tenant_rows": dict(db.execute("SELECT tenant_id, count(*) FROM messages GROUP BY 1").fetchall()),
        "over_quota": q("SELECT count(*) FROM dead_letters WHERE reason = 'over_quota'")[0],
        "latency_seconds": {"p50": round(p50, 3), "p95": round(p95, 3), "p99": round(p99, 3), "max": round(float(worst), 3)},
        "capped_usage_by_day": {str(d): n for d, n in db.execute(
            "SELECT day, stored FROM tenant_usage WHERE tenant_id = 'capped' ORDER BY day").fetchall()},
        "elapsed_seconds": round(time.monotonic() - started, 1),
    }
    report["checks"] = {
        "every_item_accounted_for": report["produced"] == report["stored"] + report["duplicates"] + report["dead"],
        "counters_match_tables": report["stored"] == report["rows_in_messages"] and report["dead"] == report["rows_in_dead_letters"],
        "queue_empty": report["left_in_queue"] == 0,
        "all_planted_violations_found": report["findings"] == report["planted_violations"],
        # The limit is per UTC day, so a run that crosses midnight may store up to 5,000 per day.
        "capped_tenant_held_to_limit": all(n <= 5000 for n in report["capped_usage_by_day"].values())
            and sum(report["capped_usage_by_day"].values()) == report["tenant_rows"].get("capped", 0),
    }
    return report


def save(name, report):
    os.makedirs("results", exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = f"results/{name}-{stamp}.json"
    with open(path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))
    print("saved", path)


def wait_for_room(db, limit):
    while db.execute("SELECT count(*) FROM inbox").fetchone()[0] > limit:
        db.commit()
        time.sleep(0.2)
    db.commit()


def drain(db):
    while db.execute("SELECT count(*) FROM inbox").fetchone()[0]:
        db.commit()
        time.sleep(0.5)
    db.commit()


def burst(items, workers, kill_at=0.3):
    setup()
    sim = Simulator(TENANTS)
    pool = [start_worker() for _ in range(workers)]
    started = time.monotonic()
    killed = False
    with psycopg.connect(ADMIN_DSN) as db:
        produced = 0
        while produced < items:
            wait_for_room(db, MAX_QUEUE)
            n = min(CHUNK, items - produced)
            enqueue(db, (sim.next() for _ in range(n)))
            produced += n
            if not killed and produced >= items * kill_at:
                pool[0].send_signal(signal.SIGKILL)
                pool[0].wait()
                pool[0] = start_worker()
                killed = True
                print("killed a worker at", produced, flush=True)
        drain(db)
        time.sleep(2)
        ingest_seconds = time.monotonic() - started
        for p in pool:
            p.terminate()
        for p in pool:
            p.wait()
        scanned, found = scan(lag_seconds=0)
        report = audit(db, produced, sim.planted, started)
    report.update(mode="burst", workers=workers, worker_killed=killed, ingest_seconds=round(ingest_seconds, 1),
                  items_per_second=round(items / ingest_seconds), scanned=scanned)
    save("burst", report)
    return all(report["checks"].values())


def soak(rate, hours, workers, kill_every_hours=6):
    setup()
    sim = Simulator(TENANTS)
    pool = [start_worker() for _ in range(workers)]
    started = time.monotonic()
    produced = kills = 0
    next_scan = started + 3600
    next_kill = started + kill_every_hours * 3600
    with psycopg.connect(ADMIN_DSN) as db:
        while (elapsed := time.monotonic() - started) < hours * 3600:
            behind = int(elapsed * rate) - produced
            if behind > 0:
                enqueue(db, [sim.next() for _ in range(behind)])
                produced += behind
            if time.monotonic() >= next_scan:
                scanned, found = scan()
                print(f"hour {round(elapsed / 3600)}: produced={produced} scanned={scanned} findings={found}", flush=True)
                next_scan += 3600
            if time.monotonic() >= next_kill:
                pool[0].send_signal(signal.SIGKILL)
                pool[0].wait()
                pool[0] = start_worker()
                kills += 1
                next_kill += kill_every_hours * 3600
            for i, p in enumerate(pool):
                if p.poll() is not None:
                    pool[i] = start_worker()
            time.sleep(1)
        drain(db)
        time.sleep(2)
        for p in pool:
            p.terminate()
        for p in pool:
            p.wait()
        time.sleep(1)
        scan(lag_seconds=0)
        report = audit(db, produced, sim.planted, started)
    report.update(mode="soak", rate=rate, hours=hours, workers=workers, workers_killed=kills)
    save("soak", report)
    return all(report["checks"].values())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("burst", "soak"))
    parser.add_argument("--items", type=int, default=1_000_000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--rate", type=int, default=15)
    parser.add_argument("--hours", type=float, default=24)
    args = parser.parse_args()
    ok = burst(args.items, args.workers) if args.mode == "burst" else soak(args.rate, args.hours, args.workers)
    sys.exit(0 if ok else 1)
