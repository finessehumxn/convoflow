"""Load test over real HTTP against a deployed API, then audit that every event was stored.

    CONVOFLOW_KEY=... python loadtest_http.py https://host --events 20000 --per-request 10 --concurrency 16

Each request is a voice call with --per-request segments, so one request becomes that many events.
The audit reads /metrics before and after and waits for the queue to drain.
"""
import argparse
import http.client
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse


def metrics(base):
    u = urlparse(base)
    conn = (http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection)(u.netloc, timeout=30)
    conn.request("GET", "/metrics")
    out = {}
    for line in conn.getresponse().read().decode().splitlines():
        name, _, value = line.rpartition(" ")
        if name.startswith("convoflow_") and "{" not in name:
            out[name[len("convoflow_"):]] = float(value)
    return out


def run(base, key, events, per_request, concurrency):
    u = urlparse(base)
    run_id = uuid.uuid4().hex[:8]
    requests = events // per_request
    next_req, lock = [0], threading.Lock()
    latencies, codes = [], {}

    def worker():
        conn = None
        while True:
            with lock:
                if next_req[0] >= requests:
                    return
                n = next_req[0]
                next_req[0] += 1
            body = json.dumps({"call_id": f"{run_id}-{n}", "segments": [
                {"speaker": "caller" if i % 2 else "agent", "text": f"load {run_id} call {n} line {i}"}
                for i in range(per_request)]})
            for attempt in range(5):
                if conn is None:
                    conn = (http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection)(u.netloc, timeout=30)
                start = time.perf_counter()
                try:
                    conn.request("POST", "/v1/events/voice", body, {"X-Api-Key": key, "Content-Type": "application/json"})
                    res = conn.getresponse()
                    res.read()
                except (OSError, http.client.HTTPException):
                    conn.close()
                    conn = None
                    with lock:
                        codes["error"] = codes.get("error", 0) + 1
                    continue
                with lock:
                    latencies.append(time.perf_counter() - start)
                    codes[res.status] = codes.get(res.status, 0) + 1
                if res.status in (429, 503):
                    time.sleep(float(res.getheader("Retry-After", "1")))
                    continue
                break

    before = metrics(base)
    started = time.perf_counter()
    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    sent_seconds = time.perf_counter() - started
    while metrics(base)["queue_depth"] > 0:
        time.sleep(1)
    drained_seconds = time.perf_counter() - started
    after = metrics(base)

    accepted = codes.get(202, 0) * per_request
    stored = after["stored_total"] - before["stored_total"]
    dups = after["duplicates_total"] - before["duplicates_total"]
    dead = after["dead_total"] - before["dead_total"]
    latencies.sort()
    pct = lambda p: round(latencies[min(len(latencies) - 1, int(p * len(latencies)))], 3)
    report = {
        "target": base, "run_id": run_id, "events_accepted": accepted, "requests": requests,
        "status_codes": {str(k): v for k, v in codes.items()},
        "concurrency": concurrency, "events_per_request": per_request,
        "send_seconds": round(sent_seconds, 1), "drain_seconds": round(drained_seconds, 1),
        "events_per_second_end_to_end": round(accepted / drained_seconds, 1),
        "http_latency_seconds": {"p50": pct(0.5), "p95": pct(0.95), "p99": pct(0.99)},
        "stored": stored, "duplicates": dups, "dead": dead,
        "checks": {"every_event_accounted_for": stored + dups + dead == accepted, "queue_drained": True},
    }
    os.makedirs("results", exist_ok=True)
    path = f"results/http-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))
    print("saved", path)
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("base")
    p.add_argument("--events", type=int, default=20000)
    p.add_argument("--per-request", type=int, default=10)
    p.add_argument("--concurrency", type=int, default=16)
    a = p.parse_args()
    run(a.base.rstrip("/"), os.environ["CONVOFLOW_KEY"], a.events, a.per_request, a.concurrency)
