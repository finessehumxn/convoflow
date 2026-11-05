# convoflow

Multi-tenant conversation ingestion on Postgres, with an hourly compliance scanner.

- Workers claim inbox rows with `FOR UPDATE SKIP LOCKED` and write results in the same transaction, so a crash returns the batch to the queue.
- Content-hash keys per tenant make replays safe.
- Row Level Security on `messages` keeps tenants apart, and workers connect as a non-owner role.
- Per-tenant daily limits hold one customer back without slowing the others.
- Bad rows go to `dead_letters` with a reason.
- The scanner runs on a window and stores the evidence for each finding.

## Run it

    .venv/bin/python -m pytest -q
    .venv/bin/python loadtest.py burst --items 1000000
    .venv/bin/python loadtest.py soak --rate 15 --hours 24

## Measured

Traffic is simulated and runs on one laptop. `results/` holds each run's audit.
