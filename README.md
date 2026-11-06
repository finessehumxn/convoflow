# convoflow

Multi-tenant conversation ingestion on Postgres, with an hourly compliance scanner.

- Workers claim inbox rows with `FOR UPDATE SKIP LOCKED` and write results in the same transaction, so a crash returns the batch to the queue.
- Content-hash keys per tenant make replays safe.
- Row Level Security on `messages` keeps tenants apart, and workers connect as a non-owner role.
- Per-tenant daily limits hold one customer back without slowing the others.
- If the database rejects a batch, each row retries alone with exponential backoff, so one poison row cannot block the others. After 3 attempts it goes to `dead_letters`.
- Bad rows go to `dead_letters` with a reason and their full payload. `replay(reason)` sends them back once, with an optional repair step.
- The scanner runs on a window and stores the evidence for each finding.

## Pieces

- `api.py`: authenticated HTTP ingest per source, plus `/health` and `/metrics` with alert rules.
- `adapters.py`: turns each source's payload into internal events.
- `convoflow.py`: queue, workers, tenant limits, channel entitlements and the scanner.
- `jobs.py`: migrations, the hourly scan and retention purge, and the CloudWatch metrics publisher.
- `migrations/`: versioned schema changes, each applied once.
- `infra/`: Terraform for the network, load balancer, ECS Fargate, RDS, secrets, autoscaling, the hourly schedule, alarms and the deploy role.

## Run it

    .venv/bin/python -m pytest -q
    .venv/bin/python loadtest.py burst --items 1000000
    .venv/bin/python loadtest.py soak --rate 15 --hours 24

## Measured

Traffic is simulated and runs on one laptop. `results/` holds each run's audit.

Noisy neighbor, one run each with 300,000 items queued by one tenant and 300 arriving from another (`noisy.py`):

| Claiming | Quiet tenant p50 | Quiet tenant p95 |
| --- | --- | --- |
| Oldest first | 3.77s | 4.65s |
| Fair share | 0.03s | 0.05s |

Fair claiming costs throughput: the 1M burst fell from about 47,800 items a second to about 40,800.

## Status

- `terraform apply` has never been run, so nothing exists on AWS.
- Everything runs on one Postgres, so this has not been tested across machines.
- The deploy workflow has never run. See `RUNBOOK.md`.
