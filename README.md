# convoflow

Multi-tenant conversation ingestion on Postgres, with an hourly compliance scanner.

- Workers claim inbox rows with `FOR UPDATE SKIP LOCKED` and write results in the same transaction, so a crash returns the batch to the queue.
- Content-hash keys per tenant make replays safe.
- Row Level Security on `messages` keeps tenants apart, and workers connect as a non-owner role.
- Per-tenant daily limits hold one customer back without slowing the others.
- Bad rows go to `dead_letters` with a reason.
- The scanner runs on a window and stores the evidence for each finding.

## Pieces

- `api.py`: authenticated HTTP ingest per source, plus `/health` and `/metrics` with alert rules.
- `adapters.py`: turns each source's payload into internal events.
- `convoflow.py`: queue, workers, tenant limits, channel entitlements and the scanner.
- `infra/`: Terraform for ECS Fargate, RDS, Secrets Manager, the hourly schedule and alarms.

## Run it

    .venv/bin/python -m pytest -q
    .venv/bin/python loadtest.py burst --items 1000000
    .venv/bin/python loadtest.py soak --rate 15 --hours 24

## Measured

Traffic is simulated and runs on one laptop. `results/` holds each run's audit.

## Not done yet

- `terraform apply` has never been run, so nothing exists on AWS.
- The Docker image has never been built, because this machine has no Docker.
- CI has never run, because the repo has no GitHub remote.
- Before first deploy, apply `schema.sql` by hand without its `DROP SCHEMA` line, then set the `app` role password to the value in the app DSN secret.
- API traffic reaches tasks directly. A load balancer with TLS is required before real customers.
- Everything runs on one Postgres. Per-tenant latency under a noisy neighbor has not been measured.
