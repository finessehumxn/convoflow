# convoflow

Multi-tenant conversation ingestion on Postgres, with an hourly compliance scanner.

Live demo: https://api-production-0f7cc354.up.railway.app (see `/health`, `/metrics`, `/alerts`)

- Workers claim inbox rows with `FOR UPDATE SKIP LOCKED` and write results in the same transaction, so a crash returns the batch to the queue.
- Content-hash keys per tenant make replays safe.
- Row Level Security on `messages` keeps tenants apart, and workers connect as a non-owner role.
- Per-tenant daily limits hold one customer back without slowing the others.
- If the database rejects a batch, each row retries alone with exponential backoff, so one poison row cannot block the others. After 3 attempts it goes to `dead_letters`.
- Bad rows go to `dead_letters` with a reason and their full payload. `replay(reason)` sends them back once, with an optional repair step.
- The scanner runs versioned rules on a window and stores masked evidence for each finding. Every rule has a labelled corpus in `test_rules.py`, and a tenant can switch rules off.
- Card numbers (Luhn-checked) and SSNs (SSA-valid) are masked before anything is written: last four digits plus a keyed fingerprint. No table holds a raw value. See `docs/adr/0003-mask-before-storage.md`.
- Twilio SMS and Zendesk webhooks are accepted only with a valid signature from that tenant's secret. Zendesk requests older than five minutes are refused.
- Backpressure: 429 with `Retry-After` past `CONVOFLOW_MAX_QUEUE`, 503 past `CONVOFLOW_MAX_IN_FLIGHT`, a 15 second socket timeout and a body limit.
- Several API keys per tenant, revocable, so keys rotate without downtime. `delete-tenant` erases a tenant's data in one transaction and records that it happened.
- OpenTelemetry traces follow one message from its HTTP request to the worker batch that stored it. Per-tenant backlog and inbox dead tuples are on `/metrics`.

Design decisions are in `docs/adr/`: Postgres as the queue, the standard-library HTTP server, masking before storage, and the UTC quota day.

## Pieces

- `api.py`: authenticated HTTP ingest per source, signed webhooks, backpressure, `/health`, `/metrics` and the demo page.
- `adapters.py`: turns each source's payload into internal events.
- `webhooks.py`: Twilio and Zendesk signature checks.
- `pii.py`: card and SSN masking.
- `tracing.py`: OpenTelemetry spans, off unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
- `convoflow.py`: queue, workers, tenant limits, channel entitlements and the scanner.
- `jobs.py`: migrations, the hourly scan and retention purge, the CloudWatch metrics publisher, and key, webhook and tenant commands.
- `migrations/`: versioned schema changes, each applied once.
- `infra/`: Terraform for the network, load balancer, ECS Fargate, RDS, secrets, autoscaling, the hourly schedule, alarms and the deploy role.

## Run it

    .venv/bin/python -m pytest -q
    .venv/bin/python loadtest.py burst --items 1000000
    .venv/bin/python loadtest.py soak --rate 15 --hours 24
    CONVOFLOW_KEY=... .venv/bin/python loadtest_http.py https://your-host --events 150000

## Operate it

    python jobs.py issue-key TENANT [DAILY_LIMIT] [LABEL]     # prints the key once
    python jobs.py list-keys TENANT
    python jobs.py revoke-key KEY_ID
    python jobs.py set-webhook TENANT twilio|zendesk SECRET
    python jobs.py delete-tenant TENANT --confirm

Twilio: point the messaging webhook at `/v1/webhooks/twilio/TENANT` and store the account auth token with `set-webhook`. Behind a proxy that rewrites the host, set `CONVOFLOW_PUBLIC_URL` so the signature is checked against the URL Twilio called.

Zendesk: create a webhook to `/v1/webhooks/zendesk/TENANT` with signing on, store its signing secret, and use this body in the trigger: `{"ticket": {"id": "{{ticket.id}}"}, "comment": {"body": "{{ticket.latest_comment}}"}}`.

Production needs `CONVOFLOW_PII_KEY` set to the same random value on every API and worker.

## Measured

Traffic is simulated and runs on one laptop. `results/` holds each run's audit.

24-hour soak at 15 items a second across 20 tenants, with 3 workers killed mid-run (`results/soak-20261106T203352Z.json`):

| Measure | Result |
| --- | --- |
| Items produced | 1,295,999 |
| Stored / duplicates / dead letters | 1,240,513 / 25,613 / 29,873 |
| Left in queue | 0 |
| Planted violations found | 6,112 of 6,112 |
| Latency p50 / p95 / p99 | 0.088s / 0.186s / 0.200s |
| Every item accounted for | pass |
| Capped tenant held to its limit | held: 5,000 on each of the 2 days the run crossed (10,000 total) |

The soak's original check reported a failure here. The quota held. The check compared the whole run against one day's limit, and the run crossed midnight. Looking into it found a real bug: each worker took "today" from its own clock, so workers in different time zones could disagree about the quota day. The day now comes from the database in UTC, and the audit checks each day separately.

Over the network, against the Railway deploy with 3 worker replicas, from one laptop in California (`results/http-*.json`):

| Run | Events | Result |
| --- | --- | --- |
| 16 connections | 20,000 | all stored, HTTP p99 0.19s, about 2,600 events a second end to end |
| 32 connections | 60,000 | all stored, HTTP p99 0.28s, about 3,650 events a second |
| 8 connections, every worker replica replaced mid-run | 150,000 | all stored, none lost or duplicated, HTTP p99 0.19s |

The replacement was a Railway redeploy, which sends SIGTERM, so it is a graceful shutdown. The hard-kill case (SIGKILL mid-batch) is covered by the local runs above.

Noisy neighbor, one run each with 300,000 items queued by one tenant and 300 arriving from another (`noisy.py`):

| Claiming | Quiet tenant p50 | Quiet tenant p95 |
| --- | --- | --- |
| Oldest first | 3.77s | 4.65s |
| Fair share | 0.03s | 0.05s |

Fair claiming costs throughput: the 1M burst fell from about 47,800 items a second to about 40,800.

## Status

- Live on Railway with two services: `api` and a `worker-with-scan` (`CONVOFLOW_ROLE` picks the process; ECS passes its own commands).
- On AWS only the ECR repository and an ARM64 image exist. The full `terraform apply` has not been run.
- Workers have run as 3 replicas on separate machines against one Postgres. Postgres itself is a single instance; a standby exists only with `production=true` on AWS.
- Email has no signed adapter yet (SendGrid Inbound Parse has no signature, so it would need a secret URL or IP allowlist). Voice takes text segments; there is no speech-to-text.
- The deploy workflow has never run. See `RUNBOOK.md`.
