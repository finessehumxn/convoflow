# 1. Postgres is the queue

Status: accepted

## Context
Messages arrive from four channels and must be stored exactly once per tenant, even when a worker dies mid-batch. A separate broker (SQS, Kafka, RabbitMQ) would mean the claim and the write happen in two systems, which needs an outbox or idempotent consumers to stay correct.

## Decision
The inbox is a Postgres table. Workers claim with `DELETE ... FOR UPDATE SKIP LOCKED` and write the results in the same transaction. A crash rolls the claim back and the rows return to the queue.

## Consequences
- One transaction covers claim, dedupe, quota and write, so there is no partial failure to reconcile.
- Measured on one machine: about 40,000 items a second in a 1M burst, and 1.3M items over 24 hours with 3 workers killed and none lost.
- The inbox is insert-then-delete, so it creates dead rows. Autovacuum runs on a fixed row count (migration 0003) and `convoflow_inbox_dead_tuples` is on `/metrics`.
- Past a few hundred thousand writes a second, or with many independent consumers, a log-based broker becomes the better fit. The `enqueue` and `claim` functions are the seam where one would go.
