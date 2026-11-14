# 4. The quota day comes from the database, in UTC

Status: accepted

## Context
The 24-hour soak reported a capped tenant at 10,000 rows against a 5,000 daily limit. The quota had held: the run crossed midnight, so it covered two quota days. The audit compared the whole run to one day's limit. While tracing it, a real bug turned up: each worker used `date.today()`, its own local date. Workers in different time zones could disagree about which day a row counts against.

## Decision
`store_for_tenant` takes the day from `(now() AT TIME ZONE 'UTC')::date` in the same transaction that updates usage. The soak audit checks each day's usage against the limit.

## Consequences
- Every worker agrees on the quota day, whatever its clock or time zone.
- Quotas reset at midnight UTC, which is not midnight for most customers. A per-tenant reset time would be a later change.
- `test_quota_day_comes_from_the_database_not_the_worker_clock` fails on the old code.
