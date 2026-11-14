# Service levels

Targets for a paid deployment. Each one names the measurement and the alert that fires before the target is missed. None of these are contractual yet; they are what the system is built and alarmed to meet.

| Objective | Target (30 days) | Measured by | Alert |
| --- | --- | --- | --- |
| Ingest availability | 99.9% of `POST /v1/events` and webhook requests get a 2xx, 400, 401 or 429 (not 5xx or timeout) | ALB `HTTPCode_Target_5XX_Count` over `RequestCount` (AWS); load-test status codes (Railway) | `api-5xx` alarm |
| Ingest latency | p99 under 300 ms | ALB `TargetResponseTime` p99; `loadtest_http.py` | none yet; measured 0.19 to 0.28 s p99 over HTTPS |
| Time to stored | 99% of accepted events stored within 60 s | `convoflow_queue_lag_seconds` | `queue_lag_seconds > 60` (ops page + CloudWatch `queue-lag`) |
| No loss | 0 accepted events lost | load-test audits: stored + duplicates + dead letters = accepted | audit check in every load run |
| Scan freshness | every stored message scanned within 2 hours | `convoflow_scan_overdue_seconds` | `scan_overdue_seconds > 7200` |
| Bad-row rate | under 5% of rows dead-lettered over 5 minutes | `convoflow_dead_letter_ratio_5m` | `dead_letter_ratio_5m > 0.05` |
| Alert delivery | findings at a tenant's minimum severity delivered within 2 hours, retried for 24 | `findings.notified_at - found_at` | `alert_failed` log events |

Error budget: 99.9% availability allows about 43 minutes of failed ingest a month. When a month's budget is spent, feature work stops until the cause is fixed.

## Data handling

- **Where data lives:** one Postgres database in the region the deployment runs in (Railway US West for the demo; `us-east-2` in the AWS Terraform). Per-region deployments are the way to offer residency; there is no cross-region replication.
- **What is stored:** message text with card numbers and SSNs masked, findings with masked evidence, dead letters (masked), and the audit log. Raw card numbers and SSNs are never stored.
- **What leaves:** with model rules on, masked message text is sent to the Anthropic API. With them off, nothing leaves except customer alerts to the URL the customer configured.
- **Retention:** messages and findings per tenant (`retention_days`, default 365), dead letters 90 days, backups the last 7.
- **Deletion:** `delete-tenant` erases a tenant's data in one transaction and records the deletion. Backups taken before the deletion still hold the data until they age out.
- **Access record:** `audit_log` records key issue and revoke, secret changes, tenant deletion and every findings read by API key.
