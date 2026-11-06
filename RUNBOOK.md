# Runbook

## First deploy

1. `cd infra && terraform init`
2. Create only the registry: `terraform apply -target=aws_ecr_repository.app -var alert_email=YOU`
3. Build and push the first image, tagged `bootstrap`, from a machine or workflow with Docker.
4. Review `terraform plan -var alert_email=YOU`, then apply it.
5. Confirm the SNS subscription email.
6. In GitHub repo variables, set `AWS_DEPLOY_ROLE_ARN`, `TASK_SUBNETS` and `TASK_SECURITY_GROUP` from the outputs. With `production=true`, also set `ASSIGN_PUBLIC_IP` to `DISABLED`.
7. Run the `deploy` workflow. It runs migrations, then rolls the services.

Set `production=true` and `certificate_arn` for real customers. Without them this is a test footprint with no TLS and no standby database.

## Add a customer

Run `python jobs.py issue-key TENANT DAILY_LIMIT` as a one-off ECS task with the migrate task's secrets. The key prints once, and only its hash is stored. Running it again replaces the key.

## Rollback

Run the `deploy` workflow with `image_tag` set to the last good commit SHA. Migrations must stay backward compatible, because rollback does not undo them.

## Alerts

| Alarm | First check |
| --- | --- |
| queue-lag | Are workers running? Check the worker logs and scale `worker_max`. |
| dead-letters | Read `reason` in `dead_letters`. Fix the cause, then `replay(reason)`. |
| scan-overdue | Check the `hourly` task logs and the schedule. |
| api-5xx | Check the `api` logs and database connections. |
| db-cpu, db-storage | Check for a backlog, then resize. |

## Restore

RDS keeps automated backups for 7 days in production. Restore to a new instance, point the DSN secrets at it, and redeploy.

## Teardown

`terraform destroy -var alert_email=YOU`. With `production=true`, turn off `deletion_protection` first.
