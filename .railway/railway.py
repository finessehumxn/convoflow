"""Railway infrastructure for the live demo (api, worker, Postgres, two volumes).

Secrets are preserve(): their values live in Railway and are never written here.
Health checks: Railway keeps the old deployment serving until the new one answers
/health, so a slow migration at boot does not turn into 502s. The worker answers
/health on CONVOFLOW_HEALTH_PORT once its claim loop is running.

Preview with `railway config plan`, apply with `railway config apply`.
"""
from railway_sdk import define_railway, postgres, preserve, project, service, volume

VOLUME = {"alerts": {"usage": {"80": {}, "95": {}, "100": {}}}, "allowOnlineResize": True,
          "region": "sfo", "sizeMB": 50000}


@define_railway
def main(ctx=None):
    db = postgres("Postgres", region="sfo")
    postgres_volume = volume("postgres-volume", VOLUME)
    worker_volume = volume("worker-volume", VOLUME)
    worker = service(
        "worker",
        replicas={"sfo": 1},  # one replica: a service with a volume cannot scale out
        volumeMounts={"/backups": worker_volume},
        healthcheck="/health",
        env={k: preserve() for k in (
            "CONVOFLOW_ADMIN_DSN", "CONVOFLOW_APP_DSN", "CONVOFLOW_APP_PASSWORD", "CONVOFLOW_BACKUP_DIR",
            "CONVOFLOW_HEALTH_PORT", "CONVOFLOW_PII_KEY", "CONVOFLOW_ROLE", "CONVOFLOW_SECRETS_KEYS",
            "PORT", "RAILWAY_RUN_UID")},
    )
    api = service(
        "api",
        replicas={"sfo": 1},
        healthcheck="/health",
        env={k: preserve() for k in (
            "CONVOFLOW_ADMIN_DSN", "CONVOFLOW_APP_DSN", "CONVOFLOW_APP_PASSWORD", "CONVOFLOW_DEMO_TENANT",
            "CONVOFLOW_METRICS_TOKEN", "CONVOFLOW_PII_KEY", "CONVOFLOW_ROLE", "CONVOFLOW_SECRETS_KEYS",
            "CONVOFLOW_SEED_KEY", "CONVOFLOW_SEED_LIMIT", "CONVOFLOW_SEED_TENANT")},
    )
    return project("convoflow", resources=[worker, db, api, postgres_volume, worker_volume])
