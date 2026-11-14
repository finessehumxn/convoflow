# Keys the application needs on every task. Generated once and kept in Secrets Manager.

resource "random_password" "pii_key" {
  length  = 64
  special = false
}

resource "random_password" "metrics_token" {
  length  = 48
  special = false
}

# A Fernet key is 32 random bytes in base64. secretbox.py accepts standard or url-safe base64.
resource "random_bytes" "secrets_key" {
  length = 32
}

resource "aws_secretsmanager_secret" "pii_key" {
  name                    = "${local.name}/pii-key"
  recovery_window_in_days = var.production ? 30 : 0
}

resource "aws_secretsmanager_secret_version" "pii_key" {
  secret_id     = aws_secretsmanager_secret.pii_key.id
  secret_string = random_password.pii_key.result
}

resource "aws_secretsmanager_secret" "secrets_keys" {
  name                    = "${local.name}/secrets-keys"
  recovery_window_in_days = var.production ? 30 : 0
}

resource "aws_secretsmanager_secret_version" "secrets_keys" {
  secret_id     = aws_secretsmanager_secret.secrets_keys.id
  secret_string = random_bytes.secrets_key.base64
}

resource "aws_secretsmanager_secret" "metrics_token" {
  name                    = "${local.name}/metrics-token"
  recovery_window_in_days = var.production ? 30 : 0
}

resource "aws_secretsmanager_secret_version" "metrics_token" {
  secret_id     = aws_secretsmanager_secret.metrics_token.id
  secret_string = random_password.metrics_token.result
}

locals {
  shared_secrets = [
    { name = "CONVOFLOW_PII_KEY", valueFrom = aws_secretsmanager_secret.pii_key.arn },
    { name = "CONVOFLOW_SECRETS_KEYS", valueFrom = aws_secretsmanager_secret.secrets_keys.arn },
    { name = "CONVOFLOW_METRICS_TOKEN", valueFrom = aws_secretsmanager_secret.metrics_token.arn },
  ]
}
