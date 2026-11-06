resource "random_password" "db" {
  length  = 32
  special = false
}

resource "random_password" "app" {
  length  = 32
  special = false
}

resource "aws_db_subnet_group" "main" {
  name       = local.name
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_db_parameter_group" "main" {
  name   = local.name
  family = "postgres17"

  parameter {
    name  = "rds.force_ssl"
    value = "1"
  }
}

resource "aws_db_instance" "main" {
  identifier                = local.name
  engine                    = "postgres"
  engine_version            = "17"
  instance_class            = var.db_instance_class
  allocated_storage         = 20
  max_allocated_storage     = 200
  storage_type              = "gp3"
  storage_encrypted         = true
  db_name                   = local.name
  username                  = local.name
  password                  = random_password.db.result
  db_subnet_group_name      = aws_db_subnet_group.main.name
  parameter_group_name      = aws_db_parameter_group.main.name
  vpc_security_group_ids    = [aws_security_group.db.id]
  multi_az                  = var.production
  backup_retention_period   = var.production ? 7 : 1
  deletion_protection       = var.production
  skip_final_snapshot       = !var.production
  final_snapshot_identifier = var.production ? "${local.name}-final" : null
}

resource "aws_secretsmanager_secret" "admin_dsn" {
  name                    = "${local.name}/admin-dsn"
  recovery_window_in_days = var.production ? 30 : 0
}

resource "aws_secretsmanager_secret_version" "admin_dsn" {
  secret_id     = aws_secretsmanager_secret.admin_dsn.id
  secret_string = "host=${aws_db_instance.main.address} dbname=${local.name} user=${local.name} password=${random_password.db.result} sslmode=require"
}

resource "aws_secretsmanager_secret" "app_dsn" {
  name                    = "${local.name}/app-dsn"
  recovery_window_in_days = var.production ? 30 : 0
}

resource "aws_secretsmanager_secret_version" "app_dsn" {
  secret_id     = aws_secretsmanager_secret.app_dsn.id
  secret_string = "host=${aws_db_instance.main.address} dbname=${local.name} user=app password=${random_password.app.result} sslmode=require"
}

resource "aws_secretsmanager_secret" "app_password" {
  name                    = "${local.name}/app-password"
  recovery_window_in_days = var.production ? 30 : 0
}

resource "aws_secretsmanager_secret_version" "app_password" {
  secret_id     = aws_secretsmanager_secret.app_password.id
  secret_string = random_password.app.result
}
