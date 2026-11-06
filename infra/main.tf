terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "me" {}

locals {
  name         = "convoflow"
  zones        = slice(data.aws_availability_zones.available.names, 0, 2)
  image        = "${aws_ecr_repository.app.repository_url}:${var.image_tag}"
  task_subnets = var.production ? aws_subnet.private[*].id : aws_subnet.public[*].id
  app_secrets = [
    { name = "CONVOFLOW_APP_DSN", valueFrom = aws_secretsmanager_secret.app_dsn.arn },
  ]
  job_secrets = [
    { name = "CONVOFLOW_ADMIN_DSN", valueFrom = aws_secretsmanager_secret.admin_dsn.arn },
    { name = "CONVOFLOW_APP_DSN", valueFrom = aws_secretsmanager_secret.app_dsn.arn },
    { name = "CONVOFLOW_APP_PASSWORD", valueFrom = aws_secretsmanager_secret.app_password.arn },
  ]
}

resource "aws_ecr_repository" "app" {
  name                 = local.name
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "app" {
  repository = aws_ecr_repository.app.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the last 20 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 20
      }
      action = { type = "expire" }
    }]
  })
}

resource "aws_cloudwatch_log_group" "main" {
  name              = "/ecs/${local.name}"
  retention_in_days = 30
}

resource "aws_ecs_cluster" "main" {
  name = local.name

  setting {
    name  = "containerInsights"
    value = var.production ? "enabled" : "disabled"
  }
}
