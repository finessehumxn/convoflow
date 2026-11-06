output "api_url" {
  value = "${var.certificate_arn == "" ? "http" : "https"}://${aws_lb.main.dns_name}"
}

output "ecr_repository" {
  value = aws_ecr_repository.app.repository_url
}

output "deploy_role_arn" {
  value = aws_iam_role.deploy.arn
}

output "task_subnets" {
  value = local.task_subnets
}

output "task_security_group" {
  value = aws_security_group.app.id
}

output "account_id" {
  value = data.aws_caller_identity.me.account_id
}
