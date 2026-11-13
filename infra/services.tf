data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${local.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "read_secrets" {
  name = "read-secrets"
  role = aws_iam_role.execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = ["secretsmanager:GetSecretValue"]
      Effect = "Allow"
      Resource = [
        aws_secretsmanager_secret.admin_dsn.arn,
        aws_secretsmanager_secret.app_dsn.arn,
        aws_secretsmanager_secret.app_password.arn,
      ]
    }]
  })
}

resource "aws_iam_role" "task" {
  name               = "${local.name}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy" "put_metrics" {
  name = "put-metrics"
  role = aws_iam_role.task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = ["cloudwatch:PutMetricData"]
      Effect    = "Allow"
      Resource  = "*"
      Condition = { StringEquals = { "cloudwatch:namespace" = "Convoflow" } }
    }]
  })
}

locals {
  containers = {
    api     = { command = ["python", "api.py"], secrets = local.app_secrets }
    worker  = { command = ["python", "-c", "import convoflow; convoflow.run_worker()"], secrets = local.app_secrets }
    monitor = { command = ["python", "jobs.py", "monitor"], secrets = local.app_secrets }
    migrate = { command = ["python", "jobs.py", "migrate"], secrets = local.job_secrets }
    hourly  = { command = ["python", "jobs.py", "hourly"], secrets = local.job_secrets }
  }
}

resource "aws_ecs_task_definition" "all" {
  for_each                 = local.containers
  family                   = "${local.name}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }
  execution_role_arn = aws_iam_role.execution.arn
  task_role_arn      = aws_iam_role.task.arn
  container_definitions = jsonencode([{
    name         = each.key
    image        = local.image
    command      = each.value.command
    stopTimeout  = 60
    secrets      = each.value.secrets
    environment  = [{ name = "AWS_DEFAULT_REGION", value = var.region }]
    portMappings = each.key == "api" ? [{ containerPort = 8080 }] : []
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.main.name
        awslogs-region        = var.region
        awslogs-stream-prefix = each.key
      }
    }
  }])
}

resource "aws_lb" "main" {
  name               = local.name
  load_balancer_type = "application"
  subnets            = aws_subnet.public[*].id
  security_groups    = [aws_security_group.alb.id]
  idle_timeout       = 30
}

resource "aws_lb_target_group" "api" {
  name        = "${local.name}-api"
  port        = 8080
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_vpc.main.id

  deregistration_delay = 30

  health_check {
    path                = "/health"
    interval            = 15
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "https" {
  count             = var.certificate_arn == "" ? 0 : 1
  load_balancer_arn = aws_lb.main.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = var.certificate_arn

  default_action {
    type = "fixed-response"
    fixed_response {
      content_type = "application/json"
      message_body = "{\"error\":\"not_found\"}"
      status_code  = "404"
    }
  }
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.main.arn
  port              = 80
  protocol          = "HTTP"

  dynamic "default_action" {
    for_each = var.certificate_arn == "" ? [] : [1]
    content {
      type = "redirect"
      redirect {
        port        = "443"
        protocol    = "HTTPS"
        status_code = "HTTP_301"
      }
    }
  }

  dynamic "default_action" {
    for_each = var.certificate_arn == "" ? [1] : []
    content {
      type = "fixed-response"
      fixed_response {
        content_type = "application/json"
        message_body = "{\"error\":\"not_found\"}"
        status_code  = "404"
      }
    }
  }
}

# Only ingest and health are public. /metrics and /alerts stay inside the network.
resource "aws_lb_listener_rule" "public_paths" {
  listener_arn = var.certificate_arn == "" ? aws_lb_listener.http.arn : aws_lb_listener.https[0].arn

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api.arn
  }

  condition {
    path_pattern {
      values = ["/v1/*", "/health"]
    }
  }
}

resource "aws_ecs_service" "api" {
  name            = "api"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.all["api"].arn
  desired_count   = var.production ? 2 : 1
  launch_type     = "FARGATE"

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "api"
    container_port   = 8080
  }

  network_configuration {
    subnets          = local.task_subnets
    security_groups  = [aws_security_group.app.id]
    assign_public_ip = !var.production
  }

  depends_on = [aws_lb_listener_rule.public_paths]
}

resource "aws_ecs_service" "worker" {
  name            = "worker"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.all["worker"].arn
  desired_count   = var.worker_min
  launch_type     = "FARGATE"

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = local.task_subnets
    security_groups  = [aws_security_group.app.id]
    assign_public_ip = !var.production
  }

  lifecycle {
    ignore_changes = [desired_count]
  }
}

resource "aws_ecs_service" "monitor" {
  name            = "monitor"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.all["monitor"].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = local.task_subnets
    security_groups  = [aws_security_group.app.id]
    assign_public_ip = !var.production
  }
}

resource "aws_appautoscaling_target" "worker" {
  service_namespace  = "ecs"
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.worker.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  min_capacity       = var.worker_min
  max_capacity       = var.worker_max
}

resource "aws_appautoscaling_policy" "worker_cpu" {
  name               = "${local.name}-worker-cpu"
  service_namespace  = aws_appautoscaling_target.worker.service_namespace
  resource_id        = aws_appautoscaling_target.worker.resource_id
  scalable_dimension = aws_appautoscaling_target.worker.scalable_dimension
  policy_type        = "TargetTrackingScaling"

  target_tracking_scaling_policy_configuration {
    target_value = 60
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
  }
}

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.name}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

resource "aws_iam_role_policy" "scheduler" {
  name = "run-hourly"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Action = ["ecs:RunTask"], Effect = "Allow", Resource = "${replace(aws_ecs_task_definition.all["hourly"].arn, "/:\\d+$/", "")}:*" },
      { Action = ["iam:PassRole"], Effect = "Allow", Resource = [aws_iam_role.task.arn, aws_iam_role.execution.arn] },
    ]
  })
}

resource "aws_scheduler_schedule" "hourly" {
  name                = "${local.name}-hourly"
  schedule_expression = "rate(1 hour)"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_ecs_cluster.main.arn
    role_arn = aws_iam_role.scheduler.arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.all["hourly"].arn
      launch_type         = "FARGATE"

      network_configuration {
        subnets          = local.task_subnets
        security_groups  = [aws_security_group.app.id]
        assign_public_ip = !var.production
      }
    }
  }
}
