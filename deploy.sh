#!/usr/bin/env bash
# Registers new task definitions for $IMAGE, runs migrations, then rolls each service.
set -euo pipefail

register() {
  aws ecs describe-task-definition --task-definition "convoflow-$1" --query taskDefinition --output json \
    | jq --arg image "$IMAGE" '.containerDefinitions[0].image = $image
        | {family, containerDefinitions, executionRoleArn, taskRoleArn, networkMode, requiresCompatibilities, cpu, memory}' \
    > "/tmp/task-$1.json"
  aws ecs register-task-definition --cli-input-json "file:///tmp/task-$1.json" \
    --query taskDefinition.taskDefinitionArn --output text
}

migrate_arn=$(register migrate)
network="awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SECURITY_GROUP],assignPublicIp=${ASSIGN_PUBLIC_IP:-ENABLED}}"
task=$(aws ecs run-task --cluster "$CLUSTER" --launch-type FARGATE --task-definition "$migrate_arn" \
  --network-configuration "$network" --query 'tasks[0].taskArn' --output text)
aws ecs wait tasks-stopped --cluster "$CLUSTER" --tasks "$task"
code=$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$task" --query 'tasks[0].containers[0].exitCode' --output text)
if [ "$code" != "0" ]; then
  echo "Migration failed with exit code $code. Services were not updated." >&2
  exit 1
fi

register hourly > /dev/null
for service in api worker monitor; do
  arn=$(register "$service")
  aws ecs update-service --cluster "$CLUSTER" --service "$service" --task-definition "$arn" > /dev/null
done
aws ecs wait services-stable --cluster "$CLUSTER" --services api worker monitor
echo "Deployed $IMAGE"
