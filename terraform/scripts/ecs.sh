#!/bin/bash
echo "ECS_CLUSTER=${ecs_cluster}" > /etc/ecs/ecs.config

systemctl stop ecs

# Wait 2 minutes
sleep 180

# Start ECS agent
systemctl start ecs
