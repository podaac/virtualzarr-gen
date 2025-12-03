#!/bin/bash
echo "ECS_CLUSTER=${ecs_cluster}" > /etc/ecs/ecs.config

sudo systemctl stop ecs

# Wait 2 minutes
sleep 120

# Start ECS agent
sudo systemctl start ecs
