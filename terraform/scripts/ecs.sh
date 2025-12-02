#!/bin/bash
echo "ECS_CLUSTER=${ecs_cluster}" >> /etc/ecs/ecs.config

# Stop the agent if it auto-started
systemctl stop ecs

# Wait 5 minutes (300 seconds) before starting
sleep 180

# Now start the ECS agent
systemctl start ecs