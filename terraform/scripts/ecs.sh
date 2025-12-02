#!/bin/bash
echo "ECS_CLUSTER=${ecs_cluster}" >> /etc/ecs/ecs.config

# Start ECS agent in a paused state or delay it
systemctl stop ecs

# Or delay the start
sleep 300 && systemctl start ecs