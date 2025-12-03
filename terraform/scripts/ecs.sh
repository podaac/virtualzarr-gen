#!/bin/bash
echo "ECS_CLUSTER=${ecs_cluster}" > /etc/ecs/ecs.config

sudo systemctl stop ecs

sleep 180

sudo systemctl start ecs
