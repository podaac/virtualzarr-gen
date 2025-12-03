#!/bin/bash

# 1. Put placeholder cluster so ECS agent does nothing
echo "ECS_CLUSTER=__DELAY_CLUSTER__" > /etc/ecs/ecs.config

# 2. Wait your delay (3 minutes in your case)
sleep 180

# 3. Replace with real cluster name
echo "ECS_CLUSTER=${ecs_cluster}" > /etc/ecs/ecs.config

# 4. Restart ECS agent cleanly
sudo systemctl restart ecs
