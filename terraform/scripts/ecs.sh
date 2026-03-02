#!/bin/bash
set -e

# --- Swap setup ---
SWAPFILE=/swapfile
SWAPSIZE=256G   # swap size (adjust as needed)

# create swap file
fallocate -l $SWAPSIZE $SWAPFILE
chmod 600 $SWAPFILE
mkswap $SWAPFILE
swapon $SWAPFILE

# persist swap across reboots
echo "$SWAPFILE swap swap defaults 0 0" >> /etc/fstab

# --- ECS agent bootstrap (your existing stuff) ---
echo "ECS_CLUSTER=${ecs_cluster}" > /etc/ecs/ecs.config
