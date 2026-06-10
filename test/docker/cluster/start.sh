#!/bin/sh

# Start 9 Redis instances for a cluster (3 primaries + 6 replicas, 2 per primary)
for port in 7000 7001 7002 7003 7004 7005 7006 7007 7008; do
  redis-server \
    --port $port \
    --cluster-enabled yes \
    --cluster-config-file nodes-${port}.conf \
    --cluster-node-timeout 5000 \
    --appendonly no \
    --daemonize yes \
    --logfile /var/log/redis-${port}.log \
    --bind 0.0.0.0 \
    --protected-mode no
done

# Wait for all instances to be ready
sleep 2

# Create the cluster with 3 primaries and 6 replicas (2 per primary, so every slot
# has more than one replica, needed to exercise replica read load-spreading).
# Using 127.0.0.1 so clients can connect from the host
yes "yes" | redis-cli --cluster create \
  127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
  127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 \
  127.0.0.1:7006 127.0.0.1:7007 127.0.0.1:7008 \
  --cluster-replicas 2

echo "Redis cluster started"

# Keep the container running
tail -f /var/log/redis-7000.log
