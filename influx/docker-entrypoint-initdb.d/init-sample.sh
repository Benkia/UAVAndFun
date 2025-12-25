#!/usr/bin/env bash
set -euo pipefail

# Wait briefly for the API to be ready inside the container
until influx ping >/dev/null 2>&1; do
  sleep 1
done


# Write sample line protocol into the default bucket created by init env vars
influx write \
  --bucket "${DOCKER_INFLUXDB_INIT_BUCKET}" \
  --precision us \
  --file /docker-entrypoint-initdb.d/out.lp