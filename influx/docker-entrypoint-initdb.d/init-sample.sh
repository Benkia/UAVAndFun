#!/usr/bin/env bash
set -euo pipefail

# Wait briefly for the API to be ready inside the container
until influx ping >/dev/null 2>&1; do
  sleep 1
done

# Build sample data with second-level timestamps within the last hour (well
# inside the 72h retention) so the write always succeeds.
now=$(date +%s)
t0=$((now - 3600))
t1=$((now - 1800))
t2=$((now - 600))

cat >/tmp/sample-data.lp <<EOF
temperature,sensor=livingroom value=23.5 ${t0}
temperature,sensor=livingroom value=23.8 ${t1}
temperature,sensor=livingroom value=24.1 ${t2}
temperature,sensor=bedroom value=21.8 ${t0}
temperature,sensor=bedroom value=22.0 ${t1}
temperature,sensor=bedroom value=22.2 ${t2}
humidity,sensor=livingroom value=41.2 ${t0}
humidity,sensor=livingroom value=42.0 ${t1}
humidity,sensor=livingroom value=42.5 ${t2}
EOF

# Write sample line protocol into the default bucket created by init env vars
influx write \
  --bucket "${DOCKER_INFLUXDB_INIT_BUCKET}" \
  --precision s \
  --file /tmp/sample-data.lp

# Write sample line protocol into the default bucket created by init env vars
influx write \
  --bucket "${DOCKER_INFLUXDB_INIT_BUCKET}" \
  --precision ms \
  --file /docker-entrypoint-initdb.d/out.lp