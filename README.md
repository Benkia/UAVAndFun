# InfluxDB + Grafana stack

This repo includes a minimal InfluxDB 2 + Grafana setup with sample line protocol data pre-loaded.

## Run the stack

```bash
cd /Users/benkiani/work/mav-analytics
docker-compose up --build
```

Services:
- InfluxDB: http://localhost:8086 (username `admin`, password `admin123`, token `supersecrettoken`, org `mav-org`, bucket `sample-bucket`)
- Grafana: http://localhost:3000 (username `admin`, password `admin123`)

## What it does
- Builds a custom InfluxDB image that copies an init script. On first start the script generates sample points with second-level timestamps from the last hour (within the 72h retention) and writes them into the `sample-bucket` bucket.
- Provisions Grafana with an InfluxDB data source (Flux) and a simple dashboard `Sample Temperature` showing the seeded measurements.

## Import DataFlash IMU/VIBE into Influx
Convert a DataFlash log to line protocol and write it into the existing bucket:
```bash
# activate the venv if needed
source .venv/bin/activate

# generate line protocol (IMU + VIBE) to stdout and pipe into influx
python scripts/dataflash_to_influx.py \
  --input input-files/ekf-lane-switch-and-yaw-reset-during-acceleration-log-3.bin \
  --precision ms \
  --output - \
  | docker exec -i mav-influx influx write \
      --org mav-org --bucket sample-bucket --precision ms --file -
```
The script focuses on accelerometer (IMU) and vibration (VIBE) messages, anchoring the first `TimeUS` to the current wall-clock to keep points inside retention. Default output precision is milliseconds to avoid timestamp range issues; pass `--precision ns` if you prefer nanoseconds (remember to match the influx write flag).

## Notes
- Data is persisted in docker volumes (`influx-data`, `grafana-data`). To reset everything, stop containers and run `docker volume rm mav-analytics_influx-data mav-analytics_grafana-data`.
- Adjust credentials/tokens in `docker-compose.yml` if you need something different.

