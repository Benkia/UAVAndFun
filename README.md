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

## Receive Influx alerts via webhook
Run a small FastAPI server that accepts InfluxDB notification webhooks and logs them to `logs/influx_alerts.log` (plus stdout):
```bash
# install deps (inside your venv)
pip install -r requirements.txt

# start the receiver from the repo root (file path run)
python alert-api/influx_alert_api.py
# or with uvicorn (module import)
uvicorn influx_alert_api:app --app-dir alert-api --host 0.0.0.0 --port 9000
```

Configure your InfluxDB notification endpoint to point at `http://localhost:9000/alerts/influx`. A simple test payload:
```bash
curl -X POST http://localhost:9000/alerts/influx \
  -H "Content-Type: application/json" \
  -d '{"status":"firing","notificationRuleName":"example","checkName":"temp high","message":"Over threshold","sourceTimestamp":"2025-01-01T00:00:00Z"}'
```

Quick health check:
```bash
curl http://localhost:9000/healthz
```

Send a minimal test alert (single line curl):
```bash
curl -X POST http://localhost:9000/alerts/influx -H "Content-Type: application/json" -d '{"status":"firing","notificationRuleName":"example","checkName":"temp high","message":"Over threshold","sourceTimestamp":"2025-01-01T00:00:00Z"}'
```

Health probe: `GET http://localhost:9000/healthz`.

