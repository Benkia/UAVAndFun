#!/usr/bin/env python
"""
Simple FastAPI receiver for InfluxDB notification endpoints.

Exposes a POST webhook that accepts InfluxDB alerts and logs them to both
stdout and a rotating file (`logs/influx_alerts.log`).
"""
from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field


LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "influx_alerts.log"

# Configure a dedicated logger to avoid interfering with root logging config.
logger = logging.getLogger("influx_alert_receiver")
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)


class AlertPayload(BaseModel):
    """Minimal fields commonly sent by InfluxDB notification endpoints."""

    status: Optional[str] = Field(None, description="Alert status (e.g., firing, resolved).")
    rule: Optional[str] = Field(None, alias="notificationRuleName")
    check: Optional[str] = Field(None, alias="checkName")
    message: Optional[str] = Field(None, description="Human-readable message.")
    source_timestamp: Optional[str] = Field(None, alias="sourceTimestamp")
    id: Optional[str] = Field(None, description="Notification id.")
    data: Dict[str, Any] = Field(default_factory=dict, description="Extra data block from Influx.")

    class Config:
        allow_population_by_field_name = True
        extra = "allow"


app = FastAPI(title="Influx Alert Receiver", version="1.0.0")


def _log_alert(alert: AlertPayload, raw_body: Dict[str, Any]) -> None:
    """Write a concise line plus the full JSON body to the log sinks."""
    summary = (
        f"status={alert.status or 'unknown'} "
        f"rule={alert.rule or '-'} "
        f"check={alert.check or '-'} "
        f"msg={alert.message or '-'}"
    )
    logger.info("Influx alert received | %s", summary)
    logger.info("Full payload: %s", json.dumps(raw_body, ensure_ascii=False))


@app.get("/healthz")
async def healthcheck() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/alerts/influx")
async def receive_alert(
    request: Request,
    alert: AlertPayload,
    background_tasks: BackgroundTasks,
) -> Dict[str, str]:
    try:
        raw_body = await request.json()
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("Failed to parse alert body")
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}") from exc

    background_tasks.add_task(_log_alert, alert, raw_body)
    return {"status": "accepted"}


# Enable `python scripts/influx_alert_api.py` for quick local runs.
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("influx_alert_api:app", host="0.0.0.0", port=9000, reload=False)

