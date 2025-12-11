FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install Python deps (alert API only: FastAPI + uvicorn).
COPY alert-api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code.
COPY alert-api/ /app/alert-api/

# Ensure log directory exists (also backed by a volume in docker-compose).
RUN mkdir -p /app/logs

EXPOSE 9000

CMD ["uvicorn", "influx_alert_api:app", "--host", "0.0.0.0", "--port", "9000", "--app-dir", "/app/alert-api"]

