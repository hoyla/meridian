# Meridian portal — Cloud Run service. Scales to zero; reads published
# snapshots from GCS (PORTAL_BUCKET). IAP sits in front (no load balancer).
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py ./

# Cloud Run injects $PORT (default 8080); bind to it.
ENV PORT=8080
CMD ["sh", "-c", "exec uvicorn app:app --host 0.0.0.0 --port ${PORT}"]
