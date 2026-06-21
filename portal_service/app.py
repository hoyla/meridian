"""Cloud Run portal service for Meridian.

Serves the latest published report snapshot from a GCS bucket, behind IAP
(Identity-Aware Proxy) with a Guardian-domain allow-list. Read-only v1: serves
the pre-rendered `index.html`; `report.json` (the canonical published artefact)
is exposed too.

Snapshots are produced by the laptop pipeline and pushed to the bucket
(`scrape.py --upload-to-portal`, see `portal_publish.py`). The live analytical
Postgres is **never** a dependency here — the portal only reads published
snapshots, so the laptop is never a cloud dependency.

Auth is handled by IAP in front of the service (the app sees only already-
authenticated requests); see portal_service/README.md.

Config (env):
  PORTAL_BUCKET   GCS bucket holding latest/{index.html,report.json}
"""
from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.gzip import GZipMiddleware
from google.cloud import storage

BUCKET = os.environ.get("PORTAL_BUCKET", "")
app = FastAPI(title="Meridian portal")
# The rendered index.html and report.json are large (hundreds of KB of text);
# gzip cuts the transfer ~5-6× — Cloud Run does not compress for us.
app.add_middleware(GZipMiddleware, minimum_size=1024)
_storage = storage.Client() if BUCKET else None

_XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _read(path: str) -> bytes | None:
    """Read a blob from the snapshot bucket, or None if absent/unconfigured.
    Read per request — low traffic + scale-to-zero make caching unnecessary
    for v1 (add a short in-memory TTL later if cold reads ever bite)."""
    if not _storage:
        return None
    blob = _storage.bucket(BUCKET).blob(path)
    return blob.download_as_bytes() if blob.exists() else None


@app.get("/healthz")
def healthz() -> dict:
    """Liveness probe — does not touch GCS (so it stays green before the first
    snapshot is published)."""
    return {"ok": True, "bucket_configured": bool(BUCKET)}


@app.get("/")
def index() -> Response:
    html = _read("latest/index.html")
    if html is None:
        raise HTTPException(status_code=503, detail="No snapshot published yet.")
    return Response(content=html, media_type="text/html")


@app.get("/report.json")
def report_json() -> Response:
    """The canonical published snapshot (the format a future client renders)."""
    data = _read("latest/report.json")
    if data is None:
        raise HTTPException(status_code=404, detail="No snapshot published yet.")
    return Response(content=data, media_type="application/json")


@app.get("/data.xlsx")
def data_xlsx() -> Response:
    """The journalist spreadsheet (every tab, every row) — the Tables tab's
    'Download Excel workbook'. Published alongside the snapshot; 404 if a
    snapshot was made without one (e.g. an --portal-snapshot-only refresh)."""
    data = _read("latest/data.xlsx")
    if data is None:
        raise HTTPException(status_code=404, detail="No workbook in this snapshot.")
    return Response(
        content=data, media_type=_XLSX_MIME,
        headers={"Content-Disposition": 'attachment; filename="meridian-data.xlsx"'},
    )
