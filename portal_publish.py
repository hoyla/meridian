"""Publish a periodic-run portal snapshot to the Cloud Run portal's GCS bucket.

Uploads `<bundle>/04_Portal/{report.json,index.html}` to `gs://<bucket>/latest/`
(what the service serves) and archives a per-period copy under
`periods/<data_period>/`. The bucket comes from `PORTAL_BUCKET` (or the `bucket`
arg). Run by hand after a cycle, like `--upload-to-drive`; needs Application
Default Credentials with write on the bucket
(`gcloud auth application-default login`).

`warm_service` optionally sets the Cloud Run service's `--min-instances=1` so a
freshly published report has no cold-start delay during its launch window — you
flip back to 0 by hand once reporters have had their look.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)

# Snapshot file -> content-type. report.json is the canonical artefact; the
# index.html is the rendered preview the v1 service serves at /.
_SNAPSHOT_FILES: dict[str, str] = {
    "report.json": "application/json",
    "index.html": "text/html",
}


def _period_from_snapshot(portal_dir: Path) -> str | None:
    """Read the data period from report.json's meta, for the per-period archive
    path. None if absent/unreadable — the archive copy is then skipped (latest/
    is still written)."""
    try:
        meta = json.loads((portal_dir / "report.json").read_text()).get("meta") or {}
        return meta.get("data_period")
    except Exception:
        return None


def publish_snapshot(bundle_dir: str, *, bucket: str | None = None) -> list[str]:
    """Upload the bundle's 04_Portal snapshot to `gs://<bucket>/latest/` plus a
    per-period archive. Returns the object paths written.

    Raises ValueError if no bucket is configured, or FileNotFoundError if the
    bundle has no 04_Portal snapshot — both validated before touching GCS, so
    the failure is cheap and clear."""
    bucket = bucket or os.environ.get("PORTAL_BUCKET")
    if not bucket:
        raise ValueError("no portal bucket — set PORTAL_BUCKET or pass bucket=")
    portal_dir = Path(bundle_dir) / "04_Portal"
    present = [(n, c) for n, c in _SNAPSHOT_FILES.items() if (portal_dir / n).is_file()]
    if not present:
        raise FileNotFoundError(
            f"no 04_Portal snapshot in {portal_dir} "
            "(run --periodic-run first to produce it)"
        )
    period = _period_from_snapshot(portal_dir)

    from google.cloud import storage  # lazy — keep GCS off the import path
    b = storage.Client().bucket(bucket)
    written: list[str] = []
    for name, ctype in present:
        src = str(portal_dir / name)
        dests = [f"latest/{name}"]
        if period:
            dests.append(f"periods/{period}/{name}")
        for dest in dests:
            blob = b.blob(dest)
            blob.cache_control = "no-cache"  # always serve the freshest latest/
            blob.upload_from_filename(src, content_type=ctype)
            written.append(dest)
            log.info("portal-publish: wrote gs://%s/%s", bucket, dest)
    return written


def warm_service(service: str, region: str, *, min_instances: int = 1) -> bool:
    """Set the Cloud Run service's min-instances (warm at launch) via gcloud.
    Best-effort: logs and returns False on failure so a warm-up hiccup never
    blocks a publish."""
    try:
        subprocess.run(
            ["gcloud", "run", "services", "update", service,
             "--region", region, f"--min-instances={min_instances}"],
            check=True, capture_output=True, text=True,
        )
        log.info("portal-publish: %s min-instances=%d", service, min_instances)
        return True
    except Exception as e:
        log.warning("portal-publish: warm-up failed (%s); continuing", e)
        return False
