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


class PriorSnapshotUnreadable(RuntimeError):
    """The live snapshot exists (or might) but could not be read — a GCS/auth/
    parse error, distinct from "no prior snapshot yet". Raised by
    `read_latest_report(required=True)` so a publish that asked to carry prior
    takes forward refuses to ship takes-less rather than silently emptying them.
    """

# Snapshot file -> content-type. report.json is the canonical artefact; the
# index.html is the rendered preview the v1 service serves at /.
_SNAPSHOT_FILES: dict[str, str] = {
    "report.json": "application/json",
    "index.html": "text/html",
}
_XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _period_from_snapshot(portal_dir: Path) -> str | None:
    """Read the data period from report.json's meta, for the per-period archive
    path. None if absent/unreadable — the archive copy is then skipped (latest/
    is still written)."""
    try:
        meta = json.loads((portal_dir / "report.json").read_text()).get("meta") or {}
        return meta.get("data_period")
    except Exception:
        return None


def read_latest_report(
    bucket: str | None = None, *, required: bool = False,
) -> dict | None:
    """Fetch and parse `gs://<bucket>/latest/report.json` — the currently-live
    snapshot — as a plain dict, or None when there is no prior snapshot to read.

    The source for the reuse-takes graft (carry prior LLM takes onto an
    LLM-less rebuild; see `portal_takes_reuse`). Bucket from the arg or
    PORTAL_BUCKET.

    Two outcomes are deliberately separated:

    - **Absent** — no bucket configured, or no `latest/report.json` yet (the
      first publish) — returns None regardless of `required`. There is nothing
      to carry forward; empty takes are correct.
    - **Read errored** — a GCS/auth/parse failure, so a prior snapshot might
      exist but we couldn't read it. With `required=False` (the default,
      best-effort) this logs and returns None. With `required=True` it raises
      `PriorSnapshotUnreadable` — used when a publish explicitly asked to reuse
      takes, so it must not silently ship them empty (a read error read as
      "absent" was the silent-regression bug this guards)."""
    bucket = bucket or os.environ.get("PORTAL_BUCKET")
    if not bucket:
        return None
    try:
        from google.cloud import storage  # lazy — keep GCS off the import path
        blob = storage.Client().bucket(bucket).blob("latest/report.json")
        if not blob.exists():
            log.info("reuse-takes: no latest/report.json in gs://%s yet", bucket)
            return None
        return json.loads(blob.download_as_bytes())
    except Exception as e:
        if required:
            raise PriorSnapshotUnreadable(
                f"could not read latest/report.json from gs://{bucket} ({e})"
            ) from e
        log.warning(
            "reuse-takes: could not read latest/report.json from gs://%s "
            "(%s); takes will be empty", bucket, e,
        )
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

    # The full workbook (the Tables tab's 'Download Excel') lives at the bundle
    # root, beside 04_Portal/. Both publish paths now produce it — --periodic-run
    # via export(), and --portal-snapshot via write_portal_snapshot(
    # write_workbook=True). Publish it to latest/data.xlsx (+ the per-period
    # archive) so /data.xlsx can serve it. If it's still missing the workbook
    # build must have failed upstream (logged there); skip rather than fail the
    # whole publish — the download 404s until the next successful build.
    xlsx = Path(bundle_dir) / "04_Data.xlsx"
    if xlsx.is_file():
        dests = ["latest/data.xlsx"]
        if period:
            dests.append(f"periods/{period}/data.xlsx")
        for dest in dests:
            blob = b.blob(dest)
            blob.cache_control = "no-cache"
            blob.upload_from_filename(str(xlsx), content_type=_XLSX_MIME)
            written.append(dest)
            log.info("portal-publish: wrote gs://%s/%s", bucket, dest)
    else:
        log.warning("portal-publish: no 04_Data.xlsx in %s (workbook build "
                    "likely failed upstream); /data.xlsx download will 404 "
                    "until the next successful build", bundle_dir)
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
