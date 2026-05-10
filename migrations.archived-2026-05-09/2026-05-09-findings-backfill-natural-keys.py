"""Backfill natural_key_hash + value_signature on existing findings.

Phase 1.1 of reviews/roadmap-2026-05-09.md. After applying the schema
migration, the 1245 existing findings have NULL natural_key_hash and
NULL value_signature — and many groups of them are *de facto*
duplicates from re-runs of the same analyser pass.

This script:

1. Computes natural_key_hash for every existing finding using the same
   logic emit_finding will use going forward.
2. Computes value_signature similarly (best-effort — using the stored
   detail JSONB to derive the same value-fields the helper would have
   recorded).
3. For each natural_key_hash group, marks all but the latest row (by id)
   as superseded with a back-pointer.
4. Verifies the partial unique index is satisfied at the end.

Idempotent: re-running over an already-backfilled DB is a no-op.

Apply with:
    set -a; source .env; set +a
    PYTHONPATH=. .venv/bin/python migrations/2026-05-09-findings-backfill-natural-keys.py
    PYTHONPATH=. GACC_TEST_DATABASE_URL_AS_DATABASE_URL=1 .venv/bin/python ...
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import datetime

import psycopg2
import psycopg2.extras

import findings_io

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def _natural_key_for(row: dict) -> tuple | None:
    """Reconstruct the natural-key tuple from a stored finding row.
    Returns None if the subkind isn't recognised (we don't backfill
    findings we don't have a natural-key definition for)."""
    sk = row["subkind"]
    detail = row["detail"] or {}
    if sk == "mirror_gap":
        iso2 = detail.get("iso2")
        # Period: pull from the first observation's release.period (already done
        # in the analyser logic). For the backfill we accept that the stored
        # detail may not carry period directly — fall back to looking it up.
        if not iso2 or not row["_period_yyyymm"]:
            return None
        return findings_io.nk_mirror_gap(iso2, row["_period_yyyymm"])
    if sk == "mirror_gap_zscore":
        iso2 = detail.get("iso2")
        period = detail.get("period")
        if not iso2 or not period:
            return None
        # detail.period is YYYY-MM-DD; convert to YYYY-MM
        return findings_io.nk_mirror_gap_zscore(iso2, period[:7])
    if sk in ("hs_group_yoy", "hs_group_yoy_export"):
        if not row["hs_group_ids"]:
            return None
        gid = row["hs_group_ids"][0]
        end = detail.get("windows", {}).get("current_end")
        if not gid or not end:
            return None
        return findings_io.nk_hs_group_yoy(gid, end[:7])
    if sk in ("hs_group_trajectory", "hs_group_trajectory_export"):
        if not row["hs_group_ids"]:
            return None
        return findings_io.nk_hs_group_trajectory(row["hs_group_ids"][0])
    return None


def _value_signature_for(row: dict) -> str | None:
    """Reconstruct the value_signature for a stored finding using the
    same fields emit_finding would have recorded. Best-effort: if a
    field is missing from the stored detail, we elide it (which is
    consistent with what would have been stored had the analyser run
    against incomplete data — the signature still differentiates rows
    correctly within this backfill batch).

    Numeric fields are rounded to match the going-forward behaviour."""
    sk = row["subkind"]
    detail = row["detail"] or {}
    if sk == "mirror_gap":
        gacc = detail.get("gacc", {})
        eu = detail.get("eurostat", {})
        return findings_io.value_signature({
            "gacc_value_eur": _round(gacc.get("value_eur_converted"), 2),
            "eurostat_total_eur": _round(eu.get("total_eur"), 2),
            "gap_eur": _round(detail.get("gap_eur"), 2),
            "gap_pct": _round(detail.get("gap_pct"), 6),
            "is_aggregate": detail.get("is_aggregate", False),
        })
    if sk == "mirror_gap_zscore":
        b = detail.get("baseline", {})
        return findings_io.value_signature({
            "gap_pct": _round(detail.get("gap_pct"), 6),
            "z_score": _round(detail.get("z_score"), 4),
            "baseline_mean": _round(b.get("mean"), 6),
            "baseline_stdev": _round(b.get("stdev"), 6),
            "baseline_n": b.get("n"),
        })
    if sk in ("hs_group_yoy", "hs_group_yoy_export"):
        t = detail.get("totals", {})
        return findings_io.value_signature({
            "yoy_pct": _round(t.get("yoy_pct"), 6),
            "current_eur": _round(t.get("current_12mo_eur"), 2),
            "prior_eur": _round(t.get("prior_12mo_eur"), 2),
            "yoy_pct_kg": _round(t.get("yoy_pct_kg"), 6),
            "current_kg": _round(t.get("current_12mo_kg"), 2),
            "unit_price_pct": _round(t.get("unit_price_pct_change"), 6),
            "low_base": t.get("low_base"),
        })
    if sk in ("hs_group_trajectory", "hs_group_trajectory_export"):
        f = detail.get("features", {})
        return findings_io.value_signature({
            "shape": detail.get("shape"),
            "last_yoy": _round(f.get("last_yoy"), 6),
            "last_period": f.get("last_period"),
            "first_period": f.get("first_period"),
            "max_yoy": _round(f.get("max_yoy", 0), 6),
            "min_yoy": _round(f.get("min_yoy", 0), 6),
            "n": f.get("n"),
            "low_base_majority": f.get("low_base_majority"),
        })
    return None


def _round(v, n):
    if v is None:
        return None
    try:
        return round(float(v), n)
    except (TypeError, ValueError):
        return None


def backfill(database_url: str) -> dict:
    counts = {"scanned": 0, "stamped": 0, "skipped_unknown_subkind": 0,
              "superseded_dupes": 0, "kept_active": 0}
    with psycopg2.connect(database_url) as conn:
        # Pull all findings, joining mirror_gap ones to their first observation
        # so we can derive the period for the natural key.
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT f.id, f.subkind, f.hs_group_ids, f.detail, f.created_at,
                       f.natural_key_hash, f.value_signature, f.superseded_at,
                       (SELECT to_char(r.period, 'YYYY-MM')
                          FROM observations o JOIN releases r ON r.id = o.release_id
                         WHERE o.id = f.observation_ids[1]) AS _period_yyyymm
                  FROM findings f
              ORDER BY f.id
                """
            )
            rows = cur.fetchall()
        log.info("Loaded %d findings for backfill", len(rows))

        # Group by natural_key_hash → list of (id, vs, created_at).
        groups: dict[str, list[tuple[int, str | None, datetime]]] = defaultdict(list)
        to_stamp: list[tuple[int, str, str]] = []  # (id, nk, vs)
        for row in rows:
            counts["scanned"] += 1
            nk_tuple = _natural_key_for(row)
            if nk_tuple is None:
                counts["skipped_unknown_subkind"] += 1
                continue
            nk = findings_io.natural_key_hash(row["subkind"], nk_tuple)
            vs = _value_signature_for(row)
            # Only stamp if not already stamped (idempotency).
            if row["natural_key_hash"] is None:
                to_stamp.append((row["id"], nk, vs))
            groups[nk].append((row["id"], vs, row["created_at"]))

        # Stamp natural_key_hash + value_signature on rows that don't have it.
        # We CANNOT do this in one batch UPDATE because the partial unique index
        # would fire on the second row of any duplicate group. Instead, walk
        # group by group: stamp the LATEST row first, then mark the older ones
        # superseded BEFORE stamping them (so the partial index excludes them).
        with conn.cursor() as cur:
            for nk, group_rows in groups.items():
                # Sort by id ascending; latest-by-id is the keeper.
                group_rows.sort(key=lambda x: x[0])
                *older, latest = group_rows
                latest_id, latest_vs, _ = latest
                # Stamp the latest first (still active).
                if any(rid == latest_id and rid in [s[0] for s in to_stamp] for rid, _, _ in [latest]):
                    cur.execute(
                        "UPDATE findings SET natural_key_hash = %s, value_signature = %s "
                        "WHERE id = %s AND natural_key_hash IS NULL",
                        (nk, latest_vs, latest_id),
                    )
                    counts["stamped"] += cur.rowcount
                counts["kept_active"] += 1
                # Mark older as superseded (in id order) and back-point to the
                # next one in the chain (or to latest if it's the immediate
                # predecessor). Stamp natural_key + vs at the same time.
                superseded_by_for = {}
                # Build chain: each older row's superseded_by is the next row by id.
                ordered = older + [latest]
                for i in range(len(ordered) - 1):
                    superseded_by_for[ordered[i][0]] = ordered[i + 1][0]
                for old_id, old_vs, _ in older:
                    cur.execute(
                        "UPDATE findings SET "
                        " natural_key_hash = %s, value_signature = %s, "
                        " superseded_at = COALESCE(superseded_at, now()), "
                        " superseded_by_finding_id = COALESCE(superseded_by_finding_id, %s) "
                        "WHERE id = %s",
                        (nk, old_vs, superseded_by_for[old_id], old_id),
                    )
                    counts["superseded_dupes"] += 1
            conn.commit()

        # Sanity: the partial unique index must hold.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT natural_key_hash, COUNT(*) "
                "  FROM findings WHERE superseded_at IS NULL AND natural_key_hash IS NOT NULL "
                "GROUP BY natural_key_hash HAVING COUNT(*) > 1"
            )
            dupes = cur.fetchall()
            if dupes:
                raise SystemExit(f"Sanity check failed: {len(dupes)} active duplicate natural keys")
    return counts


if __name__ == "__main__":
    import sys
    target = os.environ.get("DATABASE_URL")
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        target = os.environ.get("GACC_TEST_DATABASE_URL")
    if not target:
        raise SystemExit("DATABASE_URL not set")
    log.info("Backfilling against: %s", target)
    counts = backfill(target)
    log.info("Done: %s", counts)
