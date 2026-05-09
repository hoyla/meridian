"""Idempotent finding emission with revision history.

Phase 1.1 of `reviews/roadmap-2026-05-09.md`. The four anomaly passes
in `anomalies.py` previously did unconditional `INSERT INTO findings`,
which produced duplicate rows on every re-run. The exporters papered
over that with `DISTINCT ON` queries, but the LLM framing layer reading
`findings` directly would be confused by duplicates — and re-running an
analyser when Eurostat revises a release would silently add a new
finding with no link back to what it replaced.

The append-plus-supersede chain implemented here gives us:

- **Idempotency.** Re-running a pass with no underlying-data change
  doesn't insert new rows; it just bumps `last_confirmed_at` on the
  existing row.
- **Revision history.** When the underlying data DOES change (e.g.
  Eurostat republishes a corrected month and a YoY moves from +12% to
  +18%), the new value is inserted as a new row and the prior row gets
  `superseded_at = now()` + `superseded_by_finding_id` set to the new
  id. The chain is queryable; the revision is itself a finding-class
  newsworthy artefact.
- **DB-level guarantee.** A partial unique index on
  `(natural_key_hash) WHERE superseded_at IS NULL` ensures at most one
  active finding per natural key.

Each call site declares two things:

- A **natural key**: the identity tuple that says "this is the same
  finding in editorial terms" (e.g. `(hs_group_id, current_end)` for
  hs_group_yoy). Codified per-subkind in this module's `natural_key_*`
  helpers.
- A **value signature**: a deterministic hash of the meaningful values
  that would change if the underlying data revised (e.g. the YoY
  numbers, the trajectory shape). Compared on re-run; a mismatch
  triggers supersede.

The module deliberately doesn't try to figure either out from the
finding's `detail` JSONB — explicit at the call site beats clever, and
makes the editorial intent visible.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Literal

log = logging.getLogger(__name__)

EmitAction = Literal["inserted_new", "confirmed_existing", "superseded"]


def _stable_hash(*parts: Any) -> str:
    """Deterministic short hash. Each part is JSON-serialised with sorted
    keys; the parts are joined with a delimiter that can't appear in JSON
    output, then SHA-256'd and truncated. Truncation to 32 hex chars
    (128 bits) gives us collision safety vastly beyond what we need."""
    serialised = "\x00".join(json.dumps(p, sort_keys=True, default=str) for p in parts)
    return hashlib.sha256(serialised.encode("utf-8")).hexdigest()[:32]


def natural_key_hash(subkind: str, identity: tuple[Any, ...]) -> str:
    """Hash for the natural key of a finding. The subkind is included so
    e.g. an hs_group_yoy finding for group_id=5 doesn't collide with an
    hs_group_yoy_export finding for the same group."""
    return _stable_hash("nk", subkind, *identity)


def value_signature(values: dict[str, Any]) -> str:
    """Hash for the meaningful values of a finding. Keys are sorted to
    make order-of-insertion irrelevant. NULLs are preserved (None hashes
    differently from absent key, by design — call sites should be
    explicit about which fields they include)."""
    return _stable_hash("vs", values)


def emit_finding(
    cur,
    *,
    scrape_run_id: int,
    kind: str,
    subkind: str,
    natural_key: tuple[Any, ...],
    value_fields: dict[str, Any],
    observation_ids: list[int] | None = None,
    hs_group_ids: list[int] | None = None,
    score: float | None = None,
    title: str | None = None,
    body: str | None = None,
    detail: dict[str, Any] | None = None,
) -> tuple[int, EmitAction]:
    """Insert a finding, re-confirm an existing one, or supersede + insert.

    Returns (finding_id, action). Caller is responsible for transaction
    boundaries — the helper does not commit. All three branches are
    transactional within the caller's context.

    Idempotency contract:
    - If no current (un-superseded) finding exists with this
      natural_key: insert + return ('inserted_new', new_id).
    - If a current finding exists with the same natural_key AND the same
      value_signature: bump its last_confirmed_at, return
      ('confirmed_existing', existing_id).
    - If a current finding exists with the same natural_key but a
      DIFFERENT value_signature: insert the new row, mark the old row
      superseded with a back-pointer to the new id, return
      ('superseded', new_id).
    """
    nk_hash = natural_key_hash(subkind, natural_key)
    vs_hash = value_signature(value_fields)

    cur.execute(
        "SELECT id, value_signature FROM findings "
        "WHERE natural_key_hash = %s AND superseded_at IS NULL",
        (nk_hash,),
    )
    existing = cur.fetchone()

    if existing is not None:
        existing_id, existing_vs = existing[0], existing[1]
        if existing_vs == vs_hash:
            cur.execute(
                "UPDATE findings SET last_confirmed_at = now() WHERE id = %s",
                (existing_id,),
            )
            log.debug("emit_finding: confirmed existing %s (subkind=%s)", existing_id, subkind)
            return existing_id, "confirmed_existing"
        # Values changed: supersede old FIRST (clears it from the partial unique
        # index), then insert new, then back-fill the supersede pointer.
        # Order matters: the partial index `WHERE superseded_at IS NULL` would
        # reject the INSERT if both the old and new rows were active for any
        # transaction-step in between.
        cur.execute(
            "UPDATE findings SET superseded_at = now() WHERE id = %s",
            (existing_id,),
        )
        new_id = _insert(
            cur, scrape_run_id=scrape_run_id, kind=kind, subkind=subkind,
            natural_key_hash=nk_hash, value_signature=vs_hash,
            observation_ids=observation_ids, hs_group_ids=hs_group_ids,
            score=score, title=title, body=body, detail=detail,
        )
        cur.execute(
            "UPDATE findings SET superseded_by_finding_id = %s WHERE id = %s",
            (new_id, existing_id),
        )
        log.info(
            "emit_finding: superseded %s -> %s (subkind=%s)",
            existing_id, new_id, subkind,
        )
        return new_id, "superseded"

    new_id = _insert(
        cur, scrape_run_id=scrape_run_id, kind=kind, subkind=subkind,
        natural_key_hash=nk_hash, value_signature=vs_hash,
        observation_ids=observation_ids, hs_group_ids=hs_group_ids,
        score=score, title=title, body=body, detail=detail,
    )
    log.debug("emit_finding: inserted new %s (subkind=%s)", new_id, subkind)
    return new_id, "inserted_new"


def _insert(
    cur, *,
    scrape_run_id: int,
    kind: str,
    subkind: str,
    natural_key_hash: str,
    value_signature: str,
    observation_ids: list[int] | None,
    hs_group_ids: list[int] | None,
    score: float | None,
    title: str | None,
    body: str | None,
    detail: dict[str, Any] | None,
) -> int:
    cur.execute(
        """
        INSERT INTO findings (
            scrape_run_id, kind, subkind,
            observation_ids, hs_group_ids,
            score, title, body, detail,
            natural_key_hash, value_signature
        ) VALUES (
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s::jsonb,
            %s, %s
        ) RETURNING id
        """,
        (
            scrape_run_id, kind, subkind,
            observation_ids or [],
            hs_group_ids or [],
            score, title, body,
            json.dumps(detail) if detail is not None else None,
            natural_key_hash, value_signature,
        ),
    )
    return cur.fetchone()[0]


# =============================================================================
# Per-subkind natural-key builders.
# Centralised here so the editorial-identity definitions are visible in one
# place, and so the four anomaly passes don't repeatedly re-derive them.
# =============================================================================


def nk_mirror_gap(iso2: str, period_yyyymm: str) -> tuple[str, str]:
    """A mirror_gap is identified by (partner_iso2, period). Period is the
    YYYY-MM string of the underlying GACC/Eurostat releases."""
    return (iso2, period_yyyymm)


def nk_mirror_gap_zscore(iso2: str, period_yyyymm: str) -> tuple[str, str]:
    return (iso2, period_yyyymm)


def nk_hs_group_yoy(hs_group_id: int, current_end_yyyymm: str) -> tuple[int, str]:
    """An hs_group_yoy finding is identified by (group, window-end period).
    The flow direction is encoded in the subkind (`hs_group_yoy` vs
    `hs_group_yoy_export`)."""
    return (hs_group_id, current_end_yyyymm)


def nk_hs_group_trajectory(hs_group_id: int) -> tuple[int]:
    """A trajectory finding has no period in its natural key — there's only
    one current trajectory per group per flow at any time. New data lands
    via supersede. Flow direction is in the subkind."""
    return (hs_group_id,)
