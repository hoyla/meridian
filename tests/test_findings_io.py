"""Tests for the idempotent finding-emission helper.

Phase 1.1 of reviews/roadmap-2026-05-09.md. Three behaviours we need
to defend:

1. First emission inserts a new row.
2. Re-emission with identical value_fields confirms the existing row
   (no new insert, last_confirmed_at bumped).
3. Re-emission with changed value_fields inserts a new row and
   supersedes the prior one with a back-pointer.

Plus the DB-level guarantee: the partial unique index prevents two
active rows with the same natural key, so even a buggy bypass of the
helper can't produce duplicates.
"""

import time

import psycopg2
import pytest

import findings_io


@pytest.fixture
def conn(test_db_url):
    c = psycopg2.connect(test_db_url)
    with c, c.cursor() as cur:
        cur.execute(
            "TRUNCATE findings, observations, source_snapshots, eurostat_raw_rows, "
            "scrape_runs, releases RESTART IDENTITY CASCADE"
        )
    yield c
    c.close()


def _seed_run(cur) -> int:
    cur.execute(
        "INSERT INTO scrape_runs (source_url, status) VALUES ('seed', 'success') RETURNING id"
    )
    return cur.fetchone()[0]


def _emit(cur, *, run_id: int, group_id: int, period: str, yoy_pct: float):
    """Convenience wrapper for an hs_group_yoy-shaped emission."""
    return findings_io.emit_finding(
        cur,
        scrape_run_id=run_id,
        kind="anomaly",
        subkind="hs_group_yoy",
        natural_key=findings_io.nk_hs_group_yoy(group_id, period),
        value_fields={"yoy_pct": yoy_pct, "current_eur": 1e9, "prior_eur": 0.7e9},
        hs_group_ids=[group_id],
        score=abs(yoy_pct),
        title=f"yoy {group_id} {period}",
        body="b",
        detail={"yoy_pct": yoy_pct, "group": {"id": group_id}},
    )


def test_first_emission_inserts_new(conn):
    cur = conn.cursor()
    run = _seed_run(cur)
    fid, action = _emit(cur, run_id=run, group_id=1, period="2026-02", yoy_pct=0.34)
    conn.commit()
    assert action == "inserted_new"
    cur.execute("SELECT id, superseded_at, last_confirmed_at FROM findings WHERE id = %s", (fid,))
    row = cur.fetchone()
    assert row[0] == fid
    assert row[1] is None
    assert row[2] is not None


def test_re_emission_identical_confirms_existing(conn):
    cur = conn.cursor()
    run = _seed_run(cur)
    fid_a, _ = _emit(cur, run_id=run, group_id=1, period="2026-02", yoy_pct=0.34)
    conn.commit()
    cur.execute("SELECT last_confirmed_at FROM findings WHERE id = %s", (fid_a,))
    first_confirm = cur.fetchone()[0]
    # Sleep a touch so the timestamp moves at clock resolution.
    time.sleep(0.01)
    fid_b, action = _emit(cur, run_id=run, group_id=1, period="2026-02", yoy_pct=0.34)
    conn.commit()
    assert fid_b == fid_a
    assert action == "confirmed_existing"
    cur.execute("SELECT last_confirmed_at FROM findings WHERE id = %s", (fid_a,))
    second_confirm = cur.fetchone()[0]
    assert second_confirm > first_confirm
    cur.execute("SELECT count(*) FROM findings")
    assert cur.fetchone()[0] == 1


def test_re_emission_different_values_supersedes(conn):
    cur = conn.cursor()
    run = _seed_run(cur)
    fid_a, _ = _emit(cur, run_id=run, group_id=1, period="2026-02", yoy_pct=0.34)
    conn.commit()
    fid_b, action = _emit(cur, run_id=run, group_id=1, period="2026-02", yoy_pct=0.42)
    conn.commit()

    assert action == "superseded"
    assert fid_b != fid_a

    # Old row marked superseded with back-pointer to new.
    cur.execute(
        "SELECT id, superseded_at, superseded_by_finding_id FROM findings WHERE id = %s",
        (fid_a,),
    )
    a = cur.fetchone()
    assert a[1] is not None  # superseded_at set
    assert a[2] == fid_b     # back-pointer

    # New row is current, no supersede metadata.
    cur.execute(
        "SELECT superseded_at, superseded_by_finding_id FROM findings WHERE id = %s",
        (fid_b,),
    )
    b = cur.fetchone()
    assert b[0] is None
    assert b[1] is None


def test_partial_unique_index_blocks_active_collision(conn):
    """Even a direct INSERT bypassing the helper should fail when it
    would create a second active row with the same natural_key_hash."""
    cur = conn.cursor()
    run = _seed_run(cur)
    fid_a, _ = _emit(cur, run_id=run, group_id=1, period="2026-02", yoy_pct=0.34)
    conn.commit()

    cur.execute("SELECT natural_key_hash FROM findings WHERE id = %s", (fid_a,))
    nk_hash = cur.fetchone()[0]

    # Attempt to insert a second active row with the same natural_key_hash.
    with pytest.raises(psycopg2.errors.UniqueViolation):
        cur.execute(
            "INSERT INTO findings (scrape_run_id, kind, natural_key_hash, value_signature) "
            "VALUES (%s, 'anomaly', %s, 'whatever')",
            (run, nk_hash),
        )
    conn.rollback()


def test_different_natural_keys_coexist(conn):
    """Two findings for different (group_id, period) tuples should both
    insert as new — the unique-index guarantee is per natural_key_hash, not
    global."""
    cur = conn.cursor()
    run = _seed_run(cur)
    fid1, action1 = _emit(cur, run_id=run, group_id=1, period="2026-02", yoy_pct=0.34)
    fid2, action2 = _emit(cur, run_id=run, group_id=2, period="2026-02", yoy_pct=0.34)
    fid3, action3 = _emit(cur, run_id=run, group_id=1, period="2026-01", yoy_pct=0.34)
    conn.commit()
    assert action1 == action2 == action3 == "inserted_new"
    assert len({fid1, fid2, fid3}) == 3


def test_subkind_is_part_of_natural_key(conn):
    """An hs_group_yoy and hs_group_yoy_export finding for the same group
    + period must NOT collide. Subkind disambiguates flow direction."""
    cur = conn.cursor()
    run = _seed_run(cur)
    fid_imp, _ = findings_io.emit_finding(
        cur, scrape_run_id=run, kind="anomaly", subkind="hs_group_yoy",
        natural_key=findings_io.nk_hs_group_yoy(1, "2026-02"),
        value_fields={"yoy_pct": 0.34}, hs_group_ids=[1], detail={},
    )
    fid_exp, action_exp = findings_io.emit_finding(
        cur, scrape_run_id=run, kind="anomaly", subkind="hs_group_yoy_export",
        natural_key=findings_io.nk_hs_group_yoy(1, "2026-02"),
        value_fields={"yoy_pct": -0.45}, hs_group_ids=[1], detail={},
    )
    conn.commit()
    assert action_exp == "inserted_new"
    assert fid_exp != fid_imp


def test_supersede_chain_traversal(conn):
    """Three successive value changes should produce a chain:
    a -> b -> c, with a and b superseded, c current."""
    cur = conn.cursor()
    run = _seed_run(cur)
    fid_a, _ = _emit(cur, run_id=run, group_id=1, period="2026-02", yoy_pct=0.10)
    fid_b, _ = _emit(cur, run_id=run, group_id=1, period="2026-02", yoy_pct=0.20)
    fid_c, _ = _emit(cur, run_id=run, group_id=1, period="2026-02", yoy_pct=0.30)
    conn.commit()

    cur.execute(
        "SELECT id, superseded_at IS NOT NULL, superseded_by_finding_id "
        "FROM findings ORDER BY id"
    )
    rows = cur.fetchall()
    assert len(rows) == 3
    a, b, c = rows
    assert a == (fid_a, True, fid_b)
    assert b == (fid_b, True, fid_c)
    assert c == (fid_c, False, None)


def test_value_signature_robust_to_dict_ordering(conn):
    """Two emissions with identical value fields but different dict-insertion
    orders must produce the same value_signature (sorted-keys hashing)."""
    cur = conn.cursor()
    run = _seed_run(cur)
    fid_a, _ = findings_io.emit_finding(
        cur, scrape_run_id=run, kind="anomaly", subkind="hs_group_yoy",
        natural_key=findings_io.nk_hs_group_yoy(1, "2026-02"),
        value_fields={"a": 1, "b": 2, "c": 3}, hs_group_ids=[1], detail={},
    )
    fid_b, action = findings_io.emit_finding(
        cur, scrape_run_id=run, kind="anomaly", subkind="hs_group_yoy",
        natural_key=findings_io.nk_hs_group_yoy(1, "2026-02"),
        value_fields={"c": 3, "a": 1, "b": 2}, hs_group_ids=[1], detail={},
    )
    conn.commit()
    assert action == "confirmed_existing"
    assert fid_b == fid_a
