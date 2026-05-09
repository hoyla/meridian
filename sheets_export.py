"""Spreadsheet export for findings.

Renders findings into a list of SheetData (header + rows) — pure SQL → in-
memory tables — then writes via a pluggable writer. v1 ships an XlsxWriter
that produces local .xlsx files via openpyxl, and a GoogleSheetsWriter stub
that will pick up either a service-account JSON (production) or OAuth user
creds (interactive testing) once we wire it up.

The shape is identical between the two destinations: switching from XLSX to
Sheets is a one-line change of writer, no rebuild of the data layer.

CLI: `python scrape.py --export-sheet [--out-format {xlsx,sheets}]
                                       [--out-path /path/to/file.xlsx]
                                       [--spreadsheet-id ID]`

Permalink convention: every output sheet has a `finding_id` column (the
canonical reference) and a `link` column. The link column emits a Sheets
HYPERLINK formula based on the GACC_PERMALINK_BASE env var. If unset, the
column stays empty — no rotten links. When a web UI exists later, set
GACC_PERMALINK_BASE and the column lights up automatically (formula resolves
at view time, not at export time).
"""

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Protocol

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

PERMALINK_BASE_ENV = "GACC_PERMALINK_BASE"


@dataclass
class SheetData:
    """One sheet's worth of data, ready to render."""
    name: str            # sheet/tab name, max 31 chars for Excel
    description: str     # one-line description rendered as the first row
    headers: list[str]
    rows: list[list[Any]]


class Writer(Protocol):
    def write(self, sheets: list[SheetData], dest: str) -> str:
        """Write sheets to dest. Returns the final path or URL."""
        ...


def _conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _link_cell(finding_id: int) -> str:
    """Return a HYPERLINK formula if GACC_PERMALINK_BASE is set, else empty
    string. The formula resolves at view-time, so existing exports light up
    once the base is configured later."""
    base = os.environ.get(PERMALINK_BASE_ENV, "").rstrip("/")
    if not base:
        return ""
    return f'=HYPERLINK("{base}/finding/{finding_id}", "open")'


# =============================================================================
# Sheet builders — one function per output tab
# =============================================================================


def assemble_sheets() -> list[SheetData]:
    """Build the v1 set of seven journalist-facing sheets."""
    sheets: list[SheetData] = []
    sheets.append(_summary_sheet())
    sheets.append(_hs_yoy_latest_sheet(flow=1))
    sheets.append(_hs_yoy_latest_sheet(flow=2))
    sheets.append(_hs_trajectories_sheet())
    sheets.append(_mirror_gaps_latest_sheet())
    sheets.append(_trend_movers_sheet())
    sheets.append(_low_base_review_sheet())
    return sheets


def _summary_sheet() -> SheetData:
    """The 'open the spreadsheet, scan for green-vs-red rows' view: one row
    per hs_group with both import and export latest YoY side by side. Best
    starting place for editorial scanning."""
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            WITH latest_imports AS (
              SELECT DISTINCT ON (detail->'group'->>'name')
                     detail->'group'->>'name' AS group_name,
                     id AS imp_finding_id,
                     (detail->'totals'->>'current_12mo_eur')::numeric AS imp_eur,
                     (detail->'totals'->>'yoy_pct')::numeric AS imp_yoy,
                     (detail->'totals'->>'yoy_pct_kg')::numeric AS imp_yoy_kg,
                     (detail->'totals'->>'low_base')::boolean AS imp_low_base
                FROM findings
               WHERE subkind = 'hs_group_yoy'
            ORDER BY detail->'group'->>'name', (detail->'windows'->>'current_end')::date DESC, id DESC
            ),
            latest_exports AS (
              SELECT DISTINCT ON (detail->'group'->>'name')
                     detail->'group'->>'name' AS group_name,
                     id AS exp_finding_id,
                     (detail->'totals'->>'current_12mo_eur')::numeric AS exp_eur,
                     (detail->'totals'->>'yoy_pct')::numeric AS exp_yoy,
                     (detail->'totals'->>'yoy_pct_kg')::numeric AS exp_yoy_kg,
                     (detail->'totals'->>'low_base')::boolean AS exp_low_base
                FROM findings
               WHERE subkind = 'hs_group_yoy_export'
            ORDER BY detail->'group'->>'name', (detail->'windows'->>'current_end')::date DESC, id DESC
            ),
            traj_imp AS (
              SELECT DISTINCT ON (detail->'group'->>'name')
                     detail->'group'->>'name' AS group_name,
                     detail->>'shape' AS imp_shape
                FROM findings WHERE subkind = 'hs_group_trajectory'
            ORDER BY detail->'group'->>'name', created_at DESC
            ),
            traj_exp AS (
              SELECT DISTINCT ON (detail->'group'->>'name')
                     detail->'group'->>'name' AS group_name,
                     detail->>'shape' AS exp_shape
                FROM findings WHERE subkind = 'hs_group_trajectory_export'
            ORDER BY detail->'group'->>'name', created_at DESC
            )
            SELECT g.name AS group_name,
                   li.imp_finding_id, li.imp_eur, li.imp_yoy, li.imp_yoy_kg, li.imp_low_base, ti.imp_shape,
                   le.exp_finding_id, le.exp_eur, le.exp_yoy, le.exp_yoy_kg, le.exp_low_base, te.exp_shape
              FROM hs_groups g
              LEFT JOIN latest_imports li ON li.group_name = g.name
              LEFT JOIN latest_exports le ON le.group_name = g.name
              LEFT JOIN traj_imp ti       ON ti.group_name = g.name
              LEFT JOIN traj_exp te       ON te.group_name = g.name
          ORDER BY g.id
            """
        )
        rows_raw = cur.fetchall()

    headers = [
        "group", "import_eur_12mo", "import_yoy_pct", "import_yoy_kg_pct", "import_low_base", "import_shape",
        "export_eur_12mo", "export_yoy_pct", "export_yoy_kg_pct", "export_low_base", "export_shape",
        "import_finding_id", "import_link", "export_finding_id", "export_link",
    ]
    rows = []
    for r in rows_raw:
        rows.append([
            r["group_name"],
            _to_float(r["imp_eur"]), _to_float(r["imp_yoy"]),
            _to_float(r["imp_yoy_kg"]), bool(r["imp_low_base"]) if r["imp_low_base"] is not None else None,
            r["imp_shape"],
            _to_float(r["exp_eur"]), _to_float(r["exp_yoy"]),
            _to_float(r["exp_yoy_kg"]), bool(r["exp_low_base"]) if r["exp_low_base"] is not None else None,
            r["exp_shape"],
            r["imp_finding_id"], _link_cell(r["imp_finding_id"]) if r["imp_finding_id"] else "",
            r["exp_finding_id"], _link_cell(r["exp_finding_id"]) if r["exp_finding_id"] else "",
        ])
    return SheetData(
        name="summary",
        description="Latest 12-month picture per HS group: imports vs exports, value + kg YoY, low-base flags, trajectory shape. The starting point for editorial scanning.",
        headers=headers, rows=rows,
    )


def _hs_yoy_latest_sheet(flow: int) -> SheetData:
    """One row per hs_group, latest period only, with full metric set."""
    subkind = "hs_group_yoy" if flow == 1 else "hs_group_yoy_export"
    flow_label = "imports (CN→EU)" if flow == 1 else "exports (EU→CN)"
    name = f"hs_yoy_{'imports' if flow == 1 else 'exports'}_latest"
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (detail->'group'->>'name')
                id, score, title,
                detail->'group'->>'name' AS group_name,
                (detail->'windows'->>'current_end')::date AS period,
                (detail->'totals'->>'current_12mo_eur')::numeric AS current_eur,
                (detail->'totals'->>'prior_12mo_eur')::numeric AS prior_eur,
                (detail->'totals'->>'yoy_pct')::numeric AS yoy_pct,
                (detail->'totals'->>'current_12mo_kg')::numeric AS current_kg,
                (detail->'totals'->>'yoy_pct_kg')::numeric AS yoy_kg_pct,
                (detail->'totals'->>'current_unit_price_eur_per_kg')::numeric AS unit_price,
                (detail->'totals'->>'unit_price_pct_change')::numeric AS unit_price_pct,
                (detail->'totals'->>'low_base')::boolean AS low_base
              FROM findings
             WHERE subkind = %s
          ORDER BY detail->'group'->>'name', (detail->'windows'->>'current_end')::date DESC, id DESC
            """,
            (subkind,),
        )
        rows_raw = cur.fetchall()

    headers = [
        "finding_id", "link", "group", "period",
        "current_12mo_eur", "prior_12mo_eur", "yoy_pct",
        "current_12mo_kg", "yoy_kg_pct",
        "unit_price_eur_per_kg", "unit_price_pct",
        "low_base", "score",
    ]
    rows = [
        [
            r["id"], _link_cell(r["id"]), r["group_name"], r["period"].isoformat(),
            _to_float(r["current_eur"]), _to_float(r["prior_eur"]), _to_float(r["yoy_pct"]),
            _to_float(r["current_kg"]), _to_float(r["yoy_kg_pct"]),
            _to_float(r["unit_price"]), _to_float(r["unit_price_pct"]),
            bool(r["low_base"]) if r["low_base"] is not None else False,
            _to_float(r["score"]),
        ]
        for r in rows_raw
    ]
    return SheetData(
        name=name,
        description=f"Latest rolling-12mo {flow_label} per HS group. Full metrics: value + kg + €/kg, all with YoY %.",
        headers=headers, rows=rows,
    )


def _hs_trajectories_sheet() -> SheetData:
    """Both flows side-by-side per hs_group, with trajectory shape labels."""
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            WITH imp AS (
              SELECT DISTINCT ON (detail->'group'->>'name')
                     id AS imp_id, detail->'group'->>'name' AS group_name,
                     detail->>'shape' AS imp_shape,
                     (detail->'features'->>'last_yoy')::numeric AS imp_last,
                     (detail->'features'->>'max_yoy')::numeric AS imp_peak,
                     (detail->'features'->>'min_yoy')::numeric AS imp_trough,
                     (detail->'features'->>'low_base_majority')::boolean AS imp_low_base
                FROM findings WHERE subkind = 'hs_group_trajectory'
            ORDER BY detail->'group'->>'name', created_at DESC
            ),
            exp AS (
              SELECT DISTINCT ON (detail->'group'->>'name')
                     id AS exp_id, detail->'group'->>'name' AS group_name,
                     detail->>'shape' AS exp_shape,
                     (detail->'features'->>'last_yoy')::numeric AS exp_last,
                     (detail->'features'->>'max_yoy')::numeric AS exp_peak,
                     (detail->'features'->>'min_yoy')::numeric AS exp_trough,
                     (detail->'features'->>'low_base_majority')::boolean AS exp_low_base
                FROM findings WHERE subkind = 'hs_group_trajectory_export'
            ORDER BY detail->'group'->>'name', created_at DESC
            )
            SELECT g.name AS group_name,
                   imp.imp_id, imp.imp_shape, imp.imp_last, imp.imp_peak, imp.imp_trough, imp.imp_low_base,
                   exp.exp_id, exp.exp_shape, exp.exp_last, exp.exp_peak, exp.exp_trough, exp.exp_low_base
              FROM hs_groups g
              LEFT JOIN imp ON imp.group_name = g.name
              LEFT JOIN exp ON exp.group_name = g.name
          ORDER BY g.id
            """
        )
        rows_raw = cur.fetchall()

    headers = [
        "group",
        "import_shape", "import_latest_yoy", "import_peak_yoy", "import_trough_yoy", "import_low_base",
        "export_shape", "export_latest_yoy", "export_peak_yoy", "export_trough_yoy", "export_low_base",
        "import_finding_id", "import_link", "export_finding_id", "export_link",
    ]
    rows = [
        [
            r["group_name"],
            r["imp_shape"], _to_float(r["imp_last"]), _to_float(r["imp_peak"]), _to_float(r["imp_trough"]),
            bool(r["imp_low_base"]) if r["imp_low_base"] is not None else None,
            r["exp_shape"], _to_float(r["exp_last"]), _to_float(r["exp_peak"]), _to_float(r["exp_trough"]),
            bool(r["exp_low_base"]) if r["exp_low_base"] is not None else None,
            r["imp_id"], _link_cell(r["imp_id"]) if r["imp_id"] else "",
            r["exp_id"], _link_cell(r["exp_id"]) if r["exp_id"] else "",
        ]
        for r in rows_raw
    ]
    return SheetData(
        name="trajectories",
        description="Trajectory shape per group, both flows. shapes: rising / falling (+ accel/decel), u_recovery, inverse_u_peak, dip_recovery, failed_recovery, volatile, flat.",
        headers=headers, rows=rows,
    )


def _mirror_gaps_latest_sheet() -> SheetData:
    """One row per partner (latest period), with the GACC vs Eurostat values
    and gap %. Single-country and EU-bloc rows side by side."""
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (detail->>'iso2')
                id, score, title,
                detail->>'iso2' AS iso2,
                detail->'gacc'->>'partner_label_raw' AS gacc_label,
                (detail->'gacc'->>'value_eur_converted')::numeric AS gacc_eur,
                (detail->'eurostat'->>'total_eur')::numeric AS eurostat_eur,
                (detail->>'gap_eur')::numeric AS gap_eur,
                (detail->>'gap_pct')::numeric AS gap_pct,
                (detail->>'is_aggregate')::boolean AS is_aggregate,
                (SELECT to_char(r.period, 'YYYY-MM')
                   FROM observations o JOIN releases r ON r.id = o.release_id
                  WHERE o.id = f.observation_ids[1]) AS period
              FROM findings f
             WHERE subkind = 'mirror_gap'
          ORDER BY detail->>'iso2',
                   (SELECT r.period FROM observations o JOIN releases r ON r.id = o.release_id
                     WHERE o.id = f.observation_ids[1]) DESC,
                   id DESC
            """
        )
        rows_raw = cur.fetchall()

    headers = [
        "finding_id", "link", "iso2", "gacc_label", "period",
        "gacc_eur_converted", "eurostat_eur", "gap_eur", "gap_pct",
        "is_aggregate", "score",
    ]
    rows = [
        [
            r["id"], _link_cell(r["id"]), r["iso2"], r["gacc_label"], r["period"],
            _to_float(r["gacc_eur"]), _to_float(r["eurostat_eur"]),
            _to_float(r["gap_eur"]), _to_float(r["gap_pct"]),
            bool(r["is_aggregate"]) if r["is_aggregate"] is not None else False,
            _to_float(r["score"]),
        ]
        for r in rows_raw
    ]
    return SheetData(
        name="mirror_gaps_latest",
        description="China-export-to-X (GACC, EUR-converted) vs X-import-from-China (Eurostat). Latest period per partner; iso2 BLOC:eu_bloc rows are aggregates.",
        headers=headers, rows=rows,
    )


def _trend_movers_sheet() -> SheetData:
    """Mirror-gap z-score findings sorted by |z| — 'where did the gap move
    significantly compared to its own baseline'."""
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT id, score, title,
                   detail->>'iso2' AS iso2,
                   to_char((detail->>'period')::date, 'YYYY-MM') AS period,
                   (detail->>'gap_pct')::numeric AS gap_pct,
                   (detail->'baseline'->>'mean')::numeric AS baseline_mean,
                   (detail->'baseline'->>'stdev')::numeric AS baseline_stdev,
                   (detail->'baseline'->>'n')::int AS baseline_n,
                   (detail->>'z_score')::numeric AS z
              FROM findings
             WHERE subkind = 'mirror_gap_zscore'
          ORDER BY abs((detail->>'z_score')::numeric) DESC
             LIMIT 50
            """
        )
        rows_raw = cur.fetchall()

    headers = [
        "finding_id", "link", "iso2", "period", "gap_pct",
        "baseline_mean_pct", "baseline_stdev_pct", "baseline_n", "z_score",
    ]
    rows = [
        [
            r["id"], _link_cell(r["id"]), r["iso2"], r["period"],
            _to_float(r["gap_pct"]),
            _to_float(r["baseline_mean"]), _to_float(r["baseline_stdev"]),
            r["baseline_n"], _to_float(r["z"]),
        ]
        for r in rows_raw
    ]
    return SheetData(
        name="mirror_gap_movers",
        description="Mirror-trade gap shifts vs each partner's own rolling baseline. Sorted by |z|. High |z| = gap moved unusually for that partner.",
        headers=headers, rows=rows,
    )


def _low_base_review_sheet() -> SheetData:
    """All hs_group_yoy and hs_group_yoy_export findings flagged low_base.
    Editorial review queue — verify before quoting any percentages."""
    with _conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT id, score, subkind,
                   detail->'group'->>'name' AS group_name,
                   to_char((detail->'windows'->>'current_end')::date, 'YYYY-MM') AS period,
                   (detail->'totals'->>'current_12mo_eur')::numeric AS current_eur,
                   (detail->'totals'->>'prior_12mo_eur')::numeric AS prior_eur,
                   (detail->'totals'->>'yoy_pct')::numeric AS yoy_pct,
                   (detail->'totals'->>'low_base_threshold_eur')::numeric AS threshold
              FROM findings
             WHERE subkind IN ('hs_group_yoy', 'hs_group_yoy_export')
               AND (detail->'totals'->>'low_base')::boolean = true
          ORDER BY abs((detail->'totals'->>'yoy_pct')::numeric) DESC
            """
        )
        rows_raw = cur.fetchall()

    headers = [
        "finding_id", "link", "subkind", "group", "period",
        "current_12mo_eur", "prior_12mo_eur", "yoy_pct", "low_base_threshold_eur",
    ]
    rows = [
        [
            r["id"], _link_cell(r["id"]), r["subkind"], r["group_name"], r["period"],
            _to_float(r["current_eur"]), _to_float(r["prior_eur"]),
            _to_float(r["yoy_pct"]), _to_float(r["threshold"]),
        ]
        for r in rows_raw
    ]
    return SheetData(
        name="low_base_review",
        description="Findings flagged as low-base. Verify the absolute figures before quoting any YoY percentage — small denominators can exaggerate.",
        headers=headers, rows=rows,
    )


def _to_float(v: Any) -> float | None:
    """psycopg2 returns NUMERIC as Decimal; coerce to float for spreadsheet output."""
    if v is None:
        return None
    return float(v)


# =============================================================================
# Writers
# =============================================================================


class XlsxWriter:
    """Render SheetData list to a local .xlsx via openpyxl. The format is
    structurally identical to what GoogleSheetsWriter will push, so the
    iteration on data shape during prototyping carries over directly."""

    def write(self, sheets: list[SheetData], dest: str) -> str:
        from openpyxl import Workbook
        from openpyxl.styles import Font
        from openpyxl.utils import get_column_letter

        wb = Workbook()
        wb.remove(wb.active)

        for sd in sheets:
            ws = wb.create_sheet(title=sd.name[:31])
            ws.append([sd.description])
            ws["A1"].font = Font(italic=True)
            ws.append([])
            ws.append(sd.headers)
            for cell in ws[3]:
                cell.font = Font(bold=True)
            for row in sd.rows:
                ws.append(row)
            # Roughly auto-size columns (cap at 60 chars)
            for col_idx, header in enumerate(sd.headers, start=1):
                max_len = max(
                    [len(str(row[col_idx - 1])) for row in sd.rows
                     if col_idx - 1 < len(row) and row[col_idx - 1] is not None] + [len(header)],
                    default=10,
                )
                ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 2, 60)

        out_path = Path(dest)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        wb.save(out_path)
        return str(out_path)


class GoogleSheetsWriter:
    """Pushes to a Google spreadsheet via gspread. Picks up either a service
    account JSON (production) or OAuth user creds (interactive testing).
    Stub for now — implementation lands when credentials are available."""

    def write(self, sheets: list[SheetData], dest: str) -> str:
        raise NotImplementedError(
            "GoogleSheetsWriter is not yet wired up. Pending service-account credentials. "
            "Use --out-format xlsx for now; the data shape is identical."
        )


def export(out_format: str = "xlsx", out_path: str | None = None,
           spreadsheet_id: str | None = None) -> str:
    """Top-level orchestrator: assemble sheets, pick writer, write."""
    sheets = assemble_sheets()
    if out_format == "xlsx":
        if out_path is None:
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            out_path = f"./exports/findings-{ts}.xlsx"
        return XlsxWriter().write(sheets, out_path)
    elif out_format == "sheets":
        if not spreadsheet_id:
            raise ValueError("--spreadsheet-id required for --out-format sheets")
        return GoogleSheetsWriter().write(sheets, spreadsheet_id)
    else:
        raise ValueError(f"Unknown out_format: {out_format!r}")
