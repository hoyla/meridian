"""Markdown briefing-pack export for findings.

Companion to sheets_export.py. Where the spreadsheet is for editorial
scanning, the briefing pack is for narrative reading — and, by design,
for upload to NotebookLM as a one-shot exploration corpus.

Design principles:

1. Deterministic. No LLM. The pack is a structured render of what's in
   the `findings` table, grouped and sorted but otherwise untransformed.
   The LLM framing layer is a separate later step that operates over the
   same finding set.
2. Provenance-first. Every finding line ends with a canonical
   `[finding/{id}]` token (NotebookLM citation handle, future web-UI
   permalink) and a one-line method tag. A `## Sources` appendix at the
   end of the pack lists every release URL underlying the brief, grouped
   by source, with fetch timestamps. A journalist clicking through has
   third-party links one tap away.
3. Same data layer as the Sheets exporter. We re-read findings from
   Postgres, not the rendered XLSX — so the two surfaces are independent
   and any one of them can be wrong without contaminating the other.

CLI: see scrape.py `--briefing-pack`.

Package layout (refactored 2026-05-12 from a single 2,001-line module):

- `briefing_pack._helpers` — DB connection, formatters, predictability,
  threshold-fragility, scope labels, the family-universal caveat set.
- `briefing_pack.sections.*` — one module per `_section_*` builder.
- `briefing_pack.render` — `render()` / `render_leads()` / `export()`
  plus `latest_eurostat_period()` / `latest_recorded_data_period()` for
  periodic-run idempotency.

This `__init__.py` re-exports every symbol that external callers
(scrape.py CLI, periodic.py orchestrator, sheets_export.py, tests) reach
for, so the refactor is a pure file split with zero behaviour change.
"""

from __future__ import annotations

# Public API — used by scrape.py, periodic.py, sheets_export.py, tests.
from briefing_pack._helpers import (
    DEFAULT_TOP_N,
    PERMALINK_BASE_ENV,
    _ALL_UNIVERSAL_CAVEATS,
    _SCOPE_LABEL,
    _SCOPE_SUBKIND_SUFFIX,
    _Section,
    _compute_predictability_per_group,
    _construct_chinese_source_url,
    _slugify_scope,
    _threshold_fragility_annotation,
    is_threshold_fragile,
)
from briefing_pack.render import (
    export,
    latest_eurostat_period,
    latest_recorded_data_period,
    render,
    render_leads,
)

__all__ = [
    # Public API
    "DEFAULT_TOP_N",
    "PERMALINK_BASE_ENV",
    "export",
    "is_threshold_fragile",
    "latest_eurostat_period",
    "latest_recorded_data_period",
    "render",
    "render_leads",
    # Internals reached for by sheets_export.py and tests. Underscored to
    # mark "package-internal but cross-module" — not stable API.
    "_ALL_UNIVERSAL_CAVEATS",
    "_SCOPE_LABEL",
    "_SCOPE_SUBKIND_SUFFIX",
    "_Section",
    "_compute_predictability_per_group",
    "_construct_chinese_source_url",
    "_slugify_scope",
    "_threshold_fragility_annotation",
]
