"""Endnote explaining what `finding/N` citation tokens mean."""

from __future__ import annotations

from briefing_pack._helpers import _Section


def _section_about_findings() -> _Section:
    """Endnote explaining what `finding/N` citation tokens mean and how
    to look one up. Identical text in both the brief and the leads doc
    (called from both `render()` and `render_leads()`).

    Kept terse here because the deeper data-model + per-subkind detail
    lives in `docs/architecture.md` and `docs/methodology.md`. This
    endnote is just the bridge from a cited number to those docs.
    """
    lines: list[str] = []
    lines.append("---")
    lines.append("")
    lines.append("## About the `finding/N` citations")
    lines.append("")
    lines.append(
        "Every claim in this document ends with a citation token like "
        "`finding/12345`. Each refers to a row in the project's `findings` "
        "table — one per detected anomaly, carrying a JSONB `detail` "
        "blob with the totals, window dates, observation IDs, caveat "
        "codes, and method version that produced the claim."
    )
    lines.append("")
    lines.append("**Subkinds you'll see cited:**")
    lines.append("")
    lines.append(
        "- `mirror_gap`, `mirror_gap_zscore` — China-vs-EU/UK customs "
        "comparison and z-score movers."
    )
    lines.append(
        "- `hs_group_yoy*` — rolling 12-month YoY for an HS group, "
        "scoped to one of `eu_27` / `uk` / `eu_27_plus_uk`. Suffixes "
        "encode flow + scope (e.g. `_uk_export`)."
    )
    lines.append(
        "- `hs_group_trajectory*` — 24-month shape classification for "
        "the same series (12-shape vocabulary)."
    )
    lines.append(
        "- `narrative_hs_group` — LLM-scaffolded leads (companion "
        "doc only). Catalogued in `docs/methodology.md` §1."
    )
    lines.append("")
    lines.append(
        "**Stability across revisions.** A citation always points at "
        "a *specific* row. When the analyser re-runs and concludes a "
        "different value, it inserts a new row and stamps the old one "
        "with `superseded_at` + `superseded_by_finding_id`. So "
        "`finding/12345` remains a reproducible reference to the exact "
        "claim made in this document, even after the underlying numbers "
        "later move."
    )
    lines.append("")
    lines.append(
        "**Looking one up today.** Direct DB query: "
        "`SELECT * FROM findings WHERE id = 12345;` against the project's "
        "Postgres instance. A hosted finding viewer is on the roadmap "
        "(set `GACC_PERMALINK_BASE` to render citations as Markdown "
        "links instead of bare tokens once it exists)."
    )
    lines.append("")
    lines.append(
        "**For deeper context**, see "
        "[`docs/methodology.md`](../docs/methodology.md) (what each "
        "subkind measures and when to quote it) and "
        "[`docs/architecture.md`](../docs/architecture.md) (the full "
        "raw_rows → observations → findings data flow)."
    )
    lines.append("")
    return _Section(markdown="\n".join(lines))
