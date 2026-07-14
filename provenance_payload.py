"""Structured, portal-native provenance payloads — the data behind the
"click a number to see where it came from" drawers (journalist-usability
iteration 3: the self-verifying portal).

Distinct from `provenance.py`, which renders full markdown audit *documents* for
the bundle/CLI. This module returns structured data that is baked into the
portal snapshot (`report.json`) so the STATIC portal can show a per-finding
provenance drawer with NO database access:

  - the **source-URL trail** — the primary "where did this come from", a link
    per release the finding’s observations came from;
  - the headline **arithmetic** — how the figure was computed;
  - the plain-English **caveats**;
  - a collapsed **replay-SQL** "for the record".

The source trail is GENERIC — any finding’s `observation_ids` resolve to their
release source URLs — so the KPIs (`trade_balance`, `china_all_goods_share`) are
covered even though they have no per-subkind markdown renderer in
`provenance.py`. The arithmetic is best-effort per gated subkind family with a
graceful fallback to none (the drawer still shows sources + caveats).
"""
from __future__ import annotations

import logging
from typing import Any

# Formatting is deliberately NOT local (2026-07-01 fresh review, F3): a drawer
# that restates a card's value in a different rendering ("≈€1.0bn/day" under a
# "€1,027M/day" card) leaks trust at exactly the moment a journalist is
# double-checking. Values that appear on a card face use that face's formatter
# (_fmt_eur_per_day, _fmt_pct_kpi); everything else uses the bundle-wide
# _fmt_eur/_fmt_pct.
from briefing_pack._helpers import (
    _fmt_eur,
    _fmt_eur_per_day,
    _fmt_pct,
    _fmt_pct_kpi,
)
from provenance import _CAVEAT_GLOSSARY

log = logging.getLogger(__name__)

_SOURCE_LABEL = {
    "eurostat": "Eurostat", "gacc": "GACC (China Customs)", "hmrc": "HMRC",
}


def _obs_sources(cur, observation_ids: list[int]) -> list[dict]:
    """Source-URL trail for observation-based findings (trade_balance,
    china_all_goods_share, GACC bilateral …): one entry per distinct release the
    finding’s observations came from. Generic across eurostat / gacc / hmrc."""
    if not observation_ids:
        return []
    cur.execute(
        """
        SELECT r.period, r.source, r.source_url, r.title,
               array_agg(DISTINCT o.period_kind) AS period_kinds
          FROM observations o JOIN releases r ON r.id = o.release_id
         WHERE o.id = ANY(%s)
      GROUP BY r.period, r.source, r.source_url, r.title
      ORDER BY r.period
        """,
        (list(observation_ids),),
    )
    out: list[dict] = []
    for r in cur.fetchall():
        period = r["period"]
        cumulative = "cumulative_jan_feb" in (r["period_kinds"] or [])
        out.append({
            "period": period.isoformat() if period else None,
            "source": _SOURCE_LABEL.get(r["source"], r["source"]),
            "url": r["source_url"],
            "label": r["title"] or (period.strftime("%b %Y") if period else r["source_url"]),
            "coverage": "Jan+Feb cumulative" if cumulative else None,
        })
    return out


def _window_source_kinds(subkind: str) -> list[str]:
    """Which release sources a window-based (raw-row) finding draws on, inferred
    from its scope suffix. EU-27 = Eurostat; UK = HMRC; combined = both."""
    if "_combined" in subkind:
        return ["eurostat", "hmrc"]
    if "_uk" in subkind:
        return ["hmrc"]
    return ["eurostat"]


def _window_sources(cur, subkind: str, detail: dict) -> list[dict]:
    """Source-URL trail for findings that read the raw rows directly and so carry
    no observation_ids (e.g. hs_group_yoy): the source releases for each month in
    the finding’s *current* 12-month window — the figure being quoted."""
    w = (detail or {}).get("windows") or {}
    start, end = w.get("current_start"), w.get("current_end")
    if not (start and end):
        return []
    cur.execute(
        """
        SELECT period, source, source_url, title
          FROM releases
         WHERE source = ANY(%s) AND period BETWEEN %s AND %s
         ORDER BY period
        """,
        (_window_source_kinds(subkind), start, end),
    )
    out: list[dict] = []
    for r in cur.fetchall():
        period = r["period"]
        out.append({
            "period": period.isoformat() if period else None,
            "source": _SOURCE_LABEL.get(r["source"], r["source"]),
            "url": r["source_url"],
            "label": r["title"] or (period.strftime("%b %Y") if period else r["source_url"]),
            "coverage": None,
        })
    return out


def _arithmetic(subkind: str, detail: dict | None) -> list[str]:
    """Best-effort headline-arithmetic lines for the gated subkind families.
    Returns [] when the shape isn’t recognised — the drawer then shows just the
    source trail + caveats, which is still the core 'where did this come from'."""
    d = detail or {}
    if subkind.startswith("trade_balance"):
        roll = (d.get("totals") or {}).get("rolling_12mo") or {}
        imp, exp = roll.get("import_eur"), roll.get("export_eur")
        defi, per_day = roll.get("deficit_eur"), roll.get("deficit_per_day_eur")
        out = []
        if imp is not None and exp is not None:
            out.append(f"Imports {_fmt_eur(imp)} − exports {_fmt_eur(exp)} = "
                       f"{'deficit' if (defi or 0) >= 0 else 'surplus'} {_fmt_eur(abs(defi or 0))} "
                       f"(rolling 12 months).")
        if per_day is not None:
            # The card's own face format — the restated figure must match it.
            out.append(f"Over the window’s days ≈ {_fmt_eur_per_day(abs(per_day))}.")
        if roll.get("yoy_pct") is not None:
            out.append(f"Year on year: {_fmt_pct(roll['yoy_pct'])} vs the prior 12 months.")
        return out
    if subkind.startswith("china_all_goods_share"):
        roll = d.get("rolling_12mo") or {}
        num, den, share = roll.get("numerator_eur"), roll.get("denominator_eur"), roll.get("share")
        out = []
        if num is not None and den is not None and share is not None:
            # Shares stay bare .1f — the donut centre's own face format.
            out.append(f"CN+HK+MO {_fmt_eur(num)} ÷ extra-EU {_fmt_eur(den)} = "
                       f"{float(share) * 100:.1f}% (rolling 12 months, all goods).")
        if roll.get("share_cn_only") is not None:
            out.append(f"China-only comparator: {float(roll['share_cn_only']) * 100:.1f}%.")
        return out
    if subkind.startswith("hs_group_yoy"):
        t = d.get("totals") or {}
        cur_, pri = t.get("current_12mo_eur"), t.get("prior_12mo_eur")
        out = []
        if cur_ is not None and pri is not None:
            out.append(f"12 months {_fmt_eur(cur_)} vs {_fmt_eur(pri)} the prior 12 months "
                       f"= {_fmt_pct(t.get('yoy_pct'))} by value.")
        if t.get("current_12mo_kg") is not None and t.get("prior_12mo_kg") is not None:
            out.append(f"By volume: {_fmt_pct(t.get('yoy_pct_kg'))} "
                       f"({t['current_12mo_kg']:,.0f} kg vs {t['prior_12mo_kg']:,.0f} kg).")
        if t.get("low_base"):
            out.append("Low base — quote the € amount, not the percentage.")
        return out
    if subkind.startswith(("gacc_aggregate_yoy", "gacc_bilateral_aggregate_yoy")):
        # The GACC page's strip/standout figures: single-month face first
        # (that's the card's own register), then YTD and the rolling window.
        t = d.get("totals") or {}
        out = []
        sm = t.get("single_month") or {}
        if sm.get("current_eur") is not None and sm.get("prior_eur") is not None:
            out.append(f"Latest month {_fmt_eur(sm['current_eur'])} vs "
                       f"{_fmt_eur(sm['prior_eur'])} the same month last year "
                       f"= {_fmt_pct(sm.get('yoy_pct'))}.")
        ytd = t.get("ytd_cumulative") or {}
        if ytd.get("current_eur") is not None and ytd.get("prior_eur") is not None:
            mo = (f" ({ytd['months_in_ytd']}-month cumulative)"
                  if ytd.get("months_in_ytd") else "")
            out.append(f"Year to date {_fmt_eur(ytd['current_eur'])} vs "
                       f"{_fmt_eur(ytd['prior_eur'])} = "
                       f"{_fmt_pct(ytd.get('yoy_pct'))}{mo}.")
        cur_, pri = t.get("current_12mo_eur"), t.get("prior_12mo_eur")
        if cur_ is not None and pri is not None:
            out.append(f"Rolling 12 months {_fmt_eur(cur_)} vs {_fmt_eur(pri)} "
                       f"= {_fmt_pct(t.get('yoy_pct'))}.")
        if out:
            out.append("China's own customs figures (GACC), EUR-equivalent at "
                       "per-period rates — growth rates will not exactly match "
                       "GACC's published CNY/USD figures.")
        return out
    if subkind.startswith("gacc_commodity_yoy"):
        # The commodities block: CNY-basis arithmetic (GACC's own comparison
        # basis) with the derived-prior method spelled out, the volume rate,
        # and GACC's published cumulative pct beside ours — the defensible
        # published figure always one click away.
        t = d.get("totals") or {}
        sm = t.get("single_month") or {}
        ytd = t.get("ytd_cumulative") or {}
        com = d.get("commodity") or {}
        out = []
        if sm.get("current_value_cny") is not None and sm.get("prior_value_cny") is not None:
            out.append(
                f"Latest month CNY {sm['current_value_cny']:,.1f} (100M) vs a "
                f"derived CNY {sm['prior_value_cny']:,.1f} for the same month "
                f"last year = {_fmt_pct(sm.get('value_yoy_pct'))} by value "
                f"(CNY terms)."
            )
            if sm.get("prior_derivation") == "prior_ytd_adjacent_page_difference":
                out.append(
                    "The prior-year month is the difference of two prior-year "
                    "cumulative columns printed on this month's and last "
                    "month's release pages — GACC's own (revised) comparison "
                    "basis; typically ~0.3% from the originally-published "
                    "month."
                )
        if sm.get("quantity_yoy_pct") is not None and sm.get("current_quantity") is not None:
            unit = com.get("quantity_unit") or sm.get("quantity_unit") or ""
            out.append(
                f"By volume: {_fmt_pct(sm['quantity_yoy_pct'])} "
                f"({sm['current_quantity']:,.1f} vs {sm['prior_quantity']:,.1f} "
                f"{unit}) — unit counts, no currency in the comparison."
            )
        if ytd.get("value_yoy_pct") is not None:
            pub = ytd.get("published_yoy_value_pct")
            line = (f"Year to date: {_fmt_pct(ytd['value_yoy_pct'])} by value "
                    f"(both sides printed on the same release page)")
            if pub is not None:
                line += (f"; GACC's own published figure is {pub}% — the two "
                         f"are cross-checked and emission is withheld when "
                         f"they disagree beyond printed-cell rounding")
            out.append(line + ".")
        if t.get("eur_month") is not None:
            out.append(f"EUR display level: {_fmt_eur(t['eur_month'])} in the "
                       f"month, converted at the month's rate — display only; "
                       f"the growth rates above are CNY-denominated.")
        out.append("World totals from GACC's commodity catalogue — no "
                   "country split exists in this source; not an EU-specific "
                   "figure.")
        return out
    if subkind.startswith("cn8_yoy_mover"):
        t = d.get("totals") or {}
        prod = d.get("product") or {}
        cur_, pri = t.get("current_12mo_eur"), t.get("prior_12mo_eur")
        out = []
        if prod.get("cn8"):
            out.append(f"CN8 {prod['cn8']}"
                       + (f" — {prod.get('denomination') or prod.get('label_short')}"
                          if (prod.get('denomination') or prod.get('label_short')) else "")
                       + ".")
        if cur_ is not None and pri is not None:
            # _fmt_pct_kpi: the mover card's 0dp face — the restated % must
            # match the card, not the 1dp bundle style.
            out.append(f"12 months {_fmt_eur(cur_)} vs {_fmt_eur(pri)} the prior 12 months "
                       f"= {_fmt_pct_kpi(t.get('yoy_pct'))} by value.")
        persist = (d.get("persistence") or {}).get("anchor_yoys") or []
        if persist:
            out.append("Held across the last "
                       + ", ".join(_fmt_pct(y) for y in persist)
                       + " (most recent first), and survives dropping its single "
                       "largest month — so it isn't one shipment.")
        groups = d.get("parent_groups") or []
        out.append("Single product within the watched chapters"
                   + (f", inside the displayed group {groups[0]}." if groups
                      else ", outside any displayed group.")
                   + " A 'worth a look' cue, not a headline-grade finding.")
        return out
    return []


def _replay_sql(observation_ids: list[int]) -> str | None:
    """A generic fact-checker query: pull the exact observation rows (and their
    source releases) that the figure summed. Honest and runnable by anyone with
    DB access; the drawer keeps it collapsed 'for the record'."""
    if not observation_ids:
        return None
    ids = ", ".join(str(int(i)) for i in observation_ids[:300])
    more = "" if len(observation_ids) <= 300 else f"  -- (+{len(observation_ids) - 300} more ids)"
    return (
        "SELECT o.id, r.source, r.period, r.source_url, o.flow,\n"
        "       o.value_amount, o.value_currency\n"
        "  FROM observations o JOIN releases r ON r.id = o.release_id\n"
        f" WHERE o.id IN ({ids}){more}\n"
        " ORDER BY r.period, o.id;"
    )


def build_payload(cur, finding: dict) -> dict:
    """One structured provenance payload for a finding row (id, subkind, title,
    detail, observation_ids)."""
    obs = list(finding.get("observation_ids") or [])
    detail = finding.get("detail") or {}
    subkind = finding["subkind"]
    # Observation-based findings (trade_balance, china_share, GACC bilateral) cite
    # their releases via observation_ids; raw-row-based ones (hs_group_yoy) carry
    # none, so fall back to the source releases for the finding's window.
    sources = _obs_sources(cur, obs) if obs else _window_sources(cur, subkind, detail)
    caveats = [
        {"code": c, "gloss": _CAVEAT_GLOSSARY.get(c, "")}
        for c in (detail.get("caveat_codes") or [])
    ]
    return {
        "finding_id": finding["id"],
        "title": finding.get("title"),
        "sources": sources,
        "arithmetic": _arithmetic(subkind, detail),
        "caveats": caveats,
        "replay_sql": _replay_sql(obs) if obs else None,
    }


def build_payloads_for(cur, finding_ids) -> dict[str, dict]:
    """Build provenance payloads for a set of finding ids, keyed by str(id)
    (JSON object keys are strings). Best-effort per finding — a payload that
    fails to build is logged and skipped rather than sinking the snapshot."""
    ids = sorted({int(i) for i in finding_ids if i is not None})
    if not ids:
        return {}
    cur.execute(
        "SELECT id, subkind, title, detail, observation_ids "
        "FROM findings WHERE id = ANY(%s)",
        (ids,),
    )
    rows = cur.fetchall()
    out: dict[str, dict] = {}
    for f in rows:
        try:
            out[str(f["id"])] = build_payload(cur, f)
        except Exception:
            log.exception("provenance payload failed for finding %s", f.get("id"))
    return out
