"""Headlines tab — the per-release entry point.

The Headlines doc is what a journalist opens first. It is bounded by
**attention, not completeness** (the governing rule): a couple of pages,
a handful of items, everything else linked into the companion tabs. It
is assembled from material that already exists and is already trusted —
the deterministic front-page movers (`front_page._mover_sentence`) and
the "since the last pack" diff digest (`front_page._since_last_pack_lines`)
— promoted up out of the monolithic Findings doc and reframed around the
release that triggered this cycle.

Three editorial rules, ratified 2026-06-20, drive the structure:

- **Q1 — trigger variants + precedence.** Every source has a *headline
  variant* (the shape of the lead story) and a *fold-in subsection* (a
  dated secondary block). Precedence Eurostat > GACC > HMRC picks which
  variant is the headline this cycle; lower-ranked sources that also
  advanced fold in as dated subsections. HMRC is not a special case —
  it's the same rule; its variant just happens to be the UK cut, which
  leads only when HMRC is the sole trigger.
- **Q2 — restate the few, link the rest.** The headline items are
  restated inline with full numbers (copy-paste-ready) and carry their
  `finding/N` citation token plus a drill-down into the companion. Below
  the cap, everything is a link, not a restatement.
- **Q3 — "what changed" vs "where things stand".** Headlines answers
  *what changed?* (the delta — flips, new findings, top moves). The
  standing levels live in the State-of-play companion.

The two LLM blocks are **placeholders** at this stage — slots in the
layout, not generated content. The deterministic findings are the
critical path; the LLM is additive and its exact nature is deferred (see
the design discussion). The slots are rendered so the layout anticipates
them and so the visual trust-boundary (deterministic vs model) is
established now, but they emit a labelled stub, never a claim.
"""

from __future__ import annotations

from briefing_pack._helpers import _Section, _fmt_month
from briefing_pack.sections.diff import _DiffData
from briefing_pack.sections.front_page import (
    _mover_sentence,
    _since_last_pack_lines,
)

HEADLINES_HEADING = "Headlines"

# Per-source variant config. `lead` is templated with the data month.
# `available` names the kinds of headline material the source can carry,
# purely for the explanatory note — it does not gate rendering (the
# movers list is empty-safe on its own).
_VARIANTS: dict[str, dict] = {
    "eurostat": {
        "lead": "What {month}'s EU figures changed",
        "note": (
            "Triggered by new Eurostat data. The distinctive material — "
            "the China-vs-Europe mirror-trade discrepancy and the HS-sector "
            "shifts — lives here, at its freshest month."
        ),
        "has_sector_movers": True,
        "general_slot": "surface what connects {month}'s findings — and what's notably absent",
    },
    "gacc": {
        "lead": "What China's own {month} figures changed",
        "note": (
            "Triggered by new GACC data, a month ahead of Europe's. No "
            "mirror-gap or HS-sector detail at this altitude — GACC "
            "preliminary is partner/bloc totals, so the headline is "
            "macro/geographic, not sectoral."
        ),
        "has_sector_movers": False,
        "general_slot": "read what China's {month} geography shift implies — grounded in the totals above",
    },
    "hmrc": {
        "lead": "What the UK's {month} figures changed",
        "note": (
            "Triggered by new HMRC data, with no fresher Eurostat month. "
            "The UK cut of the China trade picture — rare as a sole "
            "trigger, since HMRC and Eurostat usually land together."
        ),
        "has_sector_movers": True,
        "general_slot": "read what the UK-China {month} move implies — grounded in the findings above",
    },
}

# Companion tabs the Headlines doc links into (Q2 "link the rest"). Names
# match the bundle's other documents; the per-item cross-tab links await
# the Google Docs tabs spike — for now the citation token is the hard
# provenance anchor and these are doc-level pointers.
_COMPANIONS = (
    ("State of play", "where each group and partner currently stands"),
    ("Sector detail", "the full per-HS-group YoY breakdown"),
    ("Data", "the underlying spreadsheet, one row per finding"),
    ("Glossary", "HS-group definitions and methodology"),
)


def _llm_slot(label: str, scope: str, prompt: str) -> list[str]:
    """A single LLM placeholder block. Rendered as a labelled blockquote
    so it is unmistakably a different trust class from the deterministic
    lines around it — and so it reads as a *slot*, never as a claim.
    Emits a stub only; no model is called at this stage."""
    return [
        f"> 🔶 **LLM · {label}** — _{scope}_  ",
        f"> _Placeholder — not yet generated. Will {prompt}, "
        f"interpreting only the cited findings (no new facts) and "
        f"self-attributing in-sentence so the hedge survives a "
        f"copy-paste._",
        "",
    ]


def _companion_pointers() -> list[str]:
    lines = ["**Where to go deeper**", ""]
    for name, what in _COMPANIONS:
        lines.append(f"- **{name}** — {what}.")
    lines.append("")
    return lines


def _section_headlines(
    top_movers: list[dict],
    diff: _DiffData | None,
    data_period,
    source_trigger: str = "eurostat",
) -> _Section:
    """Assemble the Headlines doc for one cycle.

    `source_trigger` selects the variant (Q1). Defaults to 'eurostat'
    because every periodic export today is Eurostat-triggered by design;
    the gacc/hmrc branches are scaffolded for when trigger-decoupling
    lands but are not yet exercised by a real cycle.
    """
    variant = _VARIANTS.get(source_trigger, _VARIANTS["eurostat"])
    month = _fmt_month(data_period)

    lines: list[str] = []
    lines.append(f"# {HEADLINES_HEADING} — China–Europe trade")
    lines.append("")
    lines.append(f"_Data to {month}. {variant['note']}_")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ----- Variant lead -----
    lines.append(f"## {variant['lead'].format(month=month)}")
    lines.append("")

    # ----- Restated headline items (Q2: restate the few) -----
    if variant["has_sector_movers"]:
        # Eurostat / HMRC: the lead is HS-sector movers (top_movers).
        if top_movers:
            lines.append(
                "*The most quotable shifts this cycle — each a 12-month "
                "total vs the prior 12 months, ending in its citation "
                "token, ready to lift into copy.*"
            )
            lines.append("")
            for i, m in enumerate(top_movers, start=1):
                lines.append(f"{i}. {_mover_sentence(m)}")
            lines.append("")
            # Q2: the specific LLM slot attaches to the lead item.
            lines.extend(_llm_slot(
                "specific", "on the lead finding above",
                "read the lead finding for a desk",
            ))
            lines.append(
                "*The smaller and shakier moves are in the "
                "**Sector detail** tab — not dropped, just not headlined.*"
            )
            lines.append("")
        else:
            lines.append(
                "_No sector-level movers cleared the headline threshold "
                "this cycle._"
            )
            lines.append("")
    else:
        # GACC: the lead is macro/geographic — China's reported exports and
        # imports by partner and bloc. Those findings exist
        # (gacc_aggregate_yoy / gacc_bilateral_aggregate_yoy) but are not
        # yet pulled into the Headlines builder — the next increment needs
        # a compute step analogous to _compute_top_movers for the GACC
        # family. Rendered as an explicit stub so the variant is honest.
        lines.append(
            "_Macro/geographic lead — China's reported exports and imports "
            "by partner and bloc — is the next increment. The findings "
            "exist; the Headlines builder doesn't yet pull them, so this "
            "block is a scaffold, not a no-op._"
        )
        lines.append("")

    # ----- What changed since the last pack (Q3: the delta) -----
    if diff is not None:
        lines.append("## What changed since the last pack")
        lines.append("")
        lines.extend(_since_last_pack_lines(diff))
        lines.append("")
        lines.append(
            "*This tab answers \"what changed?\". Where each group and "
            "partner currently stands — the standing levels — is in the "
            "**State of play** tab.*"
        )
        lines.append("")

    # ----- General LLM slot (Q: the once-per-release reflection) -----
    lines.append("## What the model flags across this release")
    lines.append("")
    lines.extend(_llm_slot(
        "general", "once per release",
        variant["general_slot"].format(month=month),
    ))

    # ----- Companion pointers (Q2: link the rest) -----
    lines.append("---")
    lines.append("")
    lines.extend(_companion_pointers())

    return _Section(markdown="\n".join(lines))
