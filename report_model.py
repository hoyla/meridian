"""Rendering-agnostic content model for a Meridian report — the spine.

This is the single source of truth a report is built into, and the format
the published snapshot is serialised to. Renderers (markdown for LLMs, the
HTML web portal, a docx fallback) each consume *this* — none of them is
canonical. Design decisions recorded in
`dev_notes/2026-06-20-web-portal-and-content-schema-design.md`.

Governing rule: the model carries **data + semantics, never
presentation**. A `HeadlineItem` holds `direction`/`pct`/`value` as
fields; whether that becomes a sentence, a card, or a chart row is a
renderer's call. The two deliberate exceptions are *editorial prose*
fields (`prose`) — the hedge wording is editorial substance, authored
once, not formatting (Fork A, ratified 2026-06-20) — and chart `series`,
which is data the renderer turns into PNG / inline SVG / interactive.

Four invariants:
1. Every leaf carries `Provenance` (finding ids, source, as_of) —
   defensibility lives in the model, not bolted on per renderer.
2. Data, not presentation (above).
3. `LLMSlot` is a first-class node with `status` + `grounded_in`, so the
   deterministic-vs-model trust boundary and the interpret-not-introduce
   constraint are in the data — no renderer can blur them.
4. Charts carry `series`, not images — kills the image-by-URL coupling at
   the model level.

`Facets` (sector/theme/partner/commodity) make findings *queryable* along
reporter-relevant axes that cut across the HS tree — what lets a web
portal hold a long tail of categories and still be navigable ("everything
steel" spans chapters, flows, partners). Added 2026-06-20.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Literal, Optional

Variant = Literal["eurostat", "gacc", "hmrc"]
ChartType = Literal[
    "bignumber", "bignumber_delta", "sparkline", "donut", "line", "bar"
]

SCHEMA_VERSION = "0.1.0"


@dataclass
class Facets:
    """Reporter-relevant axes a finding can be navigated/searched along,
    orthogonal to its place in the section tree."""
    sector: list[str] = field(default_factory=list)
    theme: list[str] = field(default_factory=list)
    partner: list[str] = field(default_factory=list)
    commodity: list[str] = field(default_factory=list)


@dataclass
class Provenance:
    """Every leaf carries this. Defensibility is in the data model."""
    finding_ids: list[int] = field(default_factory=list)
    source: str = ""  # eurostat | gacc | hmrc | cross_source
    as_of: Optional[date] = None
    caveat: Optional[str] = None  # provisional / low_base / double-count etc.


@dataclass
class SeriesPoint:
    period: date
    value: float


@dataclass
class ChartData:
    """The chart's SERIES — not a rendered image. Renderers pick the
    encoding (PNG for docx, inline SVG for HTML, interactive for the
    portal)."""
    chart_type: ChartType
    series: list[SeriesPoint] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class Indicator:
    """A vital sign — always shown, change or not (amended Q3). Carries
    figure + provenance + (optional) sparkline series; the glyph never
    travels without its number and source."""
    key: str
    label: str
    value: float
    unit: str
    formatted: str
    chart: ChartType = "bignumber"
    delta: Optional[dict] = None  # {value, direction, formatted}
    chart_data: Optional[ChartData] = None
    provenance: Provenance = field(default_factory=Provenance)


@dataclass
class HeadlineItem:
    """A restated quotable finding (Q2: restate-the-few). Holds both the
    structured metrics AND the publishable `prose` (Fork A)."""
    subject: dict  # {scope, flow, group_name}
    metrics: dict  # {direction, pct, value, volume?}
    stability: dict  # {badge, hedge_phrase}
    prose: str  # the publishable, hedge-graded sentence
    drill_down: Optional[str] = None  # ref id → Section / Finding
    provenance: Provenance = field(default_factory=Provenance)
    facets: Facets = field(default_factory=Facets)


@dataclass
class LLMSlot:
    """First-class node; a placeholder until the LLM design lands. The
    trust boundary and interpret-not-introduce constraint live here."""
    slot_type: Literal["specific", "general"]
    grounded_in: list[int] = field(default_factory=list)  # finding ids
    status: Literal["placeholder", "generated"] = "placeholder"
    content: Optional[str] = None


@dataclass
class Headline:
    """The per-release entry surface (Q1 variant-shaped)."""
    variant: Variant
    lead_title: str
    note: str
    items: list[HeadlineItem] = field(default_factory=list)
    llm_slots: list[LLMSlot] = field(default_factory=list)


@dataclass
class Shift:
    group_name: str
    subkind: str
    window_end: Optional[date] = None
    old_yoy: Optional[float] = None
    new_yoy: Optional[float] = None
    direction_flipped: bool = False


@dataclass
class WhatChanged:
    """The delta register (Q3: 'what changed?')."""
    regime: str  # first_export | method_bump | no_change | movement
    summary: str  # the 'since the last pack' digest prose
    significant: list[Shift] = field(default_factory=list)
    new_count: int = 0


@dataclass
class Finding:
    """Leaf node of the content tree. Every leaf carries provenance +
    facets."""
    finding_id: int
    subkind: str
    title: str
    metrics: dict = field(default_factory=dict)
    prose: Optional[str] = None
    chart_data: Optional[ChartData] = None
    provenance: Provenance = field(default_factory=Provenance)
    facets: Facets = field(default_factory=Facets)


@dataclass
class Section:
    """Recursive content tree — the source of navigable granularity.
    `sections` are sub-sections; `findings` are leaves. A node may carry
    either or both."""
    id: str
    title: str
    kind: str  # state_of_play | sector_detail | glossary | data
    intro: Optional[str] = None
    sections: list["Section"] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)
    facets: Facets = field(default_factory=Facets)


@dataclass
class ReportMeta:
    data_period: Optional[date]
    variant: Variant
    snapshot_id: str
    generated_at: Optional[datetime] = None
    scope: Optional[str] = None
    schema_version: str = SCHEMA_VERSION


@dataclass
class Report:
    """The whole report, rendering-agnostic. `key_indicators` is a
    top-level register (Fork B), distinct from the `sections` tree."""
    meta: ReportMeta
    key_indicators: list[Indicator] = field(default_factory=list)
    headline: Optional[Headline] = None
    what_changed: Optional[WhatChanged] = None
    sections: list[Section] = field(default_factory=list)


# --------------------------------------------------------------------------
# Serialisation — the published-snapshot format the portal reads.
# --------------------------------------------------------------------------

def _json_default(o: Any) -> Any:
    if isinstance(o, (date, datetime)):
        return o.isoformat()
    raise TypeError(f"not JSON-serialisable: {type(o)}")


def to_dict(report: Report) -> dict:
    """Plain-dict form (dates → ISO strings) for JSON or templating."""
    return json.loads(to_json(report))


def to_json(report: Report, *, indent: int | None = 2) -> str:
    return json.dumps(asdict(report), default=_json_default, indent=indent)
