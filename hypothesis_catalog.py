"""Curated catalog of causal hypotheses for China-EU/UK trade anomalies.

Phase 6.4 of dev_notes/roadmap-2026-05-09.md restructures the LLM framing
layer from "draft a top-line narrative" to "scaffold an investigation".
The LLM's job becomes: pick 2-3 hypotheses from this catalog that are
consistent with the structured facts, and write a one-line rationale
per pick. Corroboration steps are pulled deterministically from the
picked entries.

Why a catalog and not free LLM creativity:

- A catalog is editorially auditable. A journalist (or editor) can
  inspect the full vocabulary the tool can suggest, and add / remove
  / refine entries over time.
- Catalog IDs validate at the same point that numeric facts validate
  (verifier rejects any hypothesis ID not in this list). LLM
  hallucination has nowhere to hide.
- Corroboration steps are the same every time for the same hypothesis,
  so a journalist building a workflow around them gets a stable surface.
- The LLM's editorial value sits in *ranking + rationale* — the part
  it's actually good at — not in inventing investigative leads from
  thin air.

Adding entries: append to CAUSAL_HYPOTHESES with a unique id (snake_case),
a short label suitable for headings, a one-paragraph description that
will appear in the LLM prompt, and a short list of concrete corroboration
steps. Keep entries narrow enough that the LLM can match them to specific
facts; broad catch-all entries dilute the signal.
"""

from __future__ import annotations


CAUSAL_HYPOTHESES = [
    {
        "id": "tariff_preloading",
        "label": "Tariff pre-loading",
        "description": (
            "Importers accelerating shipments to clear customs before an "
            "announced or anticipated EU/UK duty increase takes effect. "
            "Typically shows as a sharp short-term import surge with no "
            "matching unit-price reduction."
        ),
        "corroboration_steps": [
            "Check DG TRADE Trade Defence Investigations register for pending probes covering this HS bracket",
            "Cross-reference EU/UK trade-policy announcements in the 6 months before the surge",
            "Compare against the 3 months immediately after any duty take-effect date for a mirror-image drop",
        ],
    },
    {
        "id": "capacity_expansion_china",
        "label": "Chinese capacity expansion",
        "description": (
            "Structural increase in Chinese production capacity reaching "
            "global markets — typically sustained multi-quarter growth "
            "with falling unit prices. Common in sectors China has "
            "designated as strategic (EVs, batteries, solar, semiconductors)."
        ),
        "corroboration_steps": [
            "Check China's NDRC industrial-policy announcements for the relevant sector",
            "Cross-reference with global capacity-utilisation data from sector trade associations",
            "Look for parallel surges into non-EU markets (US, Brazil, India) as a structural-vs-EU-specific test",
        ],
    },
    {
        "id": "eu_demand_pull",
        "label": "EU/UK demand pull",
        "description": (
            "Sector-specific EU or UK demand shock — defence procurement, "
            "energy-transition build-out, auto sector ramp, infrastructure "
            "spending — pulling Chinese imports above trend independently "
            "of any China-side supply change."
        ),
        "corroboration_steps": [
            "Check Eurostat industrial-output index for the adjacent NACE sector",
            "Cross-reference with EU policy spending (Green Deal, REPowerEU, defence funds)",
            "Compare with same-period imports from non-Chinese suppliers — a true demand pull lifts all sources",
        ],
    },
    {
        "id": "transshipment_reroute",
        "label": "Transshipment / partner reroute",
        "description": (
            "Goods are rerouted via Hong Kong, Macau, Singapore, UAE or "
            "another transshipment hub, shifting which Eurostat partner "
            "code captures the trade. May appear as a CN drop offset by "
            "an HK/MO/SG rise, or as an apparent anomaly when only "
            "partner=CN is queried."
        ),
        "corroboration_steps": [
            "Re-run with --eurostat-partners CN,HK,MO and check whether the anomaly persists",
            "Look at the partner-share ratio (CN vs HK+MO) over the same window",
            "Check for known transshipment-hub effects in the partner field — Rotterdam (NL) for EU re-exports",
        ],
    },
    {
        "id": "russia_substitution",
        "label": "Substitution after Russia/Belarus sanctions",
        "description": (
            "EU substitution of Russian or Belarusian supply with Chinese "
            "alternatives following the 2022 sanctions packages. Typical "
            "in energy carriers, fertilizers, basic metals, and some "
            "machinery. Usually shows as a step-change in early 2022, "
            "not a smooth trend."
        ),
        "corroboration_steps": [
            "Check the 2022-02 / 2022-03 inflection point in the trajectory series",
            "Cross-reference with EU sanctions packages 1-12 timing",
            "Compare against same-HS imports from Russia / Belarus over the same window for the mirror-image collapse",
        ],
    },
    {
        "id": "currency_effect",
        "label": "Currency-driven price effect",
        "description": (
            "RMB depreciation against EUR / GBP making Chinese exports "
            "cheaper in destination-currency terms. Typically shows as "
            "rising values + rising volumes + falling unit prices in "
            "destination currency, without a corresponding capacity story."
        ),
        "corroboration_steps": [
            "Check ECB EUR/CNY (and BoE GBP/CNY) monthly averages for the comparison window",
            "Look for a unit-price drop of similar magnitude to the FX move — a currency-driven price effect should track FX, not undercut it",
            "Test whether the effect is uniform across HS codes or concentrated in price-sensitive ones",
        ],
    },
    {
        "id": "friend_shoring_decline",
        "label": "Friend-shoring / supply-chain diversification",
        "description": (
            "EU or UK importers actively diversifying away from Chinese "
            "supply toward Vietnam, India, Mexico, Turkey or others. "
            "Typically shows as a sustained Chinese decline alongside "
            "rising imports of the same HS from alternative-sourcing "
            "countries."
        ),
        "corroboration_steps": [
            "Compare same-period imports of the same HS bracket from VN, IN, MX, TR, BD",
            "Check sector-specific diversification announcements (auto OEMs, electronics, pharma)",
            "Look for a slope inflection rather than a step-change — diversification is incremental",
        ],
    },
    {
        "id": "trade_defence_outcome",
        "label": "Anti-dumping / anti-subsidy outcome",
        "description": (
            "An EU or UK trade-defence investigation has concluded with "
            "duties imposed (or dropped). Imports drop sharply at the "
            "duty take-effect date, or rise after a probe is closed without "
            "duties. Often paired with a preceding tariff-pre-loading spike."
        ),
        "corroboration_steps": [
            "Check the EU OJ for anti-dumping / countervailing regulations covering this HS bracket in the relevant window",
            "Match the inflection date in the trajectory against the duty take-effect date",
            "Check for parallel UK Trade Remedies Authority decisions",
        ],
    },
    {
        "id": "cn8_reclassification",
        "label": "CN8 code reclassification",
        "description": (
            "Apparent change is partly or fully an artefact of Eurostat's "
            "annual CN8 revision (each January). Goods reclassified into "
            "or out of the HS bracket make the YoY comparison non-like-for-"
            "like. The cn8_revision caveat fires automatically on any "
            "window crossing a year boundary."
        ),
        "corroboration_steps": [
            "Check the EU CN concordance table for the relevant HS bracket",
            "Compare the 8-digit code-level breakdown across the year boundary for identical codes",
            "If a single CN8 code dominates the change, check whether it appeared / disappeared in the revision",
        ],
    },
    {
        "id": "base_effect",
        "label": "Base-effect distortion",
        "description": (
            "The prior-year baseline is unusually low or high (often due "
            "to a one-off event — a customs strike, a single shipment, a "
            "force majeure suspension), making the YoY comparison "
            "misleading even though the current period is normal."
        ),
        "corroboration_steps": [
            "Plot the 24-month trajectory and check whether the prior-year comparison-period stands out",
            "Look for a known one-off event in the prior window (port strike, factory shutdown, regulatory pause)",
            "Compare the same period two years prior to disambiguate true trend from base distortion",
        ],
    },
    {
        "id": "energy_transition",
        "label": "Energy-transition demand surge",
        "description": (
            "EU or UK demand surge tied specifically to the energy "
            "transition: solar PV cells, lithium-ion batteries, "
            "electric vehicles, wind turbine components, heat pumps, "
            "permanent magnets. Often paired with EU policy support "
            "(Green Deal Industrial Plan, REPowerEU, NZIA) creating "
            "predictable demand."
        ),
        "corroboration_steps": [
            "Cross-reference with Green Deal Industrial Plan / NZIA / REPowerEU programme spend",
            "Check capacity-installation data from SolarPower Europe, WindEurope, T&E (transport)",
            "Compare against domestic EU production growth in the same product to test substitution vs total-market growth",
        ],
    },
    {
        "id": "post_pandemic_normalisation",
        "label": "Post-pandemic normalisation",
        "description": (
            "The trajectory still reflects post-2020 normalisation rather "
            "than a fresh shock — supply chains, demand patterns, or "
            "shipping costs returning to pre-pandemic baselines. Most "
            "common in trajectories where the prior window straddles a "
            "lockdown or shipping-crisis period."
        ),
        "corroboration_steps": [
            "Compare against the same period in 2019 (pre-pandemic baseline) rather than just YoY",
            "Check the 2020-Q2 / 2021-Q3 (Suez/shipping crisis) periods in the trajectory for the original distortion",
            "If the current value is close to the 2019 baseline, the YoY change is normalisation, not new movement",
        ],
    },
]


CATALOG_BY_ID = {h["id"]: h for h in CAUSAL_HYPOTHESES}


def get_catalog_ids() -> list[str]:
    return [h["id"] for h in CAUSAL_HYPOTHESES]


def get_corroboration_steps(hypothesis_ids: list[str]) -> list[str]:
    """Union of corroboration steps for the picked hypotheses, in catalog order,
    de-duplicated. The LLM picks the hypotheses; the steps come deterministically
    from this lookup."""
    seen: set[str] = set()
    steps: list[str] = []
    for h in CAUSAL_HYPOTHESES:
        if h["id"] not in hypothesis_ids:
            continue
        for step in h["corroboration_steps"]:
            if step not in seen:
                seen.add(step)
                steps.append(step)
    return steps


def get_catalog_for_prompt() -> list[dict]:
    """The catalog representation embedded in the LLM prompt — includes id,
    label, description, but NOT corroboration steps (the LLM doesn't pick those;
    they're attached deterministically post-hoc). Keeping them out of the
    prompt also reduces the chance the LLM treats them as suggested prose."""
    return [
        {"id": h["id"], "label": h["label"], "description": h["description"]}
        for h in CAUSAL_HYPOTHESES
    ]
