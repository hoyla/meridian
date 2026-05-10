# Phase 6.5: sector-breadth review of draft HS groups

Captured 2026-05-10 alongside Phase 6.2 + 6.7. The Phase 5 strategic
review added 13 draft HS groups (8 current-affairs + 5 Guardian-DNA),
all tagged `created_by LIKE 'draft:%'` so they're queryable as a
batch. This doc surfaces each group with its current activity stats
and a recommendation. Three exit states per group: **promote** (drop
the `draft:` prefix on `created_by`), **keep-draft** (revisit later),
**drop** (delete from `hs_groups`).

The recommendation column is mine and is meant as a starting point —
final call is the user's. Confidence column flags where I'm shaky.

## How to read the activity columns

- **EU yoy**: latest active YoY finding for `hs_group_yoy` (EU-27 imports).
  `(lb)` = `low_base` flag set (current OR prior 12-month total below the
  €50M threshold), so the percentage rests on a small denominator.
- **UK yoy**: same for `hs_group_yoy_uk`.
- **Trajectory shape**: latest classified shape for the import series
  (49-month windows; trajectory was re-fitted under Phase 6.0.7's
  longest-contiguous-run logic).

## Current-affairs draft groups (8)

These were proposed for active news cycles in 2025-2026.

| Group | EU yoy | UK yoy | Shape | Recommendation | Confidence |
|---|---|---|---|---|---|
| **Critical minerals (export-controlled by China)** | -29.9% (€483M) | +23.9% lb (€57M) | peak-and-fall | **Promote** | High — China's Aug 2023 / Dec 2023 export controls on Ga, Ge, graphite + Sep 2024 Sb controls are exactly the policy moment this group was built to watch. Trajectory cleanly inverse-U (peaked, now falling) matches the controls biting. UK reading is low-base but the EU one isn't. |
| **Drones and unmanned aircraft (HS 8806)** | +39.2% (€1.10B) | +20.0% (€108M) | rising, accelerating | **Promote** | High — clean +39% YoY at €1.1B base on the EU side; trajectory accelerating; UK reading also strong. Defence + commercial dual-use angle is editorially live (UA, Red Sea, etc.). HS code created in HS2022 so pre-2022 will be empty — note for caveat handling. |
| **Honey (HS 0409)** | -1.8% (€78M) | -17.8% lb (€38M) | peak-and-fall | **Keep-draft** | Medium — flat YoY isn't itself a story right now, but the long-arc trajectory IS a story (peaked-and-falling: the original Soapbox piece's evasion narrative may be ending or shifting). Worth keeping but not currently editorial-front. |
| **Lithium chemicals (carbonate + hydroxide)** | -59.4% lb (€12M) | — | failed recovery | **Drop or shrink scope** | Low — €12M base for both lithium carbonate AND hydroxide is suspiciously small; suggests the HS bracket isn't capturing the bulk of the trade (possibly because most lithium imports arrive as battery cells under HS 8507, not as upstream chemicals). The story is real but this group probably isn't the right lens. |
| **PPE — surgical gloves and masks** | -5.8% (€1.66B) | -2.3% (€251M) | failed recovery | **Promote** | Medium — €1.66B base is meaningful; the trajectory shape (failed recovery: was falling, briefly rose, now falling again) is editorially interesting as the post-COVID PPE bubble fully deflates. Worth keeping for medium-term watching. |
| **Pharmaceutical APIs (broad)** | **+215.6% (€8.11B)** | -29.1% (€63M) | volatile | **Drop and replace** | High — the +215.6% is implausibly large at €8.11B base and almost certainly reflects the description's own caveat ("HS 2942 is broad, includes many APIs but also non-APIs"). Trajectory marked `volatile`. The HS pattern needs tightening before this group is editorially usable. Either narrow to specific APIs of journalistic interest (paracetamol HS 2924, ibuprofen HS 2916, antibiotics HS 2941) or drop entirely until refined. |
| **Semiconductor manufacturing equipment (HS 8486)** | -15.0% (€128M) | -19.7% lb (€22M) | volatile | **Promote** | High — semiconductor export-controls are a top-tier story (US, NL, Japan all have active export-control regimes against China). EU-side €128M base is meaningful. Volatility is the story (regulatory cycle). The €9.35B EU export figure (going TO China) is the more newsworthy direction. |
| **Telecoms base stations (HS 851761)** | -8.7% (€103M) | -13.7% lb (€5M) | volatile | **Promote** | High — Huawei/ZTE/5G procurement is editorially evergreen and the policy environment is moving (UK ban, Italy review, Germany ongoing debate). €103M EU base is decent; the volatility shape will carry meaning when it inflects. |

## Guardian-DNA draft groups (5)

Proposed for the Guardian's specific editorial concerns: forced labour,
deforestation, supply-chain ethics.

| Group | EU yoy | UK yoy | Shape | Recommendation | Confidence |
|---|---|---|---|---|---|
| **Cotton (raw + woven fabrics)** | -6.5% (€163M) | -19.7% lb (€7M) | volatile | **Promote** | High — Xinjiang cotton (UFLPA, EU Forced Labour Regulation) is a top-tier Guardian story. €163M EU base is meaningful. Even flat YoY is news in the context of the regulatory regime — *no* drop would also be a story. The UK figure is low-base but the EU one isn't. |
| **Plastic waste (HS 3915)** | -40.6% lb (€4M) | +187.4% lb (€6M) | peak-and-fall | **Drop or rename** | Low — National Sword (2018) collapsed Chinese plastic-waste imports to ~zero; today's €4M EU and €6M UK reflect that collapse. The peak-and-fall trajectory is the *historical* story; there's no current movement to flag. If kept, rename to "Plastic waste (post-National-Sword residual)" so the editorial intent is clear; otherwise drop. |
| **Polysilicon (solar PV upstream — Xinjiang exposure)** | +49.7% lb (€17M) | — | dip-recovery | **Keep-draft** | Medium — €17M EU base is small (most polysilicon is captured downstream in HS 8541 solar cells). The +49.7% rise on a small base is suspicious. The Xinjiang angle is editorially strong but this group probably isn't capturing the right lens. Consider extending to include the downstream solar-cell HS code as a sibling group (or revisit HS 280461 alone once base grows). |
| **Tomato paste / preserved tomatoes (HS 200290)** | -62.1% (€66M) | -25.0% lb (€11M) | volatile | **Promote** | High — Xinjiang tomato paste (Cofco, Chalkis, BBC investigations) is a Guardian-direct story. €66M base is meaningful; the -62% YoY is a real signal (worth investigating: forced-labour evasion via re-routing? regulatory bite? legitimate substitution?). |
| **Tropical timber (rough + sawn)** | -9.6% lb (€18M) | -14.4% lb (€1M) | volatile | **Keep-draft** | Medium — illegal-logging and Africa-routing angle is editorially DNA-correct but the EU-27 base is too small (€18M) to drive a story directly. Worth keeping for the trajectory-level signal but unlikely to be a quote-the-percentage source soon. |

## Summary

- **Promote** (8): Critical minerals, Drones, PPE, Semiconductor mfg eqpt, Telecoms base stations, Cotton, Tomato paste, and *conditionally* Drones (HS 8806 created in HS2022 — caveat needed for pre-2022 windows).
- **Keep-draft** (3): Honey, Polysilicon, Tropical timber.
- **Drop or rework** (2): Pharmaceutical APIs (broad) — needs tightening; Plastic waste — historical story rather than current.
- **Drop or shrink scope** (1): Lithium chemicals — wrong HS lens for the lithium-import story; consider folding into the EV-batteries group instead.

## Implementation plan once the user has decided

For each promotion, run:

```sql
UPDATE hs_groups SET created_by = REPLACE(created_by, 'draft:', '')
 WHERE name = '<group_name>';
```

For each drop, the cleanest approach is:

```sql
DELETE FROM hs_groups WHERE name = '<group_name>';
```

(`hs_group_ids` references in `findings` cascade to NULL where applicable;
the supersede chain stays intact for any historical narrative referencing
the group, but the group itself stops appearing in fresh analyser runs.)

The Pharmaceutical APIs case probably wants a deeper revision — define
new tighter HS bracket(s), then `INSERT` new group(s) and `DELETE` the
broad one, rather than mutate in place.

Re-run all hs-group analysers after the schema change so the supersede
chain catches the new group composition:

```bash
python scrape.py --analyse hs-group-yoy
python scrape.py --analyse hs-group-trajectory
python scrape.py --analyse llm-framing  # picks up new groups for lead-scaffolding
```
