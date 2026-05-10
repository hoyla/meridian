# Proposed HS groups — current-affairs additions (2026-05-09)

Eight new HS groups added to the **live DB only** (not `schema.sql`)
overnight 2026-05-09, tagged `created_by =
'draft:claude_2026_05_09_current_affairs'`. The analyser chain
(running) will pick them up; you'll see findings for them in the
morning briefing pack.

Filter to find them in the DB:
```sql
SELECT id, name, hs_patterns FROM hs_groups
WHERE created_by = 'draft:claude_2026_05_09_current_affairs'
ORDER BY name;
```

In the morning, three options per group:

1. **Promote**: change `created_by` to `seed:lisa_2026_05_09` (or whatever
   editorial brief prompted it) and add to `schema.sql`. Then it's part
   of the canonical seed.
2. **Keep as draft**: leave `created_by` as-is. The group exists in the
   live DB but not in fresh installs from `schema.sql`. OK for an
   experiment.
3. **Drop**: `DELETE FROM hs_groups WHERE id = ...`. The findings the
   analyser produced for it become orphaned (`hs_group_ids` array
   references a non-existent group). Acceptable since this DB is
   pre-journalist.

## The eight groups

### 1. Critical minerals (export-controlled by China)

```
HS patterns: 8110% (antimony), 8112% (gallium, germanium, indium,
beryllium, hafnium, etc.), 2504% (natural graphite), 3801% (artificial
graphite), 8101% (tungsten)
```

**Why now.** China has been progressively weaponising export controls
on these inputs since 2023 (gallium + germanium July 2023, graphite
December 2023, antimony September 2024, tungsten/tellurium/indium
February 2025). Watching CN→EU flows for these by HS code is exactly
the editorial signal — restrictions should show as falling YoY trends.

**Doesn't overlap with rare-earths group** (which is HS 280530,
284610, 284690 — different chemical compounds entirely).

### 2. Semiconductor manufacturing equipment

```
HS patterns: 8486%
```

**Why now.** Heart of the US-EU-China tech war. ASML (NL) lithography
restrictions, China racing to localise production. CN→EU flow tells
us about Chinese-made equipment going the other way — a smaller story
but not zero (China makes mature-node equipment that EU foundries do
buy).

**Note.** Overlaps with the broad "Electrical equipment & machinery
(chapters 84-85)" group at the chapter level. The narrower 8486 cut
isolates the policy-relevant subset.

### 3. Pharmaceutical APIs (broad)

```
HS patterns: 2937% (hormones/peptides), 2941% (antibiotics),
2942% (other organic compounds — catch-all that includes many
APIs), 2936% (vitamins)
```

**Why now.** China is the dominant supplier of bulk Active
Pharmaceutical Ingredients globally; a perennial Soapbox topic. The
EU is increasingly nervous about supply concentration.

**Caveat.** HS 2942 is broad (any "other organic chemical compound")
so this group will surface a lot of non-API chemistry too. If
findings are noisy, narrow to specific subheadings.

### 4. Drones and unmanned aircraft

```
HS patterns: 8806% (unmanned aircraft, added in HS2022)
```

**Why now.** Dual-use security concern; DJI dominates the consumer
market; military drone supply chain visible through EU import data.

**Limitation.** HS 8806 was created in HS2022 — pre-2022 periods
won't have data under this code (drones were scattered across other
codes prior). The analyser will see a sharp 2022 onset that's a
classification artefact, not a real trade event. The
`cn8_revision` caveat applies here.

### 5. Lithium chemicals (carbonate + hydroxide)

```
HS patterns: 283691% (lithium carbonate), 282520% (lithium oxide
and hydroxide)
```

**Why now.** Battery upstream. We have EV batteries (HS 850760) but
not the lithium going INTO the batteries. Tracks the "Chinese-
controlled lithium midstream" story.

### 6. PPE — surgical gloves and masks

```
HS patterns: 401511% (surgical gloves of vulcanised rubber),
630790% (other made-up textile articles incl. face masks)
```

**Why now.** Two reasons:

1. **COVID validation.** The shock-validation document
   (`shock-validation-2026-05-09.md`) flagged that without a
   PPE counter-shock the COVID validation can only show that
   most groups DROPPED in Q1 2020. PPE should RISE in the same
   period, which would be a clean methodology win for the
   trajectory classifier (`inverse_u_peak` shape on the COVID
   spike).
2. **Continuing relevance.** Pandemic-preparedness stockpiling,
   EU efforts to onshore PPE manufacturing post-2020.

**Caveat.** HS 630790 is broad ("other made-up textile articles") and
includes things other than masks. But the 2020 spike will dominate
the signal regardless.

### 7. Telecoms base stations

```
HS patterns: 851761% (base stations of telephony or telegraphy)
```

**Why now.** Huawei/ZTE story; EU 5G procurement decisions; bans
in some member states.

### 8. Honey

```
HS patterns: 0409% (natural honey)
```

**Why now.** Soapbox-classic evasion vehicle. Chinese honey is
frequently mis-declared as originating from third countries
(transshipment). Useful baseline for the cross-source mirror-gap
methodology — known anomalies in this commodity over decades.

## Notes on what was NOT added

Considered but skipped:

- **Toys (HS 9503%)**: high-volume baseline, but not a current-affairs
  story. Worth adding if a journalist wants it.
- **Wine (HS 2204%)**: anti-dumping classic but the recent EU wine
  cases were more about Australian → China than China → EU.
- **Gold (HS 7108%)**: high-value evasion vehicle, but the data
  tends to be dominated by central-bank flows that don't map well
  to commodity-trade analysis.
- **Apparel (HS 61, 62)**: huge chapters; would dominate the
  briefing pack with low-margin findings unless the threshold is
  raised.
- **Fentanyl precursors**: editorially huge but spread across many
  obscure HS codes that need expert curation.
