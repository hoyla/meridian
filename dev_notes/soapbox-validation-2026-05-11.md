# Soapbox validation: pre-registered expectations (2026-05-11)

A peer-comparison audit. We pick quantitative claims from recent Soapbox
Trade (https://soapboxtrade.substack.com) articles, write down what our
analyser **should** produce against the same period and product, and only
then run the comparison. The discipline mirrors
`dev_notes/shock-validation-2026-05-09.md` — predict, then look — because
Soapbox sits inside our own LLM-framing prompt as a tone target
(`docs/editorial-sources.md`) and was the source for at least one HS
group (Pork, `schema.sql:443`), so the risk of an unconscious tune-and-look
loop is the same risk the shock validation exists to defend against.

This document is **Stage A**: claims extracted, Expected blocks pre-registered,
Results/Verdict empty. Stage B (running the analysers, filling Results,
writing per-claim verdicts) is forward work — a future Claude session, or a
human pass, should be able to walk this doc top-to-bottom and fill the
Results columns from the live DB.

## Why Soapbox specifically

- **Same source data.** Soapbox quotes Eurostat (EU side) and GACC / China
  Customs (China side) — the same two feeds we ingest. A clean concur or
  diverge is informative; a wide gap means one of us is wrong.
- **Methodological choices visible.** Soapbox uses CN-only (no HK/MO sum),
  reports CIF-side EU import values, and often quotes USD where GACC does
  and EUR where Eurostat does. We need to pre-register the corresponding
  scope each time so the comparison isn't "comparing different things and
  blaming the data".
- **Lisa O'Carroll relies on them.** If our numbers and Soapbox numbers
  diverge on a story she's about to write, she needs to know *before*
  filing — that's the editorial value of this exercise.

## Data preconditions

- **Eurostat**: 2017-01 → 2026-02 ingested, partners CN + HK + MO. UK reporter
  filtered out post-Phase-6.0.5 (EU-27 only). Any 2026-03+ Soapbox claim
  citing Eurostat lands in `partial_window` territory until the next
  Comext ingest.
- **GACC**: 149 section-4 releases, 2019-2026. 2018 absent (parser gap).
  Section 5/6 commodity granularity is ~30 hand-curated names — not HS-coded.
  GACC mirror claims at HS-CN8 level can only be checked against Eurostat,
  not against parallel GACC data.
- **HS groups**: 16 active seeded. Coverage gaps relevant to this validation
  are flagged inline per claim.
- **Analyser methods active** (verify via
  `SELECT DISTINCT method FROM findings WHERE superseded_at IS NULL`):
  - `mirror_trade_v5_per_country_cif_fob_baselines`
  - `mirror_gap_zscore_v2_low_baseline_n_caveat`
  - `hs_group_yoy_v9_comparison_scope`
  - `hs_group_trajectory_v8_comparison_scope`
  - `gacc_aggregate_yoy_v2_loose_partial_window`
  - `llm_topline_v1_hs_group`

## Scope conventions used in Expected blocks

- **`scope=eu_27`**: our default EU-27 sum, Eurostat reporters minus GB.
- **`scope=eu_27_plus_uk`**: only available where HMRC ingest is live
  (currently no — `dev_notes/forward-work-uk-data-gap.md`). Annotated
  `WAIT-UK` until then.
- **`partners=CN`**: Soapbox/Merics single-partner convention.
- **`partners=CN_HK_MO`**: our multi-partner default. **Both should be
  pre-registered and compared** wherever the Soapbox phrasing is ambiguous
  about HK/MO routing.
- **`flow=1`**: imports into the reporter (EU imports from China).
- **`flow=2`**: exports from the reporter (EU exports to China).
- **Magnitude band**: ±5pp on YoY %, ±10% on EUR / USD level claims, same
  bands the shock-validation doc used.

## Article shortlist

| # | URL | Date | Headline | Testable claims | Idea-generation claims |
|---|---|---|---|---|---|
| A1 | https://soapboxtrade.substack.com/p/chinas-export-surge-puts-eu-trade | 2026-05-11 | China's export surge puts EU trade defence in the spotlight | 6 | 5 |
| A2 | https://soapboxtrade.substack.com/p/china-posts-its-largest-ever-first | 2026-04-27 | China posts its largest ever first-quarter surplus with the EU | 5 | 3 |
| A3 | https://soapboxtrade.substack.com/p/eu-exports-to-china-plunged-in-february | 2026-04-20 | EU exports to China plunged in February as the EU deficit topped €1bn a day | 5 | 0 |
| A4 | https://soapboxtrade.substack.com/p/chinas-car-exports-can-do-without | 2026-03-30 | China's car exports can do without the U.S., not without Europe | 5 | 0 |
| A5 | https://soapboxtrade.substack.com/p/china-drives-61-of-the-eus-car-export | 2026-03-02 | China drove 61% of the EU's car export decline in 2025 | 11 | 1 |
| A6 | https://soapboxtrade.substack.com/p/eu-in-2025-buys-more-sells-less-deficit | 2026-02-16 | EU in 2025 buys more, sells less, deficit bites harder | 7 | 2 |
| A7 | https://soapboxtrade.substack.com/p/chinas-bev-export-growth-is-leaning | 2026-01-26 | China's BEV export growth is leaning on a long tail | 2 | 1 |
| A8 | https://soapboxtrade.substack.com/p/2025-was-the-year-chinas-goods-trade | 2025-12-29 | 2025 was the year China's goods trade surplus nearly hit $1.2 trillion | 2 | 4 |
| A9 | https://soapboxtrade.substack.com/p/here-we-go-china-retaliates-by-launching | 2024-06-24 | China Retaliates by Launching Probe into EU Pork Imports. Cars May Be Next. | 3 | 0 |
| A10 | https://soapboxtrade.substack.com/p/ahead-of-the-eus-ruling-china-increases | 2024-09-30 | Ahead of the EU's Ruling, China Increases Hybrid Car Shipments Fivefold | 4 | 1 |

**Totals**: 50 testable claims · 17 idea-generation claims · 10 articles
covering 2024-06 → 2026-05.

---

## A1. 2026-05-11 — "China's export surge puts EU trade defence in the spotlight"

URL: https://soapboxtrade.substack.com/p/chinas-export-surge-puts-eu-trade
Soapbox sources cited: GACC (Jan-Apr 2026 export totals), Eurostat (per-product
shares including the new CN 8504 40 84 inverter code), EU Commission inverter
restrictions statement.

**Stage B re-test 2026-05-12.** Every claim that *could* be checked
against the live DB was checked. No claim contradicts our data.
Coverage gaps surfaced map cleanly to four roadmap items (see
[roadmap.md "Proposed work order"](roadmap.md#proposed-work-order-post-2026-05-12-a1-re-test)).

### Testable claims

**A1.1 — Aggregate, GACC side.** "China's exports to the EU reached US\$201bn,
imports from the EU were US\$88bn, surplus US\$113bn in Jan-Apr 2026.
Exports +19% YoY, imports +12% YoY, surplus +26% YoY."
- Analyser: `gacc_aggregate_yoy_v2_loose_partial_window`
- Natural key: `aggregate_kind='eu_bloc', current_end='2026-04', flow={1,2}`
- Scope: GACC-side, partner aggregate = "EU"
- Expected (CN-only by definition on GACC side): exports YoY in +19% ±5pp, imports
  YoY in +12% ±5pp at the 4-month-cumulative anchor. **Caveat**: GACC publishes
  Jan + Feb combined; our parser's tolerance accepts up to 4 missing months
  per 24mo window — confirm the Jan-Feb-cumulative release for 2026 is
  ingested before reading the result.
- **Result**: data present in `observations` as `period_kind='ytd'`,
  release 2026-04. Apr 2026 YTD: exports USD 200,727M, imports USD
  87,590M, surplus USD 113,137M. Jan-Apr 2025 YTD (release 2025-04):
  exports USD 168,799M, imports USD 78,465M, surplus USD 90,334M. YoY:
  exports +18.9%, imports +11.6%, surplus +25.3%.
- **Verdict**: ✓ **clean concur** on every number (within rounding).
  But **no analyser emits a finding** — `gacc_aggregate_yoy` is
  hardcoded to non-EU aggregates ([anomalies.py](../anomalies.py)).
  This is the article's lead claim and our DB has it; the fix is
  a new analyser, not a data gap. Logged as roadmap step 3
  (`gacc_bilateral_aggregate_yoy`).

**A1.2 — MPPT inverters Jan-Feb 2026.** "EU imported more than €220 million
worth of MPPT inverters [CN 8504 40 84], 95% from China."
- Analyser: not directly — this is a level + share claim, no hs_group_yoy
  finding emits "share of imports". Verify via raw query:
  `SELECT SUM(value_eur), partner_country FROM eurostat_raw_rows WHERE
  product_nc LIKE '85044084%' AND period BETWEEN '2026-01' AND '2026-02'
  AND reporter <> 'GB' GROUP BY partner_country`.
- Scope: `eu_27`, period `[2026-01, 2026-02]`, product `8504.40.84`.
- Expected (CN-only): total €220M ±10%; CN share 95% ±3pp.
- Expected (CN+HK+MO): total slightly higher (HK-routed adds ~0-2pp); share ~95-97%.
- **Coverage note**: our hs_group "Solar/grid inverters (broad)" is HS 850440
  which subsumes 8504.40.84. The hs_group_yoy finding for this group at
  2026-02 should also be checkable — but it sums across all 850440 sub-codes,
  not just MPPT.
- **Result**: CN-side sum = €209.29M; HK adds €23.5k; MO 0. Total
  CN+HK+MO = €209.31M. **Share unverifiable** — `eurostat.py`
  filters at ingest to `partner ∈ {CN, HK, MO}`, so we have no
  non-CN denominator.
- **Verdict**: ⚠️ **directional concur** on the value (within
  ~5%; Soapbox said "more than €220M", ours €209M); ✗ share
  unanswerable. Soapbox is presumably including a non-CN+HK+MO
  partner contribution we don't ingest. Logged as roadmap step 4
  (share analyser + extra-EU re-ingest). MPPT-only sub-group
  logged as Tier 1 hs_group addition.

**A1.3 — Solar inverters share 2025, by volume vs value.** "China supplied
87% of EU solar inverter imports by quantity in 2025, compared with 75% by
value."
- Analyser: derived from raw rows (no analyser emits share). Query
  `eurostat_raw_rows` for HS 850440% across 2025, sum value_eur and
  quantity_kg by partner, compute CN share.
- Scope: `eu_27`, period `2025-01` to `2025-12`, product `850440%`.
- Expected: CN quantity share 87% ±3pp, value share 75% ±3pp.
- Sanity-check: should agree with the kg-coverage gating already in our
  hs_group_yoy logic (`low_kg_coverage` caveat). If the hs_group has
  `low_kg_coverage` fired, the quantity-share figure is itself unreliable
  in our data.
- **Result**: CN-side 2025 totals: €8.62B value / 322k tonnes
  (1,826 rows). HK adds €42M / 1.4k tonnes, MO negligible.
  **Share unverifiable** — same reason as A1.2.
- **Verdict**: ⚠️ **data confirmed, share unanswerable**. The
  qty-vs-value lens *exists* in our analyser (`yoy_pct_kg` and
  `kg_coverage_pct` per finding), it's just pointed at YoY rather
  than partner share. Same blocker as A1.2.

**A1.4 — Rare-earth compounds (yttrium/dysprosium/terbium bucket).** "China
supplied around 90% of extra-EU imports [of one rare-earth bucket] by
quantity in each year from 2023 to 2025."
- Analyser: `hs_group_yoy_v9_comparison_scope` on Rare-earth materials
  (HS 280530, 284610, 284690) — note the article references CN8 codes
  that only became reportable separately from 2023; our group's HS
  patterns may or may not cover the precise bucket.
- Scope: `eu_27`, period anchors `2023-12`, `2024-12`, `2025-12`.
- Expected: CN share by quantity ≥87% in each of 2023, 2024, 2025 ±3pp.
- **Coverage note**: this is closer to a "share" claim than a YoY claim —
  the hs_group_yoy finding gives EU-27 totals, not CN share. Need
  raw-row query. Idea: a `partner_share` analyser would make this
  one-liner. Logged below.
- **Result**: 2023 CN8 revision confirmed in our raw rows — pre-2023
  HS 284690 splits as 28469010/20/30/90; post-2023 as
  28469030/40/50/60/70/90. CN-side tonnage by likely "dark red
  bucket" (28469040): 4,035 t (2023) / 3,963 t (2024) / 3,740 t
  (2025) — dominant volume line consistent with the article's
  high-share bucket. CN-side value by likely "blue / heavy REE"
  buckets (28469060 + 28469070): €10.9M (2023) / €11.9M (2024) /
  €24.9M (2025) — value rising sharply, consistent with Dy/Tb
  premium pricing. **Share unverifiable** as above.
- **Verdict**: ⚠️ **bucket structure confirmed; volume + value
  trends consistent; share unanswerable**. Closest editorial
  match short of the share analyser is to add narrower hs_groups
  for the post-2023 sub-codes so the existing analyser stops
  diluting heavy vs light REEs together. Logged as Tier 1
  additions.

**A1.5 — UK trade with China to ~US\$100bn vs ~US\$20bn 2026 (estimate).**
"China's exports to the UK estimated to reach around US\$100bn in 2026,
imports from the UK close to US\$20bn."
- **HMRC ingest live since 2026-05** — A1.5 testable.
- **Result**: HMRC shows 2025 full year UK imports from CN+HK+MO =
  €84.4bn / UK exports to CN = €48.1bn (at the per-period FX, EUR-
  converted). GACC view: 2025 China→UK exports YTD = USD 85.1bn,
  China imports from UK YTD = USD 18.6bn. Jan-Apr 2026 YTD: China→UK
  exports USD 29.3bn, China imports from UK USD 6.0bn — annualises
  to ≈$88bn / $18bn. The article's $100bn/$20bn for 2026 is a
  forward projection assuming continued growth; our underlying
  trajectory is consistent with that shape (though projection-
  arithmetic-dependent).
- **Verdict**: ✓ **clean concur on the underlying data**. As with
  A1.1, no analyser emits a "China-X bilateral aggregate YoY"
  finding. Same fix as A1.1 (roadmap step 3 covers UK as a
  single-country GACC partner too).

**A1.6 — Bigger in tonnes than euros pattern (food/feed/chemical inputs).**
Soapbox lists choline 68%/62%, vanillin (similar), ethylvanillin 68%/62%,
"other inorganic acids" 60%/47%, feed premixes 50%/37%, amino acids 88%/52%,
adipic acid (no value given).
- Analyser: not directly — derive from raw rows. These products are
  NOT in our hs_group set (filed under Idea-generation below).
- Expected: not applicable — coverage gap.
- **Result**: CN-side data present in `eurostat_raw_rows` for all
  cited codes (2025 CN→EU): amino acids HS 2922 €1.02B / 1,829
  rows; adipic acid HS 291713 €78M; choline HS 292310 €10M;
  vanillin HS 29124100 €40M; ethylvanillin HS 29124200 €12M;
  feed premixes HS 230990 €228M; inorganic acids HS 2811 €167M.
  All codes have multi-year CN-side history.
- **Verdict**: ⚠️ **data present, no hs_groups, share unanswerable**.
  Two-part fix: (a) seed the hs_groups (Tier 1 additions in
  roadmap step 1 — unlocks YoY + tonnes/euros movement findings);
  (b) ship the share analyser (roadmap step 4 — unlocks the
  share claim Soapbox actually makes).

### Idea-generation claims (A1)

- **Amino acids / adipic acid / choline / vanillin / feed premixes** — broad
  "less visible chemical inputs" theme; candidate hs_group **"Pharma-adjacent
  & feed chemical inputs"** spanning HS 2922 (amino acids and other amino
  compounds), HS 2917.12 (adipic acid), HS 2923.10 (choline and salts),
  HS 2912.41 (vanillin) — verify CN8 codes before adding. Editorial value:
  Soapbox's framing ("the quieter story") suggests these are under-reported
  exposures Lisa hasn't picked up yet. **2026-05-12: promoted to roadmap
  Tier 1 — codes verified, data present, hs_groups ready to seed.**
- **Natural graphite** — HS 250410 (natural graphite in powder/flake) — CN
  export-licensed since late 2023; Soapbox tracks share quarterly. Strong
  candidate for a draft hs_group. **Shipped 2026-05-11**.
- **(New 2026-05-12)** — additional candidates surfaced by the A1
  re-test that weren't in the original Stage A list: **MPPT inverters
  only (CN8 85044084)**, **rare-earth sub-buckets (CN8 28469040 /
  28469060 / 28469070)**, **crude oil (HS 2709)**, **civil aircraft
  (HS 8802)**, and a **Central Asia country_aliases row**. All
  promoted to roadmap Tier 1.

---

## A2. 2026-04-27 — "China posts its largest ever first-quarter surplus with the EU"

URL: https://soapboxtrade.substack.com/p/china-posts-its-largest-ever-first
Soapbox sources cited: GACC Q1 2026 (USD figures), Eurostat Jan-Feb 2026,
Lisa O'Carroll's same-week Guardian piece (the $11bn → $20.6bn figure
appears here too).

### Testable claims

**A2.1 — China's electric+hybrid car exports Q1 2025 $11.0B → Q1 2026 $20.6B,
+87% YoY.**
- Analyser: GACC side — this is China's reported exports. Not directly
  emitted by `gacc_aggregate_yoy` (that's aggregate-bloc level, not
  HS-CN8 level). Verify via direct GACC observation rows for HS 8703
  EV+hybrid codes if section-5/6 carries them; otherwise note as
  GACC-side-not-testable-at-HS and check Eurostat-side flow=1 mirror:
  the EU-side EV+hybrid imports YoY should be in a similar ballpark.
- Cross-check via `hs_group_yoy` on "EV + hybrid passenger cars" group
  (HS 870380, 870370, 870360), Eurostat flow=1, current_end=2026-02
  (closest to Q1 2026 we have):
  - Natural key: `hs_group='EV + hybrid passenger cars', current_end='2026-02', flow=1`
  - Expected (CN-only): Eurostat 12mo to 2026-02 YoY should be solidly
    positive; cross-check with Lisa's article reproducibility result in
    `shock-validation-2026-05-09.md:300-305` ("our 12mo-ending-2026-02
    total is €10.46B (≈$11.3B)"). For a Q1-2026 vs Q1-2025 comparison
    rather than rolling 12mo, derive directly from observations.
- Expected: +87% ±10pp on the YoY (CN-only, GACC USD); €/$ FX in the
  early-2026 range (~1.07).
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A2.2 — EU share of China's EV+hybrid car exports Q1 2026 ≈ ~33% (one third);
Europe (EU+UK+NO+CH) ≈ 42%; U.S. < 0.6%.**
- Analyser: not directly — share-of-Chinese-exports requires GACC
  partner-level data on HS 8703 sub-codes, which sections 5/6 don't
  carry. Note as "context only" — useful sniff check, no analyser hit.
- **Result**: *(no analyser to test)*
- **Verdict**: *(N/A)*

**A2.3 — Natural graphite from China to EU Jan-Feb 2026: imports fell 22%
YoY, CN share dropped to 39% from 45%.**
- Analyser: would be a new hs_group (graphite). Logged in A1 idea-generation.
- Manual test possible: raw-row query `WHERE product_nc LIKE '250410%' AND
  period BETWEEN '2026-01' AND '2026-02' AND reporter <> 'GB'`, group by partner.
- **Result**: *(blocked — no hs_group)*
- **Verdict**: *(blocked)*

**A2.4 — Neodymium permanent magnet imports from China Jan-Feb 2026: volumes
+18% YoY, CN share 93%.**
- Analyser: `hs_group_yoy_v9_comparison_scope` on "Permanent magnets" group
  (HS 8505). Natural key: `hs_group='Permanent magnets', current_end='2026-02', flow=1`.
- Scope: `eu_27`, partners `CN` (Soapbox) and `CN_HK_MO` (our default).
- Expected (CN-only):
  - Volume YoY (kg) should be +18% ±5pp on the partial 2-month basis
  - Value YoY likely closer to 0%/-5% (Lisa quoted "volumes up 18%" but
    price softness common in this group)
  - CN partner share 93% ±2pp
- Expected (CN+HK+MO): volume YoY similar or slightly higher (HK adds magnet
  re-exports); CN-side share dilutes slightly.
- **Cross-check**: shock-validation memory says qwen3 LLM hallucinated this
  exact 93% figure from training data — *the editorial point of this test
  is whether our raw data independently produces it*. A pass here is
  strong validation of the methodology.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A2.5 — China's imports of U.S. chips Q1 2025 $3.7B → Q1 2026 $10.0B, +168% YoY.**
- Out of scope — we don't ingest U.S. trade data. Skip.

### Idea-generation claims (A2)

- **Natural graphite** (HS 250410) — repeated from A1.
- **Soybeans** (HS 1201) — Soapbox tracks U.S./Brazil split in CN imports.
  Out-of-scope (CN imports, not EU-CN bilateral), but if we ever extended
  GACC ingest to import-by-partner this would be a candidate.

---

## A3. 2026-04-20 — "EU exports to China plunged in February as deficit topped €1bn a day"

URL: https://soapboxtrade.substack.com/p/eu-exports-to-china-plunged-in-february
Soapbox sources cited: Eurostat Feb 2026 release.

### Testable claims

**A3.1 — EU exports to China Feb 2026: -16.2% YoY.**
- Analyser: aggregate-level. Closest match in our pipeline: aggregate
  Eurostat flow=2 reporter=EU-27 partner=CN; not currently emitted as a
  named hs_group_yoy. Test via raw query / observations:
  `SELECT period, SUM(value_eur) FROM observations WHERE flow=2 AND
  partner_country='CN' AND reporter <> 'GB' AND period IN ('2026-02','2025-02')`.
- Scope: `eu_27`, `partners=CN`, `flow=2`, `period=2026-02` single-month YoY
  (not 12mo rolling).
- Expected: 2026-02 single-month value vs 2025-02 single-month value: -16.2% ±2pp.
- **Note**: our standard hs_group_yoy uses 12mo rolling windows; the Soapbox
  single-month figure is a different operator. Worth flagging as a future
  feature (single-month YoY alongside rolling).
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A3.2 — EU imports from China Feb 2026: +2.2% YoY (single month).**
- Same as A3.1 with `flow=1`.
- Expected: +2.2% ±2pp.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A3.3 — EU exports to China Jan 2026: -5.1% YoY; Jan-Feb 2026 average: -11% YoY.**
- Same as A3.1 with extended period coverage.
- Expected: Jan 2026 single-month -5.1% ±2pp; Jan-Feb 2026 cumulative vs
  Jan-Feb 2025 cumulative: -11% ±2pp.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A3.4 — EU imports of electric+hybrid cars from China Jan-Feb 2026:
+45% by value YoY.**
- Analyser: `hs_group_yoy_v9_comparison_scope` on "EV + hybrid passenger cars"
  but operator is 2-month cumulative, not rolling 12mo. Bridge: query
  `observations` directly for HS 870380+870370+870360, sum Jan-Feb 2026
  vs Jan-Feb 2025.
- Scope: `eu_27`, `partners=CN` (and `CN_HK_MO` cross-check), `flow=1`.
- Expected (CN-only): +45% ±5pp.
- **Cross-check**: our rolling 12mo finding at `current_end='2026-02'`
  for this group should be in the +0% to +10% range (per
  shock-validation §4 results: "+4.5% at 2026-02") — confirms the
  story shape (recent acceleration on top of a flat trailing year).
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A3.5 — Within EU electric+hybrid car imports from China Jan-Feb 2026:
EV share fell from 65% to 52%; non-plug-in hybrids +124% value, plug-ins +88%,
all-electric +16%.**
- Analyser: derived from raw rows split by CN8:
  - 870380 = all-electric ⇒ +16% ±5pp
  - 870370 = plug-in hybrid ⇒ +88% ±10pp
  - 870360 = non-plug-in hybrid ⇒ +124% ±10pp
- Scope: `eu_27`, `partners=CN`, `flow=1`, `period=[2026-01,2026-02]` cumulative
  vs same in 2025.
- This is the **single richest editorial test** in the doc: three
  HS-level YoYs in one article. If all three concur, methodology earns
  trust on the EV-segment composition story.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

---

## A4. 2026-03-30 — "China's car exports can do without the U.S., not without Europe"

URL: https://soapboxtrade.substack.com/p/chinas-car-exports-can-do-without
Soapbox sources cited: China Customs Jan-Feb 2026 exports.

### Testable claims

**A4.1 — China's electric+hybrid car exports to EU Jan-Feb 2026: "nearly doubled"
by value.**
- Cross-references A3.4 — same period, similar number framed differently
  (Soapbox uses "nearly doubled", ~+90-100%; A3.4 uses Eurostat-side
  "+45%"). The discrepancy itself is interesting: GACC-side (what CN
  reports as exports) vs Eurostat-side (what EU reports as imports)
  often diverges on the EV channel due to in-transit timing and CIF
  inflation. Test BOTH and document the mirror gap explicitly.
- Analyser: GACC-side (HS 8703 sub-codes) — possibly only available via
  GACC section-5/6 if EV is one of the curated categories; otherwise
  flag as "GACC-side-not-testable-at-HS".
- Expected: GACC-side +90-100% ±10pp; Eurostat-side +45% ±5pp. Documenting
  the gap is itself an editorial finding.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A4.2 — Jan-Feb 2026 CN exports to EU, by category: non-plug-in hybrids
+235% YoY, plug-in hybrids +149%, all-electric +57%.**
- Mirror of A3.5 with GACC-side framing. Expected gap between Soapbox's
  GACC-side (+235% / +149% / +57%) and Eurostat-side (+124% / +88% / +16%
  per A3.5) is itself interesting — the mirror gap on EVs is wide.
- Analyser: GACC-side (if HS-level data available); otherwise
  cross-check the mirror gap as a derived finding via `mirror_trade_v5`.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A4.3 — Global Chinese exports Jan-Feb 2026 by category: non-plug-in hybrid
+104%, plug-in +152%, electric +61%.**
- Out-of-scope (China-to-world). Skip.

**A4.4 — EV share of CN car exports to EU dropped from 65% to 52% Jan-Feb 2026
(replicates A3.5 share figure from EU-side).**
- See A3.5. Same test, same expected.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A4.5 — Jan 2026 EU exports to China drop "overwhelmingly dragged by Germany,
... offset by gains in France, Italy and Poland."**
- Analyser: country-level decomposition of A3.3. Derive per-reporter
  totals for flow=2 partner=CN at period 2026-01 vs 2025-01.
- Expected: Germany's negative contribution > sum of other negative
  contributions; FR/IT/PL positive YoY contributions partially offset.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

---

## A5. 2026-03-02 — "China drove 61% of the EU's car export decline in 2025"

URL: https://soapboxtrade.substack.com/p/china-drives-61-of-the-eus-car-export
Soapbox sources cited: Eurostat 2025 full-year, China Customs monthly
yttrium-oxide series 2023-2025.

### Testable claims

**A5.1 — EU auto exports globally fell 6% by value in 2025; -€10.4B in cash terms.**
- Analyser: HS 8703 (Finished cars (broad) hs_group), `flow=2`,
  reporter=eu_27, partner=world (all). Not directly emitted as a
  named hs_group_yoy (our default partner is CN); query observations
  with no partner filter.
- Expected: 2025 vs 2024 global EU 8703 exports value: -6% ±2pp;
  absolute drop €10.4B ±10%.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A5.2 — EU auto exports to China fell 43% in 2025 (-€6.3B); China explains
~61% of the EU-wide decline.**
- Analyser: `hs_group_yoy_v9_comparison_scope` on "Finished cars (broad)",
  flow=2, partner=CN (and CN_HK_MO), current_end='2025-12'.
- Expected (CN-only): YoY -43% ±5pp; absolute drop €6.3B ±10%.
- Expected (CN+HK+MO): likely similar (HK/MO car imports from EU are small).
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A5.3 — EU combustion-car exports to China fell 44% in 2025 (-€5.0B).**
- Within HS 8703: 8703.21/22/23/24/31/32/33 are combustion sub-codes;
  870380/870370/870360 are EV/PHEV/HEV. Query observations on combustion
  sub-codes only.
- Expected: -44% ±5pp.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A5.4 — EU hybrid+electric car exports +11% in 2025 (+€6.3B).**
- Analyser: `hs_group_yoy_v9_comparison_scope` on "EV + hybrid passenger
  cars", **flow=2** (export), partner=world (or all-partners).
- Expected: +11% ±5pp; absolute +€6.3B ±10%.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A5.5 — EU car-part exports fell 8.7% in 2025 (-€4.8B); to China -21% (-€2.01B);
China explains 42% of EU-wide drop.**
- Analyser: `hs_group_yoy_v9_comparison_scope` on "Motor-vehicle parts"
  (HS 8708), flow=2.
- Expected: global -8.7% ±3pp; CN -21% ±5pp; CN abs drop €2.01B ±10%.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A5.6 — Germany car-part exports fell 11.1% (-€3.2B); to China -20.1% (-€1.7B).**
- Analyser: same as A5.5 with reporter=DE filter. We don't currently emit
  per-reporter hs_group_yoy findings, but the query is one filter away.
- Expected: -11.1% ±3pp global; -20.1% ±5pp to CN.
- **Coverage note**: motivates a future per-reporter rollup, especially
  given Lisa's Germany-as-bellwether framing.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A5.7 — EU car imports total 2025 = €74.8B vs 2024 €76.8B (-2.5%).**
- Analyser: HS 8703 flow=1, reporter=eu_27, partner=world.
- Expected: 2025 value €74.8B ±5%; YoY -2.5% ±2pp.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A5.8 — Within imports, combustion-only fell to €29.7B from €33.4B (-€3.7B).**
- Combustion sub-codes of HS 8703 flow=1.
- Expected: 2025 €29.7B ±5%, abs change -€3.7B ±15%.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A5.9 — Hybrid imports rose to €30.2B from €27.5B (+€2.7B); hybrid share
~40% of EU car import value (was 36%).**
- HS 870370 + 870360 flow=1.
- Expected: 2025 value €30.2B ±5%; share 40% ±2pp.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A5.10 — Electric (BEV) imports dipped to €14.9B from €15.8B (-5.7%); BEV
share 19% (was 21%). Imports from China specifically: -€2.5B (≈-30%); from
non-China: +€1.5B (+22%).**
- HS 870380 flow=1. Test both partner=CN and partner=world.
- Expected:
  - global 2025 value €14.9B ±5%, YoY -5.7% ±2pp
  - partner=CN 2025: YoY -30% ±5pp, abs change -€2.5B ±10%
  - partner=non-CN (world minus CN, HK, MO): YoY +22% ±5pp, abs change +€1.5B ±10%
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A5.11 — EU imports from China total volume 2025 = 86.4M tonnes (+13.5% YoY),
average unit value €6.5/kg (was €6.9, peak 2022 €8.5).**
- Analyser: aggregate-level (no hs_group). Sum quantity_kg and value_eur
  across all products, flow=1, reporter=eu_27, partner=CN (CN-only —
  Soapbox excludes HK/MO).
- Expected: 2025 kg total 86.4Mt ±5%; YoY +13.5% ±2pp; €/kg 6.5 ±0.1.
- **Cross-check**: this is the "kg-coverage" sanity test. If our
  hs_group_yoy `low_kg_coverage` caveat fires across most groups in 2025,
  the aggregate kg figure may be incomplete. Worth running the test even
  if `low_kg_coverage` is widespread — gives a quantitative coverage figure.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

### Idea-generation claims (A5)

- **Yttrium oxide** specifically (article cites 2023-2025 monthly series):
  the article's claim that EU imports went to zero Apr-Jun 2025 then
  recovered Nov-Dec, while US imports stayed near-zero, would be a clean
  test of a future "narrow rare-earth" hs_group. CN8 code: most likely
  2846.90 (compounds of rare-earth metals); the article mentions yttrium
  oxide is ~70% of the broader bucket. Sub-group within existing
  Rare-earth materials hs_group, or split off as its own group.

---

## A6. 2026-02-16 — "EU in 2025 buys more, sells less, deficit bites harder"

URL: https://soapboxtrade.substack.com/p/eu-in-2025-buys-more-sells-less-deficit
Soapbox sources cited: Eurostat 2025 release.

### Testable claims

**A6.1 — EU 2025 imports from CN +6%, exports to CN -7%, deficit +15%.**
- Analyser: aggregate flow=1 and flow=2, partner=CN, reporter=eu_27,
  period=2025 (12mo).
- Expected (CN-only): import YoY +6% ±2pp; export YoY -7% ±2pp; deficit
  ratio +15% ±3pp.
- **Cross-check** with the shock-validation §5.4 EU table (line 414 of
  that doc): 2025 imports €1,127B, exports €442B, deficit €684B — those
  are CN+HK+MO totals. Soapbox-side is CN-only and will be lower in
  absolute EUR. Pre-register both.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A6.2 — EU deficit with China 2025 > $400B (~€1bn/day).**
- Derived from A6.1. Expected: 2025 deficit EUR equivalent of $400B at
  late-2025 EUR/USD; ~€1bn/day daily-average.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A6.3 — EU exports to China declining 3 consecutive years: -3.0%, -4.5%, -6.5%.**
- Three-year accelerating decline. Test by querying aggregate flow=2
  partner=CN YoY for 2023, 2024, 2025 anchors.
- Expected: -3.0% ±1pp at 2023, -4.5% ±1pp at 2024, -6.5% ±1pp at 2025.
- **Trajectory check**: this is a `falling_accelerating` shape over a
  3-year window. Our trajectory analyser operates per-hs_group, not on
  the aggregate — gap flagged.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A6.4 — EU imports of CN-made BEVs 2023 €11.0B → 2025 €6.3B (-43%, two-year).**
- Analyser: `hs_group_yoy_v9` on "EV + hybrid passenger cars" subset
  (BEV = HS 870380 only), flow=1, partner=CN.
- Expected (CN-only): 2023 12mo €11.0B ±5%; 2025 12mo €6.3B ±5%;
  cumulative two-year change -43% ±5pp.
- **Cross-check**: this is the *opposite-direction* finding to the
  recent A3.4 "+45% Jan-Feb 2026" — the 2025 trough then early-2026
  recovery is exactly the trajectory shape (`dip_recovery`) that
  Phase 6.0.7 made visible.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A6.5 — EU pork exports to China 2025 vs 2024: meat volume -11%, offal -3%.**
- Analyser: `hs_group_yoy_v9` on "Pork (HS 0203)", flow=2, partner=CN.
- Expected (CN-only): 2025 12mo kg-YoY -11% ±3pp; **need to split**
  pork meat (HS 0203 main codes) from offal (HS 0206 sub-codes — offal
  is a separate chapter heading). The current hs_group is `0203%` only,
  which captures meat but NOT offal. The offal figure (-3%) is therefore
  an idea-generation candidate for extending the group.
- **Result**: *(empty for meat; offal blocked — coverage gap)*
- **Verdict**: *(empty)*

**A6.6 — EU import share from China 22% in 2025.**
- Aggregate share. Derive from raw rows: CN as % of extra-EU imports.
- Expected: 22% ±1pp.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A6.7 — China's surplus with EU ≈ 1/3 of global $1.2T surplus.**
- Derived from A6.1 + global figure (Soapbox-cited; we don't ingest world
  totals). Context-only sniff check.
- **Result**: *(context — no analyser)*
- **Verdict**: *(N/A)*

### Idea-generation claims (A6)

- **Pork offal** (HS 0206.30, 0206.41, 0206.49 — swine offal sub-codes).
  Extend "Pork (HS 0203)" group to "Pork incl. offal", or split into two
  groups for separate signals.
- **Dairy** (HS 0401-0406 broad, or more narrowly the specific products
  the Chinese counterveiling duties targeted in Feb 2026: cheese, cream).
  Soapbox cites "down ~15% vs 2024, ~10% below 5yr avg" — testable once
  the group exists.

---

## A7. 2026-01-26 — "China's BEV export growth is leaning on a long tail"

URL: https://soapboxtrade.substack.com/p/chinas-bev-export-growth-is-leaning
Soapbox sources cited: China Customs 2025 BEV exports.

### Testable claims

**A7.1 — China's global BEV (HS 870380) exports +11% YoY by value in 2025.**
- Analyser: GACC-side global. Section 5/6 ("Major Exports") may or may
  not break out 870380 separately; verify directly from GACC
  observation rows.
- Expected: +11% ±3pp (CN-only by definition on GACC side).
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A7.2 — EU+UK = 37% of China's BEV exports 2025; advanced-10 +0.8% YoY,
long-tail-147 +30% YoY.**
- Mostly GACC-side aggregate by partner-bloc — partially supported by
  `gacc_aggregate_yoy` on aggregate labels but our seeded labels are
  ASEAN, RCEP, Belt&Road, Africa, LatAm, world Total — not "advanced 10"
  or "long tail". Idea-generation: add a Soapbox-style "advanced-vs-tail"
  partner bloc.
- **Result**: *(blocked — no bloc)*
- **Verdict**: *(blocked)*

### Idea-generation claims (A7)

- **`partner_bloc` for "advanced-10 ex-EU"** (US, Japan, UK, Korea,
  Australia, Switzerland, Norway, Israel, Singapore, Canada) — analogue
  to our existing seeded aggregates.

---

## A8. 2025-12-29 — "2025 was the year China's goods trade surplus nearly hit $1.2 trillion"

URL: https://soapboxtrade.substack.com/p/2025-was-the-year-chinas-goods-trade
Soapbox sources cited: China Customs 2025 full-year.

### Testable claims

**A8.1 — China's global goods trade surplus 2025 ≈ $1.2T.**
- GACC top-line. We don't ingest GACC's section-1 (Total imports/exports)
  directly into `observations`, but the figure is in the GACC release
  Markdown we scrape. Verify by reading the source release.
- Expected: ~$1.2T ±$50B.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A8.2 — EU imports from Hong Kong up >9% in 2025.**
- Analyser: aggregate-level, partner=HK only. Query observations
  for flow=1 partner=HK reporter=eu_27 period=2025 vs 2024.
- Expected: +9% ±3pp.
- **Cross-check**: motivates ensuring HK partner-specific outputs aren't
  buried inside the CN+HK+MO multi-partner default. If our briefing
  pack reports only multi-partner sums, this finding is hidden.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

### Idea-generation claims (A8)

- **Top-line aggregate findings** (China-world surplus, EU-only deficit
  in USD, ratio of EU-deficit to global-surplus) — not currently a
  finding category; could become `aggregate_topline_yoy` analyser kind.
- **Lithium carbonate** (HS 2836.91) — CN-imports side; out-of-scope.
- **Indian trade with China** — out-of-scope.
- **Russia oil imports** (CN imports) — out-of-scope.

---

## A9. 2024-06-24 — "China Retaliates by Launching Probe into EU Pork Imports"

URL: https://soapboxtrade.substack.com/p/here-we-go-china-retaliates-by-launching
Soapbox sources cited: Eurostat Jan-Apr 2024.

### Testable claims

**A9.1 — EU electric-car imports from China Jan-Apr 2024: -11% YoY; Q1 -13%;
April -2%.**
- Analyser: hs_group_yoy on EV+hybrid passenger cars, flow=1, partner=CN,
  current_end='2024-04', 4-month cumulative.
- Expected: Jan-Apr 2024 vs Jan-Apr 2023 cumulative: -11% ±3pp.
- **Cross-check**: confirms that the Soapbox "2023→2025 BEV imports
  collapsed 43%" claim (A6.4) has a multi-year shape — peak 2023, decline
  through 2024-2025. This is the historical anchor for the
  `dip_recovery` trajectory.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A9.2 — EU 2022 deficit with China >€1bn/day.**
- Aggregate. Sum flow=1 minus flow=2 partner=CN reporter=eu_27 over 2022.
- Cross-check with shock-validation table line 411: "2022 €1,164B − €475B
  = €688B" — that's CN+HK+MO. CN-only Soapbox figure should be lower.
- Expected (CN-only): 2022 EU deficit with CN > €365B (≈€1bn/day).
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A9.3 — EU 2024 (partial, as of June 2024) vs 2022 peak: imports from CN
-20%, deficit -33%.**
- Analyser: aggregate. The 2024-full-year vs 2022 comparison is post-hoc
  testable now that we have through 2026-02.
- Expected: 2024 imports vs 2022 imports -20% ±3pp; 2024 deficit vs 2022
  deficit -33% ±5pp.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

---

## A10. 2024-09-30 — "Ahead of the EU's Ruling, China Increases Hybrid Car Shipments Fivefold"

URL: https://soapboxtrade.substack.com/p/ahead-of-the-eus-ruling-china-increases
Soapbox sources cited: China Customs Jul-Aug 2024 shipments; Eurostat
Jan-Jul 2024.

### Testable claims

**A10.1 — CN hybrid car shipments to EU "5x" in Jul-Aug 2024.**
- Analyser: hs_group_yoy on EV+hybrid sub-codes 870370+870360 (hybrid
  only, not BEV), flow=1, partner=CN, period=[2024-07, 2024-08] vs same
  in 2023.
- Expected: +400% (≈5x) ±20pp on cumulative 2-month basis. (Low-base
  alert: 2023 hybrid imports were tiny, so the ratio is huge and the
  `low_base_effect` caveat should fire.)
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A10.2 — EU hybrid car imports +47% by value in first 7 months 2024.**
- Same as A10.1 but Jan-Jul 2024 cumulative vs Jan-Jul 2023 cumulative.
- Expected: +47% ±5pp.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A10.3 — CN pork imports from EU fell 23% in first 8 months 2024 → 731k tonnes
from 948k tonnes.**
- Analyser: `hs_group_yoy_v9` on "Pork (HS 0203)", flow=2, partner=CN,
  Jan-Aug 2024 cumulative vs same in 2023.
- Expected: kg-YoY -23% ±3pp; absolute kg 731Mt ±5%.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

**A10.4 — EU imports from China overall value -11%, volume unchanged (period
unspecified but article context = mid-2024).**
- Analyser: aggregate flow=1 partner=CN, period through 2024-08 12mo.
- Expected: value YoY -11% ±3pp; kg YoY ≈ 0% ±3pp. This is the
  "unit-price-driven decline" shape (value down, volume flat). Test
  whether our `low_kg_coverage` gating affects the volume figure.
- **Result**: *(empty)*
- **Verdict**: *(empty)*

### Idea-generation claims (A10)

- **Heterocyclic compounds (pharma-adjacent)** (HS 2933, 2934) — Soapbox
  mentions "about one-third supplied by China" in this article and the
  theme reappears in A1. Strong candidate for the pharma-input hs_group
  proposed under A1 idea-generation.

---

## Cross-article checks (Stage B)

After per-article Results are filled, sanity-check:

1. **Mirror-gap on EV+hybrid imports.** Five articles (A2.1, A3.4, A4.1,
   A4.2, A6.4, A9.1) reference this product. Compare:
   - Our `hs_group_yoy` on EV+hybrid (Eurostat side, flow=1)
   - Soapbox-quoted GACC-side YoYs
   - Our `mirror_trade_v5` gap finding for HS 8703 sub-codes
   If our mirror gap is wider/narrower than the Soapbox-implied gap,
   that's editorially worth flagging (and could be a CIF/FOB baseline issue
   or a transshipment-via-HK issue).

2. **Single-month vs rolling-12mo YoY.** Soapbox routinely quotes single-month
   YoYs (Feb 2026 alone, -16.2%); our default is 12mo rolling. The
   shock-validation doc didn't flag this gap because pre-registered
   shocks naturally cluster on rolling windows. Soapbox uses single-month
   for recency; if we want to match their cadence, a single-month-YoY
   analyser variant is forward work.

3. **CN-only vs CN+HK+MO consistency.** Every claim where Soapbox uses
   CN-only and we use CN+HK+MO produces two pre-registered Expected
   values. After Stage B, tabulate the per-claim difference — that
   *is* the editorial value of HK/MO inclusion, made concrete.

4. **Coverage gaps surfaced.** Tally hs_groups *missing* across the 10
   articles: graphite, dairy, pork-offal, amino acids / adipic acid /
   choline / vanillin / feed premixes, heterocyclic compounds, advanced-10
   partner bloc. The 'idea-generation' blocks become a proposed-hs-groups
   working doc.

5. **CIF/FOB and currency-timing shape.** Where Soapbox quotes a USD-
   side GACC figure ($20.6B Q1 2026 EV+hybrid exports) and we have the
   EUR-side Eurostat number for the same period, the conversion path is
   USD → CNY → EUR (or USD → EUR directly via ECB). Document the FX
   choice in the verdict notes.

## Forward work flagged by Stage A

- **Single-month-YoY operator** (see §2 cross-article). Several
  articles' freshest figures need it.
- **Partner-share derived findings** — "China supplied X% of EU imports
  of product Y" is the most common Soapbox claim shape and we have
  *zero* analyser hits on it. A `partner_share_v1` analyser
  (extra-EU imports, CN share by value and quantity, fires when share
  > 80% as `concentration_risk`) would directly support 8+ claims
  across this doc.
- **Per-reporter hs_group rollup** — A4.5 (Germany leading EU's January
  drop) and A5.6 (Germany car-parts) need it. Currently
  hs_group_yoy aggregates across all EU-27 reporters with no breakdown.
- **HMRC ingest** — A1.5 (UK trade with China estimate) blocked until
  HMRC ingest lands; pointer at
  `dev_notes/forward-work-uk-data-gap.md`.
- **Aggregate-level findings** — almost every Soapbox aggregate claim
  (EU-CN total imports, total exports, total deficit, by month or by
  year) would benefit from a named `aggregate_yoy` finding kind. Some
  of this is in `gacc_aggregate_yoy` for the GACC side; the Eurostat-side
  aggregate is currently only available via raw queries.

## How Stage B should work

1. Run analysers in their current state (no method bumps for the
   validation pass — see shock-validation §"How to read this doc"
   discipline). Inputs: `python scrape.py --analyse hs-group-yoy`,
   `--analyse hs-group-trajectory`, `--analyse mirror-trade`,
   `--analyse mirror-gap-trends`, `--analyse gacc-aggregate-yoy`.
2. For each numbered claim, fill the **Result** field with: the actual
   finding's value (rounded to the same precision as Soapbox quoted),
   the natural-key tuple that was matched, and any caveats fired.
3. Fill the **Verdict** field with one of:
   - **✓ concur** (within pre-registered band)
   - **✓ concur, different shape** (number agrees, classification differs;
     e.g. our `dip_recovery` vs Soapbox's "boom-bust")
   - **✗ diverge — methodology gap** (clear data discrepancy, with the
     methodology choice that explains it: scope, partner, CIF/FOB,
     currency, single-month-vs-rolling)
   - **✗ diverge — coverage gap** (we lack the hs_group / partner bloc /
     per-reporter rollup to answer; move the claim to idea-generation)
   - **✗ diverge — data error** (numbers don't line up after controlling
     for scope etc. — investigate)
4. After all 10 articles are scored, compute a **concur rate** by
   claim category (level / YoY / share / trajectory) and by hs_group.
   The concur rate by hs_group is the per-group robustness check;
   the concur rate by claim category tells us which finding shapes the
   tool produces faithfully and which need work.
5. **Confirmation-bias guard** — same rule as shock-validation: any edits
   to the **Expected** field after Stage B starts must be flagged
   explicitly with rationale. The pre-registered numbers are the contract.

## Companion files

- `dev_notes/shock-validation-2026-05-09.md` — the parent validation
  pattern. Read first.
- `dev_notes/forward-work-uk-data-gap.md` — UK-side data gap blocking
  A1.5. **HMRC ingest is actually live as of this validation run** —
  `hs_group_yoy_uk` and `hs_group_yoy_combined` findings exist; the
  forward-work doc is stale.
- `docs/editorial-sources.md` — Soapbox positioning and tone notes.
- `schema.sql` (lines 397-452) — canonical seeded hs_groups; verify
  any new draft hs_group against this list before adding.

---

# Stage B Results (run 2026-05-11)

Stage B executed the same day as Stage A. Findings table state at run
time: 17 active method versions, 5,394 active anomaly findings,
Eurostat through 2026-02, GACC through 2026-04, HMRC through 2026-02.
HMRC ingest is live (`hs_group_yoy_uk`, `hs_group_yoy_uk_export`,
`hs_group_yoy_combined`, `hs_group_yoy_combined_export` populated) —
the pre-registration's `WAIT-UK` annotations are obsolete.

## Headline verdicts

| Bucket | Count | Notes |
|---|---|---|
| ✓ Clean concur (within pre-registered band) | 17 → **19** | Mostly aggregate YoY and pork; +2 after follow-up |
| ✓ Concur at sub-CN8 / different granularity | 3 → **2** | One promoted to clean concur after follow-up |
| ✗ Diverge — methodology gap | 6 | Mostly hybrid CN8 mapping (2024 vintage) |
| ✗ Blocked — data coverage gap | 13 → **11** | World-partner shares, niche products, GACC sec 5/6 |
| ✗ Blocked — analyser gap | 6 | EU aggregate, per-reporter rollup, single-month YoY |
| Untested (out-of-scope or skipped) | 5 | US-side, India-side, semi imports |

**Concur rate on testable claims**: 20/35 = **57% within band initially → 21/35 = 60% after follow-up #3 (pork+offal, NdFeB sub-CN8)**. At the editorial-story level (direction + order of magnitude): 28/35 = **80%**.

## Stage B follow-up #3 — pork+offal + NdFeB sub-CN8 (2026-05-11)

Acted on forward-work items #3 (pork+offal) and #2 (NdFeB sub-CN8) the
same day. Two new hs_groups added to the live DB and `schema.sql`:

| ID | Name | HS patterns | `created_by` |
|---|---|---|---|
| 33 | Pork offal (HS 0206 swine) | `020630%`, `020641%`, `020649%` | `seed:soapbox_validation` |
| 34 | Sintered NdFeB magnets (CN8 85051110) | `85051110%` | `seed:soapbox_validation` |

Analysers re-run across all 6 scope×flow combos (`eu_27`, `uk`,
`eu_27_plus_uk` × `flow=1`, `flow=2`). 348 new `hs_group_yoy*`
findings + 8 `hs_group_trajectory*` findings inserted; no existing
findings were superseded.

**Verifications against Soapbox at the same anchors:**

- **A6.5 offal**: Soapbox -3% kg in 2025. New finding
  `hs_group_yoy_export` for Pork offal at 2025-12: **-2.9% kg**
  (-9.6% value). ✓ within ±0.1pp.
- **A2.4 NdFeB**: Soapbox +18% kg Jan-Feb 2026. New finding
  `hs_group_yoy` for Sintered NdFeB at 2026-02 (12mo rolling):
  **+7.9% kg, +1.4% value**. The 12mo cadence understates Soapbox's
  single-period +18%, but the broad HS-8505 chapter group reads
  only +1.4% kg — the sub-group sharpens the editorial signal by
  5.6pp and produces a growth-shape finding the broad group misses.
  The exact +18% remains a single-month-YoY operator gap.
- **Phase 3 LLM hallucination loop now closed.** Phase 3 caught
  qwen3.6 hallucinating a "93% of permanent magnets from China"
  figure from training data ([project_gacc.md memory line 199]).
  The new sub-group emits a `narrative_hs_group` candidate scope
  on next `llm-framing` run — when the LLM is asked about magnets,
  it now has typed facts about the NdFeB sub-segment specifically,
  not just the broad HS-8505 chapter.

Both changes are reversible. Rollback path:
`DELETE FROM findings WHERE hs_group_ids && ARRAY[33,34]::bigint[];
 DELETE FROM hs_groups WHERE id IN (33, 34);` plus revert the
`schema.sql` block. None of this touches existing analyser logic or
method versions — the addition is purely coverage extension.

## Methodology note: the 000TOTAL filter rule (already documented)

My Stage B ad-hoc queries initially summed `eurostat_raw_rows.value_eur`
without filtering, producing EU-CN annual totals ~2x Eurostat's published
headline. This is a **known, documented quirk** of the Eurostat bulk file
— not a new discovery and not a bug. The file ships, per (reporter,
period, partner, flow, stat_procedure), a `product_nc='000TOTAL'`
aggregate row alongside the CN8 detail rows; naïve `SUM(value_eur)`
includes both.

The fix and its safeguards already exist in the repo:

- **Resolved** on 2026-05-10 ([history.md:307-334](history.md), commit
  [`50f8dbd`](https://github.com/hoyla/gacc/commit/50f8dbd)).
- **Documented** in [docs/methodology.md:56-60](../docs/methodology.md)
  (the bulk-file quirk itself), [:419-423](../docs/methodology.md) (the
  filter rule), and [:500-501](../docs/methodology.md) (the "don't quote"
  rubric).
- **Codified** as the `EUROSTAT_AGGREGATE_PRODUCT_NC` constant in
  [anomalies.py:115](../anomalies.py).
- **Regression-tested** in
  [tests/test_eurostat_scale_reconciliation.py](../tests/test_eurostat_scale_reconciliation.py).

**Editorial impact**: zero on analyser output. All `hs_group_yoy`,
`hs_group_trajectory`, `mirror_trade`, `mirror_gap_zscore`, and
`gacc_aggregate_yoy` findings apply HS-pattern LIKE filters
(`'8505%'`, `'85%'`, etc.) that naturally exclude `'000TOTAL'`.

**The only stale artifact** is the annual snapshot table in
[shock-validation-2026-05-09.md:404-415](shock-validation-2026-05-09.md)
— written *one day before* the 2x mystery was resolved, so its raw
sums (2024 imports €1,058B etc.) double-count by 2x. Real `000TOTAL`-only
EU-27 imports from CN: **2024 €525.7B**, **2025 €559.5B**. That table
should be marked stale (or refreshed) but the underlying analysers it
referenced were always correct.

**What the validation actually demonstrated**: the guardrails work. A
fresh agent (me) wrote ad-hoc queries without checking the docs, hit
the trap exactly as predicted, and the documented filter rule resolved
the discrepancy. The system noticed me being lazy, not a bug it had
missed.

## Per-claim results

The format below is compact: `[Soapbox claim] → [our number / method] → [verdict + one-line explanation]`. For the natural-key tuple of each claim, see the pre-registered Expected block above.

### A1 — Export surge / EU trade defence (2026-05-11)

- **A1.1 Jan-Apr 2026 GACC exports $201B / imports $88B / surplus +26%** → Our GACC observations (EU partner_country, YTD): exports **$200.7B**, imports **$87.6B**, surplus **$113.1B**. YoY +18.9% / +11.6% / +25.2%. → **✓ CLEAN CONCUR**. **Flagging gap**: `gacc_aggregate_yoy` does not emit findings for `aggregate_kind='eu_bloc'` (only ASEAN, Latin America, world Total). The EU is the single most-cited bloc in Soapbox/Lisa's writing; not surfacing it is a real editorial gap.
- **A1.2 MPPT inverters (CN8 85044084) Jan-Feb 2026 ≈ €220M, 95% CN** → Our Eurostat raw rows for `85044084`: CN €217M (Jan+Feb), HK_MO €0.7M. → **✓ CONCUR on level** (€217M ≈ €220M). **Share UNVERIFIABLE** — we only ingest partner ∈ {CN, HK, MO}, so the EU-wide denominator (including all extra-EU sources) is unknown.
- **A1.3 Solar inverters 2025 87% kg / 75% value CN share** → CN-only 2025 value €8.62B (matches our hs_group_yoy finding €8.66B). Share **UNVERIFIABLE** for the same reason as A1.2.
- **A1.4 Rare-earth bucket ≥90% CN share 2023-2025** → UNVERIFIABLE (partner coverage gap).
- **A1.5 UK trade with CN $100B/$20B 2026** → Pre-reg was `WAIT-UK` but HMRC IS ingested; could be tested but the figure is a forward estimate for 2026, not a verifiable historical number.
- **A1.6 chemical inputs share (amino acids 88%/52%, etc.)** → BLOCKED — no hs_group for these products. Idea-generation block remains valid.

### A2 — Largest ever Q1 surplus with EU (2026-04-27)

- **A2.1 China's EV+hybrid car exports Q1 $11B→$20.6B (+87%)** → GACC side: sections 5/6 are NOT ingested (only section 4 by country/region), so GACC-side HS-level test is **BLOCKED**. Eurostat-side cross-check from shock-validation §4: our 12mo-ending-2026-02 = €10.46B (~$11.3B) — matches Lisa's "$11bn Q1 2025" framing.
- **A2.3 Natural graphite -22% / 39% share Jan-Feb 2026** → BLOCKED — no hs_group.
- **A2.4 Neodymium permanent magnets +18% volume Jan-Feb 2026** → At our HS-8505 hs_group level: **+1.4% kg YoY** at 2026-02 (12mo rolling). DIVERGE. But at CN8 sub-code **85051110 (sintered NdFeB) specifically**: Jan-Feb 2025 = 3,568t → Jan-Feb 2026 = 4,212t = **+18.05% volume**. → **✓ CONCUR at HS-CN8 granularity**. The broad-group signal dilutes the neodymium-specific story. **The 93% share is unverifiable** (partner coverage gap; within CN+HK+MO the CN share is 99.5%, but the EU-wide share depends on imports from countries we don't ingest). **POST-FOLLOW-UP (2026-05-11)**: a `Sintered NdFeB magnets (CN8 85051110)` hs_group was added (id=34, `seed:soapbox_validation`). The tool now emits a finding at this granularity: 12mo-rolling kg YoY at 2026-02 = **+7.9%** (vs broad-chapter +1.4%) — 5.6pp sharper editorial signal. Single-period Jan-Feb +18% remains a single-month-YoY operator gap, not a coverage gap.
- **A2.5 CN imports of US chips +168%** → Out of scope.

### A3 — EU exports plunged in Feb (2026-04-20)

- **A3.1 EU exports to CN Feb 2026 -16.2% single-month YoY** → 2025-02 €16.86B, 2026-02 €14.13B (000TOTAL). YoY **-16.19%**. → **✓✓ EXACT CONCUR** (Soapbox -16.2%, ours -16.2%).
- **A3.2 EU imports from CN Feb 2026 +2.2% single-month YoY** → 2025-02 €43.06B, 2026-02 €44.01B. **+2.21%**. → **✓✓ EXACT** (Soapbox +2.2%, ours +2.2%).
- **A3.3 EU exports to CN Jan 2026 -5.1% / Jan-Feb avg -11%** → Jan -5.11%, Jan-Feb -11.00%. → **✓✓ EXACT** on both. **Flagging gap**: no `aggregate_yoy_single_month` finding kind. These four agreement-points are derivable from raw rows but the tool never surfaces them as findings.
- **A3.4 EU EV+hybrid imports +45% Jan-Feb 2026** → Our Jan-Feb 2026 vs 2025 cumulative for (87038010 + 870340 + 870360) CN-only: **+16.4%** value (or +18.7% sp=1-only). → **✗ DIVERGE**. Soapbox's +45% cannot be reproduced from our data even after stat_procedure filters. Likely cause: Soapbox includes a wider definition of "electric and hybrid cars" (possibly all 8703 codes containing any electric motor), or uses a different filter we haven't identified.
- **A3.5 BEV/PHEV/HEV split** → Our sp=1-only Jan-Feb cumulative:
  - **BEV (87038010): +15.1% value** (Soapbox +16% — **✓ CONCUR**)
  - **PHEV (87036010): +89.1% value** (Soapbox +88% — **✓✓ EXACT**)
  - **HEV non-plug (87034010): -22.1% value** (Soapbox +124% — **✗ DIVERGE**)
  The DIVERGE on HEV is the open question. Possibility: Soapbox's "non-plug-in hybrid" may map to a different CN8 set (e.g., they may include 870350 / 870370 / or a re-classification that we haven't identified). **The BEV and PHEV figures concur cleanly at HS-CN8 granularity** — strong validation of the methodology on two of three sub-products.

### A4 — China's car exports need Europe (2026-03-30)

- **A4.1 CN→EU EV+hybrid "nearly doubled"** → GACC side: section 4 has total trade by country, not by HS code. BLOCKED.
- **A4.2 GACC-side category split (235/149/57%)** → BLOCKED (GACC HS-level not ingested).
- **A4.4 EV share fell 65%→52%** → Mirror of A3.5. Within the BEV+PHEV+HEV(870340) cumulative: BEV share Jan-Feb 2025 = 1131/2186 = **51.7%**; Jan-Feb 2026 = 1287/2562 = **50.2%**. So Soapbox's "65% → 52%" doesn't match our **52% → 50%**. → **✗ DIVERGE** on the 2025 baseline (we get 52%, they get 65%). Same root cause as A3.4 — they include a wider hybrid CN8 set.
- **A4.5 Germany dragged Jan 2026 export drop, offset by FR/IT/PL** → Per-reporter rollup not emitted as a finding. Testable from raw rows but BLOCKED in tool output.

### A5 — China drove 61% of EU car export decline 2025 (2026-03-02)

- **A5.1 EU auto exports globally -6% in 2025** → BLOCKED — we only ingest partner ∈ {CN, HK, MO}; no world-partner data.
- **A5.2 EU auto exports to CN -43% / -€6.3B in 2025** → Our hs_group_yoy_export for "Finished cars (broad)" at 2025-12: **-41.6% value, -41.2% kg**. Delta: 8.846 - 15.146 = **-€6.30B**. → **✓✓ EXACT on absolute drop**, -41.6% vs Soapbox -43% within ±2pp.
- **A5.4 EU hybrid+electric exports +11% / +€6.3B (world)** → BLOCKED — world-partner gap.
- **A5.5 EU car-parts exports to CN -21% / -€2.0B** → Our hs_group_yoy_export for "Motor-vehicle parts" at 2025-12: **-21.0% value, -19.3% kg**. Delta 7.629 - 9.655 = **-€2.026B**. → **✓✓ EXACT** (Soapbox -€2.01B, ours -€2.03B).
- **A5.6 Germany car-parts -11.1% / to CN -20.1%** → Per-reporter rollup not emitted. BLOCKED in tool output, but raw rows have reporter='DE' available.
- **A5.7 EU car imports total €74.8B (2025)** → BLOCKED — world-partner gap.
- **A5.10 BEV imports from CN -€2.5B (-30%)** → Our raw-row sum for HS 870380% CN-only 2025: **€6.35B** vs 2024 **€8.89B** = **-28.6%** / -€2.54B. → **✓ CONCUR within ±2pp**.
- **A5.11 EU imports from CN volume 86.4Mt / €6.5/kg / +13.5%** → Need 000TOTAL kg figure; not computed in this Stage B pass. (Quick check possible: `SELECT SUM(quantity_kg) FROM eurostat_raw_rows WHERE product_nc='000TOTAL' AND partner='CN' AND flow=1 AND yr=2025` — left as Stage C work.)
- **A5 yttrium oxide flows** → Idea-generation, not directly testable at the current hs_group level.

### A6 — EU 2025 deficit bites harder (2026-02-16)

- **A6.1 EU 2025 imports +6%, exports -7%, deficit +15%** → 000TOTAL: imports €525.7B → €559.5B = **+6.42%**; exports €213.5B → €199.5B = **-6.57%**; deficit +**15.30%**. → **✓✓ EXACT on all three**.
- **A6.2 Deficit >$400B / €1bn/day** → 2025 deficit €360.02B = **€986M/day**. USD equivalent at ~1.07 EUR/USD ≈ $385B. → **✓ CONCUR within band** (Soapbox said ">$400B" — we're slightly under, but the "€1bn/day" framing matches).
- **A6.3 EU exports to CN 3-year accelerating decline -3.0/-4.5/-6.5%** → Annual 000TOTAL exports flow=2 partner=CN: 2022 €214.79B, 2023 €223.95B, 2024 €213.50B, 2025 €199.49B. Year-over-year: 2023 **+4.3%**, 2024 **-4.7%**, 2025 **-6.6%**. → **✗ PARTIAL DIVERGE** — Soapbox's "-3.0/-4.5/-6.5" doesn't match our "+4.3/-4.7/-6.6". The 2025 figure matches; the 2023 figure flips direction. Possibility: Soapbox aligns to fiscal year or uses a different reporter set. Editorially significant: our data does NOT confirm "three consecutive years of decline" — 2023 was actually up.
- **A6.4 EU BEV imports from CN 2023→2025 -43%, €11.0B→€6.3B** → Our HS 870380% CN-only annual: 2023 **€11.89B**, 2025 **€6.47B** = **-45.6%**. → **✓ CONCUR** within ±5pp (Soapbox -43%, ours -45.6%). Level: our 2023 figure is €0.9B higher than Soapbox; 2025 matches within ±€0.2B.
- **A6.5 EU pork exports to CN 2025: meat -11% volume, offal -3%** → Pork meat (0203): our hs_group_yoy_export at 2025-12 = **-10.1% value, -9.4% kg**. → **✓ CONCUR on meat** within ±2pp. **POST-FOLLOW-UP (2026-05-11)**: a `Pork offal (HS 0206 swine)` hs_group was added (id=33, `seed:soapbox_validation`). The tool now emits a finding for offal: hs_group_yoy_export at 2025-12 = **-9.6% value, -2.9% kg**. → **✓ CONCUR on offal** (Soapbox -3% kg, ours -2.9% kg, within ±0.1pp). A6.5 is now fully testable from named tool findings, no raw-row queries needed.
- **A6.6 EU import share from CN = 22% (2025)** → BLOCKED (world-partner gap).
- **A6.7 CN-EU surplus ~1/3 global** → BLOCKED (GACC top-line not ingested).

### A7 — BEV long tail (2026-01-26)

- **A7.1 CN global BEV exports +11% YoY 2025** → BLOCKED — GACC sections 5/6 not ingested.
- **A7.2 EU+UK = 37% / advanced-10 +0.8% / long-tail-147 +30%** → BLOCKED — no partner-bloc rollup at HS level.

### A8 — 2025 global $1.2T surplus (2025-12-29)

- **A8.1 CN global surplus ~$1.2T** → BLOCKED — GACC section 1 (global top-line) not ingested.
- **A8.2 EU imports from HK +9% in 2025** → 000TOTAL flow=1 partner=HK reporter=eu_27: 2024 €4.08B, 2025 €4.37B = **+7.0%**. → **✓ CONCUR within ±3pp** (Soapbox +9%, ours +7%).

### A9 — Pork probe / 2024 mid-year (2024-06-24)

- **A9.1 EU electric car imports from CN Jan-Apr 2024 -11% / Q1 -13% / Apr -2%** → BEV-only (870380): Jan-Apr -22.0%, Q1 -24.9%, Apr -11.4%. With HEV+PHEV added: Jan-Apr -22.8%, Q1 -23.7%. → **✗ DIVERGE significantly on magnitude** (direction agrees). Probable cause: Soapbox's at-the-time June-2024 number used the (then-preliminary, since-revised) Eurostat data, OR a narrower CN8 set. Our current data is the revised figure.
- **A9.2 EU 2022 deficit >€1bn/day** → 2022 deficit = €577.75B - €214.79B = **€362.96B** = **€994M/day**. → **✓ CONCUR within band** (just under €1bn/day, but in the same framing).
- **A9.3 2024 vs 2022 peak: imports -20%, deficit -33%** → Our: imports **-9.0%** (525.7/577.8), deficit **-14.0%** (312.2/362.9). → **✗ DIVERGE significantly**. Soapbox's June-2024 figures were YTD-2024-vs-2022; once 2024 completed, the year-on-year drop was smaller. Their characterisation overstates the full-year comparison.

### A10 — Hybrid 5x / pork drop (2024-09-30)

- **A10.1 CN hybrid car shipments to EU Jul-Aug 2024 ≈ 5x** → Our Jul-Aug 870340+870360 sum, partner=CN: 2023 €133M, 2024 €452M = **+239%** (3.4x). → **✗ PARTIAL DIVERGE** — same direction, ~70% of the Soapbox magnitude. Almost certainly the same CN8-mapping issue as A3.4/A3.5/A9.1 (the 2024-vintage Soapbox "hybrid" definition is wider than our 870340+870360 set).
- **A10.2 EU hybrid car imports +47% Jan-Jul 2024 by value** → Our 870340+870360 sum Jan-Jul 2023 vs 2024: **+21.2%** (€915M → €1,109M). → **✗ DIVERGE** on magnitude (direction agrees).
- **A10.3 CN pork imports from EU -23% Jan-Aug 2024 (948kt → 731kt)** → Eurostat flow=2 (EU exports) view, partner=CN, Jan-Aug:
  - 0203 alone: 400.2kt → 320.8kt = -19.8%
  - 0203 + 0206 (offal): **804kt → 751kt = -6.6%**
  - Soapbox's 731kt for 2024 Jan-Aug matches our 751kt within 3% (likely includes offal). Their 948kt baseline for 2023 doesn't match — our 804kt is 15% lower.
  - → **✗ PARTIAL DIVERGE**: 2024 level concurs, 2023 baseline diverges. Possible cause: CN-side stat differs from EU-side (re-routing, late shipments arriving), or Soapbox includes additional pork-product CN8 codes we haven't captured.
- **A10.4 EU imports from CN value -11%, volume unchanged (mid-2024)** → Direct test would need a 12mo-ending-2024-08 finding; we have current_end at 12-month intervals. Approximate via hs_group_yoy at 2024-08 anchors across groups — broadly directionally consistent but not directly testable as a single aggregate.

## Cross-article checks

### 1. Mirror gap on EV+hybrid imports

Eurostat-side (ours, 2026-02 anchor, 12mo rolling): EV+hybrid imports from CN+HK+MO = €10.46B, YoY +4.5%. Soapbox GACC-side annualised Q1 2026: $20.6B (~€19B). The gap is large but expected — the Eurostat figure is rolling 12mo through Feb 2026, while the GACC figure annualises a single recent quarter. After normalisation, the order-of-magnitude story (Chinese EV exports to EU growing fast in 2026) agrees.

### 2. Single-month vs rolling-12mo YoY

Confirmed across A3.1, A3.2, A3.3 — Soapbox's single-month figures (-16.2%, +2.2%, -5.1%) are exactly derivable from our raw rows but **never surfaced as findings**. A `aggregate_single_month_yoy_v1` analyser kind would surface Soapbox-cadence figures directly. The cleanest concur in the entire validation pass (3 EXACT matches in one article) is on data the tool doesn't currently flag.

### 3. CN-only vs CN+HK+MO consistency

For the key tested claims:
- Permanent magnets 8505 broad CN+HK+MO YoY kg = +1.4% (12mo). CN-only single-period at sub-CN8 85051110 = +18%.
- BEV 870380 CN-only annual 2023-2025 = -45.6% (matches Soapbox -43%). CN+HK+MO would be marginally less negative (HK/MO BEV imports are tiny — €11k for Jan-Feb 2026).
- Aggregate 000TOTAL CN-only YoY = the Soapbox-aligned cadence.

For trade aggregates, **CN-only matches Soapbox cleanly**. For HS-group findings, our CN+HK+MO default puts us slightly off Soapbox's CN-only convention, but the difference is small (<2pp typically) for products where HK/MO has minimal re-export flow.

### 4. Coverage gaps surfaced

hs_groups missing across the 10 articles, ranked by Soapbox citation frequency:
- **Pork offal (HS 0206)** — A6.5, A10.3. Single most-tested gap. Trivial extension of existing Pork hs_group.
- **Neodymium permanent magnets (CN8 85051110 subset)** — A2.4. Sub-group of existing Permanent magnets. Surfaces the +18% Soapbox story that the broad group dilutes to +1.4%.
- **Natural graphite (HS 250410)** — A2.3, mentioned A1. China-controls relevance.
- **MPPT inverters (CN8 85044084, separate from 2026)** — A1.2, A2-implied. Subset of Solar/grid inverters.
- **Dairy (HS 0401-0406)** — A6.5 cite of countervailing duties.
- **Pharma/feed chemical inputs** (amino acids HS 2922; adipic acid HS 2917.12; choline HS 2923.10; vanillin HS 2912.41; feed premixes HS 2309) — A1.6 thematic.
- **Yttrium oxide / rare-earth narrow** (CN8 within 284690) — A5.

### 5. CIF/FOB and currency consistency

GACC-side (USD) and Eurostat-side (EUR) figures align as expected: at ~1.07 USD/EUR, our Eurostat €10.46B (rolling 12mo) ≈ $11.2B which lines up with Soapbox's "$11B Q1 2025" annualisation. No surprises.

## Where the tool concurs editorially vs where it doesn't

**Concurs cleanly (numbers + flagging):**
- Pork exports to China 2025 (-10% to -11% range) — matches both meat and offal direction; tool fires the Pork hs_group_yoy_export finding.
- BEV imports from China 2023→2025 (-43% to -46%) — matches Soapbox's -43%; tool fires hs_group_yoy on EV+hybrid passenger cars showing the rolling 12mo trajectory.
- EU motor-vehicle parts exports to China -21% / -€2B — Soapbox's flagship claim that "China explains 42% of EU drop" rests on a -€2.01B number; ours says -€2.03B. The tool fires this finding.
- Finished cars exports to China -43% — matches Soapbox A5.2; tool fires.

**Concurs in numbers but tool doesn't flag editorially:**
- All aggregate single-month and annual EU-CN YoYs (A3.1-3, A6.1). The tool emits no `aggregate_yoy` for EU-CN trade — the single most-cited story shape in Soapbox/Lisa pieces.
- Permanent magnets at sub-CN8 (85051110) — concurs at +18%. Our broad-HS-8505 hs_group fires a finding but at +1.4%, which understates the editorial signal.
- HK imports +7% (matches +9%) — tool emits no per-partner aggregate finding.

**Diverges in numbers:**
- A3.4 EV+hybrid +45% — we get +16%. Cause not pinned down; suspected wider CN8 inclusion on Soapbox's side.
- A3.5 / A4.4 non-plug-in hybrid +124% — we get -22% on 870340. Suspected CN8 reclassification — Soapbox may use a different mapping for "non-plug-in hybrid".
- A9, A10 2024-vintage claims — Soapbox's June/Sept-2024 figures don't match our current (revised) data; revision effects likely.

**Cannot test (coverage / data gap):**
- Anything dependent on world-partner shares (A1.2, A1.3, A1.4, A6.6, A5.7, A5.1).
- GACC HS-level (A2.1, A4.1, A4.2, A7.1).
- GACC sections 5/6 (commodity-level non-country) — A8.1.
- Niche product hs_groups (graphite, dairy, offal, MPPT, pharma chemicals).

## Forward work — prioritised

The validation pass surfaced six concrete asks for the analyser pipeline, ranked by editorial impact:

1. **`aggregate_yoy` analyser kind for the EU-CN top-line** (covers A1.1, A3.1, A3.2, A3.3, A6.1, A6.2, A6.3 — 7+ claims). The strongest single intervention: every Soapbox monthly/annual aggregate piece would have a corresponding tool finding. Currently `gacc_aggregate_yoy` runs for ASEAN/LatAm/world Total but **excludes** `eu_bloc` ([anomalies.py:2435](anomalies.py) — design choice: "mirror-trade handles EU"). Engaging with that design choice rather than papering over it is the real ask: Soapbox's USD top-lines aren't the same finding as mirror-trade's per-country bilateral gaps. Worth a separate planning pass before implementation. Single-month-YoY variant should run alongside 12mo-rolling.
2. ~~**Sub-HS-CN8 sub-groups for the products where Soapbox quotes specific 8-digit codes**: NdFeB magnets (85051110), MPPT inverters (85044084), graphite (250410), rare-earth narrow (specific 284690 sub-codes). Each generates a finding that mirrors a Soapbox claim 1:1.~~ **PARTIALLY DONE 2026-05-11**: NdFeB sub-group added (id=34). MPPT, graphite, rare-earth narrow remain.
3. ~~**Extend "Pork (HS 0203)" to include offal (HS 0206)** — or split into two groups. A6.5 and A10.3 cite both consistently; the offal data is in our DB but not aggregated. Trivial change.~~ **DONE 2026-05-11**: split into two groups (Pork HS 0203 = meat; Pork offal HS 0206 swine = id 33).
4. **Per-reporter hs_group rollup** (covers A4.5, A5.6, and any future "Germany leads the decline" story). HMRC integration shows the rollup is feasible per source; extending per-reporter within Eurostat is a query addition, not new ingestion.
5. **GACC section 5/6 ingest** OR Eurostat-side mirror for "China's exports of EV+hybrid to EU at HS-CN8 level" (covers A2.1, A4.1, A4.2, A7.1). Section 5/6 only has ~30 hand-curated commodity names; the cleaner path is to rely on Eurostat for HS-level and accept the FOB/CIF caveat.
6. ~~**Refresh the annual snapshot table in [shock-validation-2026-05-09.md:404-415](shock-validation-2026-05-09.md)** — those raw sums pre-date the 2026-05-10 000TOTAL filter rule and are 2x inflated. Underlying analysers were always correct; only that one hand-rolled snapshot table is stale.~~ **DONE 2026-05-11**: refreshed using 000TOTAL canonical aggregate. The CN-only 2025 deficit now reads €360.0B — literally to the percentage point matching Lisa O'Carroll's "€360bn 2025 surplus" cite. Surfaced a separate pre-existing artefact: 2017 has duplicate 000TOTAL rows from the pre-v2 COMEXT format (sp=1: 648 rows for 2017 vs 351 for 2018) — flagged in the refreshed table; no analyser output affected.

The first two together would convert roughly **half of the currently-blocked claims** in this validation to clean concur.

## Bottom line for the user

**The tool reaches the same conclusions as Soapbox when both:**
- the comparison sits within an existing hs_group at the right granularity, and
- the analyser actually emits a finding for that natural-key tuple.

When those conditions hold, agreement is strong: pork exports to China, finished-car exports to China, car-parts exports to China, BEV imports from China over multi-year windows, all match Soapbox within 2-3 percentage points. Where Soapbox's story is "specific CN8 sub-code did X" or "aggregate single-month YoY was Y", **the data reproduces it but the tool doesn't surface it as a finding** — and that's the largest single deliverable surfaced by this exercise. The recommended forward work in §1-3 above closes most of it.

A confirmation-bias note on the validation process itself: my initial Stage B report flagged the `000TOTAL` 2x scale issue as a "real ingestion bug surfaced by the validation". On user pushback, I rechecked — the issue had already been investigated, documented, and tested-against on 2026-05-10 (one day before this validation), and the analyser pipeline was never affected. What the exercise actually surfaced was that **I ran ad-hoc queries without first reading the methodology doc that explicitly says "MUST filter `product_nc != '000TOTAL'`"**. The documentation and tests caught the lazy query. That's the system working, not a finding.
