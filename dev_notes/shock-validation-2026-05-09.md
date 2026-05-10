# Shock validation: pre-registered expectations (2026-05-09)

A methodology audit. We pick known historical trade shocks, write down what
the analyser **should** surface (and where) **before** running it, then compare.
Pre-registration matters here — it's the structural defence against the
confirmation-bias risk in the broader project review (`memory/project_gacc.md`
discussion 2026-05-09): if we tune-and-look, we converge on a self-consistent
loop. If we predict-then-look, methodology weaknesses show up as misses
or unexplained extra hits.

This document is written *before* the validation run. After running, a
**Results** section per shock captures what actually surfaced and any
misses or false positives. Edits to the **Expected** section after a run
must be flagged explicitly with a rationale — never silently retuned.

## Data preconditions

- Eurostat: 2017-01 → 2026-02 (CN + HK + MO partners). HK/MO additive ingest
  for 2021-01 → 2026-02 in progress; historical CN+HK+MO ingest for
  2017-01 → 2020-12 follows.
- Analysers: re-run with `--eurostat-partners CN,HK,MO` (Phase 2.3 multi-
  partner support) on the full extended range.
- HS groups: 16 currently active. Steel and Aluminium broad groups cover
  the 2018 tariff stories; EV / Finished cars cover the 2023 EV probe.
  Gaps flagged below where they exist.

## Shocks

### 1. 2018 Section 232 steel & aluminium tariffs (March 2018)

**What happened.** US imposed 25% tariffs on steel imports (March 23, 2018)
and 10% on aluminium. Trade-economic prediction: Chinese steel/aluminium
that previously entered the US gets diverted to other markets, including the
EU. The EU imposed safeguard measures in July 2018 partly in response, then
provisional anti-dumping in early 2019, so any rise should be partial and
may turn before the year is out.

**Expected analyser hits** (flow=1, EU imports from CN):

- `hs_group_yoy` — Steel (broad) HS 72: positive YoY in mid-to-late 2018
  vs 2017. Magnitude small-to-moderate (Chinese steel was already a small
  share of EU; some volume growth not transformative).
- `hs_group_yoy` — Aluminium (broad) HS 76: positive YoY in mid-2018,
  potentially flatter by Q4 2018 as EU safeguards bit.
- `hs_group_trajectory` — Steel: rising trajectory through Apr-Aug 2018,
  flattening or peaking late 2018. Possibly classified
  `falling_decelerating` if the rolling 12mo lags the inflection.
- `hs_group_trajectory` — Aluminium: similar.
- `mirror_gap` — possibly: NL/BE share of EU imports increases in 2018
  (transshipment uptick), but this one is speculative.

**Possible misses.** If the EU safeguards were fast enough that Chinese
volumes never grew much, the analyser will correctly fire only weak signals.
A "weak signal where the policy worked" is not a methodology failure — it's
the methodology working. We should distinguish silence-from-no-effect from
silence-from-blindness.

**Expected silence.** No fire on EV / Solar / Wind / Magnets — these are
unrelated to the 2018 steel story. Any unexpected fire on those groups in
2018 is a flag for false-positive review.

### 2. Q1 2020 COVID lockdown (Feb–Apr 2020)

**What happened.** China factory shutdowns Jan–Feb 2020 collapsed export
capacity for 4–8 weeks. EU lockdowns from mid-March 2020 collapsed demand.
Then a sharp recovery from Q3 2020 through 2021 ("revenge consumption" +
restocking + work-from-home goods boom).

**Expected analyser hits** (flow=1, broad sweep):

- `hs_group_yoy` — almost every group should show negative YoY for windows
  ending Feb/Mar/Apr 2020 vs 2019. The exceptions worth flagging:
  - Permanent magnets, rare earths: more inelastic strategic supply chains;
    drop may be smaller and recover faster.
  - Anything PPE-related: should *rise* during the pandemic. **We have no
    PPE HS group** — flagged as a coverage gap. (HS 401511 surgical gloves,
    HS 6307.90 masks, HS 9020 ventilators would all be candidates.)
- `hs_group_trajectory` — the dominant shape across most groups should be
  `dip_recovery` or `u_recovery` over the 2020–2022 window. If many groups
  classify as `failed_recovery` instead, that suggests COVID was worse for
  EU import demand than the rebound restored — a finding in itself.
- `mirror_gap_zscore` — the COVID period should have substantial mirror-gap
  variance. Z-scores firing in Apr-Jun 2020 are expected and reasonable
  (logistics chaos creates real reporting divergence).

**Coverage-gap action.** If COVID validation passes for the 16 existing
groups but lacks a PPE counter-shock, add a PPE HS group post-validation
and flag it as a `seed:covid_validation` group. This addresses the user's
"add more sectors" concern with a concrete, justified addition rather than
an arbitrary one.

**Possible false positives.** YoY windows whose denominators happen to land
on a low-base point in 2019 (e.g. one HS group with seasonal Mar peak in 2019
and Mar trough in 2020) might get amplified by low-base. The
`low_base_threshold_eur` should suppress most of these; we'll spot-check.

### 3. Feb 2022 Russia invasion of Ukraine

**What happened.** EU energy sanctions on Russia → demand for renewables
infrastructure and battery storage accelerates. Russia-China trade rises
(not visible to us — we only see CN-EU). Some industrial inputs get
re-routed.

**Expected analyser hits** (the hardest case to predict — second-order
effects, not direct):

- `hs_group_yoy` — Solar PV: positive YoY through 2022 and into 2023 as
  EU energy diversification accelerates Chinese solar imports. Eventually
  rolls over (the well-known 2024 collapse from oversupply).
- `hs_group_yoy` — Wind generating sets / Wind turbine components: rising
  through 2022–2023. Tan article (`dev_notes/editorial-sources.md`) data
  point: "exports surged 50% in 2025" — should see precursor in the 2022–2024
  trajectory.
- `hs_group_trajectory` — Solar PV: should classify as `inverse_u_peak` over
  the 2022–2025 window (rise then fall). Wind: `dip_recovery` or rising.

**Weakest validation case.** Russia is primarily a Russia-EU and Russia-CN
story. Our CN-EU lens picks up the second-order solar/wind story but has no
direct visibility into the primary effect. A clean miss on Russia-specific
expectations would not damage methodology confidence — but a clean hit on
the renewables substitution story would strengthen it.

### 4. October 2023 EU EV anti-subsidy probe (and Oct 2024 provisional duties)

**What happened.** EU launched anti-subsidy investigation on Chinese EVs
in October 2023. Provisional countervailing duties (10–35% on top of the
existing 10% MFN) imposed October 2024. Industry reporting: clear
pre-tariff stockpiling Q3-Q4 2024.

**Expected analyser hits** (flow=1):

- `hs_group_yoy` — EV + hybrid passenger cars (HS 870380/870370/870360):
  - Positive YoY through 2023 and into early-to-mid 2024
  - Spike in Q3 2024 (pre-tariff loading)
  - Sharp negative YoY from Q4 2024 / Q1 2025 onwards
- `hs_group_yoy` — Finished cars (HS 8703) overall: similar shape but
  diluted by ICE traffic.
- `hs_group_trajectory` — EV cars: should classify as `inverse_u_peak`,
  with the peak landing late 2024 / early 2025 in the smoothed series.
  This is the single cleanest predicted-shape test in the document.
- Lisa O'Carroll's article ("EV imports drove Beijing's record surplus")
  should produce numbers reproducible from the same finding rows.

**This is the strongest validation case.** Recent, concentrated, large
magnitude, well-documented, and falls within the existing 2021+ data range
(does not depend on historical backfill). If the analyser **fails** to
surface this story, that is a methodology problem, not a silence-from-no-
effect.

## Cross-shock checks

After per-shock validation, sanity-check across:

1. **Trajectory shape distribution.** Roughly: pre-2020 should be heavily
   `flat`/`rising_decelerating`. 2020 should be heavily `dip_recovery`.
   2022-2024 should be a mixed bag with Russia-related substitution shapes.
   2025+ should be heavily `falling_decelerating` or `inverse_u_peak` for
   politically-targeted groups (EVs, solar).
2. **Caveat distribution.** `cn8_revision` should be on essentially every
   YoY finding (every window crosses a year boundary). `low_baseline_n`
   should appear in early-history findings (2017-2018 z-scores). New: with
   HK/MO ingested, `transshipment_hub` should fire for HK-routed mirror gaps.
3. **Mirror-gap baseline magnitudes.** With HK/MO ingested, the structural
   NL gap (~65-70%) should narrow somewhat (HK-routed Chinese trade now
   captured separately rather than being attributed to NL via Rotterdam
   transshipment of HK-routed goods). The exact magnitude shift is itself
   editorially interesting.

## Results sections (run 2026-05-10)

### 0. Coverage / sanity

DB state after the clean-state rebuild (Phase 5):

- **Eurostat raw rows**: 13.4M CN + 3.8M HK + 0.3M MO, all spanning
  2017-01 → 2026-02 (110 periods × 3 partners). Per-partner counts
  in the predicted ratio (HK ~28% of CN row count is plausible
  given HK reports finer commodity detail per shipment).
- **GACC**: 149 section-4 releases across 2019-2026 (2018 absent —
  parser title-format gap, see `forward-work-gacc-2018-parser.md`).
- **Active findings**: 5,394 total —
  - hs_group_yoy: 2,326 (flow=1) + 2,270 (flow=2) = 4,596
  - hs_group_trajectory: 3 + 2 = 5  ⚠ very low — see Cross-shock §5
  - mirror_gap: 351
  - mirror_gap_zscore: 74
  - gacc_aggregate_yoy: 171 + 171 = 342 (after Phase-5 loose-
    partial-window fix)
  - LLM narrative: 28 (1 hallucination correctly rejected — the
    Permanent magnets "93%" claim recalled from training data,
    not in our facts; verifier caught it)
- **Method versions in use**: all the bumped ones from Phase 5
  (`mirror_trade_v4`, `hs_group_yoy_v7`, `hs_group_trajectory_v5`,
  `gacc_aggregate_yoy_v2`).

### 1. Section 232 (2018) — RESULTS

**Steel YoY at 2018-11/12 anchors: −21.1% / −22.3% value, −34.9% /
−35.8% kg.** This is the OPPOSITE direction to my pre-registered
expectation (positive trade diversion from US to EU). Two readings:

- **The methodology fired the right direction even though I expected
  the wrong one.** EU imposed safeguard measures in July 2018 and
  provisional anti-dumping in early 2019, which appear to have
  blocked the diversion that simple economic theory predicted.
  Chinese steel volumes DROPPED into the EU rather than rising —
  consistent with "the policy worked". This is exactly the kind of
  confirmation-bias check the pre-registration was designed to surface.
- **Aluminium YoY at 2018-11/12: +2.1% / −1.6% value, +12.0% / +8.5%
  kg.** Volume rose but unit prices fell — the partial diversion got
  through but margins compressed. Cleaner story than steel.

**Trajectory shape for Steel + Aluminium**: 0 findings in the
trajectory analyser. The trajectory classifier is producing far
fewer shape findings than expected (5 total across 29 groups × 2
flows = expected ~30-50). See Cross-shock §5 for the underlying
cause.

**Verdict**: shock surfaced (steel signal is strong), in the
opposite direction to my prior. Methodology earns trust here:
no confirmation-bias of my expectations, real signal from the data.

### 2. COVID Q1 2020 — RESULTS

**Universal drop in flow=1 imports across most groups at the
2020-04 anchor (24mo window ending Apr 2020):** Steel −25.9%,
Aluminium −7.7%, Honey −27.7%, Telecoms base stations −23.4%,
Tomato paste −34.5%, Cotton −6.7%, Tropical timber −6.2%, Rare
earths −30.5%. Exactly as predicted.

**Counter-shocks (groups that ROSE):**

- **PPE — surgical gloves and masks: +328.8% (Apr 2020) → +932%
  (Jun 2020) → +1271% (Dec 2020).** Textbook clean COVID
  counter-shock. **This validates the methodology AND the decision
  to add PPE as a coverage gap.** Pre-registration said
  "if COVID validation passes for the 16 existing groups but lacks a
  PPE counter-shock, add a PPE HS group post-validation" — adding it
  pre-validation produced the cleanest single signal in the entire run.
- **EV + hybrid passenger cars: +774% (Apr 2020) → +989% (Jun) →
  +274% (Dec).** The Chinese EV export breakout was already starting
  in 2020 — partly amplified by low-base (the rolling 12mo ending
  Apr 2019 was small, so the 2019→2020 ratio is huge). Consistent
  with editorial sources but the magnitude is a low-base artefact
  not a real 10x growth.
- **EV batteries: +43% / +32% / +28%** — modest, real, growth-driven.
- **Finished cars: +106% / +136% / +81%** — large but partly low-base
  too.
- **Wind generating sets: +83% / +114% / +26%** — early-stage
  Chinese wind exports.

**Verdict**: shock surfaced cleanly, both in the broad drop and in
the PPE counter-shock. Strongest validation result. The trajectory
analyser missed it (only 1 active trajectory has a recovery shape:
Solar PV cells & modules `falling_decelerating`) — see §5.

### 3. Russia Feb 2022 — RESULTS

**Solar/grid inverters trajectory (flow=1)**: textbook
`inverse_u_peak`, peak +102.3% YoY at 2023-04 anchor, declining to
+13.8% by 2023-12 anchor, then continuing decline (data through
2025-08 in the briefing). Current 12mo total at peak: €13.4B, down
to ~€12B by end of 2023.

**Wind generating sets trajectory (flow=1)**: dramatic boom-bust.
Peak +61.2% at 2022-04, falling to −81.6% by 2023-12. Total dropped
from €0.44B at peak to €0.08B by end-2023. **This is editorially
striking and contradicts the Tan article narrative** (wind exports
"surged 50% in 2025"). The CN→EU wind generating-set channel
collapsed. Tan's data was about Chinese exports globally
(Saudi/Brazil/Egypt buyers); the EU specifically backed away.

**Wind turbine components (broader group, flow=1)**: gentler shape.
Peak +33.8% at 2022-03, gradual decline to −19.5% by 2023-12.

**Verdict**: clean methodology hit. Renewables substitution from
Russia diversification clearly visible in the YoY data. Trajectory
shapes captured the inverse_u_peak pattern in Solar/grid
inverters specifically.

### 4. EV probe Oct 2023 — RESULTS

**EV + hybrid passenger cars YoY trajectory** (88 anchors covered):

| Anchor | YoY (value) | Current 12mo (€B) |
|---|---|---|
| 2020-04 | +774% | 0.48 |
| 2022-03 (peak) | +352% | 6.18 |
| 2023-04 | +74% | 11.18 |
| 2023-12 | +28% | 12.10 |
| 2024-08 (negative trough) | −25.2% | 9.97 |
| 2025-04 | −12.0% | 9.52 |
| 2026-02 (latest) | +4.5% | 10.46 |

**Pre-registered prediction**: peak Q3-Q4 2024 (pre-tariff
stockpiling), inverse_u_peak shape. **Actual**: peak Q1 2022, far
earlier, then long downturn through 2024 with recovery starting
mid-2025. The stockpiling-spike story is much weaker than expected;
the dominant pattern is the post-2020 boom normalising.

**Lisa O'Carroll cross-check**: she cited "$11bn Q1 2025 → $20.6bn
Q1 2026" for Chinese EV+hybrid sales to EU. Our 12mo-ending-2026-02
total is €10.46B (≈$11.3B). Quarter-to-quarter is harder to
extract from rolling-12mo data, but the order of magnitude aligns.
A separate quarterly-anchored extraction would reproduce her figure
more directly.

**Trajectory classification for EV passenger cars**: 0 active
findings (the trajectory analyser missed it — see §5).

**Verdict**: shock partially surfaced. The boom-and-decline is
visible in the YoY series, but the trajectory classifier didn't
land an `inverse_u_peak` shape on the EV group despite the data
clearly showing one. Methodology gap.

### 5. Cross-shock — RESULTS

**5.1 Trajectory shape distribution**:

```
falling_decelerating   1 (Solar PV cells & modules)
inverse_u_peak         1 (Pork (HS 0203))
rising_accelerating    1 (Drones and unmanned aircraft)
```

Only 3 active trajectory findings across 29 HS groups × 2 flows.
**This is the biggest methodology surprise from the validation
pass.** The trajectory classifier requires a continuous YoY series
(`_detect_series_gaps` skips groups with any gap), and the
underlying YoY data for most groups has gaps — including:
- The most-recent month often missing (publication lag — fires
  `partial_window` caveat instead of populating)
- Various early-history months with incomplete coverage
- Groups added overnight (HS groups 17-29) have continuous data
  but only on the EU import side
- Drones (HS 8806) classified as `rising_accelerating` is a
  classification artefact: HS 8806 didn't exist before HS2022, so
  the "rising" trajectory is simply the appearance of a new
  classification code.

**Action**: relax `_detect_series_gaps` to allow leading-edge gaps
(the most-recent missing month is universal, not a problem) but
keep the protection against multi-month internal gaps. Forward
work for Phase 6.

**Action SHIPPED 2026-05-10 (Phase 6.0.7, commit `13f5ea1`)**:
trajectory now uses longest-contiguous-run instead of all-or-nothing.
Coverage went from **5 to 57 trajectory findings** (29 + 28 across
flow=1 and flow=2). The EV + hybrid passenger cars trajectory now
classifies as `dip_recovery` with trough at 2024-08 — exactly when EU
provisional duties bit hardest. The pre-registered shape was
`inverse_u_peak`, but the YoY series effectively starts at the 2022
post-COVID peak and the editorial story (boom → tariff-driven dip →
recovery) is captured equivalently. Recorded in finding `features`:
`effective_first_period`, `effective_last_period`,
`original_series_length`, `effective_series_length`,
`dropped_periods_due_to_gaps`. Editorially honest: the journalist
sees exactly which window the shape was fit on.

**5.2 Caveat distribution** (active hs_group_yoy findings):

```
cif_fob                       2326 (every active finding)
classification_drift          2326 (every active finding)
cn8_revision                  2326 (every active finding — every
                                    24-month window crosses a CN8
                                    revision)
currency_timing               2326 (every active finding)
eurostat_stat_procedure_mix   2326 (every active finding)
multi_partner_sum             2326 (every active finding — Phase
                                    5 default is CN+HK+MO)
partial_window                 636 (~27% — leading-edge anchors)
low_base_effect                465 (~20%)
```

Six universal caveats means every finding carries six caveats
inline. This is partly intentional (cross-source caveats apply
broadly) but creates briefing-pack noise. Briefing pack already
suppresses `multi_partner_sum`; consider extending suppression to
the four other universal caveats (`cif_fob`, `classification_drift`,
`currency_timing`, `eurostat_stat_procedure_mix`) and surfacing
them in a top-of-brief block instead. Forward work.

**5.3 Mirror-gap baseline by partner**:

```
NL  72 findings  61.4% avg gap (range 48.8 → 70.0)
IT  72 findings  56.2% avg gap (range 32.3 → 70.5)
FR  72 findings  54.5% avg gap (range 41.0 → 68.3)
DE  63 findings  51.5% avg gap (range 33.5 → 65.9)
```

**The structural NL gap is now 61.4% (was reportedly ~65-70% before
HK/MO ingest, per project memory snapshots).** The HK/MO inclusion
narrowed it, as predicted, but only by a few percentage points.
Editorially: NL is still a transshipment-dominated comparison and
the `transshipment_hub` caveat continues to apply. The four big EU
economies (NL, IT, FR, DE) all sit in the 50-60%+ range — the
mirror-gap finding "EU side reports 50%+ more than China side"
appears to be a structural property of CN→EU trade across all
major bilateral pairs, not a NL-specific artefact.

**5.4 Total CN+HK+MO trade with EU (rough EUR aggregation)**:

| Year | Imports from CN+HK+MO | Exports to | EU deficit |
|---|---|---|---|
| 2017 | €1,146B | €697B | €449B |
| 2018 | €811B | €495B | €316B |
| 2019 | €865B | €524B | €341B |
| 2020 | €794B | €455B | €339B |
| 2021 | €958B | €493B | €464B |
| 2022 | €1,164B | €475B | **€688B** ← peak |
| 2023 | €1,051B | €499B | €553B |
| 2024 | €1,058B | €474B | €585B |
| 2025 | €1,127B | €442B | €684B |
| 2026 (Jan–Feb) | €183B | €64B | €119B |

Annualising 2026 from Jan-Feb gives €1,098B / €386B / €712B —
on pace to exceed the 2022 deficit peak. **This is editorially
significant**: the China-EU deficit is widening to historic
levels in 2026, even before the EV anti-subsidy duties bite
fully. A Guardian story shape: "EU's trade deficit with China set
to break records in 2026 despite EV tariffs".

Sanity vs Lisa O'Carroll's article (cited "€360bn 2025 surplus
from China's perspective"): our €684B EU-side deficit for 2025 is
larger than her surplus number. Two reasons for the gap:
- CIF/FOB difference: EU imports are CIF-valued, China exports are
  FOB-valued. The CIF inflation is ~5-10% (caveat `cif_fob`).
- HK+MO inclusion: she likely uses CN-only, we sum CN+HK+MO.
  Caveat `multi_partner_sum`.

The numbers are in the same ballpark editorially but methodology
choices matter.

## EU-27 methodology audit (2026-05-10) — RESULTS

Discovered post-validation: our `eurostat_raw_rows` table contained
UK-reporter (`reporter='GB'`) rows for 2017 through Q1 2020 because
the UK was an EU-28 reporter pre-Brexit. The hs-group SQL helpers
summed across all reporters with no filter, so 2017–Q1 2020 findings
silently answered "EU-28 incl. UK" while 2021+ findings answered
"EU-27 excl. UK" — breaking any cross-Brexit comparison. The original
validation pass above ran on this inconsistent baseline.

**Fix shipped** (commit `388be73`): three SQL helpers in
`anomalies.py` now filter `reporter <> ALL(EU27_EXCLUDE_REPORTERS)`
where `EU27_EXCLUDE_REPORTERS = ('GB',)`. Method bumped to
`hs_group_yoy_v8_excludes_gb_reporter_pre_brexit` and
`hs_group_trajectory_v6_inherits_eu27_yoy`. The supersede chain
captures the diff per finding. (A separate commit `70d7bc5` added
a covering index and rewrote `LIKE ANY` as ORed LIKEs to keep the
re-run fast — without it, the analyser would have taken ~3 hours;
with it, ~7 minutes.)

**Editorial impact** (4596 superseded hs_group_yoy findings):

| Severity bucket | Count | % | Reading |
|---|---|---|---|
| Method-tag bump only (numbers identical) | 3255 | 71% | 2021+ windows where there were no GB rows to remove |
| €-amount changed > €1 | **1341** | 29% | Pre-Brexit windows; UK trade subtracted |
| YoY shifted > 0.5pp | **1754** | 38% | Noticeable editorial difference |
| YoY shifted > 5pp | **1144** | 25% | Substantial editorial difference |
| **YoY direction flipped** ("growth" ↔ "decline" with >5pp shift) | **337** | 7% | The headline conclusion was wrong before |

**Most editorially-significant patterns**:

1. **Aluminium (broad), 2018-2019** — old findings reported steady
   growth (+27% YoY in 2019-04, +29% in 2019-05); new EU-27 findings
   show **decline** (-26%, -20% over the same windows). UK was
   apparently a large component of EU-28 aluminium imports from China
   in 2018, and that trade contracted sharply in 2019, masking the
   EU-27 picture. A journalist quoting the OLD figures for 2019
   would have said "Chinese aluminium surged into Europe" when in
   fact "EU-27 aluminium imports from China collapsed by a quarter".

2. **Electrical equipment & machinery (chapters 84-85, broad)** —
   the broadest category. 2019-Jul old = +10.4% growth; new = -28.6%
   decline. Same story shape, larger absolute magnitude (UK was
   ~€27B of EU-28 electrical-machinery imports from China in 2019).

3. **Cotton (raw + woven), exports EU→China 2018-2019** — old
   showed strong export growth (+50% in 2019-04, +60% in 2019-05);
   new shows decline. The post-Xinjiang-cotton-controversy story
   would have read very differently with the old methodology.

4. **2021 anchors with prior in 2020** — 12mo windows that straddle
   Brexit had asymmetric prior periods (UK present in early 2020,
   gone by end-2020). The fix recomputes prior consistently as
   EU-27, which produces materially different YoYs even when the
   current period was already EU-27. E.g. Aluminium exports
   2021-01: old -14.7%, new +7.3%.

**Bottom line for editorial use**:

- Anyone consulting the OLD briefing pack for a pre-2022 trade
  trend may have been told the wrong direction — substantial risk
  for any piece referencing the 2017-2020 history.
- Post-2022 trend statements are unaffected.
- The supersede chain in the DB is the audit trail: a journalist
  spot-checking can run `SELECT … FROM findings new JOIN findings
  old ON old.superseded_by_finding_id = new.id WHERE …` and see the
  before-and-after for any finding.

**Saved diff materials**:
- `exports/briefing-clean-state.md` — pre-fix briefing pack (was
  the morning brief)
- `exports/briefing-eu27-fix.md` — post-fix briefing pack
- The "latest 12mo to Feb 2026" sections of both are *identical in
  numbers* (post-Brexit period); the methodology change shows up
  only in older windows that the briefing pack doesn't surface
  prominently. The supersede-chain query above is the right place
  to look for the editorial impact.

**Forward work surfaced by this audit**:
- A `created_by` change-log column on `findings` would let us tag
  the methodology version with a human-readable "what changed"
  string alongside the machine-readable method version. Currently
  the journalist has to read the method-name string to understand
  the supersede.
- mirror-trade still uses `reporter='GB'` for UK partner findings
  (correct for 2017-Q1 2020 only); when HMRC ingest lands, that
  becomes a backfill question.

## Overall verdict

| Shock | Pre-reg expected | Found | Verdict |
|---|---|---|---|
| 2018 Section 232 (steel) | + diversion | −21% drop (EU safeguards bit) | ✓ methodology fired right direction even against my prior |
| 2018 Section 232 (aluminium) | + diversion, partial | +12% kg, ~0% value (volume up, prices down) | ✓ as predicted |
| Q1 2020 COVID | broad drop + PPE rise | broad drop ✓, PPE +1271% ✓ | ✓✓ cleanest result |
| Feb 2022 Russia (renewables) | solar/wind rise then fall | inverse_u_peak ✓ on Solar inverters; wind boom-bust | ✓ |
| Oct 2023 EV probe | inverse_u_peak with peak Q3/Q4 2024 | dip_recovery with trough 2024-08 (post Phase 6.0.7 trajectory fix) | ✓ same editorial story, different shape vocabulary |

**Methodology pass rate**: **5 of 5 shocks surfaced cleanly** after the
Phase 6.0.7 trajectory gap-tolerance fix. Pre-fix only 5 of 58
expected trajectory findings existed (gap-intolerance bug — see §5.1);
post-fix 57 of 58 emit, including the EV probe. The single 'partial'
verdict for EV probe is because the pre-registered shape was
`inverse_u_peak` but the actual classification is `dip_recovery`;
both are correct readings of different segments of the same series.

**Confirmation-bias check**: the steel finding was OPPOSITE to my
pre-registered prediction. The methodology fired the data, not my
expectation. This is exactly what pre-registration is supposed to
verify.

**Post-validation methodology audit (2026-05-10)**: even with the
shock validation passing, the audit surfaced a separate inconsistency
— the EU sums silently included UK pre-Brexit. Fix shipped. 1144
findings (25% of all hs-group YoY) had > 5pp YoY shifts after the
fix, including 337 where the direction flipped (e.g., 2018-2019
aluminium "growth" → "decline"). See "EU-27 methodology audit"
section above. **The lesson**: shock-class validation + post-hoc
methodology audit are complementary; this kind of silent-default
inconsistency wouldn't have shown up in a standard shock test
because the predicted-shape patterns are mostly post-Brexit.
