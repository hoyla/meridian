# GACC Chinese-language site as a data source — investigation (2026-07-14)

**Status: research note, no build.** Hands-on reconnaissance (principle 6) of
GACC's Chinese-language statistics pages, triggered by two things on the same
morning: the 中文-link 404 fix ([PR #145](https://github.com/hoyla/meridian/pull/145))
and Luke's observation that the Chinese site was already carrying **June 2026**
tables while the English site still topped out at May. Luke's framing for the
decision: *"we want to be sure that we're comparing apples with apples."*

*Revised twice the same evening after Luke's challenges. First: the 统计月报
cadence was mis-stated as ~18th of M+2 — it is the **18th of M+1** (verified
for all 12 months of 2025), so the discovery gap was overstated. Second, and
more fundamental: the 18th product is not a re-publication of the preliminary —
it is the **verified vintage**. GACC's own Release Calendar (English site,
§below) draws the line: Preliminary Release on the 7th–14th, Monthly Bulletin
on the 18th, with note 2 stating that *"latest monthly statistics may differ
from those previously released because of verification after dissemination and
the verification continued till the yearbook publication."* Every claim below
about using the 月报 as an archive is therefore vintage-qualified.*

Everything below was verified live on 2026-07-14 (UK time). Where a claim
rests on a fetched file, the file's URL + SHA-256 is in the provenance
appendix.

**Decision (Luke, 2026-07-14 evening):** the observed lead — Chinese tables up
02:28 UK, English still absent at 16:50 UK, 14+ hours later — is editorially
material ("12 hours is a long time in the news media"), and with values proven
identical there is *"a very strong argument to use the Chinese."* Next step is
the build spike (WAF headless feasibility), not more research.

## TL;DR

1. **The numbers are the same.** Every value compared between the Chinese
   spreadsheets and our English-parsed DB matched **exactly** — 30/30
   partner rows × 6 values for the May-2025 Express by-country table, 26/26
   partners × 6 values between the May-2026 monthly report and our
   preliminary-sourced data, and spot-checked commodity rows. Same catalogue,
   same aggregates, same footnotes (EU-ex-UK from Feb 2020). The two sites
   publish one dataset; the English pages are a (partial) translation layer.
2. **The Chinese site leads by hours-to-a-day, not a month.** The "June on
   Chinese, May on English" gap observed this morning is mid-drop timing:
   Chinese June tables went up ~09:28 Beijing on 14 Jul; English hadn't
   posted by late afternoon UK the same day. Historical page-date evidence
   says 0–1 day; it cannot be made rigorous retrospectively (see §Timing).
3. **Two products, two vintages — GACC's own calendar draws the line.**
   Preliminary Release on the 7th–9th of M+1 (12th–14th after quarter-end
   months), Monthly Bulletin — the **verified vintage** — always on the
   **18th**, Online Query on the 20th, with verification continuing until
   the Yearbook (calendar note 2). So the preliminary leads the verified
   product by ~9–11 days in normal months, ~4–5 at quarter-ends. The
   timeliness play is preliminary-vs-preliminary only; the 26/26 equality
   below is one no-revision month, not a guarantee.
4. **Discovery of the *preliminary* is the fragile part.** The Chinese
   Express index lists *only the current drop* (no archive, no pagination);
   the site search has **no 2026 coverage** (apparently a CMS-migration
   casualty); Wayback is blocked by the WAF. The 月报 year pages are a
   complete permanent archive from the 18th — but of the **verified**
   vintage, so recovering a missed preliminary drop from them substitutes a
   different vintage (identical in practice for the month tested; GACC
   explicitly reserves the difference). The only true *preliminary* archive
   is the English site's per-year `preliminary<year>.html` pages — which we
   already hold, 2018→present.
5. **Access is two-tier.** All `www.customs.gov.cn` *HTML* is behind a
   JS-challenge WAF (curl: 412; embedded/headless browsers and Wayback's
   crawler fail; real Chrome passes) — this includes the 月报 year pages.
   The **spreadsheet attachments are not WAF-protected** — plain curl
   fetches them. A pipeline needs exactly one WAF-passing HTML fetch per
   drop to discover attachment URLs — and only for the CN preliminary
   index; every other surface has a WAF-free route.
6. **Bonus discovery:** the Monthly Bulletin is a *richer* product than
   anything we ingest today — ~19 tables × CNY/USD including
   **partner × HS-division cross-tables** ((15)/(16), the cross the
   preliminary famously lacks), full country list (277 rows), single-month
   YoY columns — and it exists **on the English site too**
   (`statics/report/monthly.html`: same 19-table matrix, Jan–May 2026
   linked, WAF-free). Verified figures for month M on the 18th of M+1 put
   China-side partner × chapter detail roughly **four weeks ahead of
   Eurostat's coverage of the same month**, with no WAF work needed.

## Site taxonomy (Chinese ↔ English)

| Chinese product | Where | English equivalent | Cadence |
|---|---|---|---|
| 统计快讯 "Statistics Express" | category `…/302274/302275/index.html` — **current drop only**, ~13 articles/month | `english.customs.gov.cn/statics/report/preliminary.html` (the source we ingest) | ~7th–14th of M+1 |
| 统计月报 "Monthly report" — **the verified vintage** | per-year pages `…/302274/302277/<YYYY>/index.html` (2025, 2026 exist; 2024 and earlier 404 at this path — pre-migration paths elsewhere) | **"Monthly Bulletin"** `statics/report/monthly.html` — same 19-table matrix, Jan–May 2026 linked, **WAF-free** | **18th of M+1** by official calendar (2025 year page confirms: all 12 months on the 18th; Jan–Feb combined → 18 Mar) |
| 数据在线查询 | `stats.customs.gov.cn` query platform | "Interactive Tables" | opens 20th of M+1 |

### The official Release Calendar (Luke's pointer — the authoritative cadence)

`english.customs.gov.cn/statistics/Statistics?ColumnId=4` publishes GACC's
advance release calendars (2023, 2024, 2025 up now; each a Statics page).
The 2025 calendar:

| Release in | Preliminary | Monthly Bulletin | Online Query |
|---|---|---|---|
| Jan | 13th | 18th | 20th |
| Feb | — | — | — |
| Mar (Jan+Feb combined) | 7th | 18th | 20th |
| Apr | 14th | 18th | 20th |
| May | 9th | 18th | 20th |
| Jun | 9th | 18th | 20th |
| Jul | 14th | 18th | 20th |
| Aug | 7th | 18th | 20th |
| Sep | 8th | 18th | 20th (+ Yearbook 30th) |
| Oct | 13th | 18th | 20th |
| Nov | 7th | 18th | 20th |
| Dec | 8th | 18th | 20th |

2024 is the same shape (prelim 7th–14th, Bulletin always 18th). The
calendar's notes, verbatim where it matters: *(2)* "Latest monthly
statistics may differ from those previously released because of
**verification after dissemination** and the verification continued till
the yearbook publication." *(4)* Jan+Feb combined: preliminary Mar 7th,
Bulletin Mar 18th, Online Query Mar 20th.

Three consequences. **First**, the Monthly Bulletin is officially the
verified vintage — prelim↔Bulletin equality (observed May-2026) is an
empirical regularity, never a guarantee; the two must not be mixed without
labelling. **Second**, the mid-month preliminary dates after quarter-ends
(13th/14th for Dec/Mar/Jun/Sep data) are *scheduled*, corroborating the
empirical `month_lag_days` override from PR #139 — the pipeline could
ingest this calendar and drive `release_calendar.py` from GACC's own
schedule instead of inference. **Third**, today (14 Jul) is the *scheduled*
preliminary date for June — the Chinese side posted on schedule at 09:28
Beijing; the English translation is the laggard.

### The 2026 calendar exists only in Chinese — and calendars are not advance-published

Luke's follow-up challenge ("the most recent calendar I can see on the
English site is 2025") exposed two more facts:

- **Publication timing:** calendars appear **January–March of the year
  they cover**, not ahead of it. The CN SDDS category (数据公布时间表,
  `…/302274/302278/302279/index.html`) holds 2015–2023 with path dates
  telling the story: 2023's published 2023-03-18, 2022's 2022-03-13,
  2021's 2021-01-07, 2020's 2020-01-23. The **2026 schedule** was
  published as **公告2025年第240号** ("on the publication times of China
  customs statistics for 2026") on **2026-03-11**, in the 统计制度
  category (`…/302274/tjzd/index.html`), article
  `/customs/2026-03/11/article_2026031116150585435.html`. So each
  January–February runs on *no* published current-year schedule — the
  empirical `month_lag_days` override stays as the fallback even if the
  calendar is ingested.
- **No English 2026 edition** as of 14 Jul 2026 — the English Release
  Calendar page stops at 2025. The 2026 schedule (快讯/月刊/在线查询):
  Jan 14/18/20, Feb —, Mar 10/18/20, Apr 14/18/20, May 9/18/20,
  Jun 9/18/20, **Jul 14/18/20**, Aug 7/18/20, Sep 8/18/20 (+年鉴 30th),
  Oct 14/18/20, Nov 10/18/20, Dec 8/18/20. Its notes 二/三 state the
  vintage rule in plainer terms than the English 2025 edition: the 快讯 is
  月度**初步**汇总数据 (preliminary aggregation), superseded by the
  corrected 月度**正式**数据 (official monthly data) on which the 月刊 is
  built — 以最新公布数据为准 (the latest published figure prevails).

Same 统计制度 category bonus: the official **2026 commodity catalogues**
for both products (海关统计快讯进口/出口重点商品目录（2026年）,
海关统计月报进口/出口主要商品目录（2026年）) — the canonical anchor for
the commodity-highlights label-drift gotchas (Machine-tools-2021 etc.).

Express article pages carry **no HTML table** — one `.xls` attachment each
(this is true of the 2025 articles too, despite the search index suggesting
otherwise; the search indexes the attachment *content*). Article URLs are
CMS-dated (`/customs/<yyyy-mm>/<dd>/article_<id>.html`) — the path date is
the publish date; the long id embeds a timestamp. Attachments live under
`/customs/attachDir/…` or `/customs/fileDir/…` with unguessable ids.

### Table numbering drifts; keyword/sheet-code matching is mandatory

The by-country Express table was titled `（6）` through 2025 and `（4）` in
June 2026; combined Jan–Feb releases renumber to `（5）`; suffixes drift
（人民币）↔（人民币值）. **Do not key on the printed table number.** Two
stable alternatives, verified:

- **Chinese xls sheet names carry stable internal codes**: `M139RMBADD`/
  `M139USDADD` = Express by-country (2025 and 2026 files both), `M102RMB` =
  monthly-report by-country, `M113RMB` = monthly-report export commodities.
  (English xls sheet names are just `Sheet1` — the M-codes are CN-side only.)
- Title keywords: 主要国别（地区）总值表 = Express by-country;
  国别（地区）总值表 *without* 主要 = monthly-report all-country;
  出口/进口重点商品量值表 = Express commodities; 出口/进口主要商品量值表 =
  monthly-report commodities.

## Values: apples-to-apples verification

All comparisons are against `observations` rows parsed from the English
preliminary pages (release `section_number` 4/5; CNY basis).

**Express by-country, May 2025** (`cn_may25_s4_cny.xls`, published
2025-06-09): all **30 rows matched our DB by exact 6-value tuple**
(total/export/import × month/ytd, at our stored 0.1 亿元 rounding) — with
zero label mapping needed; the match itself derived the zh↔en dictionary
(总值=Total, 欧洲联盟=European Union, 其中：德国=of which: Germany, …,
区域全面经济伙伴关系协定=RCEP, 共建一带一路国家=B&R aggregate).

**Monthly report vs preliminary, May 2026** (`cn_may26_monthly_s2_cny.xls`,
published 2026-06-18): 26/26 mapped partners × 6 values **exact** against
our preliminary-sourced values — at our stored 0.1 亿元 rounding. So for
this month, verification produced no visible revision, and the Bulletin
re-publishes the figures at higher precision (full decimals; unit 万元 not
亿元) with ~250 additional countries and continent groupings. **Vintage
caveat** (the Release Calendar's note 2, and Luke's catch): the Bulletin is
officially the *verified* vintage, revisable until the Yearbook — this
one-month equality shows revisions are typically nil/below-rounding, not
that they never happen. Prelim and Bulletin figures must carry distinct
vintage labels if both are ever ingested.

**Monthly-report export commodities, May 2026**
(`cn_may26_monthly_s13_cny.xls`) vs our section-5 rows: spot rows exact —
农产品 607.0 / 3,007.9 亿元 (DB: 607.0 / 3007.9), 水产品 119.4 / 554.4
(DB: same; quantity 39 vs 39.1 is display rounding in the 月报 face). Note
the monthly report's commodity table is ~220 rows with nested sub-lines vs
the Express's ~30-line catalogue, and adds **single-month YoY** columns the
Express doesn't print.

**Unit traps for any future parser**: Express by-country = 亿元 / the
monthly report = 万元 (10,000×); monthly-report cells are comma-formatted
*strings* (`'6,070,062'`) where Express cells are floats; files are
Excel-97 `.xls`, code page 936 (GBK) — `xlrd` handles both.

## Timing: does Chinese lead English?

Evidence assembled (page dates are CMS stamps, so treat with care):

| Ref month | CN Express (URL path date) | EN preliminary (`atcl-date` printed) | Read |
|---|---|---|---|
| 2025-05 | 2025-06-09 | 2025-06-10 | CN +1 day |
| 2025-06 (quarter-end) | 2025-07-14 | 2025-07-14 | same day |
| 2025-09 (quarter-end) | 2025-10-13 | **2025-10-08 (!)** | printed date not credible — the Q3 presser was 13 Oct; the English `atcl-date` can predate actual posting |
| 2026-06 (quarter-end) | **2026-07-14 09:28 Beijing = 02:28 UK (observed live)** | still not posted at 16:50 UK same day | CN leads ≥ **14 hours** and counting |

The 2025-09 row is the methodological caveat: our `releases.publication_date`
comes from the English page's printed `atcl-date` div (parse.py:269), which
is evidently a CMS stamp that can *predate* availability. **Printed dates
cannot settle the lead question; only prospective observation can** — i.e.
poll both indexes around drop windows and log first-seen timestamps. The
existing routine already polls the English index daily; adding a CN-index
poll (needs the WAF-passing fetch, below) would measure the true lead within
a couple of cycles.

Working conclusion: CN leads by **hours (drop-day) to one day**, not weeks.
The month-wide gap that prompted this investigation was simply what drop-day
looks like when you check between the two postings. But drop-day hours are
not nothing: today's observed lead passed 14 hours with English still
absent, and (Luke) *"12 hours is a long time in the news media"* — on the
days this tool exists for, a CN-first trigger moves the chat-notify ping
from evening (or the next morning) to breakfast time. Plus resilience if
the English translation ever stalls outright.

## Discovery & recovery (narrower than it first looked)

The Express surface alone is weak on history:

- Express index = **current drop only**. No pagination, no year pages
  (`302275/2025/index.html` → 404), no archive.
- Site search (`search.customs.gov.cn`, POST `keyWords`) reaches Express
  articles **through 2025 but nothing from 2026** — consistent with the
  visible CMS migration (2025 articles' ids were re-stamped
  `article_20260122…` while keeping original path dates). Search indexes
  attachment content (values searchable), results link via a redirect
  portlet.
- Wayback: essentially useless here — its crawler receives the WAF
  challenge (captures are 412 shells).

*But* — as Luke pointed out, collapsing the first version of this section —
the **月报 year pages are a real archive**: every month of 2025 and 2026
(to date) is permanently listed at `…/302274/302277/<YYYY>/index.html`,
published on the 18th of M+1. The second Luke-correction qualifies what
that archive *is*: the **verified vintage**, not a copy of the
preliminary. Recovering a missed preliminary drop from it substitutes
verified figures for preliminary ones — identical for the month tested,
explicitly revisable per the calendar's note 2.

The genuine exposure for the *preliminary* series is therefore:

- a missed Express drop is recoverable **as-preliminary only from the
  English site** (per-year `preliminary<year>.html` archive, WAF-free —
  we already hold 2018→present from it), whenever the English translation
  has posted;
- the 月报 (18th, either language) is the fallback if the English
  preliminary itself never materialises — ingested as its own vintage,
  never silently substituted;
- **January** is the outlier month in any recovery plan (combined Jan–Feb
  cycle: prelim 7 Mar, Bulletin 18 Mar).

Pipeline implication: snapshot every drop when it happens (append-only, as
ever — principle 4); the English preliminary archive remains the canonical
same-vintage recovery; the Bulletin (CN or EN — the English one is
WAF-free) is the vintage-labelled backstop and the richer product.

## Access

| Surface | curl / headless | Real browser |
|---|---|---|
| `english.customs.gov.cn` (all pages + xls, incl. **Monthly Bulletin** and the **Release Calendar**) | ✅ (today's pipeline) | ✅ |
| `www.customs.gov.cn` HTML (index, articles, search) | ❌ 412 JS-challenge WAF (curl, embedded browser, Wayback crawler all fail) | ✅ (Chrome passes; WAF appends a per-request query token) |
| `www.customs.gov.cn` attachments (`attachDir`/`fileDir` xls) | ✅ **plain curl, no challenge** (verified ×4 files) | ✅ |

So the *only* WAF-gated step — for the whole programme — is reading the CN
Express index (and per-article pages) to discover attachment URLs on drop
day: roughly one HTML fetch per drop, in service of the hours-level lead.
Everything else (English preliminary, English Monthly Bulletin, the Release
Calendar, all CN attachments once discovered) is WAF-free. Build-phase
options, untested here: Playwright/headless-Chrome with stealth (the
challenge is a JS cookie gate; whether it passes headless needs an actual
spike), a scheduled fetch using a real browser profile, or falling back to
English-index-triggered discovery with CN attachments fetched for
verification only. The WAF cookie's replayability into curl was not
testable in this session.

## Build sketch (next step: the WAF spike)

CN becomes the *primary trigger* (per the decision above), with the English
site retained as the verification layer and blind-window/pre-2025 fallback —
defensibility: the English pages remain what most Guardian readers/editors
can eyeball, so every CN-sourced figure should stay traceable to an English
page once it exists:

1. Poll the Express index (WAF-passing fetch) alongside the existing English
   poll; log first-seen for both (settles the lead question with data).
2. On a new drop: snapshot article HTML + xls bytes (append-only,
   `source_snapshots` pattern), parse xls via xlrd (GBK; unit map per table
   kind; comma-string numerics for 月报 tables).
3. Key tables by title keywords + sheet M-code, **never** the printed
   number; map partners via the zh↔en dictionary derived above (stored, not
   inferred at runtime — principle 3: store alongside, never overwrite).
4. Natural keys stay currency-agnostic per period/section-kind as today, so
   an early CN ingest and the later EN ingest of the same release supersede
   cleanly rather than double-count; add a `language`/`site` provenance
   column on `releases` — and, if the Bulletin is ever ingested, a
   **vintage** discriminator (`preliminary` vs `monthly_bulletin`): the two
   are different GACC products by the official calendar, never
   interchangeable silently.
5. Cross-check assert per drop: CN-parsed values == EN-parsed values for
   the *same product* (we now know they should be exactly equal); any
   mismatch is an alert, not a silent preference.
6. Ingest the official Release Calendar (English site, WAF-free) to drive
   `release_calendar.py` expectations from GACC's own schedule rather than
   the empirical `month_lag_days` override (PR #139) — which the calendar
   independently corroborates.

Two build tracks fall out, separable:

- **Track 1 — the hours (needs the WAF spike):** CN Express index poll →
  earlier drop-day trigger + chat ping. Value: the observed 14-hour lead.
- **Track 2 — the crosses (no WAF at all):** ingest the Monthly Bulletin
  from the *English* site — (2) all-country, (15)/(16) partner × HS-division
  — verified figures at the 18th, ~4 weeks ahead of Eurostat's same-month
  coverage, as a new vintage-labelled source. This is the bigger analytical
  prize and is pure known-technology work (same fetch path as today's
  pipeline, xls parsing as scoped here).

Open pre-build questions: WAF headless feasibility (Track 1's one real
unknown); whether the June-2026 EN preliminary, when it lands, matches the
CN file already in hand (pre-registered below — **run this diff first**);
how often verification actually moves published figures between the
preliminary and the Bulletin/Yearbook (observable over time once both
vintages are ingested).

## Pre-registered diff: June 2026 (run when the English release lands)

CN Express by-country CNY, fetched 2026-07-14, before any English June
posting existed. When English June arrives, parse as usual and diff against
this table; every value should match at our rounding. Headline rows
(亿元, month / Jan–Jun cum / cum-YoY%):

- 总值 Total: 47,822.92 / 254,686.45 / +16.9 (exp +13.4, imp +22.1)
- 欧洲联盟 EU: 5,726.49 / 31,053.94 / +10.2 (exp +12.7, imp +4.9)
- 美国 US: 3,972.54 / 20,036.86 / −3.6
- 东南亚国家联盟 ASEAN: 8,173.94 / 43,380.40 / +18.2
- 中国香港 HK: 3,438.44 / 17,315.09 / +50.6 (imports +167.1 — expect the
  low-base/extreme-swing guards to engage)

Full 30-row CSV:

```csv
label_zh,total_jun,total_jan_jun,export_jun,export_jan_jun,import_jun,import_jan_jun,yoy_cum_total_pct,yoy_cum_export_pct,yoy_cum_import_pct
"总值",47822.9181152,254686.45189996,28206.68965572,147314.43286081,19616.22845948,107372.01903915,16.9,13.4,22.1
"欧洲联盟",5726.49047623,31053.93912485,3987.6076804,21651.61013382,1738.88279583,9402.32899103,10.2,12.7,4.9
"其中：德国",1434.30779831,7810.49509113,876.47916676,4678.16363336,557.82863155,3132.33145777,7.6,14.8,-1.7
"荷兰",766.03732016,4033.74734223,643.79432948,3461.18303154,122.24299068,572.56431069,5.8,6.8,-0.1
"法国",585.62192856,3170.92043954,301.93150157,1777.41020851,283.69042699,1393.51023103,12.7,8.2,19.0
"意大利",539.82604501,2973.78234865,359.75783161,2012.28166965,180.0682134,961.500679,16.4,18.1,13.1
"美国",3972.53953181,20036.86494201,2973.70869887,14966.19785527,998.83083294,5070.66708674,-3.6,-3.3,-4.4
"东南亚国家联盟",8173.94057449,43380.39948287,5357.70192414,27450.62442154,2816.23865035,15929.77506133,18.2,18.5,17.6
"其中：越南",2413.13797891,12333.35494128,1553.13338316,8111.02352055,860.00459575,4222.33142073,25.9,21.5,35.4
"马来西亚",1309.30058254,7209.79826482,925.95913011,4560.87505539,383.34145243,2648.92320943,-1.7,22.7,-26.8
"泰国",1220.00588953,6590.43627937,832.78797422,4550.33772865,387.21791531,2040.09855072,20.7,25.3,11.4
"新加坡",1004.07216962,4941.69224153,705.53220972,3223.65302813,298.5399599,1718.0392134,19.5,9.1,45.6
"印度尼西亚",1223.59173072,7020.95054136,655.57862987,3313.12731177,568.01310085,3707.82322959,27.8,13.7,43.7
"菲律宾",621.59265926,3082.89179212,436.77125007,2287.49111527,184.82140919,795.40067685,17.1,13.5,28.6
"日本",2218.60262831,12419.66848936,982.24201844,5769.9951249,1236.36060987,6649.67336446,13.8,3.4,24.6
"中国香港",3438.43926298,17315.09158091,3039.43275598,15344.26209744,399.006507,1970.82948347,50.6,42.6,167.1
"韩国",3092.17518129,16015.98081291,1195.5597094,6423.02902917,1896.61547189,9592.95178374,42.3,26.2,55.5
"中国台湾",2340.056743,12821.25272538,684.98712008,3594.27068054,1655.06962292,9226.98204484,20.8,28.1,18.2
"澳大利亚",1758.97891186,9118.6420687,540.02438267,2910.23887687,1218.95452919,6208.40319183,32.0,17.9,39.8
"俄罗斯",1663.22725062,9289.03925177,780.49240138,4194.07472446,882.73484924,5094.96452731,21.0,23.7,18.8
"印度",1217.458815,6359.35213421,1067.31183139,5505.4128896,150.14698361,853.93924461,19.3,17.5,32.4
"英国",726.64026631,3799.80326119,607.82118611,3173.2384285,118.8190802,626.56483269,7.0,9.4,-3.8
"加拿大",734.36419928,3704.60074412,360.07270335,1832.80777485,374.29149593,1871.79296927,11.7,5.4,18.7
"新西兰",156.13519694,811.35460737,55.5330717,299.46209498,100.60212524,511.89251239,4.0,13.7,-1.0
"拉丁美洲",4053.77173981,21303.84591714,2180.58929644,11053.83314565,1873.18244337,10250.01277149,16.2,8.9,25.2
"其中：巴西",1522.92704572,7400.50963579,537.74832661,2905.67102923,985.17871911,4494.83860656,23.7,18.7,27.2
"非洲",2703.93868433,14111.4092015,1716.74207547,9019.69341086,987.19660886,5091.71579064,19.6,21.8,15.9
"其中：南非",415.64598112,2187.53055749,173.72570834,942.16715293,241.92027278,1245.36340456,20.8,26.0,17.2
"区域全面经济伙伴关系协定（RCEP）成员国",15397.78914844,81735.69940498,8129.01907289,42843.10598156,7268.77007555,38892.59342342,22.8,17.2,29.6
"共建“一带一路”国家和地区",23936.02632767,129709.64933804,14244.06552463,74606.38475037,9691.96080304,55103.26458767,14.8,13.8,16.3
```

## Provenance

All fetched 2026-07-14 (UK), plain curl with a desktop UA unless noted.

| File | Source URL | SHA-256 |
|---|---|---|
| CN Express by-country CNY, **Jun 2026** (article `/customs/2026-07/14/article_2026071409284366427.html`) | `http://www.customs.gov.cn/customs/attachDir/2026/07/2026071409284388537.xls` | `d075853caa8819d004ce9a1bd5af579529c9ebbb7d6e1afea02e5a0537ef4fbe` |
| CN Express by-country CNY, **May 2025** (article `/customs/2025-06/09/article_2026012219104957632.html`) | `http://www.customs.gov.cn/customs/fileDir/resource/cms/2025/06/2025060910341867284.xls` | `42bdafb9eb5f983a5deefd098ef98f3c04b6a03fc278251fdcb32a49c5c5e99a` |
| CN monthly-report by-country CNY, **May 2026** (article `/customs/2026-06/18/article_2026061809490493199.html`) | `http://www.customs.gov.cn/customs/attachDir/2026/06/2026061809490453790.xls` | `655a963eb355b6b2c50eb5524cb0791012df140cc141ee0ac6b1b14fd5f7528f` |
| CN monthly-report export commodities CNY, **May 2026** (article `/customs/2026-06/18/article_2026061809491316541.html`) | `http://www.customs.gov.cn/customs/attachDir/2026/06/2026061809491325949.xls` | `99f34fff18f6f31d95e2f715dd6b746cd4594bd68303defbce19144f1a30a9cc` |
| EN preliminary by-country CNY xls, **May 2026** (structure comparison) | `http://english.customs.gov.cn/Excel/1_7-en-人民币-2026-05_N21.xls` | `8999fd4e624110026aa8f272de869f57e5a8eb1c8f96a052c1d507b1154e42c4` |

DB comparisons ran against `observations` joined to `releases`
(`source='gacc'`, CNY, section 4/5) on the live DB. The June-2025 Express
article URL (for the timing table): `/customs/2025-07/14/article_2026012219105085809.html`;
September-2025: `/customs/2025-10/13/article_2026012219105635275.html`.
English June absence re-verified at 16:50 UK via the preliminary index
(`Feb./Mar./Apr./May.` only — 14+ hours after the CN posting). WAF
behaviour: `curl` → HTTP 412 on all `www.customs.gov.cn` HTML; the in-app
embedded browser fails the challenge (412 → 400); real Chrome passes and
the origin appends a challenge token to requests; Wayback captures of
article pages are 3.7 KB challenge shells.

月报 cadence verification (the same-evening correction): the 2025 year page
`…/302274/302277/2025/index.html` lists all 38 table rows × all 12 months,
every article path dated the **18th of M+1** without exception (1月/2月 both
`2025-03/18`; 3月 `2025-04/18` … 11月 `2025-12/18`; 12月 `2026-01/18`). The
2026 page shows the same pattern through May (`2026-06/18`).

Release Calendar (the second same-evening correction, Luke's pointer):
section page `http://english.customs.gov.cn/statistics/Statistics?ColumnId=4`;
2025 calendar `…/Statics/fc662cee-21c3-474e-a7fb-4768bb1e295a.html`; 2024
`…/Statics/a017721e-39be-4e2d-995f-ab5597d85b86.html`; 2023
`…/Statics/3b5c2f31-daf5-488d-89f9-33b507ed0813.html` — all WAF-free via
curl. English Monthly Bulletin index:
`http://english.customs.gov.cn/statics/report/monthly.html` (19-table ×
12-month matrix, unquoted-href anchors — note for any parser — with
Jan–May 2026 linked as of 14 Jul 2026).
