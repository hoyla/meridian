# GACC Chinese-language site as a data source — investigation (2026-07-14)

**Status: research note, no build.** Hands-on reconnaissance (principle 6) of
GACC's Chinese-language statistics pages, triggered by two things on the same
morning: the 中文-link 404 fix ([PR #145](https://github.com/hoyla/meridian/pull/145))
and Luke's observation that the Chinese site was already carrying **June 2026**
tables while the English site still topped out at May. Luke's framing for the
decision: *"we want to be sure that we're comparing apples with apples."*

Everything below was verified live on 2026-07-14 (UK time). Where a claim
rests on a fetched file, the file's URL + SHA-256 is in the provenance
appendix.

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
3. **The catch is discovery, not data.** The Chinese Express index lists
   *only the current drop* (no archive, no pagination); the site search has
   **no 2026 coverage** (apparently a CMS-migration casualty); Wayback is
   blocked by the WAF. Miss a drop window and the on-site recovery path is
   the monthly report ~5 weeks later — or the English site.
4. **Access is two-tier.** All `www.customs.gov.cn` *HTML* is behind a
   JS-challenge WAF (curl: 412; embedded/headless browsers and Wayback's
   crawler fail; real Chrome passes). The **spreadsheet attachments are not
   WAF-protected** — plain curl fetches them. A pipeline needs exactly one
   WAF-passing HTML fetch per drop to discover attachment URLs.
5. **Bonus discovery:** the 统计月报 (monthly report) is a *richer* product
   than anything we ingest today — 18 tables × CNY/USD including
   **partner × HS-chapter cross-tables** (the cross the preliminary release
   famously lacks), full country list (277 rows), single-month YoY columns —
   with per-year archive pages (2025–2026 at the current path), published
   ~18th of M+2, values identical to the preliminary (verified May-2026).

## Site taxonomy (Chinese ↔ English)

| Chinese product | Where | English equivalent | Cadence |
|---|---|---|---|
| 统计快讯 "Statistics Express" | category `…/302274/302275/index.html` — **current drop only**, ~13 articles/month | `english.customs.gov.cn/statics/report/preliminary.html` (the source we ingest) | ~7th–14th of M+1 |
| 统计月报 "Monthly report" | per-year pages `…/302274/302277/<YYYY>/index.html` (2025, 2026 exist; 2024 and earlier 404 at this path — pre-migration paths elsewhere) | `statics/report/monthly*.html` (we don't ingest) | ~18th of M+2 |
| 数据在线查询 | `stats.customs.gov.cn` query platform | — | interactive |

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
our preliminary-sourced values. So the first monthly-report vintage does
not revise the Express figures (for this month) — it re-publishes them at
higher precision (the file carries full decimals; unit 万元 not 亿元) with
~250 additional countries and continent groupings.

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
| 2026-06 (quarter-end) | **2026-07-14 ~09:28 Beijing (observed live)** | not posted as of ~17:00 UK same day | CN leads ≥ several hours |

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
looks like when you check between the two postings. A CN-first trigger buys
drop-day hours (wire-style value; the chat-notify ping lands earlier) and
resilience if the English translation ever stalls — it does not buy a
calendar lead.

## Discovery & recovery (the real weakness)

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
- Therefore: **a month that has rotated off the index but not yet into the
  search index (e.g. May 2026, today) is undiscoverable on-site** unless you
  already hold its article/attachment URL.

Pipeline implication: snapshot every drop when it happens (append-only, as
ever — principle 4); treat the English site as the recovery/backfill source
(it keeps its full per-year archive `preliminary<year>.html`, which is how
we hold 2018→present); the monthly report becomes a second recovery source
at M+2 (2025+ only at the current path).

## Access

| Surface | curl / headless | Real browser |
|---|---|---|
| `english.customs.gov.cn` (all pages + xls) | ✅ (today's pipeline) | ✅ |
| `www.customs.gov.cn` HTML (index, articles, search) | ❌ 412 JS-challenge WAF (curl, embedded browser, Wayback crawler all fail) | ✅ (Chrome passes; WAF appends a per-request query token) |
| `www.customs.gov.cn` attachments (`attachDir`/`fileDir` xls) | ✅ **plain curl, no challenge** (verified ×4 files) | ✅ |

So the *only* WAF-gated step for a CN-first pipeline is reading the Express
index (and per-article pages) to discover attachment URLs — roughly one
HTML fetch per drop. Build-phase options, untested here: Playwright/
headless-Chrome with stealth (the challenge is a JS cookie gate; whether it
passes headless needs an actual spike), a scheduled fetch using a real
browser profile, or falling back to English-index-triggered discovery with
CN attachments fetched for verification only. The WAF cookie's replayability
into curl was not testable in this session.

## If B is built — design sketch (not scheduled)

CN adapter as an *additional* source, never a replacement (defensibility:
the English pages remain what most Guardian readers/editors can eyeball):

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
   column on `releases`.
5. Cross-check assert per drop: CN-parsed values == EN-parsed values (we
   now know they should be exactly equal); any mismatch is an alert, not a
   silent preference.

Open pre-build questions: WAF headless feasibility (the one real unknown);
whether the June-2026 EN release, when it lands, matches the CN file already
in hand (pre-registered below — **run this diff first**); how often GACC
revises Express figures after the fact (the May-2026 月报 says "not at
M+2"; a year-end revision sweep is still possible and only observable over
time).

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
English June absence re-verified at ~17:00 UK via the preliminary index
(`Feb./Mar./Apr./May.` only). WAF behaviour: `curl` → HTTP 412 on all
`www.customs.gov.cn` HTML; the in-app embedded browser fails the challenge
(412 → 400); real Chrome passes and the origin appends a challenge token to
requests; Wayback captures of article pages are 3.7 KB challenge shells.
