# GACC Chinese-side discovery: the routine wiring (2026-08-07)

> **⚠️ CORRECTED LATER THE SAME DAY — read the "Corrections" section at the
> foot before relying on anything below.** Two of this note's load-bearing
> claims did not survive contact with a second vantage point: (1) that
> `tjs.customs.gov.cn` is served outside the WAF, and (2) that the table
> bytes are unreachable. Both were wrong. The findings below are preserved
> as written (they were accurate from where they were made) rather than
> edited in place.

**Status: shipped.** Closes the gap the July investigation
(2026-07-14-gacc-chinese-source-investigation.md) left open: the routine's
GACC probe walked only the English `SEED_INDEXES`, so a Chinese-first drop
was invisible to it — `gacc_cn.py` could ingest a CN Express xls, but only
one you pointed it at by hand. Luke's ask (2026-08-07): *(a)* check whether
the Chinese release is public, *(b)* build the wiring to use it routinely
when it lands first.

All observations below were made live on **2026-08-07 — drop day for the
July 2026 reference month** (公告2025年第240号 schedules it for 7 Aug, and it
landed on schedule). Timestamps UK unless noted.

## (a) The July release: public on the Chinese site, English lagging again

- CN Express July tables published **10:30 Beijing (03:30 UK)** — the list
  page and every article carry the date; Xinhua ran the headline figures
  within the hour (前7个月 total ¥30.13tn, +17.3%; July exports +23.9% y/y
  in USD terms).
- English preliminary index checked **~15:15 UK, twelve hours later**: still
  tops out at June — every "Jul." cell a dead `<span>`, no release pages, and
  the formulaic English xls URL for July
  (`english.customs.gov.cn/Excel/1_7-en-人民币-2026-07_N21.xls`) returns an
  IIS 404 while June's serves bytes. So the June pattern (CN 02:28,
  EN ~18h later) repeated in July: **the lead is real and recurring, not a
  one-off**.

## The discovery landscape moved under us (in our favour)

The July note's central obstacle was the JS-challenge WAF (瑞数-style) on all
`www.customs.gov.cn` HTML. Two findings change that picture:

1. **GACC's Statistics Department now runs a subdomain,
   `tjs.customs.gov.cn`** (part of the late-2025 site redesign — template
   assets are stamped `header2025`/`footer2025`). Its Express index,
   `http://tjs.customs.gov.cn/tjs/sjgb/tjkx/index.html`, is served as
   **static HTML outside the WAF**: a plain GET (desktop UA, no cookies, no
   JS) returned the full page — all ten July tables with titles, publish
   dates and article links, plus **pagination: 239 pages / 2,381 articles**,
   an archive the old "current drop only" index never had. Article pages
   (`/tjs/YYYY-MM/DD/article_<id>.html`) are equally plain-fetchable.
   **Discovery is therefore ordinary code now — no WAF work at all.**
2. **But the redesigned articles no longer link the xls directly.** The old
   shape (one `attachDir`/`fileDir` xls per article, WAF-free — the shape
   `gacc_cn.py ingest` consumes) survives only on the `www` mirror of each
   article, whose HTML still serves the challenge shell. The tjs articles
   embed the table in a **WPS web-office viewer iframe**
   (`/wps/weboffice/office/s/<file_id>?...`) and the `/wps/` path *is*
   WAF-gated (challenge shell on the viewer URL and on the obvious
   `/download` guess). Headless rendering does not pass (re-confirmed via a
   commercial scraping API's browser drivers, which failed the challenge —
   consistent with the July finding that only real Chrome passes).

So as of today: **the "is it out?" signal is free; the bytes are not yet.**
Other byte routes checked and closed: the English xls does not upload before
the English page (July 404s on drop evening); the regional-branch mirrors
(gdfs.customs.gov.cn etc.) stopped mirroring statistics at the Dec-2025
redesign; www article/attachment ids share a to-the-second timestamp prefix
but differ in a 5-digit suffix (not guessable); Wayback still receives the
challenge.

## What shipped

- **`gacc_cn.py` discovery layer** (`CN_EXPRESS_INDEX_URL`,
  `discover_express_articles`, `discover_article_attachments`,
  `probe_cn_express`, CLI `discover` subcommand):
  - Index parsing keys on the CMS-dated article URL pattern
    (`/(customs|tjs)/YYYY-MM/DD/article_<id>.html`) plus the **same title
    regexes the xls parser trusts** (主要国别（地区）总值表 /
    出口·进口重点商品量值表, printed （N） never used) — so discovery and
    parse cannot drift apart on what counts as an ingestable table. Titles
    that don't classify (总值表 (1), trade-mode (2)/(3), the cumulative
    1至N月 variants, nav links that happen to match the URL pattern) are
    skipped; a template reskin degrades to an empty list, never a wrong
    parse.
  - Dedup falls out of the releases table: an article is only fetched while
    the DB lacks a release for its (section, period, currency) cell — the
    same natural key both sites share — so the day after the English walk
    lands, the CN probe is one index fetch and silence. No per-article
    bookkeeping, and the 2,381-article archive is never crawled.
  - Per-article: a direct xls link → the existing `ingest_cn_release` path
    (snapshot → parse → floor checks → release + observations — nothing
    about the ingest contract changed); no direct xls (the WPS shape) → the
    article is reported as **published-awaiting-bytes** and rechecked next
    walk. Combined Jan–Feb titles are skipped pre-fetch (the parser refuses
    them by design until a fixture exists).
  - `probe_cn_express` never raises: index unreachable → status
    `unavailable`, and the caller treats every CN outcome as additive.
- **`scrape.probe_source("gacc")`** runs the CN probe immediately before the
  English walk. Ingest counts flow into the existing new_data/held-back
  logic (a CN xls that fails the floor lands in `_failed_gacc_runs_since`
  and surfaces as `error`, same as an English one). The notes line now
  carries the CN state, e.g. on a drop morning:
  `no_change × due … ; CN Express for 2026-07 is published upstream (6
  table(s)) with no direct xls — bytes WAF-locked, awaiting English release`
  — which is the difference between "quiet day" and "English is the
  laggard", and stops a lagging translation reading as a slipped release as
  it drifts toward `overdue`.
- **Plumbing**: `api_client.fetch` accepts a `user_agent` override (CN
  surfaces get a desktop UA — the July recon and today's verification both
  used one; overridable via `GACC_CN_USER_AGENT`); `_ARTICLE_DATE_RE`
  accepts `/tjs/` paths; `from typing import Any` added to `anomalies.py`
  (latent NameError on Pythons < 3.14, where signature annotations evaluate
  eagerly — it made the module unimportable under 3.11).
- **Tests** (`tests/test_gacc_cn_discovery.py` + probe wiring cases in
  `test_probe_source.py`; fixtures are the real tjs index and article pages
  captured today): index classification incl. the negative cases, WPS vs
  direct-attachment articles, published-awaiting-bytes, no-new fetches only
  the index, direct-xls end-to-end ingest into the test DB with idempotent
  re-walk, unavailable-never-breaks-the-walk. Suite: 819 passed.

## What this does and doesn't buy

**Does:** same-morning, code-level detection that the Chinese release is out
(the breakfast-time signal), an honest probe log on drop day, automatic
ingest the moment any discovered article carries a direct xls again — GACC
reverting the redesign, the www mirror shape resurfacing on tjs, or any
future byte route that yields a URL — with zero further wiring.

**Doesn't (yet):** ingest the table values hours early while the only byte
surface is the WPS viewer. The remaining piece is one WAF-passing fetch per
drop (~6 files). Options, in rough order of appeal: a real-browser fetch
from a machine that passes the challenge (Luke's Chrome did in July —
`gacc_cn.py ingest --xls-url` already consumes the result; the routine's
published-awaiting-bytes note is the prompt to do it); a headless spike with
stealth hardening (July's open question, now lower-value since discovery no
longer needs it); or simply riding the English release, now with the lag
made visible instead of silent.

## Provenance (all fetched 2026-08-07, plain GET unless noted)

| Surface | URL | Observed |
|---|---|---|
| CN Express index (tjs) | `http://tjs.customs.gov.cn/tjs/sjgb/tjkx/index.html` | full static HTML, 10 July tables, `createDate 2026-08-07 10:38:59`, 第1/239页 总条数：2381 |
| CN article, s4 CNY Jul (tjs) | `http://tjs.customs.gov.cn/tjs/2026-08/07/article_2026080710320733327.html` | static HTML; 发布时间 2026-08-07 10:30; WPS iframe `_w_third_file_id=0bda337af3234ea6a35aa6535c0d4ea0`; no xls link |
| Same article, www mirror | `http://www.customs.gov.cn/customs/2026-08/07/article_2026080710320710188.html` | WAF challenge shell |
| WPS viewer (+ `/download` guess) | `http://tjs.customs.gov.cn/wps/weboffice/office/s/0bda337af…` | WAF challenge shell |
| EN preliminary index | `http://english.customs.gov.cn/statics/report/preliminary.html` | June is the last linked month on every row (~15:15 UK) |
| EN xls, formulaic URL | `…/Excel/1_7-en-人民币-2026-07_N21.xls` / `…-2026-06_N21.xls` | July → IIS 404; June → xls bytes (control) |
| gdfs regional mirror | `http://gdfs.customs.gov.cn/customs/302249/zfxxgk/2799825/302274/index.html` | static HTML but stale — newest statistics rows 2025-12 |

The tjs index and article fixtures in `tests/fixtures/` are trimmed copies
of the first two rows above.

---

## Corrections (2026-08-07, evening — same day, second vantage point)

Re-ran the shipped code from Luke's machine ~17:40 UK, ~7h after the recon
above. Two central claims failed immediately.

### 1. `tjs.customs.gov.cn` is NOT reliably outside the WAF

The claim above — "served as static HTML **outside** the WAF … discovery is
therefore ordinary code now, no WAF work at all" — did not reproduce.
`gacc_cn.py discover` returned **412 + the 瑞数 `$_ts` challenge shell**, and so
did every plain-HTTP shape tried: bare desktop UA, full browser header set
(Accept / Accept-Language / Accept-Encoding / Connection /
Upgrade-Insecure-Requests), and https (which refuses connections outright).

So WAF-free tjs is **a property of a vantage point, not of the host** — the
morning's clean 200s were presumably an IP or timing accident. Anything built
on "discovery is free" needs to treat blindness as the normal case, not the
exception.

Also newly measured: the in-app/headless browser is *hard-rejected* — it runs
the challenge, gets a cookie set, and the reload then returns **400 with an
empty body**. Real Chrome still passes first time, as in July. The July
finding ("only real Chrome passes") stands and now covers tjs too.

**Shipped in response:** `CnDiscoveryOutcome` gains a `challenged` status,
distinct from `unavailable`, detected via the `$_ts` bootstrap marker or a
bare 412 — including the dangerous **200-with-shell** shape, which parses to
zero articles and would otherwise read as a quiet `no_new` while a release is
live upstream. The routine note now reads `CN index behind the JS-challenge
WAF — CN discovery blind, English-only this run`.

### 2. The bytes are NOT unreachable — the WPS viewer gives them up

The claim above — redesigned tjs articles embed a WAF-gated WPS viewer, so
"the bytes are not [free] yet" — is true only of the article's **raw HTML**.
It is false once the viewer **renders**:

```
tjs article (real browser)
  └─ WPS iframe renders
       └─ iframe.contentDocument contains ONE 19-digit attachment id
            └─ http://www.customs.gov.cn/customs/attachDir/YYYY/MM/<id>.xls
                 └─ NOT WAF-gated: plain httpx → 200 application/vnd.ms-excel
```

The id is absent from the article's raw HTML (it arrives via the WPS API) and
is **self-describing**: its first six digits are the YYYYMM of the attachment
directory, so the URL needs no other input. Equivalently, the **`www` mirror
article** still carries the old direct `attachDir` link in its raw HTML, once
a real browser holds the www challenge cookie.

Verified for all six July tables:

| Table | Attachment id | Bytes |
|---|---|---|
| §4 by-country CNY | 2026080710320733553 | 200, 14,848 B |
| §4 by-country USD | 2026080710320760159 | 200, 15,360 B |
| §5 exports CNY | 2026080710320578150 | 200 |
| §5 exports USD | 2026080710320525758 | 200 |
| §6 imports CNY | 2026080710320686370 | 200 |
| §6 imports USD | 2026080710320646079 | 200 |

Note also that the `www` 海关统计 index
(`302249/zfxxgk/2799825/302274/index.html`) is **frozen at 2025-11** — the
articles publish but that index doesn't list them. Use tjs for discovery.

**Shipped in response:** `attachment_url_from_id()` (fails loud on anything
that isn't a 19-digit id, so a mistyped paste can't become a silent 404
mid-drop), `find_wps_file_id()` so the probe logs the viewer handle an
operator needs, and a **`gacc_cn.py bridge`** subcommand taking the harvested
ids and running them through the unchanged ingest contract:

```bash
gacc_cn.py bridge --attachment-id 2026080710320733553 --attachment-id …
```

This does not remove the browser step — full automation still waits on a
WAF-passing fetch. It narrows the manual part from "hunt for six
spreadsheets" to "copy six ids", and one bad id no longer aborts the rest.

### 3. Ordering: English first, Chinese as the fallback (Luke's call)

As shipped, the probe ran CN discovery **before** the English walk, on the
reasoning that CN publishes first. Luke's objection: if English is the
preferred vintage (translation risk), check English first and fall back to
Chinese only when it isn't there.

Correct, and for a stronger reason than ordering aesthetics. Both sites write
the same release rows (shared natural key) and English supersedes CN
provenance when it lands, so the **end state is identical either way** — this
was never a correctness bug. What differed is the audit trail: on any run
where both sites had the month (the normal case after any spell of CN
blindness), CN-first fetched and ingested six tables, wrote six snapshots,
then immediately flipped their `source_url`s to the English pages. An ingest
and a provenance churn against the preferred vintage that never needed to
happen.

English-first makes the CN probe **self-cancelling**: its dedup is against the
DB, so once the English walk has landed the month there is nothing missing and
CN costs one index fetch. The timeliness play is untouched — on drop morning
English genuinely doesn't have it, the English walk finds nothing, and CN
ingests exactly as before.

What we give up is the incidental both-sites cross-check on catch-up runs. The
normal timeline preserves it anyway (CN lands ~7th, the English walk
re-verifies those same rows ~15th — June 2026 gave 620/620 unchanged), and
`gacc_cn.py verify` does it deliberately when the diff is actually wanted.

**Also fixed while in there:** `cn_note` was computed *inside* the English
walk's `try`, so an exception from `run_scrape` discarded the CN note
entirely — losing the Chinese signal in precisely the run where the fallback
matters most. The CN probe now runs in its own block regardless of the
English walk's fate, and its note lands on the error row too.

### 4. `gacc_cn.py` never loaded `.env`

The documented drop-day manual bridge died on `KeyError: 'DATABASE_URL'` when
run standalone: only `scrape.py` calls `load_dotenv()` (at import), so the env
was present only when `gacc_cn` was imported *through* it. `main()` now loads
it.

## What actually happened to the July release

Ingested from the Chinese site 2026-08-07 ~17:50 UK via the route in §2 —
releases **1247–1252**, 620 observations (§4 180×2, §5 62×2, §6 68×2), re-run
idempotent (0 inserted / 180 unchanged). Structure and label sets identical to
June (30 partners, 31 export / 34 import commodities); units unchanged. Total
July exports USD 397,851.7mn independently corroborates Xinhua's +23.9% y/y.
The English site was still showing June.
