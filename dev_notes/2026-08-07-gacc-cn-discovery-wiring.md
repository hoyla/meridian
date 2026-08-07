# GACC Chinese-side discovery: the routine wiring (2026-08-07)

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
