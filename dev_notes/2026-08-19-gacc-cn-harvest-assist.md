# GACC Chinese-side bytes: the WAF spike resolved, and the Tier-1 harvest assist (2026-08-19)

**Status: shipped (Tier 1).** Closes the open question the two prior notes left
hanging — *can we get the CN Express spreadsheet bytes without a human hunting
for six attachment ids?* — by settling, empirically and against the live site,
which clients pass GACC's 瑞数 (Ruishu) JS-challenge WAF and where the 19-digit
attachment id actually comes from. See
[`2026-07-14-gacc-chinese-source-investigation.md`](2026-07-14-gacc-chinese-source-investigation.md)
and [`2026-08-07-gacc-cn-discovery-wiring.md`](2026-08-07-gacc-cn-discovery-wiring.md)
for the run-up; this note supersedes their open "WAF spike" item.

All observations below are **live, 2026-08-19**, made by driving the real
Chrome the extension exposes (claude-in-chrome, "Work Chrome") and, as a
control, the in-app/headless browser. The July 2026 reference month is still
the newest CN Express drop (August ref month is due ~7 Sep), and its articles
are still served, so the spike ran on real current pages.

## The WAF question, answered

| Client | Index / article HTML | Verdict |
|---|---|---|
| **Real Chrome, driven by claude-in-chrome** | `412` → challenge JS runs → reload `200`, full DOM | **passes** |
| In-app / headless browser | `412` → challenge JS runs → reload `400`, empty `<body>` (39-char shell) | hard-rejected |
| `curl` / httpx (any header dressing) | `412` + `$_ts` challenge shell | rejected |

So the July note's "only real Chrome passes" now covers the tjs subdomain too,
**and covers real Chrome under *automation*** — the pivotal unknown. The
challenge is not defeated or spoofed; a real browser simply executes the gate
JS the WAF is designed to admit. Driving it changes nothing the WAF can see.

### Why there is no pure-httpx shortcut (Options C and D are dead)

The 19-digit attachment id — the one thing needed to build the WAF-free xls
URL — is **not** in the article's raw HTML. It arrives when the WPS web-office
viewer renders, via its backend calls:

```
POST http://tjs.customs.gov.cn/wps/weboffice?8h6a7FPl=aqDgnbqAqWof…
POST http://tjs.customs.gov.cn/wps/weboffice?8h6a7FPl=aoaO9baAqWof…   ← different token
POST http://tjs.customs.gov.cn/wps/weboffice?8h6a7FPl=anVgaCAAqWof…   ← different again
```

Every one carries a **distinct `8h6a7FPl` value** — the 瑞数 per-request
dynamic token, a signed nonce the WAF's JS computes fresh for each request from
a session secret. You cannot forge it from httpx (**C**) or replay a static
cookie into it (**D**): the id-bearing API is behind the same token wall as the
HTML. The browser step is therefore irreducible. What is *not* behind the wall
is the byte route:

```
attachDir xls  →  http://www.customs.gov.cn/customs/attachDir/2026/08/2026080710320733553.xls
                  curl, desktop UA, no cookie, no token  →  200 application/vnd.ms-excel, 14,848 B
```

Clean split: **id = behind the WAF (needs real Chrome); bytes = WAF-free (plain
httpx)**. That is exactly the half we already had, and it is why Tier 1 is a
browser-assisted *id harvest*, not a scraper.

## The proven end-to-end loop

Every leg verified live today:

1. **Index → worklist.** Real Chrome renders `…/tjs/sjgb/tjkx/index.html`
   (title "统计快讯", 142,917-char DOM); all article links extract, and the
   canonical classifier keeps the 6 ingestable tables (§4/5/6 × CNY/USD) and
   drops the （2）/（3） trade-mode + cumulative variants and the 微博/微信/hotline
   nav links.
2. **Article → id.** Real Chrome renders an article
   (`…/article_2026080710320733327.html`, `200`, WPS iframe `200`); after the
   viewer paints (~3–7 s), the same-origin `iframe.contentDocument.title` is the
   19-digit id `2026080710320733553`.
3. **id → bytes.** `attachment_url_from_id` builds the attachDir URL; plain curl
   returns the xls (`200`, 14,848 B — matches the 7 Aug capture exactly).
4. **bytes → DB.** The unchanged `gacc_cn.py bridge` ingest contract (proven on
   the 7 Aug live ingest, releases 1247–1252).

## What shipped in this PR

- **`gacc_cn.py harvest-plan --index-html <path|->`** — feed it the index HTML a
  real browser fetched; it applies the *same* classifier (`discover_express_articles`)
  and the *same* DB dedup (`_cn_release_missing`) the unattended walk uses, and
  prints the render worklist plus a ready-to-fill `bridge` scaffold (each
  `<id-…>` placeholder kept on one line with its article URL, so a harvested id
  can't be pasted against the wrong table). Pure over the supplied HTML — no
  network — because the index is WAF-gated to plain HTTP. New tests in
  `tests/test_gacc_cn_discovery.py` cover the populated, deduped, jan-feb-excluded
  and empty cases plus the formatter.
- **Alert prose** (`notify.py`): the published-awaiting-bytes chat alert now
  points at this assisted flow first, manual bridge as the fallback.

Deliberately **not** shipped: persisting the pending article URLs into
`routine_check_log` so the alert could print them. That needs a schema
migration and is redundant — the harvest session re-reads the index in Chrome
and gets the URLs directly. `routine_check_log` keeps its rule that the notifier
keys on the structured `signal`, never on `notes` prose.

## Runbook — the drop-morning harvest (a Claude session with claude-in-chrome)

Triggered by the chat alert (`published_awaiting_bytes`), or run any time the CN
month is out before the English one. ~5 minutes.

1. **Connect Chrome.** `list_connected_browsers`; pick the real Chrome (not the
   in-app browser — it fails the WAF). Expect two one-time gates a human clears:
   the browser-picker if several Chromes are connected, and Chrome's
   *"this site doesn't support a secure connection"* interstitial (tjs is
   HTTP-only and refuses HTTPS — accept it).
2. **Render the index**, wait ~4 s for the challenge to clear
   (`http://tjs.customs.gov.cn/tjs/sjgb/tjkx/index.html`), then capture its HTML:
   ```js
   document.documentElement.outerHTML   // write to a scratch .html file
   ```
3. **Get the worklist + bridge scaffold:**
   ```bash
   gacc_cn.py harvest-plan --index-html /path/to/index.html
   ```
   "nothing to harvest" ⇒ the DB already has the month (English got there first);
   stop.
4. **For each worklist URL:** navigate, wait ~4–7 s for the WPS viewer to paint,
   then read the id off the **iframe** (the top document also contains a spurious
   19-digit template asset id — do not regex the top HTML):
   ```js
   document.querySelector('iframe').contentDocument.title   // the 19-digit id
   ```
5. **Bridge** the harvested ids into the scaffold from step 3 and run it:
   ```bash
   gacc_cn.py bridge --attachment-id <id> --article-url <url> …   # add --dry-run to rehearse
   ```
   `bridge` is idempotent and fails loud on a malformed id; one bad id skips only
   its own table. The English walk supersedes CN provenance when it lands (~15th),
   so nothing here is load-bearing beyond the timeliness gain.

## Tier 2 (not built): what full unattended automation would still need

The three gates in step 1 are exactly why the 08:10 scheduled run can't do this
untended today. For the record, unattended would need: a pinned browser via
`select_browser(deviceId)` (no picker); HTTPS-First disabled for the tjs origin
(no interstitial); a kept-alive real-Chrome profile the routine session can
reach; and the appetite to drive Work Chrome unprompted at breakfast. None are
blockers, but each is real, and the payoff is only the ~0.5–1.5-day lead over
the English release. Revisit after Tier 1 has a drop or two under it.

## Provenance (all 2026-08-19; real Chrome via claude-in-chrome unless noted)

| Surface | URL | Observed |
|---|---|---|
| CN Express index | `http://tjs.customs.gov.cn/tjs/sjgb/tjkx/index.html` | `412`→`200`; title 统计快讯; 13 article links (6 ingestable) |
| Article, §4 CNY Jul | `…/tjs/2026-08/07/article_2026080710320733327.html` | `200`, 143,920-char DOM; WPS iframe `200` |
| — rendered iframe | (same, `iframe.contentDocument`) | `.title` = `2026080710320733553` |
| WPS backend | `…/wps/weboffice?8h6a7FPl=<token>` (POST ×n) | per-request distinct 瑞数 token — not replayable |
| attachDir xls | `http://www.customs.gov.cn/customs/attachDir/2026/08/2026080710320733553.xls` | plain curl `200`, application/vnd.ms-excel, 14,848 B |
| Control: in-app browser | index (same URL) | `412`→challenge JS→`400`, 39-char empty shell |
