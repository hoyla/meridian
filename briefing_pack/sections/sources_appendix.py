"""Final appendix listing every release URL underlying the brief."""

from __future__ import annotations

import psycopg2.extras

import eurostat

from briefing_pack._helpers import _Section, _construct_chinese_source_url


def _section_sources_appendix(cur, release_ids: set[int]) -> _Section:
    """Final appendix listing every release URL underlying the brief.

    Eurostat: synthesises the bulk-file URL via eurostat.bulk_file_url, since
    the canonical URL is deterministic per period (and we deliberately don't
    store the 44 MB 7z bytes). GACC: the actual source_url from the release
    row, plus the fetched_at from source_snapshots so a journalist knows
    the page state we read."""
    lines: list[str] = []
    lines.append("## Sources")
    lines.append("")
    lines.append(
        "Every release whose data fed any finding above. Eurostat URLs are "
        "the deterministic monthly bulk-file URLs; the raw CSV rows we extracted "
        "from each are preserved verbatim in the project DB (`eurostat_raw_rows`). "
        "GACC URLs are the actual customs.gov.cn pages we scraped — the page "
        "bytes are stored in `source_snapshots` so the read is reproducible "
        "even if the page is later revised or removed."
    )
    lines.append("")
    if not release_ids:
        lines.append("*No releases referenced.*")
        lines.append("")
        return _Section(markdown="\n".join(lines))

    cur.execute(
        """
        SELECT r.id, r.source, r.source_url, r.period, r.first_seen_at, r.last_seen_at,
               r.section_number, r.currency, r.release_kind,
               (SELECT MAX(s.fetched_at) FROM source_snapshots s
                  JOIN scrape_runs sr ON sr.id = s.scrape_run_id
                 WHERE s.url = r.source_url) AS snapshot_fetched_at
          FROM releases r
         WHERE r.id = ANY(%s)
      ORDER BY r.source, r.period DESC, r.id
        """,
        (sorted(release_ids),),
    )
    rels = cur.fetchall()

    by_source: dict[str, list[psycopg2.extras.DictRow]] = {}
    for r in rels:
        by_source.setdefault(r['source'], []).append(r)

    if 'eurostat' in by_source:
        lines.append("### Eurostat monthly bulk files")
        lines.append("")
        lines.append(
            "*Eurostat occasionally re-publishes corrected files at the same URL. "
            "The `as_of` timestamp is when we fetched and parsed the file into "
            "`eurostat_raw_rows` — that is the ground truth we used.*"
        )
        lines.append("")
        for r in by_source['eurostat']:
            url = eurostat.bulk_file_url(r['period'])
            as_of = r['first_seen_at'].strftime('%Y-%m-%d') if r['first_seen_at'] else '—'
            lines.append(
                f"- **{r['period'].strftime('%Y-%m')}** — as_of {as_of} — <{url}>"
            )
        lines.append("")

    if 'gacc' in by_source:
        lines.append("### GACC release pages")
        lines.append("")
        lines.append(
            "*Page bytes preserved in `source_snapshots`. The `fetched_at` "
            "timestamp is when we last successfully read the page; the EN "
            "link below points to the live page. The CN link is GACC's "
            "Chinese-language statistics index (see note below).*"
        )
        lines.append("")
        for r in by_source['gacc']:
            ts = r['snapshot_fetched_at'] or r['last_seen_at']
            ts_str = ts.strftime('%Y-%m-%d') if ts else '—'
            kind_bits = " ".join(filter(None, [
                f"section {r['section_number']}" if r['section_number'] else None,
                r['currency'],
                r['release_kind'],
            ]))
            chinese_url = _construct_chinese_source_url(r['source_url'])
            cn_link = f" / CN: <{chinese_url}>" if chinese_url else ""
            lines.append(
                f"- **{r['period'].strftime('%Y-%m')}** "
                f"({kind_bits}) — fetched {ts_str} — EN: <{r['source_url']}>{cn_link}"
            )
        lines.append("")

    lines.append("### Known gaps in source coverage")
    lines.append("")
    lines.append(
        "- The `CN:` link points to GACC's Chinese-language *统计快讯* "
        "(Statistics Express) index — not a per-release page. GACC's Chinese "
        "and English sites use unrelated CMS path schemes (Chinese tables live "
        "at `www.customs.gov.cn/customs/<yyyy-mm>/<dd>/article_<id>.html`, with "
        "no equivalent of the English `Statics/<UUID>.html` URL), so a "
        "per-release link can't be derived by host substitution. The index is "
        "the stable Chinese analogue of the English `preliminary.html` listing "
        "and carries the current month at the top. A journalist clicking "
        "through in a real browser lands on the Chinese-language tables for the "
        "same release — useful for in-language verification or when the English "
        "translation drops a nuance. (The Chinese site fronts a JavaScript "
        "anti-bot challenge that blocks headless `curl`, so we don't verify the "
        "link automatically; it resolves in a normal browser.) Note the Chinese "
        "index frequently publishes a given month *ahead* of the English site."
    )
    lines.append(
        "- Caveat codes referenced inline (e.g. `cif_fob`, `low_base_effect`) "
        "have full definitions in the project's `caveats` table."
    )
    lines.append("")
    return _Section(markdown="\n".join(lines))
