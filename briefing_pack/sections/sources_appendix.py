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
            "link below points to the live page. The CN link is the "
            "constructed Chinese-language equivalent (see note below).*"
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
        "- The `CN:` Chinese-language URLs above are *constructed* from the "
        "English URL by host substitution (`english.customs.gov.cn` → "
        "`www.customs.gov.cn`); GACC keeps the same `Statics/<UUID>.html` "
        "path on both. We don't verify these links automatically — the "
        "Chinese site fronts a JavaScript anti-bot challenge that blocks "
        "headless `curl` — but a journalist clicking through in a real "
        "browser will land on the Chinese-language version of the same "
        "release. Useful for in-language verification or when the English "
        "translation drops a nuance."
    )
    lines.append(
        "- Caveat codes referenced inline (e.g. `cif_fob`, `low_base_effect`) "
        "have full definitions in the project's `caveats` table."
    )
    lines.append("")
    return _Section(markdown="\n".join(lines))
